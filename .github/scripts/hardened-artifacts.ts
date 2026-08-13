#!/usr/bin/env bun

import { createHash } from "node:crypto"
import { lstat, mkdir, readdir, readFile, rm, stat, writeFile } from "node:fs/promises"
import path from "node:path"

export const RELEASE = {
  sourceCommit: "1554487639c28df9eb294c93257ed52114aa24c5",
  sourceTree: "d9c931acf04736d065a108115bd71c6e6721bf65",
  baseCommit: "49c69c5ed3ccf706b61b3febb43c8aaff7f8325e",
  patchSha256: "1c80d32f7ad745e97abb7298b69a01062e22c88a3ccd5837cfbcff84e8edc506",
  version: "1.18.4-agentteams.1",
  tag: "v1.18.4-agentteams.1",
  bunVersion: "1.3.14",
  productionEligible: false as const,
} as const

export const PLATFORMS = [
  { name: "opencode-linux-x64", os: "linux", arch: "x64", archive: "tar.gz" },
  { name: "opencode-linux-arm64", os: "linux", arch: "arm64", archive: "tar.gz" },
  { name: "opencode-darwin-x64", os: "darwin", arch: "x64", archive: "zip" },
  { name: "opencode-darwin-arm64", os: "darwin", arch: "arm64", archive: "zip" },
  { name: "opencode-windows-x64", os: "windows", arch: "x64", archive: "zip" },
] as const

type Asset = {
  archive: string
  archiveSha256: string
  archiveSize: number
  binaryPath: string
  binarySha256: string
  binarySize: number
  platform: string
  os: string
  arch: string
  signing: {
    binaryStatus: "unsigned"
    reason: string
    provenanceAction: string
    provenanceStatus: "required-after-manifest"
  }
}

const fail = (message: string): never => {
  throw new Error(message)
}

const sha256 = async (file: string) =>
  createHash("sha256")
    .update(await readFile(file))
    .digest("hex")

const run = (command: string[], cwd = process.cwd()) => {
  const result = Bun.spawnSync(command, { cwd, stdout: "pipe", stderr: "pipe", env: process.env })
  if (result.exitCode !== 0) {
    fail(`${command.join(" ")} failed: ${result.stderr.toString().trim()}`)
  }
  return result.stdout.toString().trim()
}

const requireValue = (args: string[], flag: string) => {
  const index = args.indexOf(flag)
  if (index === -1 || !args[index + 1]) fail(`missing ${flag}`)
  return args[index + 1]
}

export const validateConstants = () => {
  if (RELEASE.productionEligible !== false) fail("productionEligible must remain hardcoded false")
  if (RELEASE.tag !== `v${RELEASE.version}`) fail("tag/version mismatch")
  for (const key of ["sourceCommit", "baseCommit"] as const) {
    if (!/^[0-9a-f]{40}$/.test(RELEASE[key])) fail(`invalid ${key}`)
  }
  if (!/^[0-9a-f]{64}$/.test(RELEASE.patchSha256)) fail("invalid patchSha256")
  if (new Set(PLATFORMS.map((item) => item.name)).size !== PLATFORMS.length) fail("duplicate platform")
}

const identity = async (args: string[]) => {
  validateConstants()
  const patch = path.resolve(requireValue(args, "--patch"))
  const repository = path.resolve(requireValue(args, "--repo"))
  if ((await sha256(patch)) !== RELEASE.patchSha256) fail("immutable patch SHA-256 mismatch")
  if (run(["git", "rev-parse", "HEAD"], repository) !== RELEASE.sourceCommit)
    fail("checkout is not exact source commit")
  if (run(["git", "rev-parse", "HEAD^"], repository) !== RELEASE.baseCommit)
    fail("source parent is not exact base commit")

  const temporary = await Bun.$`mktemp -d`.text().then((value) => value.trim())
  try {
    run(["git", "worktree", "add", "--detach", temporary, RELEASE.baseCommit], repository)
    run(["git", "apply", "--check", patch], temporary)
    run(["git", "apply", "--index", patch], temporary)
    const patchedTree = run(["git", "write-tree"], temporary)
    const sourceTree = run(["git", "rev-parse", `${RELEASE.sourceCommit}^{tree}`], repository)
    if (sourceTree !== RELEASE.sourceTree)
      fail(`source tree ${sourceTree} differs from frozen tree ${RELEASE.sourceTree}`)
    if (patchedTree !== sourceTree) fail(`patch tree ${patchedTree} differs from source tree ${sourceTree}`)
  } finally {
    Bun.spawnSync(["git", "worktree", "remove", "--force", temporary], {
      cwd: repository,
      stdout: "ignore",
      stderr: "ignore",
    })
  }
}

const archive = async (args: string[]) => {
  validateConstants()
  const dist = path.resolve(requireValue(args, "--dist"))
  const output = path.resolve(requireValue(args, "--output"))
  await mkdir(output, { recursive: true })
  for (const platform of PLATFORMS) {
    const binary = path.join(dist, platform.name, "bin", platform.os === "windows" ? "opencode.exe" : "opencode")
    if (!(await Bun.file(binary).exists())) fail(`missing binary ${binary}`)
    const target = path.join(output, `${platform.name}.${platform.archive}`)
    if (platform.archive === "tar.gz") {
      run([
        "tar",
        "--sort=name",
        "--mtime=@0",
        "--owner=0",
        "--group=0",
        "--numeric-owner",
        "-czf",
        target,
        "-C",
        path.dirname(binary),
        path.basename(binary),
      ])
      continue
    }
    // bsdtar/libarchive emits stable ZIP metadata when the input mtime is fixed.
    run(["touch", "-t", "198001010000", binary])
    run(["zip", "-X", "-q", target, path.basename(binary)], path.dirname(binary))
  }
}

const listFiles = async (directory: string) =>
  (await readdir(directory, { withFileTypes: true }))
    .filter((entry) => entry.isFile())
    .map((entry) => entry.name)
    .sort()

const compare = async (args: string[]) => {
  const left = path.resolve(requireValue(args, "--left"))
  const right = path.resolve(requireValue(args, "--right"))
  const leftFiles = await listFiles(left)
  const rightFiles = await listFiles(right)
  if (JSON.stringify(leftFiles) !== JSON.stringify(rightFiles)) fail("clean builds produced different asset sets")
  for (const file of leftFiles) {
    if ((await sha256(path.join(left, file))) !== (await sha256(path.join(right, file)))) {
      fail(`non-reproducible archive: ${file}`)
    }
  }
}

export const validateArchiveEntry = (entry: string) => {
  const normalized = entry.replaceAll("\\", "/")
  if (!normalized || normalized.startsWith("/") || normalized.split("/").includes("..")) {
    fail(`unsafe archive entry: ${entry}`)
  }
}

const extract = async (archivePath: string, destination: string) => {
  const entries = run(["tar", "-tf", archivePath]).split("\n")
  entries.forEach(validateArchiveEntry)
  if (entries.length !== 1 || !["opencode", "opencode.exe"].includes(entries[0])) {
    fail(`archive must contain exactly one root binary: ${archivePath}`)
  }
  await mkdir(destination, { recursive: true })
  run(["tar", "-xf", archivePath, "-C", destination])
  const binary = path.join(destination, entries[0])
  if (!(await lstat(binary)).isFile()) fail(`archive payload is not a regular file: ${archivePath}`)
  return binary
}

const verify = async (args: string[]) => {
  const archivePath = path.resolve(requireValue(args, "--archive"))
  const platform = PLATFORMS.find((item) => path.basename(archivePath) === `${item.name}.${item.archive}`)
  if (!platform) fail(`archive is not in the published platform allowlist: ${archivePath}`)
  const expectedOs = process.platform === "win32" ? "windows" : process.platform
  if (platform.os !== expectedOs || platform.arch !== process.arch) {
    fail(`native verification mismatch: ${platform.name} on ${expectedOs}-${process.arch}`)
  }
  const destination = path.join(process.env.RUNNER_TEMP ?? "/tmp", `hardened-verify-${crypto.randomUUID()}`)
  try {
    const binary = await extract(archivePath, destination)
    if (platform.os !== "windows") run(["chmod", "755", binary])
    const output = run([binary, "--version"])
    if (!output.includes(RELEASE.version)) fail(`binary version mismatch: ${output}`)
  } finally {
    await rm(destination, { recursive: true, force: true })
  }
}

const manifest = async (args: string[]) => {
  validateConstants()
  const directory = path.resolve(requireValue(args, "--assets"))
  const output = path.resolve(requireValue(args, "--output"))
  const patch = path.resolve(requireValue(args, "--patch"))
  if ((await sha256(patch)) !== RELEASE.patchSha256) fail("immutable patch SHA-256 mismatch")
  const assets: Asset[] = []
  for (const platform of PLATFORMS) {
    const archiveName = `${platform.name}.${platform.archive}`
    const archivePath = path.join(directory, archiveName)
    const extracted = path.join(process.env.RUNNER_TEMP ?? "/tmp", `manifest-${crypto.randomUUID()}`)
    try {
      const binary = await extract(archivePath, extracted)
      assets.push({
        archive: archiveName,
        archiveSha256: await sha256(archivePath),
        archiveSize: (await stat(archivePath)).size,
        binaryPath: path.basename(binary),
        binarySha256: await sha256(binary),
        binarySize: (await stat(binary)).size,
        platform: platform.name,
        os: platform.os,
        arch: platform.arch,
        signing: {
          binaryStatus: "unsigned",
          reason: "non-production fork prerelease",
          provenanceAction: "actions/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8",
          provenanceStatus: "required-after-manifest",
        },
      })
    } finally {
      await rm(extracted, { recursive: true, force: true })
    }
  }
  const result = {
    schemaVersion: 1,
    release: { ...RELEASE, patchSize: (await stat(patch)).size },
    workflow: {
      repository: process.env.GITHUB_REPOSITORY ?? "local",
      workflow: process.env.GITHUB_WORKFLOW ?? "local",
      runId: process.env.GITHUB_RUN_ID ?? "local",
      runAttempt: process.env.GITHUB_RUN_ATTEMPT ?? "local",
      actor: process.env.GITHUB_ACTOR ?? "local",
      ref: process.env.GITHUB_REF ?? "local",
      sha: process.env.GITHUB_SHA ?? "local",
    },
    assets,
  }
  await writeFile(output, `${JSON.stringify(result, null, 2)}\n`, { flag: "wx" })
}

const main = async () => {
  const [command, ...args] = process.argv.slice(2)
  if (command === "identity") return identity(args)
  if (command === "archive") return archive(args)
  if (command === "compare") return compare(args)
  if (command === "verify") return verify(args)
  if (command === "manifest") return manifest(args)
  fail(`unknown command: ${command ?? "<missing>"}`)
}

if (import.meta.main) await main()
