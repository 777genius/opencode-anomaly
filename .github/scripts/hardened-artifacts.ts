#!/usr/bin/env bun

import { createHash } from "node:crypto"
import { lstat, mkdir, readdir, readFile, rm, stat, writeFile } from "node:fs/promises"
import path from "node:path"

export const RELEASE = {
  sourceCommit: "cee24c1de70a220253c115822d8846b973b9e5a4",
  sourceTree: "970e7191f1751dbc591333209607a8cc422b3204",
  artifactTree: "c28c1abe9e5711579ee07be6ea00c4e4323f0faf",
  baseCommit: "ef2880f379129aa048be9e9353e30aa168d42c17",
  patchSha256: "ebd5d063ee774a17f5f4aa64277a9d2b48d8648719b62e2f8ca6da569d49e30c",
  version: "1.18.23-agentteams.1",
  tag: "v1.18.23-agentteams.1",
  bunVersion: "1.4.0",
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
  return index === -1 || !args[index + 1] ? fail(`missing ${flag}`) : args[index + 1]
}

export const validateConstants = () => {
  if (RELEASE.productionEligible !== false) fail("productionEligible must remain hardcoded false")
  if (RELEASE.tag !== `v${RELEASE.version}`) fail("tag/version mismatch")
  for (const key of ["sourceCommit", "sourceTree", "artifactTree", "baseCommit"] as const) {
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
  if (run(["git", "merge-base", RELEASE.baseCommit, RELEASE.sourceCommit], repository) !== RELEASE.baseCommit)
    fail("source does not descend from exact base commit")
  if (run(["git", "rev-parse", `${RELEASE.sourceCommit}^{tree}`], repository) !== RELEASE.sourceTree)
    fail("source commit tree differs from frozen source tree")
  const expected = Bun.spawnSync(
    [
      "git",
      "diff",
      "--binary",
      "--full-index",
      "--no-ext-diff",
      "--no-textconv",
      "--no-renames",
      "--diff-algorithm=myers",
      "--no-color",
      "--src-prefix=a/",
      "--dst-prefix=b/",
      RELEASE.baseCommit,
      RELEASE.sourceCommit,
      "--",
      "packages",
    ],
    { cwd: repository, stdout: "pipe", stderr: "pipe", env: process.env },
  )
  if (expected.exitCode !== 0) fail(`exact source diff failed: ${expected.stderr.toString().trim()}`)
  if (!expected.stdout.equals(await readFile(patch)))
    fail("patch is not the exact packages diff from base to accepted source commit")

  const temporary = await Bun.$`mktemp -d`.text().then((value) => value.trim())
  try {
    run(["git", "worktree", "add", "--detach", temporary, RELEASE.baseCommit], repository)
    run(["git", "apply", "--check", "--whitespace=error-all", patch], temporary)
    run(["git", "apply", "--index", "--whitespace=error-all", patch], temporary)
    const patchedTree = run(["git", "write-tree"], temporary)
    if (patchedTree !== RELEASE.artifactTree)
      fail(`patch tree ${patchedTree} differs from frozen artifact tree ${RELEASE.artifactTree}`)
  } finally {
    Bun.spawnSync(["git", "worktree", "remove", "--force", temporary], {
      cwd: repository,
      stdout: "ignore",
      stderr: "ignore",
    })
  }
}

const materialize = async (args: string[]) => {
  validateConstants()
  const patch = path.resolve(requireValue(args, "--patch"))
  const repository = path.resolve(requireValue(args, "--repo"))
  if ((await sha256(patch)) !== RELEASE.patchSha256) fail("immutable patch SHA-256 mismatch")
  if (run(["git", "rev-parse", "HEAD"], repository) !== RELEASE.baseCommit)
    fail("materialization checkout is not exact base commit")
  run(["git", "apply", "--check", "--whitespace=error-all", patch], repository)
  run(["git", "apply", "--index", "--whitespace=error-all", patch], repository)
  const tree = run(["git", "write-tree"], repository)
  if (tree !== RELEASE.artifactTree)
    fail(`materialized tree ${tree} differs from frozen artifact tree ${RELEASE.artifactTree}`)
}

export const archive = async (args: string[]) => {
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
    // Info-ZIP omits extra metadata with -X and emits stable timestamps when the input mtime is fixed.
    await rm(target, { force: true })
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

const archiveEntries = (archivePath: string) => {
  if (archivePath.endsWith(".zip") && process.platform !== "win32") {
    return run(["unzip", "-Z1", archivePath]).split("\n")
  }
  return run(["tar", "-tf", archivePath]).split("\n")
}

const extractArchive = (archivePath: string, destination: string) => {
  if (archivePath.endsWith(".zip") && process.platform !== "win32") {
    run(["unzip", "-q", archivePath, "-d", destination])
    return
  }
  run(["tar", "-xf", archivePath, "-C", destination])
}

const extract = async (archivePath: string, destination: string) => {
  const entries = archiveEntries(archivePath)
  entries.forEach(validateArchiveEntry)
  if (entries.length !== 1 || !["opencode", "opencode.exe"].includes(entries[0])) {
    fail(`archive must contain exactly one root binary: ${archivePath}`)
  }
  await mkdir(destination, { recursive: true })
  extractArchive(archivePath, destination)
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
  await validateReleaseManifest(
    result,
    path.resolve(args.includes("--schema") ? requireValue(args, "--schema") : schemaFile),
  )
  await writeFile(output, `${JSON.stringify(result, null, 2)}\n`, { flag: "wx" })
}

type JsonSchema = {
  type?: string
  const?: unknown
  enum?: unknown[]
  required?: string[]
  properties?: Record<string, JsonSchema>
  additionalProperties?: boolean
  minItems?: number
  maxItems?: number
  items?: JsonSchema
  pattern?: string
  exclusiveMinimum?: number
}

const schemaFile = new URL("../hardened/release-manifest.schema.json", import.meta.url).pathname

const valueType = (value: unknown) => {
  if (Array.isArray(value)) return "array"
  if (value === null) return "null"
  if (Number.isInteger(value)) return "integer"
  return typeof value
}

const validateSchemaNode = (schema: JsonSchema, value: unknown, location = "$"): void => {
  if (schema.const !== undefined && !Object.is(value, schema.const)) fail(`${location} must equal its schema constant`)
  if (schema.enum && !schema.enum.some((item) => Object.is(item, value))) fail(`${location} is not in its schema enum`)
  if (schema.type) {
    const actual = valueType(value)
    if (actual !== schema.type && !(schema.type === "number" && actual === "integer")) {
      fail(`${location} must be ${schema.type}, got ${actual}`)
    }
  }
  if (typeof value === "string" && schema.pattern && !new RegExp(schema.pattern).test(value)) {
    fail(`${location} does not match ${schema.pattern}`)
  }
  if (typeof value === "number" && schema.exclusiveMinimum !== undefined && value <= schema.exclusiveMinimum) {
    fail(`${location} must be greater than ${schema.exclusiveMinimum}`)
  }
  if (Array.isArray(value)) {
    if (schema.minItems !== undefined && value.length < schema.minItems) fail(`${location} has too few items`)
    if (schema.maxItems !== undefined && value.length > schema.maxItems) fail(`${location} has too many items`)
    if (schema.items) value.forEach((item, index) => validateSchemaNode(schema.items!, item, `${location}[${index}]`))
  }
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    const record = value as Record<string, unknown>
    for (const key of schema.required ?? []) {
      if (!Object.hasOwn(record, key)) fail(`${location}.${key} is required`)
    }
    if (schema.additionalProperties === false) {
      for (const key of Object.keys(record)) {
        if (!Object.hasOwn(schema.properties ?? {}, key)) fail(`${location}.${key} is not allowed`)
      }
    }
    for (const [key, child] of Object.entries(schema.properties ?? {})) {
      if (Object.hasOwn(record, key)) validateSchemaNode(child, record[key], `${location}.${key}`)
    }
  }
}

export const validateReleaseManifest = async (value: unknown, schemaPath = schemaFile) => {
  const schema = JSON.parse(await readFile(schemaPath, "utf8")) as JsonSchema
  validateSchemaNode(schema, value)
}

const validateManifest = async (args: string[]) => {
  const manifestPath = path.resolve(requireValue(args, "--manifest"))
  const schemaPath = path.resolve(requireValue(args, "--schema"))
  await validateReleaseManifest(JSON.parse(await readFile(manifestPath, "utf8")), schemaPath)
}

const main = async () => {
  const [command, ...args] = process.argv.slice(2)
  if (command === "identity") return identity(args)
  if (command === "materialize") return materialize(args)
  if (command === "archive") return archive(args)
  if (command === "compare") return compare(args)
  if (command === "verify") return verify(args)
  if (command === "manifest") return manifest(args)
  if (command === "validate-manifest") return validateManifest(args)
  fail(`unknown command: ${command ?? "<missing>"}`)
}

if (import.meta.main) await main()
