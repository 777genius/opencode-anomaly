import { describe, expect, test } from "bun:test"
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import {
  archive,
  PLATFORMS,
  RELEASE,
  validateArchiveEntry,
  validateConstants,
  validateReleaseManifest,
} from "./hardened-artifacts"

describe("hardened release contract", () => {
  test("freezes identity and non-production eligibility", () => {
    expect(validateConstants()).toBeUndefined()
    expect(RELEASE).toEqual({
      sourceCommit: "94540b2cbd116bb5bcd6c9ac8f4734f3df637a2b",
      sourceTree: "762a2f4322b572639b12f2ec30a126de72138d18",
      baseCommit: "826d9ad46a22bef0294998e08daa3c4904fea28f",
      patchSha256: "c8a1389e6e296e25d5cb7f9da563d09c5d87560ea2ca1a0aab0d6dc1c9a11968",
      version: "1.18.21-agentteams.1",
      tag: "v1.18.21-agentteams.1",
      bunVersion: "1.3.14",
      productionEligible: false,
    })
  })

  test("keeps schema identity constants synchronized with RELEASE", async () => {
    const schema = await Bun.file(new URL("../hardened/release-manifest.schema.json", import.meta.url)).json()
    for (const [key, value] of Object.entries(RELEASE)) {
      expect(schema.properties.release.properties[key].const).toBe(value)
    }
    expect(schema.properties.release.properties.patchSize.const).toBe(85072)
  })

  test("enforces the manifest schema and rejects malformed assets", async () => {
    const asset = {
      archive: "opencode-linux-x64.tar.gz",
      archiveSha256: "a".repeat(64),
      archiveSize: 1,
      binaryPath: "opencode",
      binarySha256: "b".repeat(64),
      binarySize: 1,
      platform: "opencode-linux-x64",
      os: "linux",
      arch: "x64",
      signing: {
        binaryStatus: "unsigned",
        reason: "non-production fork prerelease",
        provenanceAction: "actions/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8",
        provenanceStatus: "required-after-manifest",
      },
    }
    const value = {
      schemaVersion: 1,
      release: { ...RELEASE, patchSize: 85072 },
      workflow: {
        repository: "local",
        workflow: "local",
        runId: "local",
        runAttempt: "local",
        actor: "local",
        ref: "local",
        sha: "local",
      },
      assets: Array.from({ length: 5 }, () => ({ ...asset })),
    }
    await expect(validateReleaseManifest(value)).resolves.toBeUndefined()
    await expect(
      validateReleaseManifest({ ...value, assets: [{ ...asset, archiveSize: "1" }, ...value.assets.slice(1)] }),
    ).rejects.toThrow("$.assets[0].archiveSize must be integer")
    await expect(
      validateReleaseManifest({ ...value, assets: [{ ...asset, unexpected: true }, ...value.assets.slice(1)] }),
    ).rejects.toThrow("$.assets[0].unexpected is not allowed")
  })

  test("publishes only the explicit native verification matrix", () => {
    expect(PLATFORMS.map((item) => `${item.os}-${item.arch}`)).toEqual([
      "linux-x64",
      "linux-arm64",
      "darwin-x64",
      "darwin-arm64",
      "windows-x64",
    ])
  })

  test("replaces Info-ZIP archives on rerun instead of retaining stale entries", async () => {
    const temporary = await mkdtemp(path.join(tmpdir(), "hardened-archive-test-"))
    const dist = path.join(temporary, "dist")
    const output = path.join(temporary, "output")
    const originalPath = process.env.PATH
    try {
      const commands = path.join(temporary, "commands")
      await mkdir(commands)
      await writeFile(path.join(commands, "zip"), '#!/bin/sh\nif [ -e "$3" ]; then exit 42; fi\nprintf fresh >"$3"\n', {
        mode: 0o755,
      })
      process.env.PATH = `${commands}:${originalPath}`
      for (const platform of PLATFORMS) {
        const binaryDirectory = path.join(dist, platform.name, "bin")
        await mkdir(binaryDirectory, { recursive: true })
        await writeFile(
          path.join(binaryDirectory, platform.os === "windows" ? "opencode.exe" : "opencode"),
          platform.name,
        )
      }
      await archive(["--dist", dist, "--output", output])
      const target = path.join(output, "opencode-darwin-x64.zip")
      await writeFile(target, "stale")
      await archive(["--dist", dist, "--output", output])
      expect(await Bun.file(target).text()).toBe("fresh")
    } finally {
      process.env.PATH = originalPath
      await rm(temporary, { recursive: true, force: true })
    }
  })

  test("rejects absolute paths and traversal before extraction", () => {
    expect(() => validateArchiveEntry("../opencode")).toThrow("unsafe archive entry")
    expect(() => validateArchiveEntry("bin/../../opencode")).toThrow("unsafe archive entry")
    expect(() => validateArchiveEntry("/usr/bin/opencode")).toThrow("unsafe archive entry")
    expect(() => validateArchiveEntry("opencode")).not.toThrow()
  })

  test("keeps mutations in the protected dispatch-only final job", async () => {
    const workflow = await Bun.file(new URL("../workflows/hardened-cli-release.yml", import.meta.url)).text()
    const parsed = Bun.YAML.parse(workflow) as any
    expect(parsed.jobs["hardened-release"].if).toBe(
      "github.event_name == 'workflow_dispatch' && github.repository == '777genius/opencode-anomaly'",
    )
    expect(parsed.jobs["hardened-release"].environment).toBe("hardened-release")
    expect(parsed.jobs["hardened-release"].permissions).toEqual({
      contents: "write",
      "id-token": "write",
      attestations: "write",
    })
    expect(workflow.match(/persist-credentials: false/g)).toHaveLength(9)
    expect(workflow).not.toContain("persist-credentials: true")
    expect(workflow).toContain('require_absent "repos/$GITHUB_REPOSITORY/git/ref/tags/$TAG"')
    expect(workflow).toContain("validate-manifest --manifest release/release-manifest.json")
    for (const line of workflow.split("\n").filter((line) => line.trim().startsWith("uses:"))) {
      expect(line).toMatch(/@[0-9a-f]{40}(?:\s+#.*)?$/)
    }
  })
})
