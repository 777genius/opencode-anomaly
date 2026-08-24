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
      sourceCommit: "3186244c3103eb02d95a255b593847b14488b070",
      sourceTree: "8fba45aecd63ec61f334a856694cbd3da037df90",
      baseCommit: "47b6b6f5f4f9b42d2bce7af1c4e5bf6efaf22ba7",
      patchSha256: "3845fdf3b5991ac7b798cd74e8fa50bdfa2007f863d54b76565d92b8585c3d4c",
      version: "1.18.22-agentteams.1",
      tag: "v1.18.22-agentteams.1",
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
    const steps = parsed.jobs["hardened-release"].steps
    const environmentGuard = steps.find(
      (step: any) => step.name === "Verify live hardened-release environment protections",
    )
    const preflight = steps.find((step: any) => step.name === "Refuse an existing release and validate candidate assets")
    const release = steps.find(
      (step: any) => step.name === "Atomically own exact-source tag and create draft prerelease",
    )
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
    expect(workflow).toContain("validate-manifest --manifest release/release-manifest.json")
    expect(steps.indexOf(environmentGuard)).toBeLessThan(
      steps.findIndex((step: any) => step.uses?.startsWith("actions/attest-build-provenance@")),
    )
    expect(environmentGuard.run).toContain('test "$GITHUB_REF" = "refs/heads/hardened-release"')
    expect(environmentGuard.run).toContain('environments/hardened-release" >"$environment"')
    expect(environmentGuard.run).toContain('select(.type == "required_reviewers")')
    expect(environmentGuard.run).toContain('has("prevent_self_review")')
    expect(environmentGuard.run).toContain(".can_admins_bypass == false")
    expect(environmentGuard.run).toContain(".deployment_branch_policy.custom_branch_policies == true")
    expect(environmentGuard.run).toContain('environments/hardened-release/deployment-branch-policies')
    expect(environmentGuard.run).toContain('.total_count == 1')
    expect(environmentGuard.run).toContain('.branch_policies[0].name == "hardened-release"')
    expect(environmentGuard.run).toContain('.branch_policies[0].type == "branch"')
    expect(preflight.run).toContain('require_absent "repos/$GITHUB_REPOSITORY/releases/tags/$TAG"')
    expect(preflight.run).not.toContain('require_absent "repos/$GITHUB_REPOSITORY/git/ref/tags/$TAG"')
    expect(release.run).toContain('gh api --method POST "repos/$GITHUB_REPOSITORY/git/refs"')
    expect(release.run).toContain('-f ref="refs/tags/$TAG"')
    expect(release.run).toContain('-f sha="$SOURCE_COMMIT"')
    expect(release.run).toContain('commits/$TAG" --jq .sha')
    expect(release.run).toContain('gh release create "$TAG" release/* --verify-tag')
    expect(release.run).not.toContain("--target")
    for (const line of workflow.split("\n").filter((line) => line.trim().startsWith("uses:"))) {
      expect(line).toMatch(/@[0-9a-f]{40}(?:\s+#.*)?$/)
    }
  })
})
