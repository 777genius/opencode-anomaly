import { describe, expect, test } from "bun:test"
import { createHash } from "node:crypto"
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
  test("fails closed when required reviewers do not prevent self-review", async () => {
    const workflow = await Bun.file(new URL("../workflows/hardened-cli-release.yml", import.meta.url)).text()
    const environmentFilter = workflow.match(/jq -e '([\s\S]*?)' "\$environment"/)?.[1]
    expect(environmentFilter).toBeDefined()
    const environment = {
      protection_rules: [
        {
          type: "required_reviewers",
          reviewers: [{ type: "User", reviewer: { login: "release-reviewer" } }],
          prevent_self_review: true,
        },
      ],
      can_admins_bypass: false,
      deployment_branch_policy: {
        protected_branches: false,
        custom_branch_policies: true,
      },
    }
    expect(
      Bun.spawnSync([
        "jq",
        "-e",
        "--null-input",
        "--argjson",
        "environment",
        JSON.stringify(environment),
        `$environment | ${environmentFilter}`,
      ]).exitCode,
    ).toBe(0)
    for (const preventSelfReview of [undefined, false, null, "true"]) {
      const requiredReviewers = { ...environment.protection_rules[0], prevent_self_review: preventSelfReview }
      if (preventSelfReview === undefined) delete requiredReviewers.prevent_self_review
      expect(
        Bun.spawnSync([
          "jq",
          "-e",
          "--null-input",
          "--argjson",
          "environment",
          JSON.stringify({ ...environment, protection_rules: [requiredReviewers] }),
          `$environment | ${environmentFilter}`,
        ]).exitCode,
      ).not.toBe(0)
    }
  })

  test("freezes identity and non-production eligibility", () => {
    expect(validateConstants()).toBeUndefined()
    expect(RELEASE).toEqual({
      sourceCommit: "e4b6665ff3bd17807040d319d62f80827fb714f2",
      sourceTree: "3a1ad994a90347c5f9ab1977b31522b83bec5b10",
      artifactTree: "29fcc8d259281bd4d3aafe7ca30d0f9a615da928",
      baseCommit: "ef2880f379129aa048be9e9353e30aa168d42c17",
      patchSha256: "dcaccc39b62b4e66ead320860ce8fd47c66bdd98a16d5a9bd74467e1dc27502c",
      version: "1.18.23-agentteams.1",
      tag: "v1.18.23-agentteams.1",
      bunVersion: "1.3.14",
      productionEligible: false,
    })
  })

  test("freezes an exact packages-only source patch", async () => {
    const patch = new URL("../hardened/opencode-hosted-approval-v2-r4.patch", import.meta.url)
    const bytes = await Bun.file(patch).bytes()
    expect(createHash("sha256").update(bytes).digest("hex")).toBe(RELEASE.patchSha256)
    expect(bytes.byteLength).toBe(92491)
    const numstat = Bun.spawnSync(["git", "apply", "--numstat", patch.pathname], {
      cwd: new URL("../..", import.meta.url).pathname,
      stdout: "pipe",
    })
    expect(numstat.exitCode).toBe(0)
    const paths = numstat.stdout
      .toString()
      .trim()
      .split("\n")
      .map((line) => line.split("\t")[2])
    expect(paths).toHaveLength(20)
    expect(paths.every((item) => item.startsWith("packages/"))).toBe(true)
    expect(paths).toContain("packages/opencode/src/project/instance-context.ts")
    expect(paths).toContain("packages/opencode/test/project/instance.test.ts")
  })

  test("builds the frozen artifact from the exact base plus patch", async () => {
    const workflow = await Bun.file(new URL("../workflows/hardened-cli-release.yml", import.meta.url)).text()
    const build = workflow.match(/\n  build:\n([\s\S]*?)\n  reproducible:/)?.[1]
    if (!build) throw new Error("build job is missing")
    expect(workflow).toContain(`SOURCE_COMMIT: ${RELEASE.sourceCommit}`)
    expect(workflow).toContain(`BASE_COMMIT: ${RELEASE.baseCommit}`)
    expect(build).toContain("ref: ${{ env.BASE_COMMIT }}")
    expect(build).not.toContain("ref: ${{ env.SOURCE_COMMIT }}")
    expect(build).toContain('materialize --repo source --patch "infra/$PATCH"')
    expect(build.indexOf("materialize --repo source")).toBeLessThan(build.indexOf("bun install --frozen-lockfile"))
  })

  test("keeps schema identity constants synchronized with RELEASE", async () => {
    const schema = await Bun.file(new URL("../hardened/release-manifest.schema.json", import.meta.url)).json()
    for (const [key, value] of Object.entries(RELEASE)) {
      expect(schema.properties.release.properties[key].const).toBe(value)
    }
    expect(schema.properties.release.properties.patchSize.const).toBe(92491)
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
      release: { ...RELEASE, patchSize: 92491 },
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

  test("attests only the immutable same-repository pull request candidate", async () => {
    const workflow = await Bun.file(new URL("../workflows/hardened-cli-release.yml", import.meta.url)).text()
    const parsed = Bun.YAML.parse(workflow) as any
    const manifest = parsed.jobs.manifest
    const provenance = parsed.jobs["pull-request-provenance"]
    const candidate = manifest.steps.find((step: any) => step.id === "candidate")
    const headGuard = provenance.steps.find(
      (step: any) => step.name === "Reject non-same-repository pull request heads",
    )
    const download = provenance.steps.find((step: any) => step.uses?.startsWith("actions/download-artifact@"))
    const verify = provenance.steps.find(
      (step: any) => step.name === "Verify exact candidate files and manifest digests",
    )
    const attest = provenance.steps.find((step: any) => step.id === "provenance")
    const upload = provenance.steps.find(
      (step: any) =>
        step.uses?.startsWith("actions/upload-artifact@") && step.with?.name === "hardened-release-provenance",
    )
    const subjects = [
      "candidate/opencode-linux-x64.tar.gz",
      "candidate/opencode-linux-arm64.tar.gz",
      "candidate/opencode-darwin-x64.zip",
      "candidate/opencode-darwin-arm64.zip",
      "candidate/opencode-windows-x64.zip",
      "candidate/release-manifest.json",
    ]

    expect(manifest.outputs).toEqual({
      "candidate-artifact-id": "${{ steps.candidate.outputs.artifact-id }}",
    })
    expect(candidate.uses).toBe("actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02")
    expect(candidate.with).toEqual({
      name: "hardened-release-candidate",
      path: "release",
      "if-no-files-found": "error",
      "retention-days": 7,
    })
    expect(provenance.needs).toBe("manifest")
    expect(provenance.if).toBe("github.event_name == 'pull_request'")
    expect(provenance.permissions).toEqual({
      contents: "read",
      "id-token": "write",
      attestations: "write",
    })
    expect(provenance.steps).toHaveLength(5)
    expect(headGuard.env).toEqual({
      HEAD_REPOSITORY: "${{ github.event.pull_request.head.repo.full_name }}",
    })
    expect(headGuard.run).toBe('test "$HEAD_REPOSITORY" = "$GITHUB_REPOSITORY"')
    expect(provenance.steps.indexOf(headGuard)).toBeLessThan(provenance.steps.indexOf(download))
    expect(download.uses).toBe("actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093")
    expect(download.with).toEqual({
      "artifact-ids": "${{ needs.manifest.outputs.candidate-artifact-id }}",
      path: "candidate",
      "merge-multiple": true,
    })
    expect(verify.run).toContain(
      `test "$(find candidate -mindepth 1 -maxdepth 1 -printf x | wc -c | tr -d ' ')" = 6`,
    )
    expect(verify.run).toContain(
      "test \"$(jq '.assets | length' candidate/release-manifest.json)\" = \"${#assets[@]}\"",
    )
    expect(verify.run).toContain(".archiveSha256")
    expect(verify.run).toContain("| select(length == 1)")
    expect(verify.run).toContain('| select(test("^[0-9a-f]{64}$"))')
    expect(verify.run).toContain("sha256sum --check --strict -")
    expect(verify.run).not.toMatch(
      /(?:^|[|;&]|\$\()\s*(?:bun|node|deno|python|ruby|perl|bash|source|eval|exec|chmod|tar|unzip)\b/m,
    )
    expect(verify.run).not.toMatch(/^\s*(?:\.\/)?candidate\//m)
    for (const subject of subjects) expect(verify.run).toContain(subject.replace("candidate/", ""))
    expect(attest.uses).toBe("actions/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8")
    expect(attest.with["subject-path"].trim().split("\n")).toEqual(subjects)
    expect(upload.uses).toBe("actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02")
    expect(upload.with).toEqual({
      name: "hardened-release-provenance",
      path: "${{ steps.provenance.outputs.bundle-path }}",
      "if-no-files-found": "error",
      "retention-days": 7,
    })
    expect(provenance.steps.filter((step: any) => step.uses).map((step: any) => step.uses)).toEqual([
      "actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
      "actions/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8",
      "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02",
    ])
    expect(provenance.steps.every((step: any) => step["working-directory"] === undefined)).toBe(true)
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
    const attest = steps.find((step: any) => step.uses?.startsWith("actions/attest-build-provenance@"))
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
    expect(environmentGuard.run).toContain(
      'all($review_rules[]; has("prevent_self_review") and .prevent_self_review == true)',
    )
    expect(environmentGuard.run).toContain(".can_admins_bypass == false")
    expect(environmentGuard.run).toContain(".deployment_branch_policy.custom_branch_policies == true")
    expect(environmentGuard.run).toContain('environments/hardened-release/deployment-branch-policies')
    expect(environmentGuard.run).toContain('.total_count == 1')
    expect(environmentGuard.run).toContain('.branch_policies[0].name == "hardened-release"')
    expect(environmentGuard.run).toContain('.branch_policies[0].type == "branch"')
    expect(preflight.run).toContain('require_absent "repos/$GITHUB_REPOSITORY/releases/tags/$TAG"')
    expect(preflight.run).not.toContain('require_absent "repos/$GITHUB_REPOSITORY/git/ref/tags/$TAG"')
    expect(preflight.run).toContain(
      `test "$(find release -maxdepth 1 -type f | wc -l | tr -d ' ')" = 6`,
    )
    expect(attest.with).toEqual({ "subject-path": "release/*" })
    expect(release.run).toContain('gh api --method POST "repos/$GITHUB_REPOSITORY/git/refs"')
    expect(release.run).toContain('-f ref="refs/tags/$TAG"')
    expect(release.run).toContain('-f sha="$SOURCE_COMMIT"')
    expect(release.run).toContain('commits/$TAG" --jq .sha')
    expect(release.run).toContain('gh release create "$TAG" release/* --verify-tag')
    expect(release.run).not.toContain("--target")
    const actionPins = new Map([
      ["actions/checkout", "11d5960a326750d5838078e36cf38b85af677262"],
      ["oven-sh/setup-bun", "0c5077e51419868618aeaa5fe8019c62421857d6"],
      ["actions/download-artifact", "d3f86a106a0bac45b974a628896c90dbdf5c8093"],
      ["actions/upload-artifact", "ea165f8d65b6e75b540449e92b4886f43607fa02"],
      ["actions/attest-build-provenance", "4d101475d8b20a2381f78447822ac1eab6504dd8"],
    ])
    const uses = workflow
      .split("\n")
      .flatMap((line) => line.trim().match(/^(?:- )?uses: (\S+)/)?.[1] ?? [])
    expect([...new Set(uses.map((item) => item.split("@")[0]))].sort()).toEqual([...actionPins.keys()].sort())
    for (const item of uses) {
      const [action, pin] = item.split("@")
      expect(pin).toBe(actionPins.get(action))
      expect(pin).toMatch(/^[0-9a-f]{40}$/)
    }
  })
})
