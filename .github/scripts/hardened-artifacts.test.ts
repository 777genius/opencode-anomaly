import { describe, expect, test } from "bun:test"
import { PLATFORMS, RELEASE, validateArchiveEntry, validateConstants } from "./hardened-artifacts"

describe("hardened release contract", () => {
  test("freezes identity and non-production eligibility", () => {
    expect(validateConstants()).toBeUndefined()
    expect(RELEASE).toEqual({
      sourceCommit: "1554487639c28df9eb294c93257ed52114aa24c5",
      sourceTree: "d9c931acf04736d065a108115bd71c6e6721bf65",
      baseCommit: "49c69c5ed3ccf706b61b3febb43c8aaff7f8325e",
      patchSha256: "1c80d32f7ad745e97abb7298b69a01062e22c88a3ccd5837cfbcff84e8edc506",
      version: "1.18.4-agentteams.1",
      tag: "v1.18.4-agentteams.1",
      bunVersion: "1.3.14",
      productionEligible: false,
    })
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

  test("rejects absolute paths and traversal before extraction", () => {
    expect(() => validateArchiveEntry("../opencode")).toThrow("unsafe archive entry")
    expect(() => validateArchiveEntry("bin/../../opencode")).toThrow("unsafe archive entry")
    expect(() => validateArchiveEntry("/usr/bin/opencode")).toThrow("unsafe archive entry")
    expect(() => validateArchiveEntry("opencode")).not.toThrow()
  })

  test("keeps mutations in the protected dispatch-only final job", async () => {
    const workflow = await Bun.file(new URL("../workflows/hardened-cli-release.yml", import.meta.url)).text()
    expect(workflow).toContain("if: github.event_name == 'workflow_dispatch'")
    expect(workflow).toContain("environment: hardened-release")
    expect(workflow.match(/contents: write/g)).toHaveLength(1)
    expect(workflow.match(/id-token: write/g)).toHaveLength(1)
    expect(workflow.match(/attestations: write/g)).toHaveLength(1)
    for (const line of workflow.split("\n").filter((line) => line.trim().startsWith("uses:"))) {
      expect(line).toMatch(/@[0-9a-f]{40}(?:\s+#.*)?$/)
    }
  })
})
