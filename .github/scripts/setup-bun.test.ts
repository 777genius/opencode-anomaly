import { describe, expect, test } from "bun:test"

const root = new URL("../", import.meta.url)
const action = await Bun.file(new URL("actions/setup-bun/action.yml", root)).text()
const parsed = Bun.YAML.parse(action) as {
  runs: { steps: Array<{ name?: string; run?: string; with?: Record<string, string> }> }
}
const step = (name: string) => parsed.runs.steps.find((item) => item.name === name)

describe("Setup Bun action contract", () => {
  test("pins and verifies Bun 1.4.0", async () => {
    const pkg = await Bun.file(new URL("../package.json", root)).json()
    expect(pkg.packageManager).toBe("bun@1.4.0")
    expect(step("Setup Bun")?.with?.["bun-version-file"]).toContain("package.json")
    expect(step("Verify pinned Bun version")?.run).toContain('test "$actual" = "$expected"')
  })

  test("uses one frozen default-linker install on every OS", () => {
    const install = step("Install dependencies")?.run
    expect(install).toBe("bun install --frozen-lockfile ${{ inputs.install-flags }}")
    expect(install).not.toContain("--linker")
    expect(install).not.toContain("--network-concurrency")
    expect(install).not.toContain("BUN_INSTALL_CACHE_DIR")
    expect(install).not.toContain("RUNNER_OS")
  })
})
