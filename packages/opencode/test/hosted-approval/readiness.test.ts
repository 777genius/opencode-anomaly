import { describe, expect, test } from "bun:test"
import { spawnSync } from "node:child_process"
import { closeSync, openSync, readFileSync } from "node:fs"
import { readinessFormat, readinessLine, readinessMaxBytes } from "../../src/hosted-approval/readiness"
import { contractSha256 } from "../../src/hosted-approval/provenance"
import { tmpdir } from "../fixture/fixture"

const snapshot = {
  runtimeInstanceId: `runtime_instance_${"1".repeat(32)}`,
  configGeneration: `config_generation_${"2".repeat(32)}`,
  endpoint: { protocol: "http:" as const, address: "127.0.0.1", port: 45123, baseUrl: "http://127.0.0.1:45123" },
}

describe("hosted coordinator readiness", () => {
  test("encodes one bounded versioned line with only the selected non-secret fields", () => {
    const bytes = readinessLine({ ...snapshot, password: "must-not-appear", capsule: "private" } as typeof snapshot)
    expect(bytes.byteLength).toBeLessThanOrEqual(readinessMaxBytes)
    const line = bytes.toString("utf8")
    expect(line.split("\n")).toHaveLength(2)
    expect(line.endsWith("\n")).toBe(true)
    expect(line).not.toContain("must-not-appear")
    expect(line).not.toContain("private")
    expect(JSON.parse(line)).toEqual({ format: readinessFormat, version: 1, ...snapshot })
  })

  test.each<Partial<typeof snapshot>>([
    { runtimeInstanceId: "predicted" },
    { runtimeInstanceId: `${snapshot.runtimeInstanceId}\n` },
    { configGeneration: "config_generation_invalid" },
    { configGeneration: `${snapshot.configGeneration}\n` },
    { endpoint: { ...snapshot.endpoint, address: "localhost" } },
    { endpoint: { ...snapshot.endpoint, port: 0 } },
    { endpoint: { ...snapshot.endpoint, port: 65536 } },
    { endpoint: { ...snapshot.endpoint, port: 45123.5 } },
    { endpoint: { ...snapshot.endpoint, baseUrl: "http://user:secret@127.0.0.1:45123" } },
    { endpoint: { ...snapshot.endpoint, baseUrl: `${snapshot.endpoint.baseUrl}/` } },
    { endpoint: { ...snapshot.endpoint, baseUrl: "http://127.0.0.1:45124" } },
  ])("rejects malformed identity or endpoint before publication: %j", (change) => {
    expect(() => readinessLine({ ...snapshot, ...change })).toThrow("hosted-approval-readiness")
  })

  test("keeps actual IPv6 and wildcard endpoints without substituting a loopback host", () => {
    for (const address of ["::1", "::"]) {
      expect(JSON.parse(readinessLine({
        ...snapshot, endpoint: { protocol: "http:", address, port: 45123, baseUrl: `http://[${address}]:45123` },
      }).toString()).endpoint.address).toBe(address)
    }
  })

  test.skipIf(process.platform !== "linux").each(["ordinary", "configured"])(
    "actual listener and serve publication: %s",
    async (mode) => {
      await using tmp = await tmpdir({ config: { formatter: false, lsp: false } })
      const timeline = openSync(`${tmp.path}/timeline.jsonl`, "ax", 0o600)
      try {
        const effects = openSync(`${tmp.path}/effects.jsonl`, "ax", 0o600)
        try {
          const result = spawnSync(process.execPath, [`${import.meta.dir}/readiness-listen.fixture.ts`, mode], {
            cwd: `${import.meta.dir}/../..`,
            env: {
              PATH: process.env.PATH,
              HOME: tmp.path,
              XDG_DATA_HOME: `${tmp.path}/data`, XDG_CACHE_HOME: `${tmp.path}/cache`,
              XDG_CONFIG_HOME: `${tmp.path}/config`, XDG_STATE_HOME: `${tmp.path}/state`,
              OPENCODE_TEST_HOME: tmp.path, OPENCODE_TEST_MANAGED_CONFIG_DIR: `${tmp.path}/managed`,
              OPENCODE_DB: ":memory:", OPENCODE_DISABLE_AUTOUPDATE: "1", OPENCODE_DISABLE_MODELS_FETCH: "1",
              OPENCODE_DISABLE_DEFAULT_PLUGINS: "1", OPENCODE_MODELS_PATH: `${import.meta.dir}/../tool/fixtures/models-api.json`,
              OPENCODE_SERVER_PASSWORD: "readiness-fixture-secret", OPENCODE_SERVER_USERNAME: "opencode",
            },
            stdio: ["ignore", "pipe", "pipe", "ignore", "ignore", "ignore", "ignore", "ignore", "ignore", timeline, effects],
            encoding: "utf8", timeout: 30_000, maxBuffer: 64 * 1024,
          })
          expect(result.error).toBeUndefined()
          expect({ status: result.status, diagnostic: result.status === 0 ? "" : result.stderr }).toEqual({ status: 0, diagnostic: "" })
          expect(result.stdout).toContain(`readiness-${mode}-ok`)
          expect(result.stdout).not.toContain("readiness-fixture-secret")
          const lines = result.stdout.split("\n").filter((line) => line.startsWith(`{"format":"${readinessFormat}"`))
          expect(lines).toHaveLength(mode === "configured" ? 1 : 0)
          const records = (path: string) => readFileSync(path, "utf8").split("\n").filter(Boolean).map((line) => JSON.parse(line))
          const native = records(`${tmp.path}/timeline.jsonl`)
          const ledger = records(`${tmp.path}/effects.jsonl`)
          if (mode === "ordinary") {
            expect(native).toEqual([])
            expect(ledger).toEqual([])
            return
          }
          expect(native.map((record) => record.recordType)).toEqual(["producer-open", "hosted-capability", "producer-close"])
          expect(ledger.map((record) => record.recordType)).toEqual(["producer-open", "producer-close"])
          const ready = JSON.parse(lines[0])
          expect(Buffer.byteLength(`${lines[0]}\n`)).toBeLessThanOrEqual(readinessMaxBytes)
          expect(native[1].native.runtimeInstanceId).toBe(ready.runtimeInstanceId)
          expect(native[1].native.configGeneration).toBe(ready.configGeneration)
          expect([...native, ...ledger].every((record) => record.contractSha256 === contractSha256)).toBe(true)
          expect(native[1].activation).toEqual({ controllerNonce: "a".repeat(64), runId: "run_readiness_fixture", stackManifestSha256: "b".repeat(64) })
        } finally {
          closeSync(effects)
        }
      } finally {
        closeSync(timeline)
      }
    },
    40_000,
  )
})
