import { describe, expect, test } from "bun:test"
import { canonicalJson, digest, rawPermission } from "../../src/hosted-approval/protocol"
import { readHostedReplyBody } from "../../src/server/routes/instance/httpapi/handlers/permission"
import { Effect, Stream } from "effect"

describe("hosted approval protocol", () => {
  test("canonical digest matches the frozen golden vector", () => {
    expect(digest({ requestID: "permission_1", sessionID: "session_1", tool: "bash" })).toBe(
      "bf6bdf3651bc31505a7430699c5b3e55b6c51c787cd1ab27cc2017736fa679b2",
    )
  })

  test("canonical digest matches the full permission golden vector", () => {
    expect(digest({
      always: [],
      id: "per_1",
      metadata: { cwd: "/tmp", risk: 1 },
      patterns: ["ls", "pwd"],
      permission: "bash",
      sessionID: "ses_1",
    })).toBe("04c8063915eb1c84563b998516a670e59885c0fc453318e7ca8545a94b8accca")
  })

  test("sorts object keys recursively while preserving arrays", () => {
    expect(canonicalJson({ z: [{ b: 2, a: 1 }], a: true })).toBe('{"a":true,"z":[{"a":1,"b":2}]}')
  })

  test("sorts divergent Unicode keys by unsigned UTF-8 bytes", () => {
    const reversed = { "\u{10000}": 1, "\uE000": 2 }
    const canonical = '{"\uE000":2,"\u{10000}":1}'

    expect(canonicalJson(reversed)).toBe(canonical)
    expect(digest(reversed)).toBe("daec79d8b6582badd3b46bd783a72379bc0b5ae29f5d59f6133025aad69bc8d4")
    expect(canonicalJson({ "\uE000": 2, "\u{10000}": 1 })).toBe(canonical)
    expect(canonicalJson({ nested: [reversed, "\u{10000}", "\uE000"] })).toBe(
      '{"nested":[{"\uE000":2,"\u{10000}":1},"\u{10000}","\uE000"]}',
    )
  })

  test("digests the JSON wire projection without undefined fields", () => {
    const internal = { id: "permission_1", tool: undefined, metadata: { value: 1, absent: undefined } }
    const wire = rawPermission(internal) as unknown
    expect(wire).toEqual({ id: "permission_1", metadata: { value: 1 } })
    expect(() => digest(wire)).not.toThrow()
  })

  test("rejects unsupported canonical values", () => {
    expect(() => canonicalJson({ value: BigInt(1) })).toThrow("Unsupported hosted approval canonical value")
  })

  test("rejects undefined at every nesting position", () => {
    const sparse: unknown[] = []
    sparse.length = 1
    expect(() => canonicalJson(undefined)).toThrow("Unsupported hosted approval canonical value")
    expect(() => canonicalJson([undefined])).toThrow("Unsupported hosted approval canonical value")
    expect(() => canonicalJson(sparse)).toThrow("Unsupported hosted approval canonical value")
    expect(() => canonicalJson({ nested: { value: undefined } })).toThrow(
      "Unsupported hosted approval canonical value",
    )
  })
})

test("bounded request reader stops before consuming chunks after the limit", async () => {
  let consumed = 0
  const stream = Stream.fromIterable([
    new Uint8Array(10),
    new Uint8Array(7),
    new Uint8Array(1),
  ]).pipe(Stream.tap(() => Effect.sync(() => { consumed += 1 })))
  const exit = await Effect.runPromiseExit(readHostedReplyBody(stream, 16))
  expect(exit._tag).toBe("Failure")
  expect(consumed).toBe(2)
})
