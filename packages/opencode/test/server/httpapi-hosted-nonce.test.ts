import { afterEach, describe, expect, test } from "bun:test"
import { Effect, Fiber } from "effect"
import { HttpServerResponse } from "effect/unstable/http"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { Permission } from "../../src/permission"
import { InstanceStore } from "../../src/project/instance-store"
import { SessionID } from "../../src/session/schema"
import { operationNonceHeader, withOperationNonce } from "../../src/hosted-approval/operation-header"
import { sha256 } from "../../src/hosted-approval/provenance"
import { app, auth, capture, disposeApps } from "../fixture/hosted-approval"
import { disposeAllInstances, tmpdir } from "../fixture/fixture"
import { resetDatabase } from "../fixture/db"

const capabilityPath = "/experimental/agent-teams/hosted-approval-capability"
const observePath = "/experimental/agent-teams/hosted-approval/session/ses_nonce/permissions"
const replyPath = "/experimental/agent-teams/hosted-approval/session/ses_nonce/permission/per_nonce/reply"

afterEach(async () => {
  await disposeApps()
  await disposeAllInstances()
  await resetDatabase()
})

describe("hosted operation HTTP nonce", () => {
  test("the header helper preserves the exact finalized body and status", () => {
    const response = HttpServerResponse.uint8Array(new Uint8Array([0xff, 0, 0x0a]), { status: 409 })
    const wrapped = withOperationNonce(response, "a".repeat(64))
    expect(wrapped.body).toBe(response.body)
    expect(wrapped.status).toBe(409)
    expect(wrapped.headers[operationNonceHeader]).toBe("a".repeat(64))
    expect(response.headers[operationNonceHeader]).toBeUndefined()
    expect(withOperationNonce(response, undefined)).toBe(response)
  })

  test("identical capability/observe bodies have distinct actual operation nonces", async () => {
    await using dir = await tmpdir({ config: { formatter: false, lsp: false } })
    const captured = capture()
    const request = app("secret", captured.producer)
    const headers = { "accept-encoding": "identity", authorization: auth(), "x-opencode-directory": dir.path }
    for (const path of [capabilityPath, observePath]) {
      const responses = await Promise.all([request(path, { headers }), request(path, { headers })])
      const bodies = await Promise.all(responses.map((response) => response.text()))
      expect(bodies[0]).toBe(bodies[1])
      expect(responses[0].headers.get(operationNonceHeader)).not.toBe(responses[1].headers.get(operationNonceHeader))
      responses.forEach((response, index) => {
        expect(response.status).toBe(200)
        const nonce = response.headers.get(operationNonceHeader)
        expect(nonce).toMatch(/^[0-9a-f]{64}$/)
        const facts = captured.records.filter((entry) => entry.record.operationNonce === nonce)
        expect(facts).toHaveLength(1)
        expect(facts[0].record.recordType).toBe(path === capabilityPath ? "hosted-capability" : "hosted-observe")
        expect(facts[0].record.native).toMatchObject({ responseSha256: sha256(bodies[index]), status: response.status })
      })
    }
    captured.producer.assertHealthy()
  })

  test("unavailable capability/observe have no operation but unavailable reply retains its raw nonce", async () => {
    await using dir = await tmpdir({ config: { formatter: false, lsp: false } })
    const captured = capture()
    const request = app(undefined, captured.producer)
    const headers = { "accept-encoding": "identity", "x-opencode-directory": dir.path }
    for (const path of [capabilityPath, observePath]) {
      const response = await request(path, { headers })
      expect(response.status).toBe(404)
      expect(await response.text()).toBe('{"_tag":"HostedApprovalUnavailable"}')
      expect(response.headers.has(operationNonceHeader)).toBe(false)
    }
    expect(captured.records).toEqual([])
    expect(captured.nonces()).toBe(2) // Only the two producer-open emission nonces.
    const response = await request(replyPath, { method: "POST", headers, body: "{" })
    expect(response.status).toBe(404)
    expect(await response.text()).toBe("")
    expect(captured.records).toHaveLength(1)
    expect(response.headers.get(operationNonceHeader)).toBe(captured.records[0].record.operationNonce)
    expect(captured.records[0].record.native).toMatchObject({ outcome: "unavailable", requestBodySha256: null })
  })

  test("no producer and pre-handler authentication failures never acquire a header", async () => {
    await using dir = await tmpdir({ config: { formatter: false, lsp: false } })
    const headers = { authorization: auth(), "accept-encoding": "identity", "x-opencode-directory": dir.path }
    const ordinary = app("secret")
    for (const path of [capabilityPath, observePath]) {
      expect((await ordinary(path, { headers })).headers.has(operationNonceHeader)).toBe(false)
    }
    expect((await ordinary(replyPath, { method: "POST", headers, body: "{" })).headers.has(operationNonceHeader)).toBe(false)
    const captured = capture()
    const request = app("secret", captured.producer)
    const response = await request(capabilityPath, { headers: { ...headers, authorization: auth("wrong") } })
    expect(response.status).toBe(401)
    expect(response.headers.has(operationNonceHeader)).toBe(false)
    expect(captured.records).toEqual([])
  })

  test("every raw and typed rejection keeps actual bytes, empty bodies and variant-specific fields", async () => {
    await using dir = await tmpdir({ config: { formatter: false, lsp: false } })
    const captured = capture()
    const request = app("secret", captured.producer)
    const headers = { authorization: auth(), "accept-encoding": "identity", "content-type": "application/json", "x-opencode-directory": dir.path }
    const capability = await (await request(capabilityPath, { headers })).json()
    const payload = {
      schemaVersion: 2, protocol: "agent-teams-hosted-approval-v2",
      runtimeInstanceId: capability.runtimeInstanceId, expectedConfigGeneration: capability.configGeneration,
      requestId: "per_nonce", sessionId: "ses_nonce",
      sessionIncarnation: `session_incarnation_${"3".repeat(32)}`,
      requestIncarnation: `request_incarnation_${"4".repeat(32)}`,
      expectedPermissionDigest: "5".repeat(64), decision: "allow_once",
    }
    const cases = [
      { body: new Uint8Array([0xff, 0xfe]), outcome: "invalid-json", status: 400 },
      { body: "{", outcome: "invalid-json", status: 400 },
      { body: '{"x":"\\q"}', outcome: "invalid-json", status: 400 },
      { body: '{\v"x":1}', outcome: "invalid-json", status: 400 },
      { body: '{"schemaVersion":2,"schemaVersion":2}', outcome: "invalid-json", status: 400 },
      { body: '{"schemaVersion":2,"\\u0073chemaVersion":2}', outcome: "invalid-json", status: 400 },
      { body: "null", outcome: "invalid-schema", status: 400 },
      { body: "[]", outcome: "invalid-schema", status: 400 },
      { body: "{}", outcome: "invalid-schema", status: 400 },
      { body: JSON.stringify({ ...payload, decision: "always" }), outcome: "invalid-schema", status: 400 },
      { body: JSON.stringify({ ...payload, extra: true }), outcome: "invalid-schema", status: 400 },
      { body: "x".repeat(16 * 1024 + 1), outcome: "body-read-failed", status: 400 },
      { body: JSON.stringify({ ...payload, requestId: "per_body", sessionId: "ses_body" }), outcome: "bad-request", status: 400 },
      { body: JSON.stringify(payload, null, 2), outcome: "conflict", status: 409 },
      { body: JSON.stringify({ ...payload, runtimeInstanceId: `runtime_instance_${"0".repeat(32)}` }), outcome: "precondition-failed", status: 412 },
    ] as const
    for (const item of cases) {
      const response = await request(replyPath, { method: "POST", headers, body: item.body })
      expect(response.status).toBe(item.status)
      expect(await response.text()).toBe("")
      const nonce = response.headers.get(operationNonceHeader)
      expect(nonce).toMatch(/^[0-9a-f]{64}$/)
      const group = captured.records.filter((entry) => entry.record.operationNonce === nonce).map((entry) => entry.record)
      const typed = item.outcome === "bad-request" || item.outcome === "conflict" || item.outcome === "precondition-failed"
      expect(group.map((record) => record.recordType)).toEqual(typed ? ["hosted-reply-raw", "hosted-reply"] : ["hosted-reply-raw"])
      expect(group[0].native).toEqual({
        configGeneration: null, runtimeInstanceId: null, requestIncarnation: null, sessionIncarnation: null,
        requestId: "per_nonce", sessionId: "ses_nonce", outcome: item.outcome, status: item.status,
        requestBodySha256: item.outcome === "body-read-failed" ? null : sha256(item.body),
        responseSha256: sha256(new Uint8Array()),
      })
      if (typed) {
        expect(group[1].native).toEqual({
          configGeneration: null, runtimeInstanceId: null, requestIncarnation: null, sessionIncarnation: null,
          requestId: item.outcome === "bad-request" ? "per_body" : "per_nonce",
          sessionId: item.outcome === "bad-request" ? "ses_body" : "ses_nonce",
          outcome: item.outcome, status: item.status, decision: "allow_once", permissionDigest: payload.expectedPermissionDigest,
        })
        expect(group[1].native).not.toHaveProperty("responseSha256")
      }
    }
    const response = await request(replyPath, {
      method: "POST", headers,
      body: new ReadableStream<Uint8Array>({ start(controller) { controller.error(new Error("body stream failed")) } }),
    })
    expect(response.status).toBe(400)
    expect(await response.text()).toBe("")
    expect(captured.records.at(-1)!.record).toMatchObject({
      operationNonce: response.headers.get(operationNonceHeader),
      native: { outcome: "body-read-failed", requestBodySha256: null },
    })
    expect(captured.records.filter((entry) => entry.record.recordType === "conditional-reply-effect")).toEqual([])
  })

  test.each(["write", "sync"] as const)("a capability %s failure prevents nonce publication and permanently poisons", async (phase) => {
    await using dir = await tmpdir({ config: { formatter: false, lsp: false } })
    const captured = capture({ failRecord: "hosted-capability", failPhase: phase })
    const request = app("secret", captured.producer)
    const headers = { authorization: auth(), "accept-encoding": "identity", "x-opencode-directory": dir.path }
    for (let attempt = 0; attempt < 2; attempt++) {
      const result = await request(capabilityPath, { headers }).then(
        (response) => ({ response }), (error: unknown) => ({ error }),
      )
      if ("response" in result) {
        expect(result.response.status).toBe(500)
        expect(result.response.headers.has(operationNonceHeader)).toBe(false)
      }
      expect(() => captured.producer.assertHealthy()).toThrow("producer-provenance-fatal")
    }
  })

  test.each(["malformed", "duplicate", "throw"] as const)("a %s allocator cannot advertise an HTTP operation", async (failure) => {
    await using dir = await tmpdir({ config: { formatter: false, lsp: false } })
    const captured = capture()
    const request = app("secret", captured.producer)
    const headers = { authorization: auth(), "accept-encoding": "identity", "x-opencode-directory": dir.path }
    const first = await request(capabilityPath, { headers })
    expect(first.status).toBe(200)
    captured.failNextAllocation(failure === "duplicate" ? first.headers.get(operationNonceHeader)!
      : failure === "throw" ? new Error("allocator failed") : `${"a".repeat(64)}\n`)
    for (let attempt = 0; attempt < 2; attempt++) {
      const result = await request(capabilityPath, { headers }).then(
        (response) => ({ response }), (error: unknown) => ({ error }),
      )
      if ("response" in result) {
        expect(result.response.status).toBe(500)
        expect(result.response.headers.has(operationNonceHeader)).toBe(false)
      }
      expect(() => captured.producer.assertHealthy()).toThrow("producer-provenance-fatal")
    }
    expect(captured.records).toHaveLength(1)
  })

  test.each(["count", "bytes"] as const)("observe %s overflow publishes its actual 500 and one native nonce", async (bound) => {
    await using dir = await tmpdir({ config: { formatter: false, lsp: false } })
    const captured = capture()
    const request = app("secret", captured.producer)
    const sessionID = SessionID.make("ses_nonce")
    const count = bound === "count" ? 257 : 1
    request.runFork(InstanceStore.Service.use((store) => store.provide(
      { directory: dir.path }, Permission.Service.use((permission) => Effect.forEach(
        Array.from({ length: count }, (_, index) => index),
        (index) => permission.ask({
          id: PermissionV1.ID.make(`per_overflow_${index}`), sessionID, permission: "bash",
          patterns: ["pwd"], metadata: bound === "bytes" ? { value: "x".repeat(1024 * 1024) } : {}, always: [], ruleset: [],
        }), { concurrency: "unbounded" },
      )),
    )))
    await Effect.runPromise(Effect.gen(function* () {
      while (true) {
        const pending = yield* Fiber.join(request.runFork(InstanceStore.Service.use((store) => store.provide(
          { directory: dir.path }, Permission.Service.use((permission) => permission.hostedList()),
        ))))
        if (pending.length === count) return
        yield* Effect.sleep("10 millis")
      }
    }).pipe(Effect.timeout("5 seconds")))
    const response = await request(observePath, {
      headers: { authorization: auth(), "accept-encoding": "identity", "x-opencode-directory": dir.path },
    })
    expect(response.status).toBe(500)
    const body = await response.text()
    expect(body).toBe('{"_tag":"InternalServerError"}')
    const group = captured.records.filter((entry) => entry.record.operationNonce === response.headers.get(operationNonceHeader))
    expect(group).toHaveLength(1)
    expect(group[0].record).toMatchObject({
      recordType: "hosted-observe", native: { outcome: "overflow", permissionCount: count, status: 500, responseSha256: sha256(body) },
    })
  })
})
