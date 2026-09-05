import { afterEach, describe, expect, test } from "bun:test"
import { ConfigProvider, Effect, Exit, Fiber, Layer, ManagedRuntime } from "effect"
import { HttpRouter } from "effect/unstable/http"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { AppLayer } from "../../src/effect/app-runtime"
import { attach } from "../../src/effect/run-service"
import { Permission } from "../../src/permission"
import { InstanceStore } from "../../src/project/instance-store"
import { HttpApiApp } from "../../src/server/routes/instance/httpapi/server"
import { ServerAuth } from "../../src/server/auth"
import { SessionID } from "../../src/session/schema"
import { disposeAllInstances, tmpdir } from "../fixture/fixture"
import { resetDatabase } from "../fixture/db"
import {
  HostedApprovalProvenance,
  type NativeRecord,
  type Producer,
  type Stream,
} from "../../src/hosted-approval/provenance"

const apps = new Set<{ dispose: () => Promise<void> }>()

function app(password?: string, producer?: Producer) {
  const memoMap = Layer.makeMemoMapUnsafe()
  const runtime = ManagedRuntime.make(AppLayer, { memoMap })
  const web = HttpRouter.toWebHandler(
    (producer
      ? HttpApiApp.createRoutes(undefined, HostedApprovalProvenance.makeLayer(producer))
      : HttpApiApp.routes).pipe(
      Layer.provide(
        ConfigProvider.layer(
          ConfigProvider.fromUnknown({
            OPENCODE_DISABLE_DEFAULT_PLUGINS: "true",
            OPENCODE_SERVER_PASSWORD: password,
          }),
        ),
      ),
    ),
    { disableLogger: true, memoMap },
  )
  apps.add({
    dispose: async () => {
      await web.dispose()
      await runtime.dispose()
    },
  })
  return Object.assign(
    (path: string, init?: RequestInit) =>
      web.handler(new Request(new URL(path, "http://localhost"), init), HttpApiApp.context),
    {
      runFork: <A, E>(effect: Effect.Effect<A, E, ManagedRuntime.ManagedRuntime.Services<typeof runtime>>) =>
        runtime.runFork(attach(effect)),
    },
  )
}

function capture() {
  const records: Array<{ stream: Stream; record: NativeRecord }> = []
  let nonce = 0
  let failTimeline = false
  let failed = false
  const producer: Producer = {
    controllerNonce: "a".repeat(64),
    runId: "run_http_capture",
    operationNonce: () => (++nonce).toString(16).padStart(64, "0"),
    emit: (stream, record) => {
      if (failed) throw new Error("capture already poisoned")
      records.push({ stream, record })
      if (!failTimeline || stream !== "openCodeTimeline") return
      failed = true
      throw new Error("timeline capture failed")
    },
    poison: (reason) => {
      failed = true
      throw new Error(reason)
    },
    close: () => {},
  }
  return { producer, records, failTimeline: () => { failTimeline = true } }
}

function auth(password = "secret") {
  return ServerAuth.header({ username: "opencode", password }) ?? ""
}

afterEach(async () => {
  await Promise.all(Array.from(apps, (web) => web.dispose()))
  apps.clear()
  await disposeAllInstances()
  await resetDatabase()
})

describe("hosted approval v2 HttpApi", () => {
  test("captures actually read invalid bytes as one raw operation and no typed or effect fact", async () => {
    await using dir = await tmpdir({ git: true, config: { formatter: false, lsp: false } })
    const captured = capture()
    const request = app("secret", captured.producer)
    const body = '{ "schemaVersion": 2, bad json'
    const response = await request(
      "/experimental/agent-teams/hosted-approval/session/ses_invalid/permission/per_invalid/reply",
      {
        method: "POST",
        headers: {
          "accept-encoding": "identity",
          authorization: auth(),
          "content-type": "application/json",
          "x-opencode-directory": dir.path,
        },
        body,
      },
    )
    expect(response.status).toBe(400)
    expect(await response.text()).toBe("")
    expect(captured.records).toHaveLength(1)
    expect(captured.records[0]).toMatchObject({
      stream: "openCodeTimeline",
      record: {
        recordType: "hosted-reply-raw",
        native: {
          outcome: "invalid-json",
          requestBodySha256: HostedApprovalProvenance.sha256(body),
          responseSha256: HostedApprovalProvenance.sha256(new Uint8Array()),
          status: 400,
        },
      },
    })
  })

  test("maps unavailable capability and observe errors through their declared response contract", async () => {
    await using dir = await tmpdir({ git: true, config: { formatter: false, lsp: false } })
    const request = app()
    const headers = { "x-opencode-directory": dir.path }
    const capability = await request("/experimental/agent-teams/hosted-approval-capability", { headers })
    const observe = await request(
      "/experimental/agent-teams/hosted-approval/session/ses_missing/permissions",
      { headers },
    )
    const observeBody = await observe.text()
    expect(capability.status).toBe(404)
    expect(await capability.json()).toEqual({ _tag: "HostedApprovalUnavailable" })
    expect({ status: observe.status, body: observeBody }).toEqual({
      status: 404,
      body: JSON.stringify({ _tag: "HostedApprovalUnavailable" }),
    })
  })

  test("is unavailable before reading malformed bodies when server auth is absent", async () => {
    await using dir = await tmpdir({ git: true, config: { formatter: false, lsp: false } })
    const request = app()
    const response = await request(
      "/experimental/agent-teams/hosted-approval/session/ses_missing/permission/per_missing/reply",
      {
        method: "POST",
        headers: { "content-type": "application/json", "x-opencode-directory": dir.path },
        body: '{"duplicate":1,"duplicate":2}',
      },
    )
    expect(response.status).toBe(404)
  })

  test("isolates hosted availability between app runtimes", async () => {
    await using dir = await tmpdir({ git: true, config: { formatter: false, lsp: false } })
    const unavailable = app()
    const available = app("secret")
    const path = "/experimental/agent-teams/hosted-approval-capability"
    const headers = { "x-opencode-directory": dir.path }

    expect((await unavailable(path, { headers })).status).toBe(404)
    expect((await available(path, { headers: { ...headers, authorization: auth() } })).status).toBe(200)
  })

  test("requires standard Basic auth for capability", async () => {
    await using dir = await tmpdir({ git: true, config: { formatter: false, lsp: false } })
    const request = app("secret")
    const path = "/experimental/agent-teams/hosted-approval-capability"
    const headers = { "x-opencode-directory": dir.path }
    expect((await request(path, { headers })).status).toBe(401)
    expect((await request(path, { headers: { ...headers, authorization: auth("wrong") } })).status).toBe(401)
    const accepted = await request(path, { headers: { ...headers, authorization: auth() } })
    expect(accepted.status).toBe(200)
    expect(await accepted.json()).toMatchObject({
      schemaVersion: 2,
      protocol: "agent-teams-hosted-approval-v2",
      authentication: "opencode-basic",
      runtimeInstanceId: expect.stringMatching(/^runtime_instance_[0-9a-f]{32}$/),
      configGeneration: expect.stringMatching(/^config_generation_[0-9a-f]{32}$/),
    })
  })

  test("rejects extra reply keys before any permission effect", async () => {
    await using dir = await tmpdir({ git: true, config: { formatter: false, lsp: false } })
    const request = app("secret")
    const headers = {
      "content-type": "application/json",
      "x-opencode-directory": dir.path,
      authorization: auth(),
    }
    const capability = await (await request("/experimental/agent-teams/hosted-approval-capability", { headers })).json()
    const body = {
      schemaVersion: 2,
      protocol: "agent-teams-hosted-approval-v2",
      runtimeInstanceId: capability.runtimeInstanceId,
      expectedConfigGeneration: capability.configGeneration,
      requestId: "per_missing",
      sessionId: "ses_missing",
      sessionIncarnation: `session_incarnation_${"3".repeat(32)}`,
      requestIncarnation: `request_incarnation_${"1".repeat(32)}`,
      expectedPermissionDigest: "2".repeat(64),
      decision: "allow_once",
      extra: true,
    }
    const response = await request(
      "/experimental/agent-teams/hosted-approval/session/ses_missing/permission/per_missing/reply",
      { method: "POST", headers, body: JSON.stringify(body) },
    )
    expect(response.status).toBe(400)
    const observe = await request(
      "/experimental/agent-teams/hosted-approval/session/ses_missing/permissions",
      { headers },
    )
    expect((await observe.json()).permissions).toEqual([])
  })

  test("returns explicit no-effect statuses for malformed, duplicate, oversize, conflict, and precondition", async () => {
    await using dir = await tmpdir({ git: true, config: { formatter: false, lsp: false } })
    const captured = capture()
    const request = app("secret", captured.producer)
    const headers = {
      "accept-encoding": "identity",
      "content-type": "application/json",
      "x-opencode-directory": dir.path,
      authorization: auth(),
    }
    const capability = await (await request("/experimental/agent-teams/hosted-approval-capability", { headers })).json()
    const path = "/experimental/agent-teams/hosted-approval/session/ses_missing/permission/per_missing/reply"
    const valid = {
      schemaVersion: 2,
      protocol: "agent-teams-hosted-approval-v2",
      runtimeInstanceId: capability.runtimeInstanceId,
      expectedConfigGeneration: capability.configGeneration,
      requestId: "per_missing",
      sessionId: "ses_missing",
      sessionIncarnation: `session_incarnation_${"3".repeat(32)}`,
      requestIncarnation: `request_incarnation_${"1".repeat(32)}`,
      expectedPermissionDigest: "2".repeat(64),
      decision: "allow_once",
    }
    const post = (body: BodyInit) => request(path, { method: "POST", headers, body })
    expect((await post("{" )).status).toBe(400)
    expect((await post('{"schemaVersion":2,"schemaVersion":2}')).status).toBe(400)
    expect((await post("x".repeat(16 * 1024 + 1))).status).toBe(400)
    const legacy = Object.fromEntries(Object.entries(valid).filter(([key]) => key !== "sessionIncarnation"))
    expect((await post(JSON.stringify(legacy))).status).toBe(400)
    const noncanonical = JSON.stringify(valid, null, 2)
    const conflict = await post(noncanonical)
    expect(conflict.status).toBe(409)
    expect(await conflict.text()).toBe("")
    expect((await request("/config", {
      method: "PATCH",
      headers,
      body: JSON.stringify({ formatter: false }),
    })).status).toBe(200)
    expect((await post(JSON.stringify(valid))).status).toBe(412)
    expect(captured.records.filter((item) => item.record.recordType === "conditional-reply-effect")).toEqual([])
    const raw = captured.records.flatMap((item) => item.record.recordType === "hosted-reply-raw" ? [item.record] : [])
    const conflictRaw = raw.find((record) => record.native.outcome === "conflict")!
    expect(conflictRaw.native.requestBodySha256).toBe(HostedApprovalProvenance.sha256(noncanonical))
    expect(raw.every((record) => record.native.responseSha256 === HostedApprovalProvenance.sha256(new Uint8Array()))).toBe(true)
    const typed = captured.records.flatMap((item) => item.record.recordType === "hosted-reply" ? [item.record] : [])
    expect(typed).toHaveLength(2)
    expect(typed.every((record) => !("responseSha256" in record.native))).toBe(true)
    for (const record of typed) {
      expect(raw.find((candidate) => candidate.native.outcome === record.native.outcome)?.operationNonce)
        .toBe(record.operationNonce)
    }
  })

  test("authenticates and applies one real pending request exactly once", async () => {
    await using dir = await tmpdir({ git: true, config: { formatter: false, lsp: false } })
    const captured = capture()
    const request = app("secret", captured.producer)
    const headers = {
      "accept-encoding": "identity",
      "content-type": "application/json",
      "x-opencode-directory": dir.path,
      authorization: auth(),
    }
    const capability = await (await request("/experimental/agent-teams/hosted-approval-capability", { headers })).json()
    const requestID = PermissionV1.ID.make("per_hosted_http_once")
    const sessionID = SessionID.make("ses_hosted_http_once")
    const asked = request.runFork(
      InstanceStore.Service.use((store) =>
        store.provide(
          { directory: dir.path },
          Permission.Service.use((permission) =>
            permission.ask({
              id: requestID,
              sessionID,
              permission: "bash",
              patterns: ["ls"],
              metadata: {},
              always: [],
              ruleset: [],
            }),
          ),
        ),
      ),
    )
    const observePath = `/experimental/agent-teams/hosted-approval/session/${sessionID}/permissions`
    const observed = await Effect.runPromise(
      Effect.gen(function* () {
        while (true) {
          const response = yield* Effect.promise(() => request(observePath, { headers }))
          const body = yield* Effect.promise(() => response.json())
          if (body.permissions.length === 1) return body
          yield* Effect.sleep("10 millis")
        }
      }).pipe(Effect.timeout("2 seconds")),
    )
    const pending = observed.permissions[0]
    const body = {
      schemaVersion: 2,
      protocol: "agent-teams-hosted-approval-v2",
      runtimeInstanceId: capability.runtimeInstanceId,
      expectedConfigGeneration: capability.configGeneration,
      requestId: pending.requestId,
      sessionId: pending.sessionId,
      sessionIncarnation: pending.sessionIncarnation,
      requestIncarnation: pending.requestIncarnation,
      expectedPermissionDigest: pending.permissionDigest,
      decision: "allow_once",
    }
    const path = `/experimental/agent-teams/hosted-approval/session/${sessionID}/permission/${requestID}/reply`
    const replies = await Promise.all([
      request(path, { method: "POST", headers, body: JSON.stringify(body) }),
      request(path, { method: "POST", headers, body: JSON.stringify(body) }),
    ])
    const applied = replies.find((response) => response.status === 200)!
    expect(applied.status).toBe(200)
    expect(await applied.json()).toMatchObject({
      status: "applied",
      requestId: requestID,
      sessionId: sessionID,
      permissionDigest: pending.permissionDigest,
      decision: "allow_once",
    })
    expect(replies.map((response) => response.status).sort()).toEqual([200, 409])
    expect(Exit.isSuccess(await Effect.runPromise(Fiber.await(asked)))).toBe(true)
    expect((await (await request(observePath, { headers })).json()).permissions).toEqual([])
    const raw = captured.records.filter((item) => item.record.recordType === "hosted-reply-raw")
    const typed = captured.records.filter((item) => item.record.recordType === "hosted-reply")
    const effects = captured.records.filter((item) => item.record.recordType === "conditional-reply-effect")
    expect(raw.map((item) => item.record.native.outcome).sort()).toEqual(["applied", "conflict"])
    expect(typed.map((item) => item.record.native.outcome).sort()).toEqual(["applied", "conflict"])
    expect(effects).toHaveLength(1)
    expect(effects[0]).toMatchObject({
      stream: "protectedEffectLedger",
      record: {
        native: {
          decision: "once",
          outcome: "applied",
          requestId: requestID,
          sessionId: sessionID,
        },
      },
    })
    const appliedRaw = raw.find((item) => item.record.native.outcome === "applied")!
    const appliedTyped = typed.find((item) => item.record.native.outcome === "applied")!
    expect(new Set([appliedRaw.record.operationNonce, appliedTyped.record.operationNonce, effects[0].record.operationNonce]).size).toBe(1)
  })

  test("captures one actual reject settlement and does not emit an effect for its duplicate", async () => {
    await using dir = await tmpdir({ git: true, config: { formatter: false, lsp: false } })
    const captured = capture()
    const request = app("secret", captured.producer)
    const headers = {
      "accept-encoding": "identity",
      authorization: auth(),
      "content-type": "application/json",
      "x-opencode-directory": dir.path,
    }
    const capability = await (await request("/experimental/agent-teams/hosted-approval-capability", { headers })).json()
    const requestID = PermissionV1.ID.make("per_hosted_http_reject")
    const sessionID = SessionID.make("ses_hosted_http_reject")
    const asked = request.runFork(
      InstanceStore.Service.use((store) =>
        store.provide(
          { directory: dir.path },
          Permission.Service.use((permission) => permission.ask({
            id: requestID,
            sessionID,
            permission: "bash",
            patterns: ["rm -rf build"],
            metadata: {},
            always: [],
            ruleset: [],
          })),
        ),
      ),
    )
    const observePath = `/experimental/agent-teams/hosted-approval/session/${sessionID}/permissions`
    const observed = await Effect.runPromise(
      Effect.gen(function* () {
        while (true) {
          const response = yield* Effect.promise(() => request(observePath, { headers }))
          const body = yield* Effect.promise(() => response.json())
          if (body.permissions.length === 1) return body.permissions[0]
          yield* Effect.sleep("10 millis")
        }
      }).pipe(Effect.timeout("2 seconds")),
    )
    const body = JSON.stringify({
      schemaVersion: 2,
      protocol: "agent-teams-hosted-approval-v2",
      runtimeInstanceId: capability.runtimeInstanceId,
      expectedConfigGeneration: capability.configGeneration,
      requestId: observed.requestId,
      sessionId: observed.sessionId,
      sessionIncarnation: observed.sessionIncarnation,
      requestIncarnation: observed.requestIncarnation,
      expectedPermissionDigest: observed.permissionDigest,
      decision: "reject",
    })
    const path = `/experimental/agent-teams/hosted-approval/session/${sessionID}/permission/${requestID}/reply`
    expect((await request(path, { method: "POST", headers, body })).status).toBe(200)
    expect((await request(path, { method: "POST", headers, body })).status).toBe(409)
    expect(Exit.isFailure(await Effect.runPromise(Fiber.await(asked)))).toBe(true)
    const effects = captured.records.filter((item) => item.record.recordType === "conditional-reply-effect")
    expect(effects).toHaveLength(1)
    expect(effects[0]).toMatchObject({
      stream: "protectedEffectLedger",
      record: {
        native: {
          decision: "reject",
          outcome: "applied",
          permissionDigest: observed.permissionDigest,
          requestId: requestID,
          sessionId: sessionID,
        },
      },
    })
  })

  test("capture failure after settlement remains poisoned and cannot become a retry-safe conflict", async () => {
    await using dir = await tmpdir({ git: true, config: { formatter: false, lsp: false } })
    const captured = capture()
    const request = app("secret", captured.producer)
    const headers = {
      "accept-encoding": "identity",
      authorization: auth(),
      "content-type": "application/json",
      "x-opencode-directory": dir.path,
    }
    const capability = await (await request("/experimental/agent-teams/hosted-approval-capability", { headers })).json()
    const requestID = PermissionV1.ID.make("per_hosted_capture_failure")
    const sessionID = SessionID.make("ses_hosted_capture_failure")
    const asked = request.runFork(
      InstanceStore.Service.use((store) =>
        store.provide(
          { directory: dir.path },
          Permission.Service.use((permission) => permission.ask({
            id: requestID,
            sessionID,
            permission: "bash",
            patterns: ["pwd"],
            metadata: {},
            always: [],
            ruleset: [],
          })),
        ),
      ),
    )
    const observePath = `/experimental/agent-teams/hosted-approval/session/${sessionID}/permissions`
    const observed = await Effect.runPromise(
      Effect.gen(function* () {
        while (true) {
          const response = yield* Effect.promise(() => request(observePath, { headers }))
          const body = yield* Effect.promise(() => response.json())
          if (body.permissions.length === 1) return body.permissions[0]
          yield* Effect.sleep("10 millis")
        }
      }).pipe(Effect.timeout("2 seconds")),
    )
    const body = JSON.stringify({
      schemaVersion: 2,
      protocol: "agent-teams-hosted-approval-v2",
      runtimeInstanceId: capability.runtimeInstanceId,
      expectedConfigGeneration: capability.configGeneration,
      requestId: observed.requestId,
      sessionId: observed.sessionId,
      sessionIncarnation: observed.sessionIncarnation,
      requestIncarnation: observed.requestIncarnation,
      expectedPermissionDigest: observed.permissionDigest,
      decision: "allow_once",
    })
    const path = `/experimental/agent-teams/hosted-approval/session/${sessionID}/permission/${requestID}/reply`
    captured.failTimeline()
    const first = await request(path, { method: "POST", headers, body }).then(
      (response) => response.status,
      () => 0,
    )
    const second = await request(path, { method: "POST", headers, body }).then(
      (response) => response.status,
      () => 0,
    )
    expect(first).not.toBe(200)
    expect(second).not.toBe(409)
    expect(Exit.isSuccess(await Effect.runPromise(Fiber.await(asked)))).toBe(true)
    expect(captured.records.filter((item) => item.record.recordType === "conditional-reply-effect")).toHaveLength(1)
    expect(captured.records.filter((item) => item.record.recordType === "hosted-reply-raw")).toHaveLength(1)
  })

  test("config mutation rotates the same authority observed by hosted routes", async () => {
    await using dir = await tmpdir({ git: true, config: { formatter: false, lsp: false } })
    const request = app("secret")
    const headers = {
      "content-type": "application/json",
      "x-opencode-directory": dir.path,
      authorization: auth(),
    }
    const before = await (await request("/experimental/agent-teams/hosted-approval-capability", { headers })).json()
    const updated = await request("/config", { method: "PATCH", headers, body: JSON.stringify({ formatter: false }) })
    expect(updated.status).toBe(200)
    const after = await (await request("/experimental/agent-teams/hosted-approval-capability", { headers })).json()
    expect(after.runtimeInstanceId).toBe(before.runtimeInstanceId)
    expect(after.configGeneration).not.toBe(before.configGeneration)
  })
})
