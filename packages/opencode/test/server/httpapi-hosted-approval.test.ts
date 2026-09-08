import { afterEach, describe, expect, test } from "bun:test"
import { Cause, Deferred, Effect, Exit, Fiber } from "effect"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { Permission } from "../../src/permission"
import { EventV2Bridge } from "../../src/event-v2-bridge"
import { InstanceStore } from "../../src/project/instance-store"
import { SessionID } from "../../src/session/schema"
import { disposeAllInstances, tmpdir } from "../fixture/fixture"
import { resetDatabase } from "../fixture/db"
import { HostedApprovalProvenance } from "../../src/hosted-approval/provenance"
import { operationNonceHeader } from "../../src/hosted-approval/operation-header"
import { app, auth, capture, disposeApps } from "../fixture/hosted-approval"

function observe<A>(promise: Promise<A>) {
  return promise.then(
    (value) => ({ _tag: "success" as const, value }),
    (error: unknown) => ({ _tag: "failure" as const, error }),
  )
}

function bounded<A>(promise: Promise<A>, label: string) {
  return Promise.race([
    promise,
    Bun.sleep(2_000).then(() => Promise.reject(new Error(`timed out waiting for ${label}`))),
  ])
}

afterEach(async () => {
  await disposeApps()
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
    const receiptBytes = new Uint8Array(await applied.arrayBuffer())
    expect(JSON.parse(new TextDecoder().decode(receiptBytes))).toEqual({
      schemaVersion: 2,
      protocol: "agent-teams-hosted-approval-v2",
      status: "applied",
      runtimeInstanceId: capability.runtimeInstanceId,
      configGeneration: capability.configGeneration,
      requestId: requestID,
      sessionId: sessionID,
      sessionIncarnation: pending.sessionIncarnation,
      requestIncarnation: pending.requestIncarnation,
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
    const nonce = applied.headers.get(operationNonceHeader)
    expect(nonce).toBe(appliedRaw.record.operationNonce)
    expect(captured.records.filter((item) => item.record.operationNonce === nonce)).toHaveLength(3)
    expect(appliedRaw.record.native).toMatchObject({
      requestBodySha256: HostedApprovalProvenance.sha256(JSON.stringify(body)),
      responseSha256: HostedApprovalProvenance.sha256(receiptBytes),
    })
    expect(appliedTyped.record.native).toHaveProperty("responseSha256", HostedApprovalProvenance.sha256(receiptBytes))
    expect(captured.records.indexOf(appliedRaw)).toBeLessThan(captured.records.indexOf(appliedTyped))
    expect(replies.find((response) => response.status === 409)!.headers.get(operationNonceHeader)).not.toBe(nonce)
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

  test.each(["abort", "shutdown"] as const)("finalizes a settlement when the request reaches publication then %s occurs", async (boundary) => {
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
    const requestID = PermissionV1.ID.make(`per_hosted_${boundary}_boundary`)
    const sessionID = SessionID.make(`ses_hosted_${boundary}_boundary`)
    const asked = request.runFork(
      InstanceStore.Service.use((store) => store.provide(
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
      )),
    )
    const observePath = `/experimental/agent-teams/hosted-approval/session/${sessionID}/permissions`
    const pending = await Effect.runPromise(
      Effect.gen(function* () {
        while (true) {
          const response = yield* Effect.promise(() => request(observePath, { headers }))
          const body = yield* Effect.promise(() => response.json())
          if (body.permissions.length === 1) return body.permissions[0]
          yield* Effect.sleep("10 millis")
        }
      }).pipe(Effect.timeout("2 seconds")),
    )
    const registered = Effect.runSync(Deferred.make<void>())
    const published = Effect.runSync(Deferred.make<void>())
    const release = Effect.runSync(Deferred.make<void>())
    const listener = request.runFork(Effect.gen(function* () {
      const events = yield* EventV2Bridge.Service
      const unsubscribe = yield* events.listen((event) => {
        if (
          event.type !== Permission.Event.Replied.type ||
          (event.data as { requestID: PermissionV1.ID }).requestID !== requestID
        ) return Effect.void
        Deferred.doneUnsafe(published, Effect.void)
        return Deferred.await(release)
      })
      Deferred.doneUnsafe(registered, Effect.void)
      yield* Effect.never.pipe(Effect.ensuring(unsubscribe))
    }))
    await Effect.runPromise(Deferred.await(registered))
    const controller = new AbortController()
    const response = request(
      `/experimental/agent-teams/hosted-approval/session/${sessionID}/permission/${requestID}/reply`,
      {
        method: "POST",
        headers,
        signal: controller.signal,
        body: JSON.stringify({
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
        }),
      },
    ).then((value) => value, () => undefined)
    await Effect.runPromise(Deferred.await(published))
    const shutdown = boundary === "shutdown" ? request.dispose() : Promise.resolve()
    if (boundary === "abort") controller.abort()
    Deferred.doneUnsafe(release, Effect.void)
    await Promise.all([response, shutdown])
    const askedExit = await Effect.runPromise(Fiber.await(asked))
    if (boundary === "abort") expect(Exit.isSuccess(askedExit)).toBe(true)
    if (boundary === "shutdown") {
      // Runtime disposal may interrupt this consumer after Permission has already settled its Deferred.
      const interrupted = Exit.isFailure(askedExit) && Cause.interruptors(askedExit.cause).size > 0
      expect(Exit.isSuccess(askedExit) || interrupted).toBe(true)
    }
    const effect = captured.records.find((item) => item.record.recordType === "conditional-reply-effect")!.record
    expect(captured.records.filter((item) => item.record.operationNonce === effect.operationNonce && item.record.recordType === "conditional-reply-effect")).toHaveLength(1)
    expect(captured.records.filter((item) => item.record.operationNonce === effect.operationNonce && item.record.recordType === "hosted-reply-raw")).toHaveLength(1)
    expect(captured.records.filter((item) => item.record.operationNonce === effect.operationNonce && item.record.recordType === "hosted-reply")).toHaveLength(1)
    captured.producer.assertHealthy()
    if (boundary === "abort") await Effect.runPromise(Fiber.interrupt(listener))
  })

  test.each(["no capsule", "capture"] as const)("cancels a validated reply waiting behind publication with %s", async (mode) => {
    await using dir = await tmpdir({ git: true, config: { formatter: false, lsp: false } })
    const captured = mode === "capture" ? capture() : undefined
    const request = app("secret", captured?.producer)
    const headers = {
      "accept-encoding": "identity",
      authorization: auth(),
      "content-type": "application/json",
      "x-opencode-directory": dir.path,
    }
    const capability = await (await request("/experimental/agent-teams/hosted-approval-capability", { headers })).json()
    const requestID = PermissionV1.ID.make(`per_hosted_coordinator_a_${mode.replace(" ", "_")}`)
    const requestIDB = PermissionV1.ID.make(`per_hosted_coordinator_b_${mode.replace(" ", "_")}`)
    const sessionID = SessionID.make(`ses_hosted_coordinator_${mode.replace(" ", "_")}`)
    const ask = (id: PermissionV1.ID, command: string) => request.runFork(
      InstanceStore.Service.use((store) => store.provide(
        { directory: dir.path },
        Permission.Service.use((permission) => permission.ask({
          id,
          sessionID,
          permission: "bash",
          patterns: [command],
          metadata: {},
          always: [],
          ruleset: [],
        })),
      )),
    )
    const asked = ask(requestID, "pwd")
    const askedB = ask(requestIDB, "whoami")
    const observePath = `/experimental/agent-teams/hosted-approval/session/${sessionID}/permissions`
    const observed = await Effect.runPromise(
      Effect.gen(function* () {
        while (true) {
          const response = yield* Effect.promise(() => request(observePath, { headers }))
          const body = yield* Effect.promise(() => response.json())
          if (body.permissions.length === 2) return body.permissions
          yield* Effect.sleep("10 millis")
        }
      }).pipe(Effect.timeout("2 seconds")),
    )
    const pending = new Map(observed.map((item: { requestId: string }) => [item.requestId, item]))
    const bodyFor = (item: (typeof observed)[number]) => JSON.stringify({
      schemaVersion: 2,
      protocol: "agent-teams-hosted-approval-v2",
      runtimeInstanceId: capability.runtimeInstanceId,
      expectedConfigGeneration: capability.configGeneration,
      requestId: item.requestId,
      sessionId: item.sessionId,
      sessionIncarnation: item.sessionIncarnation,
      requestIncarnation: item.requestIncarnation,
      expectedPermissionDigest: item.permissionDigest,
      decision: "allow_once",
    })
    const path = (id: PermissionV1.ID) => `/experimental/agent-teams/hosted-approval/session/${sessionID}/permission/${id}/reply`
    const registered = Effect.runSync(Deferred.make<void>())
    const published = Effect.runSync(Deferred.make<void>())
    const release = Effect.runSync(Deferred.make<void>())
    const listener = request.runFork(Effect.gen(function* () {
      const events = yield* EventV2Bridge.Service
      const unsubscribe = yield* events.listen((event) => {
        if (
          event.type !== Permission.Event.Replied.type ||
          (event.data as { requestID: PermissionV1.ID }).requestID !== requestID
        ) return Effect.void
        Deferred.doneUnsafe(published, Effect.void)
        return Deferred.await(release)
      })
      Deferred.doneUnsafe(registered, Effect.void)
      yield* Effect.never.pipe(Effect.ensuring(unsubscribe))
    }))
    await Effect.runPromise(Deferred.await(registered).pipe(Effect.timeout("2 seconds")))
    const firstResponse = observe(request(path(requestID), {
      method: "POST",
      headers,
      body: bodyFor(pending.get(requestID)!),
    }))
    const controller = new AbortController()
    try {
      const publication = await Promise.race([
        Effect.runPromise(Deferred.await(published)).then(() => ({ _tag: "published" as const })),
        firstResponse.then((result) => ({ _tag: "response" as const, result })),
        Bun.sleep(2_000).then(() => ({ _tag: "timeout" as const })),
      ])
      if (publication._tag === "response" && publication.result._tag === "failure") throw publication.result.error
      expect(publication._tag).toBe("published")

      const nonceBeforeB = captured?.nonces()
      const bodyRead = Promise.withResolvers<void>()
      const secondResponse = observe(request(path(requestIDB), {
        method: "POST",
        headers,
        signal: controller.signal,
        body: new ReadableStream({
          pull(stream) {
            stream.enqueue(new TextEncoder().encode(bodyFor(pending.get(requestIDB)!)))
            stream.close()
            bodyRead.resolve()
          },
        }),
        duplex: "half",
      } as RequestInit))
      try {
        const body = await Promise.race([
          bodyRead.promise.then(() => ({ _tag: "read" as const })),
          secondResponse.then((result) => ({ _tag: "response" as const, result })),
          Bun.sleep(2_000).then(() => ({ _tag: "timeout" as const })),
        ])
        if (body._tag === "response" && body.result._tag === "failure") throw body.result.error
        expect(body._tag).toBe("read")

        if (captured) {
          const deadline = Date.now() + 2_000
          while (captured.nonces() === nonceBeforeB && Date.now() < deadline) {
            const stage = await Promise.race([
              secondResponse.then((result) => ({ _tag: "response" as const, result })),
              Bun.sleep(10).then(() => ({ _tag: "waiting" as const })),
            ])
            if (stage._tag === "response" && stage.result._tag === "failure") throw stage.result.error
            expect(stage._tag).toBe("waiting")
          }
          expect(captured.nonces()).not.toBe(nonceBeforeB)
        }

        // The public API has no queue-entry receipt, so require B to remain live across a timer turn.
        const waiting = await Promise.race([
          secondResponse.then((result) => ({ _tag: "response" as const, result })),
          Bun.sleep(500).then(() => ({ _tag: "waiting" as const })),
        ])
        if (waiting._tag === "response" && waiting.result._tag === "failure") throw waiting.result.error
        expect(waiting._tag).toBe("waiting")
        controller.abort()
      } finally {
        Deferred.doneUnsafe(release, Effect.void)
        if (!controller.signal.aborted) controller.abort()
      }

      const first = await bounded(firstResponse, "A response after publication release")
      if (first._tag === "failure") throw first.error
      const second = await bounded(secondResponse, "aborted B response")
      if (second._tag === "failure") expect(controller.signal.aborted).toBe(true)
      expect(first.value.status).toBe(200)
      if (second._tag === "success") expect(second.value.status).not.toBe(200)
      expect(Exit.isSuccess(await Effect.runPromise(Fiber.await(asked).pipe(Effect.timeout("2 seconds"))))).toBe(true)
      expect(askedB.pollUnsafe()).toBeUndefined()
      const remaining = await Effect.runPromise(Fiber.join(request.runFork(
        InstanceStore.Service.use((store) => store.provide(
          { directory: dir.path },
          Permission.Service.use((permission) => permission.hostedList()),
        )),
      )).pipe(Effect.timeout("2 seconds")))
      expect(remaining.map((item) => item.request.id)).toContain(requestIDB)
      if (captured) {
        expect(captured.records.filter((item) => item.record.recordType === "conditional-reply-effect")).toHaveLength(1)
        expect(captured.records.filter((item) => item.record.recordType === "hosted-reply-raw")).toHaveLength(1)
        expect(captured.records.filter((item) => item.record.recordType === "hosted-reply")).toHaveLength(1)
        captured.producer.assertHealthy()
      }
    } finally {
      Deferred.doneUnsafe(release, Effect.void)
      if (!controller.signal.aborted) controller.abort()
      await Effect.runPromise(Fiber.interrupt(listener))
    }
  })

  test.each(["effect", "raw-write", "raw-sync", "typed-write", "typed-sync"] as const)(
    "%s capture failure after A settlement leaves B pending and unsettled without a nonce header", async (failure) => {
    await using dir = await tmpdir({ git: true, config: { formatter: false, lsp: false } })
    const captured = failure === "effect" ? capture() : capture({
      failRecord: failure.startsWith("raw") ? "hosted-reply-raw" : "hosted-reply",
      failPhase: failure.endsWith("write") ? "write" : "sync",
    })
    const request = app("secret", captured.producer)
    const headers = {
      "accept-encoding": "identity",
      authorization: auth(),
      "content-type": "application/json",
      "x-opencode-directory": dir.path,
    }
    const capability = await (await request("/experimental/agent-teams/hosted-approval-capability", { headers })).json()
    const requestID = PermissionV1.ID.make("per_hosted_capture_failure_a")
    const requestIDB = PermissionV1.ID.make("per_hosted_capture_failure_b")
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
    const askedB = request.runFork(
      InstanceStore.Service.use((store) =>
        store.provide(
          { directory: dir.path },
          Permission.Service.use((permission) => permission.ask({
            id: requestIDB,
            sessionID,
            permission: "bash",
            patterns: ["whoami"],
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
          if (body.permissions.length === 2) return body.permissions
          yield* Effect.sleep("10 millis")
        }
      }).pipe(Effect.timeout("2 seconds")),
    )
    const pending = new Map(observed.map((item: { requestId: string }) => [item.requestId, item]))
    const bodyFor = (item: (typeof observed)[number]) => JSON.stringify({
      schemaVersion: 2,
      protocol: "agent-teams-hosted-approval-v2",
      runtimeInstanceId: capability.runtimeInstanceId,
      expectedConfigGeneration: capability.configGeneration,
      requestId: item.requestId,
      sessionId: item.sessionId,
      sessionIncarnation: item.sessionIncarnation,
      requestIncarnation: item.requestIncarnation,
      expectedPermissionDigest: item.permissionDigest,
      decision: "allow_once",
    })
    const path = (id: PermissionV1.ID) => `/experimental/agent-teams/hosted-approval/session/${sessionID}/permission/${id}/reply`
    const registered = Effect.runSync(Deferred.make<void>())
    const published = Effect.runSync(Deferred.make<void>())
    const release = Effect.runSync(Deferred.make<void>())
    const listener = request.runFork(Effect.gen(function* () {
      const events = yield* EventV2Bridge.Service
      const unsubscribe = yield* events.listen((event) => {
        if (
          event.type !== Permission.Event.Replied.type ||
          (event.data as { requestID: PermissionV1.ID }).requestID !== requestID
        ) return Effect.void
        Deferred.doneUnsafe(published, Effect.void)
        return Deferred.await(release)
      })
      Deferred.doneUnsafe(registered, Effect.void)
      yield* Effect.never.pipe(Effect.ensuring(unsubscribe))
    }))
    await Effect.runPromise(Deferred.await(registered).pipe(Effect.timeout("2 seconds")))
    if (failure === "effect") captured.failEffects()
    const firstResponse = observe(request(path(requestID), {
      method: "POST",
      headers,
      body: bodyFor(pending.get(requestID)!),
    }))
    try {
      const publication = await Promise.race([
        Effect.runPromise(Deferred.await(published)).then(() => ({ _tag: "published" as const })),
        firstResponse.then((result) => ({ _tag: "response" as const, result })),
        Bun.sleep(2_000).then(() => ({ _tag: "timeout" as const })),
      ])
      if (publication._tag === "response" && publication.result._tag === "failure") throw publication.result.error
      expect(publication._tag).toBe("published")

      const nonceBeforeB = captured.nonces()
      const secondResponse = observe(request(path(requestIDB), {
        method: "POST",
        headers,
        body: bodyFor(pending.get(requestIDB)!),
      }))
      try {
        const deadline = Date.now() + 2_000
        while (captured.nonces() === nonceBeforeB && Date.now() < deadline) {
          const stage = await Promise.race([
            secondResponse.then((result) => ({ _tag: "response" as const, result })),
            Bun.sleep(10).then(() => ({ _tag: "waiting" as const })),
          ])
          if (stage._tag === "response" && stage.result._tag === "failure") throw stage.result.error
          expect(stage._tag).toBe("waiting")
        }
        expect(captured.nonces()).not.toBe(nonceBeforeB)

        const waiting = await Promise.race([
          secondResponse.then((result) => ({ _tag: "response" as const, result })),
          Bun.sleep(500).then(() => ({ _tag: "waiting" as const })),
        ])
        if (waiting._tag === "response" && waiting.result._tag === "failure") throw waiting.result.error
        expect(waiting._tag).toBe("waiting")
      } finally {
        Deferred.doneUnsafe(release, Effect.void)
      }

      const [first, second] = await bounded(Promise.all([firstResponse, secondResponse]), "failed A and B responses")
      const retry = await bounded(observe(request(path(requestID), {
        method: "POST",
        headers,
        body: bodyFor(pending.get(requestID)!),
      })), "failed A retry response")
      if (first._tag === "failure") expect(String(first.error)).toMatch(/effect capture failed|producer-provenance-fatal/)
      if (second._tag === "failure") expect(String(second.error)).toMatch(/effect capture failed|producer-provenance-fatal/)
      if (retry._tag === "failure") expect(String(retry.error)).toMatch(/effect capture failed|producer-provenance-fatal/)
      if (first._tag === "success") {
        expect(first.value.status).not.toBe(200)
        expect(first.value.headers.has(operationNonceHeader)).toBe(false)
      }
      if (second._tag === "success") {
        expect(second.value.status).not.toBe(409)
        expect(second.value.headers.has(operationNonceHeader)).toBe(false)
      }
      if (retry._tag === "success") {
        expect(retry.value.status).not.toBe(409)
        expect(retry.value.headers.has(operationNonceHeader)).toBe(false)
      }
      expect(Exit.isSuccess(await Effect.runPromise(Fiber.await(asked).pipe(Effect.timeout("2 seconds"))))).toBe(true)
      expect(askedB.pollUnsafe()).toBeUndefined()
      const remaining = await Effect.runPromise(Fiber.join(request.runFork(
        InstanceStore.Service.use((store) => store.provide(
          { directory: dir.path },
          Permission.Service.use((permission) => permission.hostedList()),
        )),
      )).pipe(Effect.timeout("2 seconds")))
      expect(remaining.map((item) => item.request.id)).toContain(requestIDB)
      expect(captured.records.filter((item) => item.record.recordType === "conditional-reply-effect")).toHaveLength(1)
      expect(captured.records.filter((item) => item.record.recordType === "hosted-reply-raw")).toHaveLength(failure.startsWith("typed") ? 1 : 0)
      expect(captured.records.filter((item) => item.record.recordType === "hosted-reply")).toHaveLength(0)
      expect(() => captured.producer.assertHealthy()).toThrow("producer-provenance-fatal")
    } finally {
      Deferred.doneUnsafe(release, Effect.void)
      await Effect.runPromise(Fiber.interrupt(listener))
    }
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
