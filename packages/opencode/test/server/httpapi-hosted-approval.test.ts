import { afterEach, describe, expect, test } from "bun:test"
import { ConfigProvider, Layer } from "effect"
import { HttpRouter } from "effect/unstable/http"
import { HttpApiApp } from "../../src/server/routes/instance/httpapi/server"
import { ServerAuth } from "../../src/server/auth"
import { disposeAllInstances, tmpdir } from "../fixture/fixture"
import { resetDatabase } from "../fixture/db"

function app(password?: string) {
  const handler = HttpRouter.toWebHandler(
    HttpApiApp.routes.pipe(
      Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ OPENCODE_SERVER_PASSWORD: password }))),
    ),
    { disableLogger: true },
  ).handler
  return (path: string, init?: RequestInit) =>
    handler(new Request(new URL(path, "http://localhost"), init), HttpApiApp.context)
}

function auth(password = "secret") {
  return ServerAuth.header({ username: "opencode", password }) ?? ""
}

afterEach(async () => {
  await disposeAllInstances()
  await resetDatabase()
})

describe("hosted approval v2 HttpApi", () => {
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
    const request = app("secret")
    const headers = {
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
      requestIncarnation: `request_incarnation_${"1".repeat(32)}`,
      expectedPermissionDigest: "2".repeat(64),
      decision: "allow_once",
    }
    const post = (body: BodyInit) => request(path, { method: "POST", headers, body })
    expect((await post("{" )).status).toBe(400)
    expect((await post('{"schemaVersion":2,"schemaVersion":2}')).status).toBe(400)
    expect((await post("x".repeat(16 * 1024 + 1))).status).toBe(400)
    expect((await post(JSON.stringify(valid))).status).toBe(409)
    expect((await post(JSON.stringify({ ...valid, runtimeInstanceId: `runtime_instance_${"f".repeat(32)}` }))).status).toBe(412)
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
