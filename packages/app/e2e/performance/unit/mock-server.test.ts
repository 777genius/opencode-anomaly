import { expect, test } from "bun:test"
import type { Page, Route } from "@playwright/test"
import { mockOpenCodeServer, mockServerRoutePattern } from "../../utils/mock-server"

const backend = "http://127.0.0.1:4096"
const app = "http://127.0.0.1:3000"

test.each([
  "/path",
  "/project",
  "/project/current",
  "/project/proj_1",
  "/agent",
  "/vcs",
  "/session",
  "/skill",
  "/command",
  "/lsp",
  "/formatter",
  "/vcs/status",
  "/vcs/diff",
  "/global/config",
  "/config",
  "/provider/auth",
  "/mcp",
  "/experimental/resource",
  "/global/event",
  "/event",
  "/global/health",
  "/experimental/capabilities",
  "/provider",
  "/auth/provider_1",
  "/instance/dispose",
  "/permission",
  "/question",
  "/session/status",
  "/file",
  "/file/content",
  "/find/file",
  "/question/q_1/reply",
  "/question/q_1/reject",
  "/session/s_1",
  "/session/s_1/permissions/p_1",
  "/session/s_1/message/m_1",
  "/session/s_1/todo",
  "/session/s_1/children",
  "/session/s_1/diff",
  "/session/s_1/message",
  "/api/event",
  "/api/health",
  "/api/reference",
  "/api/agent",
  "/api/command",
  "/api/mcp",
  "/api/mcp/resource",
  "/api/integration/provider_1",
  "/api/integration/provider_1/connect/key",
  "/api/project",
  "/api/project/current",
  "/api/project/proj_1",
  "/api/path",
  "/api/permission/request",
  "/api/question/request",
  "/api/vcs",
  "/api/vcs/status",
  "/api/vcs/diff",
  "/api/pty/shells",
  "/api/pty/pty_1/connect-token",
  "/api/session",
  "/api/session/active",
  "/api/session/s_1",
  "/api/session/s_1/message",
  "/api/session/s_1/shell",
  "/api/session/s_1/question/q_1/reply",
  "/api/session/s_1/question/q_1/reject",
  "/api/session/s_1/permission/p_1/reply",
  "/api/session/s_1/archive",
  "/api/session/s_1/rename",
  "/api/session/s_1/interrupt",
  "/api/session/s_1/revert/clear",
  "/api/session/s_1/revert/commit",
  "/session/s%2F1/message/m%3F1",
])("registers the supported fixture path %s on both origins with queries", (path) => {
  const pattern = mockServerRoutePattern(backend, app)
  const queries = ["", "?", "?directory=C%3A%5COpenCode&limit=50&cursor=cursor_1", "?path=src/worker.js&raw"]
  ;[backend, app].forEach((origin) => {
    queries.forEach((query) => expect(pattern.test(`${origin}${path}${query}`)).toBe(true))
  })
})

test.each([
  "/",
  "/index.html",
  "/QzovT3BlbkNvZGU/session/s_1",
  "/settings",
  "/@vite/client",
  "/@react-refresh",
  "/@id/__x00__virtual:module",
  "/src/entry.tsx",
  "/src/context/session.tsx?t=123",
  "/src/pierre/worker.ts?worker_file&type=module",
  "/@fs/C:/work/node_modules/@pierre/diffs/dist/worker/worker.js?worker_file&type=module",
  "/node_modules/.vite/deps/shiki.js?v=123",
  "/node_modules/@shikijs/engine-oniguruma/dist/wasm-inlined.mjs",
  "/node_modules/hast-util-to-html/lib/index.js",
  "/assets/onig.wasm",
  "/assets/app.css",
  "/fonts/Inter.woff2",
  "/assets/logo.svg",
  "/favicon.ico",
  "/image.png",
  "/photo.webp",
  "/src/api/session.ts",
  "/@fs/C:/work/src/session/message.ts?url=/api/session",
  "/?next=/api/session",
  "/worker.js?path=/global/event",
  "/api.js",
  "/api/worker.js",
  "/api/session-extra",
  "/api/session/s_1/message/worker.js",
  "/api/unknown",
  "/global/event/worker.js",
  "/file/content/worker.js",
  "/find/file.js",
  "/provider/auth/worker.js",
  "/session/s_1/message/m_1/worker.js",
  "/unknown",
])("does not register app documents or static/non-API URL %s", (path) => {
  expect(mockServerRoutePattern(backend, app).test(`${app}${path}`)).toBe(false)
  expect(mockServerRoutePattern(app, app).test(`${app}${path}`)).toBe(false)
})

test.each([
  "http://localhost:4096/api/session",
  "http://other.example:4096/api/session",
  "http://other.example:3000/api/session",
  "http://127.0.0.1:40960/api/session",
  "http://127.0.0.1:3001/api/session",
  "https://127.0.0.1:4096/api/session",
  "https://127.0.0.1:3000/api/session",
  "http://127.0.0.1:4096.other.example/api/session",
])("does not match a different origin %s", (url) => {
  expect(mockServerRoutePattern(backend, app).test(url)).toBe(false)
})

test("uses canonical configured origins including default ports, paths and IPv6", () => {
  const pattern = mockServerRoutePattern("http://[::1]:80/backend/", "https://app.example:443/ui/?query=1")
  expect(pattern.test(new URL("http://[::1]:80/unknown?x=1").href)).toBe(true)
  expect(pattern.test(new URL("https://app.example:443/api/session?limit=50").href)).toBe(true)
  expect(pattern.test("https://appXexample/api/session")).toBe(false)
  expect(pattern.test("https://app.example:444/api/session")).toBe(false)
  expect(pattern.test("https://app.example/ui/api/session")).toBe(false)
  expect(pattern.test("http://[::2]/api/session")).toBe(false)
})

test("retains unknown backend paths, including extension-like paths, without widening the app origin", () => {
  const pattern = mockServerRoutePattern(backend, app)
  expect(pattern.test(`${backend}/unknown?path=worker.js`)).toBe(true)
  expect(pattern.test(`${backend}/unknown.js`)).toBe(true)
  expect(pattern.test(`${app}/unknown?path=worker.js`)).toBe(false)
  const sharedPort = mockServerRoutePattern("http://backend.example:3000", app)
  expect(sharedPort.test("http://backend.example:3000/unknown")).toBe(true)
  expect(sharedPort.test(`${app}/src/worker.ts?worker_file&type=module`)).toBe(false)
  expect(mockServerRoutePattern(app, app).test(`${app}/api/session/s_1/message`)).toBe(true)
})

test("registers a native RegExp and retains the existing unknown-backend dispatch", async () => {
  const routes: { pattern: RegExp; handler: (route: Route) => Promise<void> }[] = []
  await mockOpenCodeServer(
    {
      route: (pattern: RegExp, handler: (route: Route) => Promise<void>) => {
        routes.push({ pattern, handler })
        return Promise.resolve()
      },
    } as unknown as Page,
    {
      provider: {},
      directory: "C:/OpenCode",
      project: {},
      sessions: [],
      pageMessages: () => ({ items: [] }),
    },
  )
  const serverPort = process.env.PLAYWRIGHT_SERVER_PORT ?? "4096"
  const server = `http://${process.env.PLAYWRIGHT_SERVER_HOST ?? "127.0.0.1"}:${serverPort}`
  const frontend = process.env.PLAYWRIGHT_BASE_URL ?? `http://127.0.0.1:${process.env.PLAYWRIGHT_PORT ?? "3000"}`
  expect(routes).toHaveLength(1)
  expect(routes[0]!.pattern).toBeInstanceOf(RegExp)
  expect(routes[0]!.pattern.source).toBe(mockServerRoutePattern(server, frontend).source)
  expect(routes[0]!.pattern.test(`${new URL(frontend).origin}/@vite/client`)).toBe(false)
  const responses: unknown[] = []
  const url = new URL("/unknown-backend-request", server)
  await routes[0]!.handler({
    request: () => ({ url: () => url.href, method: () => "GET" }),
    fulfill: (response: unknown) => {
      responses.push(response)
      return Promise.resolve()
    },
    fallback: () => {
      responses.push("fallback")
      return Promise.resolve()
    },
  } as unknown as Route)
  // Preserve the pre-existing port-based final dispatch, including shared-port fallback.
  if (url.port !== serverPort || serverPort === new URL(frontend).port) {
    expect(responses).toEqual(["fallback"])
    return
  }
  expect(responses).toHaveLength(1)
  expect(responses[0]).toMatchObject({ status: 200, contentType: "application/json", body: "{}" })
})

test("applies message latency after a list response gate is released", async () => {
  const events: string[] = []
  const gate = Promise.withResolvers<void>()
  let handler: ((route: Route) => Promise<void>) | undefined
  const page = {
    route: (_url: string, callback: (route: Route) => Promise<void>) => {
      handler = callback
      return Promise.resolve()
    },
  } as unknown as Page
  await mockOpenCodeServer(page, {
    provider: {},
    directory: "C:/OpenCode",
    project: {},
    sessions: [{ id: "session" }],
    messageDelay: 25,
    beforeMessagesResponse: () => {
      events.push("before")
      return gate.promise
    },
    onMessages: (request) => events.push(request.phase),
    pageMessages: () => {
      events.push("page")
      return { items: [] }
    },
  })

  const response = handler!({
    request: () => ({ url: () => "http://127.0.0.1:4096/session/session/message" }),
    fulfill: () => {
      events.push("fulfill")
      return Promise.resolve()
    },
  } as unknown as Route)
  expect(events).toEqual(["start", "before"])

  const released = performance.now()
  gate.resolve()
  await response
  expect(performance.now() - released).toBeGreaterThanOrEqual(20)
  expect(events).toEqual(["start", "before", "page", "end", "fulfill"])
})
