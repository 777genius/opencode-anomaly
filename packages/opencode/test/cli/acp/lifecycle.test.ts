import { describe, expect } from "bun:test"
import type {
  CloseSessionResponse,
  ListSessionsResponse,
  LoadSessionResponse,
  ResumeSessionResponse,
} from "@agentclientprotocol/sdk"
import { Cause, Deferred, Duration, Effect, Exit, Fiber, Queue } from "effect"
import { TestClock } from "effect/testing"
import { cliIt, type CliFixture } from "../../lib/cli-process"
import { it } from "../../lib/effect"
import { createAcpClient, expectOk, selectConfigOption } from "./acp-test-client"
import { initialize, newSession, verifierConfig } from "./helpers"

describe("opencode acp lifecycle subprocess", () => {
  cliIt.live(
    "stdin EOF exits cleanly",
    ({ opencode }) =>
      Effect.gen(function* () {
        const acp = yield* opencode.acp()
        acp.close()

        const code = yield* acp.exitAfterStartup
        expect(code).toBe(0)
      }),
    60_000,
  )

  cliIt.live(
    "stdin EOF after initialize exits cleanly",
    ({ opencode }) =>
      Effect.gen(function* () {
        const acp = yield* opencode.acp()
        yield* acp.ready
        yield* acp.send({
          jsonrpc: "2.0",
          id: 1,
          method: "initialize",
          params: { protocolVersion: 1, clientCapabilities: {} },
        })
        expect(yield* acp.receive.pipe(Effect.timeout(Duration.seconds(15)))).toMatchObject({
          jsonrpc: "2.0",
          id: 1,
          result: { protocolVersion: 1 },
        })
        acp.close()
        const code = yield* Effect.promise(() => acp.exited).pipe(
          Effect.timeoutOrElse({
            duration: Duration.seconds(5),
            orElse: () => Effect.fail(new Error(`Post-initialize EOF exit deadline exceeded\n${acp.diagnostics()}`)),
          }),
        )
        expect(code).toBe(0)
      }),
    60_000,
  )

  cliIt.live(
    "close capability and close request",
    ({ home, llm, opencode }) =>
      Effect.gen(function* () {
        const acp = yield* createLifecycleClient(
          { opencode },
          { OPENCODE_CONFIG_CONTENT: JSON.stringify(verifierConfig(llm.url)) },
        )
        const initialized = yield* initialize(acp)
        expect(initialized.agentCapabilities?.sessionCapabilities?.close).toEqual({})

        const session = yield* newSession(acp, home)
        expectOk(yield* acp.request<CloseSessionResponse>("session/close", { sessionId: session.sessionId }))
      }),
    60_000,
  )

  cliIt.live(
    "loadSession capability and load request return session config options",
    ({ home, llm, opencode }) =>
      Effect.gen(function* () {
        const acp = yield* createLifecycleClient(
          { opencode },
          { OPENCODE_CONFIG_CONTENT: JSON.stringify(verifierConfig(llm.url)) },
        )
        const initialized = yield* initialize(acp)
        expect(initialized.agentCapabilities?.loadSession).toBe(true)
        const session = yield* newSession(acp, home)
        const loaded = expectOk(
          yield* acp.request<LoadSessionResponse>("session/load", {
            cwd: home,
            sessionId: session.sessionId,
            mcpServers: [],
          }),
        )

        expect(selectConfigOption(loaded.configOptions, "model")?.category).toBe("model")
      }),
    60_000,
  )

  cliIt.live(
    "list request includes a live ACP-created session",
    ({ home, llm, opencode }) =>
      Effect.gen(function* () {
        const acp = yield* createLifecycleClient(
          { opencode },
          { OPENCODE_CONFIG_CONTENT: JSON.stringify(verifierConfig(llm.url)) },
        )
        yield* initialize(acp)
        const session = yield* newSession(acp, home)
        const listed = expectOk(yield* acp.request<ListSessionsResponse>("session/list", { cwd: home }))

        expect(listed.sessions.some((item) => item.sessionId === session.sessionId)).toBe(true)
      }),
    60_000,
  )

  cliIt.live(
    "resume capability advertisement",
    ({ opencode }) =>
      Effect.gen(function* () {
        const initialized = yield* initialize(yield* createLifecycleClient({ opencode }))

        expect(initialized.agentCapabilities?.sessionCapabilities?.resume).toEqual({})
      }),
    60_000,
  )

  cliIt.live(
    "resume request returns session config options",
    ({ home, llm, opencode }) =>
      Effect.gen(function* () {
        const acp = yield* createLifecycleClient(
          { opencode },
          { OPENCODE_CONFIG_CONTENT: JSON.stringify(verifierConfig(llm.url)) },
        )
        yield* initialize(acp)
        const session = yield* newSession(acp, home)
        const resumed = expectOk(
          yield* acp.request<ResumeSessionResponse>("session/resume", {
            cwd: home,
            sessionId: session.sessionId,
            mcpServers: [],
          }),
        )

        expect(selectConfigOption(resumed.configOptions, "model")?.category).toBe("model")
      }),
    60_000,
  )
})

// Lifecycle RPC deadlines begin at transport readiness. Keep this local so the
// raw harness and other tests can still exercise requests/EOF during startup.
function createLifecycleClient(input: { opencode: Pick<CliFixture["opencode"], "acp"> }, env?: Record<string, string>) {
  return Effect.gen(function* () {
    const acp = yield* input.opencode.acp(env ? { env } : undefined)
    yield* acp.ready
    const client = createAcpClient(acp)
    return {
      ...client,
      request: <T>(method: string, params?: unknown) =>
        client.request<T>(method, params).pipe(Effect.timeout(Duration.seconds(15))),
    }
  })
}

for (const answered of [false, true]) {
  it.effect(`ACP lifecycle total RPC deadline with notifications: answered=${answered}`, () =>
    Effect.gen(function* () {
      const ready = yield* Deferred.make<void>()
      const sent = yield* Deferred.make<object>()
      const received = yield* Deferred.make<void>()
      const responses = yield* Queue.unbounded<unknown>()
      const creating = yield* createLifecycleClient({
        opencode: {
          acp: () =>
            Effect.succeed({
              ready: Deferred.await(ready),
              exitAfterStartup: Effect.succeed(0),
              send: (message: object) => Deferred.succeed(sent, message).pipe(Effect.asVoid),
              receive: Queue.take(responses).pipe(Effect.tap(() => Deferred.succeed(received, undefined))),
              close: () => {},
              exited: Promise.resolve(0),
              diagnostics: () => "in-memory lifecycle transport",
            }),
        },
      }).pipe(Effect.forkChild({ startImmediately: true }))
      yield* TestClock.adjust("20 seconds")
      expect(creating.pollUnsafe()).toBeUndefined()
      expect(yield* Deferred.isDone(sent)).toBe(false)
      yield* Deferred.succeed(ready, undefined)
      const client = yield* Fiber.join(creating)
      const waiting = yield* client
        .request("initialize")
        .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
      expect(yield* Deferred.await(sent)).toMatchObject({ id: 1, method: "initialize" })
      yield* TestClock.adjust("14 seconds")
      yield* Queue.offer(responses, { jsonrpc: "2.0", method: "session/update", params: {} })
      yield* Deferred.await(received)
      yield* TestClock.adjust("999 millis")
      expect(waiting.pollUnsafe()).toBeUndefined()
      if (answered) {
        const response = { jsonrpc: "2.0", id: 1, result: { protocolVersion: 1 } }
        yield* Queue.offer(responses, response)
        expect(yield* Fiber.join(waiting)).toEqual(Exit.succeed(response))
        return
      }
      yield* TestClock.adjust("1 milli")
      const result = yield* Fiber.join(waiting)
      expect(Exit.isFailure(result)).toBe(true)
      if (Exit.isFailure(result)) {
        expect(Cause.squash(result.cause)).toMatchObject({ _tag: "TimeoutError" })
      }
    }),
  )
}
