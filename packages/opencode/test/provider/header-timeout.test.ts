import { afterEach, expect } from "bun:test"
import { createServer, type Server } from "node:http"
import { streamText } from "ai"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Effect } from "effect"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { disposeAllInstances, provideTmpdirInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { testProviderConfig } from "../lib/test-provider"
import { Env } from "@/env"
import { Plugin } from "@/plugin"
import { Provider } from "@/provider/provider"
import { ProviderError } from "@/provider/error"

afterEach(async () => {
  await disposeAllInstances()
})

const it = testEffect(
  LayerNode.compile(LayerNode.group([Provider.node, Env.node, Plugin.node, CrossSpawnSpawner.node])),
)

it.live("headerTimeout does not abort delayed SSE body after headers arrive", () =>
  Effect.gen(function* () {
    const server = yield* Effect.acquireRelease(
      Effect.promise(() => delayedBodyServer(1_000)),
      (server) => Effect.sync(() => server.server.close()),
    )

    yield* provideTmpdirInstance(
      () =>
        Effect.gen(function* () {
          const provider = yield* Provider.Service
          const model = yield* provider.getModel(ProviderV2.ID.make("test"), ModelV2.ID.make("test-model"))
          const result = streamText({
            model: yield* provider.getLanguage(model),
            messages: [{ role: "user", content: "hello" }],
          })

          expect(yield* Effect.promise(() => result.text)).toBe("late")
        }),
      { config: providerConfig(server.url, { headerTimeout: 500 }) },
    )
  }),
)

it.live("chunkTimeout raises a response stream error when SSE body stalls", () =>
  Effect.gen(function* () {
    const server = yield* Effect.acquireRelease(
      Effect.promise(() => delayedBodyServer(250)),
      (server) => Effect.sync(() => server.server.close()),
    )

    yield* provideTmpdirInstance(
      () =>
        Effect.gen(function* () {
          const provider = yield* Provider.Service
          const model = yield* provider.getModel(ProviderV2.ID.make("test"), ModelV2.ID.make("test-model"))
          const result = streamText({
            model: yield* provider.getLanguage(model),
            onError() {},
            messages: [{ role: "user", content: "hello" }],
          })

          const errors = yield* Effect.promise(async () => {
            const errors: unknown[] = []
            try {
              for await (const part of result.fullStream) {
                if (part.type === "error") errors.push(part.error)
              }
            } catch (error) {
              errors.push(error)
            }
            return errors
          })
          expect(errors).toHaveLength(1)
          expect(errors[0]).toBeInstanceOf(ProviderError.ResponseStreamError)
          expect(errors[0]).toHaveProperty("message", "SSE read timed out")
        }),
      { config: providerConfig(server.url, { chunkTimeout: 50 }) },
    )
  }),
)

it.live("SSE timeout preserves its cause when abort errors the upstream reader", () =>
  Effect.promise(async () => {
    const ctl = new AbortController()
    const upstream = new Error("upstream aborted")
    const source = new ReadableStream<Uint8Array>({
      start(ctrl) {
        ctl.signal.addEventListener("abort", () => ctrl.error(upstream), { once: true })
      },
    })
    const reader = Provider.wrapSSE(
      new Response(source, { headers: { "content-type": "text/event-stream" } }),
      10,
      ctl,
    ).body!.getReader()

    const error = await reader.read().catch((error) => error)
    expect(error).toBeInstanceOf(ProviderError.ResponseStreamError)
    expect(error.message).toBe("SSE read timed out")
    expect(ctl.signal.reason).toBe(error)
    expect(await reader.read().catch((error) => error)).toBe(error)
  }),
)

it.live("SSE timeout handles rejecting cancellation without replacing the stream error", () =>
  Effect.promise(async () => {
    const ctl = new AbortController()
    const reasons: unknown[] = []
    const source = new ReadableStream<Uint8Array>({
      cancel(reason) {
        reasons.push(reason)
        return Promise.reject(new Error("cleanup failed"))
      },
    })
    const reader = Provider.wrapSSE(
      new Response(source, { headers: { "content-type": "text/event-stream" } }),
      10,
      ctl,
    ).body!.getReader()

    const error = await reader.read().catch((error) => error)
    expect(error).toBeInstanceOf(ProviderError.ResponseStreamError)
    expect(reasons).toEqual([error])
    expect(ctl.signal.reason).toBe(error)
    expect(await reader.read().catch((error) => error)).toBe(error)
  }),
)

it.live("SSE explicit cancellation preserves cancellation failure during a pending read", () =>
  Effect.promise(async () => {
    const ctl = new AbortController()
    const reason = new Error("consumer stopped")
    const failure = new Error("cancel failed")
    const started = Promise.withResolvers<void>()
    const source = new ReadableStream<Uint8Array>({
      pull() {
        started.resolve()
      },
      cancel(value) {
        expect(value).toBe(reason)
        return Promise.reject(failure)
      },
    })
    const reader = Provider.wrapSSE(
      new Response(source, { headers: { "content-type": "text/event-stream" } }),
      1_000,
      ctl,
    ).body!.getReader()
    const pending = reader.read()
    await started.promise

    expect(await reader.cancel(reason).catch((error) => error)).toBe(failure)
    expect(await pending).toEqual({ done: true, value: undefined })
    expect(ctl.signal.reason).toBe(reason)
  }),
)

it.live("SSE forwards upstream read failure without turning it into a timeout", () =>
  Effect.promise(async () => {
    const ctl = new AbortController()
    const failure = new Error("upstream failed")
    const source = new ReadableStream<Uint8Array>({
      pull(ctrl) {
        ctrl.error(failure)
      },
    })
    const reader = Provider.wrapSSE(
      new Response(source, { headers: { "content-type": "text/event-stream" } }),
      1_000,
      ctl,
    ).body!.getReader()

    expect(await reader.read().catch((error) => error)).toBe(failure)
    expect(ctl.signal.aborted).toBe(false)
  }),
)

it.live("headerTimeout aborts when response headers do not arrive", () =>
  Effect.gen(function* () {
    const server = yield* Effect.acquireRelease(
      Effect.promise(() => delayedHeaderServer(250)),
      (server) => Effect.sync(() => server.server.close()),
    )

    yield* provideTmpdirInstance(
      () =>
        Effect.gen(function* () {
          const provider = yield* Provider.Service
          const model = yield* provider.getModel(ProviderV2.ID.make("test"), ModelV2.ID.make("test-model"))
          const result = streamText({
            model: yield* provider.getLanguage(model),
            onError() {},
            messages: [{ role: "user", content: "hello" }],
          })

          const errors = yield* Effect.promise(async () => {
            const errors: string[] = []
            for await (const part of result.fullStream) {
              if (part.type === "error") errors.push(String(part.error))
            }
            return errors
          })
          expect(errors.join("\n")).toContain("response headers timed out")
        }),
      { config: providerConfig(server.url, { headerTimeout: 50 }) },
    )
  }),
)

it.live("headerTimeout is opt-in for non-OpenAI providers", () =>
  Effect.gen(function* () {
    const server = yield* Effect.acquireRelease(
      Effect.promise(() => delayedHeaderServer(100)),
      (server) => Effect.sync(() => server.server.close()),
    )

    yield* provideTmpdirInstance(
      () =>
        Effect.gen(function* () {
          const provider = yield* Provider.Service
          const model = yield* provider.getModel(ProviderV2.ID.make("test"), ModelV2.ID.make("test-model"))
          const result = streamText({
            model: yield* provider.getLanguage(model),
            messages: [{ role: "user", content: "hello" }],
          })

          expect(yield* Effect.promise(() => result.text)).toBe("ok")
        }),
      { config: providerConfig(server.url) },
    )
  }),
)

it.live("OpenAI Codex headerTimeout default can be disabled by config", () =>
  Effect.gen(function* () {
    yield* withAuthContent(
      Effect.gen(function* () {
        yield* provideTmpdirInstance(
          () =>
            Effect.gen(function* () {
              const provider = yield* Provider.Service
              const openai = yield* provider.getProvider(ProviderV2.ID.openai)
              expect(openai.options.headerTimeout).toBe(false)
            }),
          { config: { provider: { openai: { options: { headerTimeout: false } } } } },
        )
      }),
    )
  }),
)

it.live("OpenAI API auth gets default headerTimeout", () =>
  Effect.gen(function* () {
    yield* withAuthContent(
      Effect.gen(function* () {
        yield* provideTmpdirInstance(() =>
          Effect.gen(function* () {
            const provider = yield* Provider.Service
            const openai = yield* provider.getProvider(ProviderV2.ID.openai)
            expect(openai.options.headerTimeout).toBe(300_000)
          }),
        )
      }),
      { openai: { type: "api", key: "sk-test" } },
    )
  }),
)

function providerConfig(url: string, options: Record<string, unknown> = {}) {
  const config = testProviderConfig(url)
  return {
    ...config,
    provider: {
      test: {
        ...config.provider.test,
        options: { ...config.provider.test.options, ...options },
      },
    },
  }
}

async function delayedHeaderServer(delay: number): Promise<{ server: Server; url: string }> {
  const server = createServer((_, res) => {
    setTimeout(() => {
      res.writeHead(200, { "content-type": "text/event-stream" })
      res.end('data: {"choices":[{"delta":{"content":"ok"}}]}\n\ndata: [DONE]\n\n')
    }, delay)
  })
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve))
  const address = server.address()
  if (!address || typeof address === "string") throw new Error("server did not bind to a TCP port")
  return { server, url: `http://127.0.0.1:${address.port}` }
}

async function delayedBodyServer(delay: number): Promise<{ server: Server; url: string }> {
  const server = createServer((_, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" })
    res.flushHeaders()
    setTimeout(() => {
      res.end('data: {"choices":[{"delta":{"content":"late"}}]}\n\ndata: [DONE]\n\n')
    }, delay)
  })
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve))
  const address = server.address()
  if (!address || typeof address === "string") throw new Error("server did not bind to a TCP port")
  return { server, url: `http://127.0.0.1:${address.port}` }
}

function withAuthContent<A, E, R>(self: Effect.Effect<A, E, R>, value: Record<string, unknown> = defaultAuthContent()) {
  return Effect.acquireUseRelease(
    Effect.sync(() => {
      const previous = process.env.OPENCODE_AUTH_CONTENT
      process.env.OPENCODE_AUTH_CONTENT = JSON.stringify(value)
      return previous
    }),
    () => self,
    (previous) =>
      Effect.sync(() => {
        if (previous === undefined) delete process.env.OPENCODE_AUTH_CONTENT
        else process.env.OPENCODE_AUTH_CONTENT = previous
      }),
  )
}

function defaultAuthContent() {
  return {
    openai: { type: "oauth", refresh: "refresh", access: "access", expires: Date.now() + 60_000 },
  }
}
