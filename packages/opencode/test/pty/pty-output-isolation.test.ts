import { describe, expect, test } from "bun:test"
import { AppRuntime } from "../../src/effect/app-runtime"
import { Effect } from "effect"
import { Instance } from "../../src/project/instance"
import { Pty } from "../../src/pty"
import { tmpdir } from "../fixture/fixture"

const echo = {
  command: process.execPath,
  args: [
    "-e",
    'process.stdout.write("READY"); process.stdin.setRawMode?.(true); process.stdin.resume(); process.stdin.on("data", (chunk) => process.stdout.write(chunk))',
  ],
}

async function waitFor(output: string[], text: string) {
  const deadline = Date.now() + 5_000
  while (!output.join("").includes(text)) {
    if (Date.now() >= deadline) throw new Error(`Timed out waiting for PTY output: ${text}`)
    await Bun.sleep(10)
  }
}

function send(output: string[]) {
  return (data: unknown) => {
    output.push(typeof data === "string" ? data : Buffer.from(data as Uint8Array).toString("utf8"))
  }
}

describe("pty", () => {
  test("does not leak output when websocket objects are reused", async () => {
    await using dir = await tmpdir()

    await Instance.provide({
      directory: dir.path,
      fn: () =>
        AppRuntime.runPromise(
          Effect.gen(function* () {
            const pty = yield* Pty.Service
            const a = yield* pty.create({ ...echo, title: "a" })
            const b = yield* pty.create({ ...echo, title: "b" })
            try {
              const outA: string[] = []
              const outB: string[] = []

              yield* pty.connect(a.id, {
                readyState: 1,
                data: { events: { connection: "source-a" } },
                send: send(outA),
                close() {},
              } as any)
              yield* Effect.promise(() => waitFor(outA, "READY"))

              const ws = {
                readyState: 1,
                data: { events: { connection: "a" } },
                send: (_data: unknown) => {},
                close: () => {
                  // no-op (simulate abrupt drop)
                },
              }

              yield* pty.connect(a.id, ws as any)

              ws.data = { events: { connection: "b" } }
              ws.send = send(outB)
              yield* pty.connect(b.id, ws as any)
              yield* Effect.promise(() => waitFor(outB, "READY"))

              outA.length = 0
              outB.length = 0

              yield* pty.write(a.id, "SOURCE_A")
              yield* Effect.promise(() => waitFor(outA, "SOURCE_A"))
              yield* pty.write(b.id, "SOURCE_B")
              yield* Effect.promise(() => waitFor(outB, "SOURCE_B"))

              expect(outA.join("")).toContain("SOURCE_A")
              expect(outB.join("")).not.toContain("SOURCE_A")
            } finally {
              yield* pty.remove(a.id)
              yield* pty.remove(b.id)
            }
          }),
        ),
    })
  })

  test("does not leak output when Bun recycles websocket objects before re-connect", async () => {
    await using dir = await tmpdir({ git: true })

    await Instance.provide({
      directory: dir.path,
      fn: () =>
        AppRuntime.runPromise(
          Effect.gen(function* () {
            const pty = yield* Pty.Service
            const a = yield* pty.create({ ...echo, title: "a" })
            try {
              const outA: string[] = []
              const outB: string[] = []

              yield* pty.connect(a.id, {
                readyState: 1,
                data: { events: { connection: "source-a" } },
                send: send(outA),
                close() {},
              } as any)
              yield* Effect.promise(() => waitFor(outA, "READY"))

              const ws = {
                readyState: 1,
                data: { events: { connection: "a" } },
                send: (_data: unknown) => {},
                close: () => {
                  // no-op (simulate abrupt drop)
                },
              }

              yield* pty.connect(a.id, ws as any)
              outA.length = 0

              ws.data = { events: { connection: "b" } }
              ws.send = send(outB)

              yield* pty.write(a.id, "SOURCE_A")
              yield* Effect.promise(() => waitFor(outA, "SOURCE_A"))

              expect(outA.join("")).toContain("SOURCE_A")
              expect(outB.join("")).not.toContain("SOURCE_A")
            } finally {
              yield* pty.remove(a.id)
            }
          }),
        ),
    })
  })

  test("treats in-place socket data mutation as the same connection", async () => {
    await using dir = await tmpdir({ git: true })

    await Instance.provide({
      directory: dir.path,
      fn: () =>
        AppRuntime.runPromise(
          Effect.gen(function* () {
            const pty = yield* Pty.Service
            const a = yield* pty.create({ ...echo, title: "a" })
            try {
              const out: string[] = []

              const ctx = { connId: 1 }
              const ws = {
                readyState: 1,
                data: ctx,
                send: (data: unknown) => {
                  out.push(typeof data === "string" ? data : Buffer.from(data as Uint8Array).toString("utf8"))
                },
                close: () => {
                  // no-op
                },
              }

              yield* pty.connect(a.id, ws as any)
              yield* Effect.promise(() => waitFor(out, "READY"))
              out.length = 0

              ctx.connId = 2

              yield* pty.write(a.id, "SOURCE_A")
              yield* Effect.promise(() => waitFor(out, "SOURCE_A"))

              expect(out.join("")).toContain("SOURCE_A")
            } finally {
              yield* pty.remove(a.id)
            }
          }),
        ),
    })
  })
})
