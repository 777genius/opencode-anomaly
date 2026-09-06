import { expect } from "bun:test"
import { Duration, Effect, Fiber } from "effect"
import { once } from "node:events"
import { PassThrough } from "node:stream"
import { waitForStdinEnd } from "../../../src/cli/acp-stdin"
import { it, pollWithTimeout } from "../../lib/effect"

for (const before of [true, false]) {
  it.live(
    before ? "ACP waits on already-ended stdin" : "ACP retains EOF before executing the wait",
    () =>
      Effect.gen(function* () {
        const stdin = new PassThrough()
        yield* Effect.addFinalizer(() => Effect.sync(() => stdin.destroy()))
        const wait = before ? undefined : waitForStdinEnd(stdin)
        const ended = once(stdin, "end")
        stdin.resume()
        stdin.end()
        yield* Effect.promise(() => ended)
        expect(stdin.readableEnded).toBe(true)
        yield* (wait ?? waitForStdinEnd(stdin))
        expect(stdin.listenerCount("end")).toBe(0)
        expect(stdin.listenerCount("error")).toBe(0)
      }).pipe(Effect.timeout(Duration.seconds(1))),
    5_000,
  )
}

it.live(
  "ACP waits for future EOF and removes completion listeners",
  () =>
    Effect.gen(function* () {
      const stdin = new PassThrough()
      yield* Effect.addFinalizer(() => Effect.sync(() => stdin.destroy()))
      const waiting = yield* waitForStdinEnd(stdin).pipe(Effect.forkChild)
      yield* pollWithTimeout(
        Effect.sync(() => (stdin.listenerCount("end") === 1 && stdin.listenerCount("error") === 1 ? true : undefined)),
        "ACP did not subscribe to stdin",
      )
      stdin.resume()
      stdin.end()
      yield* Fiber.join(waiting)
      expect(stdin.readableEnded).toBe(true)
      expect(stdin.listenerCount("end")).toBe(0)
      expect(stdin.listenerCount("error")).toBe(0)
    }).pipe(Effect.timeout(Duration.seconds(1))),
  5_000,
)
