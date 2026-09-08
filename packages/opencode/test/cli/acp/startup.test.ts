import { expect } from "bun:test"
import { Clock, Deferred, Effect, Fiber } from "effect"
import { TestClock } from "effect/testing"
import { trackAcpStartup } from "../../lib/acp-startup"
import { it } from "../../lib/effect"

const marker = "[acp-profile] cli.acp.stdin.wait.mark 5045ms ended=false\n"

it.effect("ACP with no startup event exhausts 30s from spawn and scope closure cancels the exit observer", () =>
  Effect.gen(function* () {
    const stopped = yield* Deferred.make<void>()
    yield* Effect.gen(function* () {
      const startup = yield* trackAcpStartup({
        started: yield* Clock.currentTimeMillis,
        exited: Effect.never.pipe(Effect.ensuring(Deferred.succeed(stopped, undefined))),
        exitCode: () => null,
      })
      yield* startup.observe("startup stalled\n")
      // Delaying the first wait must not grant another 30s.
      yield* TestClock.adjust("8 seconds")
      const waiting = yield* startup.exitAfterStartup.pipe(Effect.flip, Effect.forkChild)
      yield* TestClock.adjust("21999 millis")
      expect(waiting.pollUnsafe()).toBeUndefined()
      yield* TestClock.adjust("1 milli")
      const error = yield* Fiber.join(waiting)
      expect(error.message).toContain("ACP startup deadline exceeded (30000ms)")
      expect(error.message).toContain("startup stalled")
      expect(yield* Deferred.isDone(stopped)).toBe(false)
    }).pipe(Effect.scoped)
    expect(yield* Deferred.isDone(stopped)).toBe(true)
  }),
)

it.effect("ACP immediate EOF allows cold startup but only 5s after the observed ready marker", () =>
  Effect.gen(function* () {
    const exited = yield* Deferred.make<number>()
    const startup = yield* trackAcpStartup({
      started: yield* Clock.currentTimeMillis,
      exited: Deferred.await(exited),
      exitCode: () => null,
    })
    const waiting = yield* startup.exitAfterStartup.pipe(Effect.flip, Effect.forkChild)
    yield* TestClock.adjust("8 seconds")
    expect(waiting.pollUnsafe()).toBeUndefined()
    yield* startup.observe(marker)
    yield* TestClock.adjust("4999 millis")
    expect(waiting.pollUnsafe()).toBeUndefined()
    yield* TestClock.adjust("1 milli")
    expect((yield* Fiber.join(waiting)).message).toContain("5000ms after readiness")
  }),
)

it.effect("ACP readiness rejects unrelated profile events and markers beyond the startup budget", () =>
  Effect.gen(function* () {
    const startup = yield* trackAcpStartup({
      started: yield* Clock.currentTimeMillis,
      exited: Effect.never,
      exitCode: () => null,
    })
    yield* startup.observe("[acp-profile] cli.acp.connection.create.mark 5038ms\n")
    const waiting = yield* startup.ready.pipe(Effect.flip, Effect.forkChild)
    yield* TestClock.adjust("29999 millis")
    expect(waiting.pollUnsafe()).toBeUndefined()
    yield* TestClock.adjust("1 milli")
    expect((yield* Fiber.join(waiting)).message).toContain("ACP startup deadline exceeded (30000ms)")
    yield* TestClock.adjust("1 milli")
    yield* startup.observe(marker)
    expect((yield* startup.ready.pipe(Effect.flip)).message).toContain("ACP startup deadline exceeded (30000ms)")
  }),
)

it.effect("ACP retains a chunked ready marker and does not restart shutdown time at the first wait", () =>
  Effect.gen(function* () {
    const startup = yield* trackAcpStartup({
      started: yield* Clock.currentTimeMillis,
      exited: Effect.never,
      exitCode: () => null,
    })
    yield* TestClock.adjust("8 seconds")
    yield* startup.observe(marker.slice(0, 23))
    yield* startup.observe(marker.slice(23).replace("\n", "\r\n"))
    yield* startup.ready
    yield* TestClock.adjust("4 seconds")
    // Repeated observation must retain the first readiness timestamp.
    yield* startup.observe(marker)
    const waiting = yield* startup.exitAfterStartup.pipe(Effect.flip, Effect.forkChild)
    yield* TestClock.adjust("999 millis")
    expect(waiting.pollUnsafe()).toBeUndefined()
    yield* TestClock.adjust("1 milli")
    expect((yield* Fiber.join(waiting)).message).toContain("5000ms after readiness")
  }),
)

for (const code of [0, 7]) {
  it.effect(`ACP settles an already-exited process without a marker (code=${code})`, () =>
    Effect.gen(function* () {
      const startup = yield* trackAcpStartup({
        started: yield* Clock.currentTimeMillis,
        exited: Effect.succeed(code),
        exitCode: () => code,
      })
      expect(yield* startup.exitAfterStartup).toBe(code)
      expect((yield* startup.ready.pipe(Effect.flip)).message).toContain(
        `ACP exited before transport readiness (code=${code})`,
      )
    }),
  )
}

it.effect("ACP settles process failure before a marker", () =>
  Effect.gen(function* () {
    const error = new Error("process observation failed")
    const startup = yield* trackAcpStartup({
      started: yield* Clock.currentTimeMillis,
      exited: Effect.fail(error),
      exitCode: () => null,
    })
    expect(yield* startup.exitAfterStartup.pipe(Effect.flip)).toBe(error)
    expect(yield* startup.ready.pipe(Effect.flip)).toBe(error)
  }),
)

it.effect("ACP settles exit while waiting for a missing marker", () =>
  Effect.gen(function* () {
    const exited = yield* Deferred.make<number>()
    const startup = yield* trackAcpStartup({
      started: yield* Clock.currentTimeMillis,
      exited: Deferred.await(exited),
      exitCode: () => null,
    })
    const waiting = yield* startup.exitAfterStartup.pipe(Effect.forkChild)
    yield* TestClock.adjust("8 seconds")
    yield* Deferred.succeed(exited, 0)
    expect(yield* Fiber.join(waiting)).toBe(0)
  }),
)

it.effect("ACP accepts clean EOF after readiness within the shutdown deadline", () =>
  Effect.gen(function* () {
    const exited = yield* Deferred.make<number>()
    const startup = yield* trackAcpStartup({
      started: yield* Clock.currentTimeMillis,
      exited: Deferred.await(exited),
      exitCode: () => null,
    })
    const waiting = yield* startup.exitAfterStartup.pipe(Effect.forkChild)
    yield* TestClock.adjust("8 seconds")
    yield* startup.observe(marker)
    yield* TestClock.adjust("32 millis")
    yield* Deferred.succeed(exited, 0)
    expect(yield* Fiber.join(waiting)).toBe(0)
  }),
)

it.effect("ACP cancellation releases the startup race without consuming later readiness", () =>
  Effect.gen(function* () {
    const observing = yield* Deferred.make<void>()
    const stopped = yield* Deferred.make<void>()
    const observations = { count: 0 }
    yield* Effect.gen(function* () {
      const startup = yield* trackAcpStartup({
        started: yield* Clock.currentTimeMillis,
        exited: Effect.sync(() => observations.count++).pipe(
          Effect.andThen(Deferred.succeed(observing, undefined)),
          Effect.andThen(Effect.never),
          Effect.ensuring(Deferred.succeed(stopped, undefined)),
        ),
        exitCode: () => null,
      })
      yield* startup.observe("[acp-profile] cli.acp.connection.create.mark 5038ms\n")
      yield* Effect.forEach([startup.ready, startup.exitAfterStartup.pipe(Effect.asVoid), startup.ready], (wait) =>
        Effect.gen(function* () {
          const released = yield* Deferred.make<void>()
          // Effect beta.83 starts race children synchronously. Finish registering
          // the waiting fiber before the test can interrupt it reentrantly.
          const waiting = yield* wait.pipe(
            Effect.ensuring(Deferred.succeed(released, undefined)),
            Effect.forkChild({ startImmediately: true }),
          )
          yield* Deferred.await(observing)
          yield* Fiber.interrupt(waiting)
          expect(yield* Deferred.isDone(released)).toBe(true)
          expect(yield* Deferred.isDone(stopped)).toBe(false)
          expect(observations.count).toBe(1)
        }),
      )
      yield* startup.observe(marker)
      yield* startup.ready
      yield* startup.ready
      expect(observations.count).toBe(1)
    }).pipe(Effect.scoped)
    expect(yield* Deferred.isDone(stopped)).toBe(true)
  }),
)

for (const ready of [false, true]) {
  for (const timely of [false, true]) {
    it.effect(`ACP delayed exit wait: ready=${ready}, timely=${timely}`, () =>
      Effect.gen(function* () {
        const exited = yield* Deferred.make<number>()
        const observed = yield* Deferred.make<void>()
        const observations = { count: 0 }
        const startup = yield* trackAcpStartup({
          started: yield* Clock.currentTimeMillis,
          exited: Effect.sync(() => observations.count++).pipe(
            Effect.andThen(Deferred.await(exited)),
            Effect.ensuring(Deferred.succeed(observed, undefined)),
          ),
          exitCode: () => 0,
        })
        yield* TestClock.adjust("8 seconds")
        if (ready) yield* startup.observe(marker)
        yield* TestClock.adjust(ready ? (timely ? "4 seconds" : "6 seconds") : timely ? "21 seconds" : "23 seconds")
        yield* Deferred.succeed(exited, 0)
        yield* Deferred.await(observed)
        // Flush the observer's completion before moving time past both budgets.
        yield* TestClock.adjust("40 seconds")
        yield* Effect.forEach([0, 1, 2], () =>
          Effect.gen(function* () {
            if (timely) {
              expect(yield* startup.exitAfterStartup).toBe(0)
              return
            }
            expect((yield* startup.exitAfterStartup.pipe(Effect.flip)).message).toContain(
              ready ? "5000ms after readiness" : "ACP startup deadline exceeded (30000ms)",
            )
          }),
        )
        expect(observations.count).toBe(1)
      }),
    )
  }
}

it.effect("ACP keeps a bounded stderr tail without losing a marker in a large chunk", () =>
  Effect.gen(function* () {
    const startup = yield* trackAcpStartup({
      started: yield* Clock.currentTimeMillis,
      exited: Effect.never,
      exitCode: () => null,
    })
    yield* startup.observe(marker + "x".repeat(7000))
    yield* startup.ready
    expect(startup.diagnostics().split("stderr (last 6000):\n")[1]).toBe("x".repeat(6000))
  }),
)
