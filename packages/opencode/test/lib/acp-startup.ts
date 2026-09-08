import { Clock, Deferred, Duration, Effect, Fiber } from "effect"

// The harness runs TypeScript source, including Bun transpilation and Instance
// loading. Use its existing 30s process budget for that phase, independently of
// the 15s RPC and 5s EOF deadlines. This is measured from spawn, not first wait.
const startupTimeoutMs = 30_000
const shutdownTimeoutMs = 5_000

export function trackAcpStartup(input: {
  started: number
  exited: Effect.Effect<number, Error>
  exitCode: () => number | null
}) {
  return Effect.gen(function* () {
    const ready = yield* Deferred.make<number>()
    // Observe the process once for the handle's scope, independently of waiters.
    // Retain exit time even when the first wait happens after the deadline.
    const exited = yield* input.exited.pipe(
      Effect.flatMap((code) => Clock.currentTimeMillis.pipe(Effect.map((at) => ({ code, at })))),
      Effect.forkScoped({ startImmediately: true }),
    )
    const stderr = { tail: "" }
    const diagnostics = () =>
      `ACP elapsed since spawn=${Date.now() - input.started}ms exitCode=${input.exitCode()}\n` +
      `stderr (last 6000):\n${stderr.tail}`
    const startupExpired = () => Effect.fail(new Error(`ACP startup deadline exceeded (30000ms)\n${diagnostics()}`))
    const shutdownExpired = () =>
      Effect.fail(new Error(`Immediate EOF exit deadline exceeded (5000ms after readiness)\n${diagnostics()}`))
    const observe = (chunk: string) =>
      Effect.gen(function* () {
        const text = stderr.tail + chunk
        stderr.tail = text.slice(-6000)
        // Existing profile mark: connection constructed, immediately before
        // resuming/waiting on stdin. Match across stderr chunk boundaries.
        if (!/(?:^|\n)\[acp-profile\] cli\.acp\.stdin\.wait\.mark \d+ms ended=(?:true|false)\r?\n/.test(text)) return
        const now = yield* Clock.currentTimeMillis
        if (now - input.started > startupTimeoutMs) return
        yield* Deferred.succeed(ready, now)
      })
    const startup = Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis
      const result = yield* Deferred.await(ready).pipe(
        Effect.map((at) => ({ type: "ready" as const, at })),
        Effect.raceFirst(Fiber.join(exited).pipe(Effect.map((exit) => ({ type: "exited" as const, ...exit })))),
        Effect.timeoutOrElse({
          duration: Duration.millis(Math.max(0, input.started + startupTimeoutMs - now)),
          orElse: startupExpired,
        }),
      )
      if (result.at > input.started + startupTimeoutMs) return yield* startupExpired()
      return result
    })

    return {
      observe,
      diagnostics,
      ready: startup.pipe(
        Effect.flatMap((result) =>
          result.type === "ready"
            ? Effect.void
            : Effect.fail(new Error(`ACP exited before transport readiness (code=${result.code})\n${diagnostics()}`)),
        ),
      ),
      // Only for stdin closed immediately after spawn. A natural exit before
      // the marker is valid; otherwise allow 5s from observed transport ready.
      exitAfterStartup: Effect.gen(function* () {
        const result = yield* startup
        if (result.type === "exited") return result.code
        const now = yield* Clock.currentTimeMillis
        const exit = yield* Fiber.join(exited).pipe(
          Effect.timeoutOrElse({
            duration: Duration.millis(Math.max(0, result.at + shutdownTimeoutMs - now)),
            orElse: shutdownExpired,
          }),
        )
        if (exit.at > result.at + shutdownTimeoutMs) return yield* shutdownExpired()
        return exit.code
      }),
    }
  })
}
