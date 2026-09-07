import { test, type TestOptions } from "bun:test"
import { Cause, Effect, Exit, Layer } from "effect"
import type * as Scope from "effect/Scope"
import * as TestClock from "effect/testing/TestClock"
import * as TestConsole from "effect/testing/TestConsole"

type Body<A, E, R> = Effect.Effect<A, E, R> | (() => Effect.Effect<A, E, R>)

const body = <A, E, R>(value: Body<A, E, R>) => Effect.suspend(() => (typeof value === "function" ? value() : value))

const run = <A, E, R, E2>(
  value: Body<A, E, R | Scope.Scope>,
  layer: Layer.Layer<R, E2>,
  phase?: (label: string) => void,
) => {
  phase?.("test.entry")
  const promise = Effect.gen(function* () {
    const exit = yield* body(
      phase
        ? () => {
            phase("fixture.ready")
            return typeof value === "function" ? value() : value
          }
        : value,
    ).pipe(Effect.scoped, Effect.provide(layer), Effect.exit)
    if (Exit.isFailure(exit)) {
      for (const err of Cause.prettyErrors(exit.cause)) {
        yield* Effect.logError(err)
      }
    }
    return yield* exit
  }).pipe(Effect.runPromise)
  if (!phase) return promise
  // Observe the original runPromise only after both body and supplied-layer scopes close.
  return promise.then(
    (value) => {
      phase("promise.fulfilled")
      return value
    },
    (error: unknown) => {
      phase("promise.rejected")
      throw error
    },
  )
}

const make = <R, E>(
  testLayer: Layer.Layer<R, E>,
  liveLayer: Layer.Layer<R, E>,
  phase?: (label: string) => void,
) => {
  const effect = <A, E2>(name: string, value: Body<A, E2, R | Scope.Scope>, opts?: number | TestOptions) =>
    test(name, () => run(value, testLayer, phase), opts)

  effect.only = <A, E2>(name: string, value: Body<A, E2, R | Scope.Scope>, opts?: number | TestOptions) =>
    test.only(name, () => run(value, testLayer, phase), opts)

  effect.skip = <A, E2>(name: string, value: Body<A, E2, R | Scope.Scope>, opts?: number | TestOptions) =>
    test.skip(name, () => run(value, testLayer, phase), opts)

  const live = <A, E2>(name: string, value: Body<A, E2, R | Scope.Scope>, opts?: number | TestOptions) =>
    test(name, () => run(value, liveLayer, phase), opts)

  live.only = <A, E2>(name: string, value: Body<A, E2, R | Scope.Scope>, opts?: number | TestOptions) =>
    test.only(name, () => run(value, liveLayer, phase), opts)

  live.skip = <A, E2>(name: string, value: Body<A, E2, R | Scope.Scope>, opts?: number | TestOptions) =>
    test.skip(name, () => run(value, liveLayer, phase), opts)

  return { effect, live }
}

// Test environment with TestClock and TestConsole
const testEnv = Layer.mergeAll(TestConsole.layer, TestClock.layer())

// Live environment - uses real clock, but keeps TestConsole for output capture
const liveEnv = TestConsole.layer

export const it = make(testEnv, liveEnv)

export const testEffect = <R, E>(layer: Layer.Layer<R, E>, phase?: (label: string) => void) =>
  make(Layer.provideMerge(layer, testEnv), Layer.provideMerge(layer, liveEnv), phase)
