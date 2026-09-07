import { expect, spyOn, test } from "bun:test"
import { Cause, Deferred, Effect, Exit, Fiber, Scope } from "effect"
import fs from "node:fs"
import {
  DiagnosticOwner,
  diagnosticBody,
  diagnosticCallback,
  diagnosticContext,
  diagnosticDrain,
  diagnosticRegistration,
  diagnosticTest,
  unitDiagnostic,
  unitDiagnosticEnabled,
} from "./unit-diagnostic"

// Root runs these with the real pinned Effect and the Windows opt-in enabled.
// Capture only this module's immediate writer; never start CLI children here.
function capture() {
  const records: Array<{ test: string; testid: number | null; phase: string; childpid: number | null }> = []
  const writer = spyOn(fs, "writeSync").mockImplementation((...args) => {
    const text = String(args[1])
    records.push(JSON.parse(text))
    return text.length
  })
  return { records, [Symbol.dispose]: () => writer.mockRestore() }
}

const diagnostic = unitDiagnosticEnabled ? test : test.skip

test("registration has zero arity and never forwards Bun's done callback as owner", async () => {
  using output = capture()
  const owners: Array<DiagnosticOwner | undefined> = []
  const doneCalls: unknown[] = []
  const callback = diagnosticRegistration("registration", (owner) => {
    owners.push(owner)
    return Promise.resolve(42)
  })
  expect(callback.length).toBe(0)
  // Use Bun's callback signature without a cast and simulate its supplied argument.
  const registered: (done: (err?: unknown) => void) => void | Promise<unknown> = callback
  const result = registered((error) => { doneCalls.push(error) })
  expect(owners).toHaveLength(1)
  expect(await result).toBe(42)
  expect(doneCalls).toEqual([])
  if (!unitDiagnosticEnabled) {
    expect(owners).toEqual([undefined])
    expect(output.records).toEqual([])
    return
  }
  const owner = owners[0]
  if (!owner) throw new Error("enabled registration did not retain its owner")
  expect(owner.test).toBe("registration")
  expect(owner.id).toBeNumber()
  expect(output.records.map((record) => [record.phase, record.testid])).toEqual([
    ["test.entry", owner.id], ["scope.settled", owner.id],
  ])
})

test("registration preserves rejection identity and invokes once", async () => {
  using output = capture()
  const error = new Error("registration failure")
  const calls: number[] = []
  const callback = diagnosticRegistration("rejecting", () => {
    calls.push(1)
    return Promise.reject(error)
  })
  await expect(callback()).rejects.toBe(error)
  expect(calls).toEqual([1])
  expect(output.records.map((record) => record.phase)).toEqual(
    unitDiagnosticEnabled ? ["test.entry", "scope.settled"] : [],
  )
})

diagnostic("concurrent suspended nested fibers retain child and cleanup owners", async () => {
  using output = capture()
  const ready = await Effect.runPromise(Deferred.make<void>())
  const resume = await Effect.runPromise(Deferred.make<void>())
  await Promise.all(
    ["owner-a", "owner-b"].map((name, index) =>
      diagnosticRegistration(name, (owner) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(index === 0 ? ready : resume, undefined)
          yield* Deferred.await(index === 0 ? resume : ready)
          const child = yield* diagnosticBody(
            Effect.gen(function* () {
              yield* Effect.yieldNow
              const inherited = yield* DiagnosticOwner
              const mark = unitDiagnostic("run", undefined, inherited)
              mark("spawn", index + 100)
              yield* Effect.addFinalizer(() => diagnosticBody(Effect.sync(() => mark("exited", index + 100)), "cleanup"))
              return yield* diagnosticBody(Effect.succeed(name), "callback")
            }).pipe(Effect.scoped),
          ).pipe(Effect.forkScoped)
          expect(yield* Fiber.join(child)).toBe(name)
        }).pipe(Effect.scoped, (effect) => diagnosticContext(effect, owner), Effect.runPromise),
      )(),
    ),
  )
  for (const [index, name] of ["owner-a", "owner-b"].entries()) {
    const entry = output.records.find((record) => record.test === name && record.phase === "test.entry")!
    expect(entry.testid).toBeNumber()
    const records = output.records.filter((record) => record.test === name)
    expect(records.map((record) => record.phase)).toEqual([
      "test.entry", "body.entry", "spawn", "callback.entry", "callback.settled",
      "cleanup.entry", "exited", "cleanup.settled", "body.settled", "scope.settled",
    ])
    expect(records.every((record) => record.testid === entry.testid)).toBe(true)
    expect(output.records.filter((record) => record.childpid === index + 100).every((record) => record.test === name)).toBe(true)
  }
  expect(output.records.find((record) => record.test === "owner-a")!.testid)
    .not.toBe(output.records.find((record) => record.test === "owner-b")!.testid)
})

diagnostic("leaked suspended cleanup outlives its test and keeps its owner while another test runs", async () => {
  using output = capture()
  const scope = await Effect.runPromise(Scope.make())
  const entered = await Effect.runPromise(Deferred.make<void>())
  const release = await Effect.runPromise(Deferred.make<void>())
  await diagnosticRegistration("departed", (owner) =>
    Effect.gen(function* () {
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          yield* Deferred.succeed(entered, undefined)
          yield* Deferred.await(release)
          // Construct the emitter and nested wrapper after resumption.
          const inherited = yield* DiagnosticOwner
          const mark = unitDiagnostic("acp", undefined, inherited)
          mark("cleanup.start", 201)
          yield* diagnosticBody(diagnosticDrain(Effect.void, mark, "stderr", 201), "cleanup")
        }),
      )
    }).pipe(Effect.provideService(Scope.Scope, scope), (effect) => diagnosticContext(effect, owner), Effect.runPromise),
  )()
  const closing: Promise<void>[] = []
  await diagnosticRegistration("current", (owner) =>
    Effect.gen(function* () {
      closing.push(Effect.runPromise(diagnosticContext(Scope.close(scope, Exit.void), owner)))
      yield* Deferred.await(entered)
      yield* diagnosticBody(Effect.void, "callback")
      // Missing fiber identity must never borrow the currently running test.
      yield* diagnosticContext(diagnosticBody(Effect.void, "unowned"))
    }).pipe(Effect.scoped, (effect) => diagnosticContext(effect, owner), Effect.runPromise),
  )()
  await Effect.runPromise(Deferred.succeed(release, undefined))
  await Promise.all(closing)
  const entry = output.records.find((record) => record.test === "departed" && record.phase === "test.entry")!
  const cleanup = output.records.filter((record) => record.phase.startsWith("cleanup") || record.childpid === 201)
  expect(cleanup.map((record) => record.phase)).toEqual([
    "cleanup.start", "cleanup.entry", "stderr.drain.start", "stderr.drain.complete", "cleanup.settled",
  ])
  expect(cleanup.every((record) => record.test === "departed" && record.testid === entry.testid)).toBe(true)
  expect(output.records.findIndex((record) => record.phase === "cleanup.start"))
    .toBeGreaterThan(output.records.findIndex((record) => record.test === "current" && record.phase === "scope.settled"))
  expect(output.records.findIndex((record) => record.phase === "cleanup.start"))
    .toBeGreaterThan(output.records.findIndex((record) => record.test === "departed" && record.phase === "scope.settled"))
  expect(output.records.filter((record) => record.phase.startsWith("unowned")).map((record) => [record.test, record.testid]))
    .toEqual([["unattributed cleanup", null], ["unattributed cleanup", null]])
})

diagnostic("callback entry precedes synchronous invocation and synchronous throw settles once", async () => {
  using output = capture()
  const error = new Error("callback defect")
  const calls: string[] = []
  const caught: unknown[] = []
  await diagnosticRegistration("throwing", async (owner) => {
    try {
      diagnosticCallback(() => {
      calls.push("invoked")
      expect(output.records.at(-1)?.phase).toBe("callback.entry")
      throw error
      }, owner)
    } catch (cause) {
      caught.push(cause)
    }
  })()
  expect(calls).toEqual(["invoked"])
  expect(caught).toHaveLength(1)
  expect(caught[0]).toBe(error)
  expect(output.records.map((record) => record.phase)).toEqual([
    "test.entry", "callback.entry", "callback.settled", "scope.settled",
  ])
})

diagnostic("body preserves success, failure and interruption exits", async () => {
  using output = capture()
  const failure = { reason: "sentinel" }
  for (const effect of [Effect.succeed(42), Effect.fail(failure)]) {
    const expected = await Effect.runPromiseExit(effect)
    expect(await Effect.runPromiseExit(diagnosticBody(effect))).toEqual(expected)
  }
  await Effect.runPromise(Effect.gen(function* () {
    const entered = yield* Deferred.make<void>()
    const fiber = yield* diagnosticBody(
      Effect.gen(function* () {
        yield* Deferred.succeed(entered, undefined)
        yield* Effect.never
      }),
    ).pipe(Effect.forkScoped)
    yield* Deferred.await(entered)
    yield* Fiber.interrupt(fiber)
    const exit = yield* Fiber.await(fiber)
    expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true)
  }).pipe(Effect.scoped))
  expect(output.records.map((record) => record.phase)).toEqual([
    "body.entry", "body.settled", "body.entry", "body.settled", "body.entry", "body.settled",
  ])
})

;(unitDiagnosticEnabled ? test.skip : test)("disabled wrappers preserve input identity and invoke callback once", () => {
  const effect = Effect.succeed(42)
  const run = () => Promise.resolve(42)
  expect(diagnosticTest("disabled", run)).toBe(run)
  expect(diagnosticContext(effect)).toBe(effect)
  expect(diagnosticBody(effect)).toBe(effect)
  expect(diagnosticDrain(effect, unitDiagnostic("run"), "stderr", 1)).toBe(effect)
  const calls: number[] = []
  expect(diagnosticCallback(() => { calls.push(1); return effect })).toBe(effect)
  expect(calls).toEqual([1])
})
