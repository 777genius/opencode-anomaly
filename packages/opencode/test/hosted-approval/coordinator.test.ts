import { expect, test } from "bun:test"
import { Deferred, Effect, Fiber } from "effect"
import { HostedApprovalCoordinator } from "../../src/hosted-approval/coordinator"

test("coordinator uses process-unique tokens and rotates generation after mutation", async () => {
  const result = await Effect.gen(function* () {
    const coordinator = yield* HostedApprovalCoordinator.Service
    const before = coordinator.snapshot()
    let visibleDuringMutation = ""
    yield* coordinator.withConfigMutation(
      Effect.sync(() => {
        visibleDuringMutation = coordinator.snapshot().configGeneration
      }),
    )
    return { before, after: coordinator.snapshot(), visibleDuringMutation }
  }).pipe(Effect.provide(HostedApprovalCoordinator.layer), Effect.runPromise)

  expect(result.before.runtimeInstanceId).toMatch(/^runtime_instance_[0-9a-f]{32}$/)
  expect(result.before.configGeneration).toMatch(/^config_generation_[0-9a-f]{32}$/)
  expect(result.after.runtimeInstanceId).toBe(result.before.runtimeInstanceId)
  expect(result.after.configGeneration).not.toBe(result.before.configGeneration)
  expect(result.visibleDuringMutation).toBe(result.before.configGeneration)
})

test("separate coordinator boots never reuse runtime or config incarnation", async () => {
  const read = Effect.gen(function* () {
    return (yield* HostedApprovalCoordinator.Service).snapshot()
  })
  const [first, second] = await Promise.all([
    Effect.runPromise(read.pipe(Effect.provide(HostedApprovalCoordinator.makeTestLayer()))),
    Effect.runPromise(read.pipe(Effect.provide(HostedApprovalCoordinator.makeTestLayer()))),
  ])
  expect(first.runtimeInstanceId).not.toBe(second.runtimeInstanceId)
  expect(first.configGeneration).not.toBe(second.configGeneration)
})

test("all providers of the production layer share one process authority", async () => {
  const snapshots = await Effect.all(
    [
      Effect.gen(function* () {
        return (yield* HostedApprovalCoordinator.Service).snapshot()
      }).pipe(Effect.provide(HostedApprovalCoordinator.layer)),
      Effect.gen(function* () {
        return (yield* HostedApprovalCoordinator.Service).snapshot()
      }).pipe(Effect.provide(HostedApprovalCoordinator.layer)),
    ],
    { concurrency: "unbounded" },
  ).pipe(Effect.runPromise)
  expect(snapshots[0]).toEqual(snapshots[1])
})

test("cancellation cannot split a committed mutation from generation rotation", async () => {
  const result = await Effect.gen(function* () {
    const coordinator = yield* HostedApprovalCoordinator.Service
    const locked = yield* Deferred.make<void>()
    const unlock = yield* Deferred.make<void>()
    const committed = yield* Deferred.make<void>()
    const before = coordinator.snapshot().configGeneration
    const lock = yield* coordinator.withConditionalReply(
      Effect.gen(function* () {
        yield* Deferred.succeed(locked, undefined)
        yield* Deferred.await(unlock)
      }),
    ).pipe(Effect.forkScoped)
    yield* Deferred.await(locked)
    const fiber = yield* coordinator.withConfigMutation(
      Effect.gen(function* () {
        yield* Deferred.succeed(committed, undefined)
        return true
      }),
    ).pipe(Effect.forkScoped)
    yield* Deferred.await(committed)
    yield* Fiber.interrupt(fiber).pipe(Effect.forkScoped)
    yield* Deferred.succeed(unlock, undefined)
    yield* Fiber.await(lock)
    yield* Fiber.await(fiber)
    return { before, after: coordinator.snapshot().configGeneration }
  }).pipe(Effect.scoped, Effect.provide(HostedApprovalCoordinator.makeTestLayer()), Effect.runPromise)
  expect(result.after).not.toBe(result.before)
})

test("config work does not hold the process coordinator", async () => {
  const result = await Effect.gen(function* () {
    const coordinator = yield* HostedApprovalCoordinator.Service
    const started = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const before = coordinator.snapshot().configGeneration
    const mutation = yield* coordinator.withConfigMutation(
      Effect.gen(function* () {
        yield* Deferred.succeed(started, undefined)
        yield* Deferred.await(release)
      }),
    ).pipe(Effect.forkScoped)
    yield* Deferred.await(started)
    const during = yield* coordinator.withConditionalReply(
      Effect.sync(() => coordinator.snapshot().configGeneration),
    ).pipe(Effect.timeout("250 millis"))
    yield* Deferred.succeed(release, undefined)
    yield* Fiber.join(mutation)
    return { before, during, after: coordinator.snapshot().configGeneration }
  }).pipe(Effect.scoped, Effect.provide(HostedApprovalCoordinator.makeTestLayer()), Effect.runPromise)
  expect(result.during).toBe(result.before)
  expect(result.after).not.toBe(result.before)
})

test("conditional operations remain single-writer atomic", async () => {
  const order = await Effect.gen(function* () {
    const coordinator = yield* HostedApprovalCoordinator.Service
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const values: string[] = []
    const first = yield* coordinator.withConditionalReply(
      Effect.gen(function* () {
        values.push("first-enter")
        yield* Deferred.succeed(entered, undefined)
        yield* Deferred.await(release)
        values.push("first-exit")
      }),
    ).pipe(Effect.forkScoped)
    yield* Deferred.await(entered)
    const second = yield* coordinator.withConditionalReply(
      Effect.sync(() => values.push("second")),
    ).pipe(Effect.forkScoped)
    yield* Deferred.succeed(release, undefined)
    yield* Fiber.join(first)
    yield* Fiber.join(second)
    return values
  }).pipe(Effect.scoped, Effect.provide(HostedApprovalCoordinator.makeTestLayer()), Effect.runPromise)
  expect(order).toEqual(["first-enter", "first-exit", "second"])
})
