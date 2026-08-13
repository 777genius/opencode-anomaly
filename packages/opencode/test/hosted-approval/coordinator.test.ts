import { expect, test } from "bun:test"
import { Deferred, Effect, Fiber } from "effect"
import { HostedApprovalCoordinator } from "../../src/hosted-approval/coordinator"

test("coordinator uses process-unique tokens and rotates generation before mutation", async () => {
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
    const committed = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const before = coordinator.snapshot().configGeneration
    const fiber = yield* coordinator.withConfigMutation(
      Effect.gen(function* () {
        yield* Deferred.succeed(committed, undefined)
        yield* Deferred.await(release)
        return true
      }),
    ).pipe(Effect.forkScoped)
    yield* Deferred.await(committed)
    yield* Fiber.interrupt(fiber).pipe(Effect.forkScoped)
    yield* Deferred.succeed(release, undefined)
    yield* Fiber.await(fiber)
    return { before, after: coordinator.snapshot().configGeneration }
  }).pipe(Effect.scoped, Effect.provide(HostedApprovalCoordinator.makeTestLayer()), Effect.runPromise)
  expect(result.after).not.toBe(result.before)
})
