import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Context, Effect, Layer, Semaphore } from "effect"
import { randomUUID } from "node:crypto"

export interface Snapshot {
  readonly runtimeInstanceId: string
  readonly configGeneration: string
}

export interface Interface {
  readonly snapshot: () => Snapshot
  readonly withConfigMutation: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    changed?: (result: A) => boolean,
  ) => Effect.Effect<A, E, R>
  readonly withConditionalReply: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/HostedApprovalCoordinator") {}

function token(prefix: string) {
  return `${prefix}_${randomUUID().replaceAll("-", "")}`
}

export function makeTestLayer() {
  return Layer.succeed(
    Service,
    (() => {
      const mutex = Semaphore.makeUnsafe(1)
      const runtimeInstanceId = token("runtime_instance")
      let configGeneration = token("config_generation")
      const service: Interface = {
        snapshot: () => ({ runtimeInstanceId, configGeneration }),
        withConfigMutation: (effect, changed = () => true) =>
          Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              // Config I/O remains interruptible and does not hold the process
              // coordinator. Only the generation's linearization point is shared.
              const result = yield* restore(effect)
              if (changed(result)) {
                yield* mutex.withPermit(
                  Effect.sync(() => {
                    configGeneration = token("config_generation")
                  }),
                )
              }
              return result
            }),
          ),
        withConditionalReply: (effect) => mutex.withPermit(effect),
      }
      return Service.of(service)
    })(),
  )
}

// One process authority value. Reusing this Layer in independently assembled
// route/service graphs cannot mint a second identity or mutex.
export const layer = makeTestLayer()

export const node = LayerNode.make({ service: Service, layer, deps: [] })

export const HostedApprovalCoordinator = { Service, layer, makeTestLayer, node }
