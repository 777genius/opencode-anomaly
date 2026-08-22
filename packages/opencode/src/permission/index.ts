import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ConfigPermissionV1 } from "@opencode-ai/core/v1/config/permission"
import { InstanceState } from "@/effect/instance-state"
import { Wildcard } from "@opencode-ai/core/util/wildcard"
import { Deferred, Effect, Layer, Context, Semaphore } from "effect"
import os from "os"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { EventV2Bridge } from "@/event-v2-bridge"
import { randomUUID } from "node:crypto"
import { HostedApprovalCoordinator } from "@/hosted-approval/coordinator"

export const Event = PermissionV1.Event

export interface Interface {
  readonly ask: (input: PermissionV1.AskInput) => Effect.Effect<void, PermissionV1.Error>
  readonly reply: (input: PermissionV1.ReplyInput) => Effect.Effect<void, PermissionV1.NotFoundError>
  /**
   * Claims and settles exactly one pending request when its full external
   * incarnation still matches. Unlike the interactive reply operation this
   * never cascades to other requests and never persists an approval rule.
   */
  readonly conditionalReply: (
    input: ConditionalReplyInput,
  ) => Effect.Effect<ConditionalReplyResult>
  readonly list: () => Effect.Effect<ReadonlyArray<PermissionV1.Request>>
  readonly hostedList: () => Effect.Effect<ReadonlyArray<HostedPendingRequest>>
}

export interface ConditionalReplyInput {
  readonly requestID: PermissionV1.ID
  readonly sessionID: PermissionV1.Request["sessionID"]
  readonly sessionIncarnation: string
  readonly requestIncarnation: string
  readonly reply: "once" | "reject"
  readonly message?: string
  /** Runs synchronously while the permission state lock is held. */
  readonly matches: (request: PermissionV1.Request) => boolean
}

export type ConditionalReplyResult =
  | { readonly status: "applied"; readonly request: PermissionV1.Request }
  | { readonly status: "mismatch" }

export interface HostedPendingRequest {
  readonly request: PermissionV1.Request
  readonly sessionIncarnation: string
  readonly requestIncarnation: string
}

interface PendingEntry {
  info: PermissionV1.Request
  sessionIncarnation: string
  requestIncarnation: string
  deferred: Deferred.Deferred<void, PermissionV1.RejectedError | PermissionV1.CorrectedError>
}

interface State {
  pending: Map<PermissionV1.ID, PendingEntry>
  approved: PermissionV1.Rule[]
  sessionIncarnations: Map<PermissionV1.Request["sessionID"], string>
}

function incarnation(prefix: "session_incarnation" | "request_incarnation") {
  return `${prefix}_${randomUUID().replaceAll("-", "")}`
}

export function evaluate(permission: string, pattern: string, ...rulesets: PermissionV1.Ruleset[]): PermissionV1.Rule {
  return (
    rulesets
      .flat()
      .findLast((rule) => Wildcard.match(permission, rule.permission) && Wildcard.match(pattern, rule.pattern)) ?? {
      action: "ask",
      permission,
      pattern: "*",
    }
  )
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Permission") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const events = yield* EventV2Bridge.Service
    const hostedApproval = yield* HostedApprovalCoordinator.Service
    const mutex = Semaphore.makeUnsafe(1)
    const state = yield* InstanceState.make<State>(
      Effect.fn("Permission.state")(function* (ctx) {
        void ctx
        const state = {
          pending: new Map<PermissionV1.ID, PendingEntry>(),
          approved: [],
          sessionIncarnations: new Map<PermissionV1.Request["sessionID"], string>(),
        }

        const unsubscribe = yield* events.listen((event) => {
          if (event.location?.directory !== ctx.directory) return Effect.void
          if (event.type !== SessionV1.Event.Created.type && event.type !== SessionV1.Event.Deleted.type) {
            return Effect.void
          }
          const data = event.data as { sessionID: PermissionV1.Request["sessionID"] }
          return hostedApproval.withConditionalReply(
            mutex.withPermit(
              Effect.sync(() => {
                if (event.type === SessionV1.Event.Deleted.type) {
                  state.sessionIncarnations.delete(data.sessionID)
                  return
                }
                // Creation only rotates an identity already observed by this
                // permission service. New sessions are minted lazily on ask.
                if (state.sessionIncarnations.has(data.sessionID)) {
                  state.sessionIncarnations.set(data.sessionID, incarnation("session_incarnation"))
                }
              }),
            ),
          )
        })
        yield* Effect.addFinalizer(() => unsubscribe)

        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            for (const item of state.pending.values()) {
              yield* Deferred.fail(item.deferred, new PermissionV1.RejectedError())
            }
            state.pending.clear()
            state.sessionIncarnations.clear()
          }),
        )

        return state
      }),
    )

    const ask = Effect.fn("Permission.ask")(function* (input: PermissionV1.AskInput) {
      const deferred = yield* Deferred.make<void, PermissionV1.RejectedError | PermissionV1.CorrectedError>()
      const pendingID = yield* hostedApproval.withConditionalReply(mutex.withPermit(
        Effect.gen(function* () {
          const { approved, pending, sessionIncarnations } = yield* InstanceState.get(state)
          const { ruleset, ...request } = input
          let needsAsk = false
          for (const pattern of request.patterns) {
            const rule = evaluate(request.permission, pattern, ruleset, approved)
            yield* Effect.logInfo("evaluated", { permission: request.permission, pattern, action: rule })
            if (rule.action === "deny") {
              return yield* new PermissionV1.DeniedError({
                ruleset: ruleset.filter((rule) => Wildcard.match(request.permission, rule.permission)),
              })
            }
            if (rule.action !== "allow") needsAsk = true
          }
          if (!needsAsk) return undefined

          const id = request.id ?? PermissionV1.ID.ascending()
          // Caller-provided IDs are allowed by the public schema, but replacing
          // a live entry would enable request-incarnation ABA and orphan its deferred.
          if (pending.has(id)) return yield* new PermissionV1.DeniedError({ ruleset: [] })
          const info: PermissionV1.Request = {
            id,
            sessionID: request.sessionID,
            permission: request.permission,
            patterns: request.patterns,
            metadata: request.metadata,
            always: request.always,
            tool: request.tool,
          }
          const sessionIncarnation = sessionIncarnations.get(info.sessionID) ?? incarnation("session_incarnation")
          sessionIncarnations.set(info.sessionID, sessionIncarnation)
          yield* Effect.logInfo("asking", { id, permission: info.permission, patterns: info.patterns })
          pending.set(id, {
            info,
            sessionIncarnation,
            requestIncarnation: incarnation("request_incarnation"),
            deferred,
          })
          yield* events.publish(Event.Asked, info)
          return id
        }),
      ))
      if (!pendingID) return
      return yield* Effect.ensuring(
        Deferred.await(deferred),
        hostedApproval.withConditionalReply(mutex.withPermit(
          Effect.gen(function* () {
            // Delete only the exact entry owned by this ask. A future request
            // that reuses the ID must not be removed by this finalizer.
            const { pending } = yield* InstanceState.get(state)
            if (pending.get(pendingID)?.deferred === deferred) pending.delete(pendingID)
          }),
        )),
      )
    })

    const reply = Effect.fn("Permission.reply")((input: PermissionV1.ReplyInput) => {
      const settle = mutex.withPermit(
        Effect.gen(function* () {
          const { approved, pending } = yield* InstanceState.get(state)
          const existing = pending.get(input.requestID)
          if (!existing) return yield* new PermissionV1.NotFoundError({ requestID: input.requestID })

          pending.delete(input.requestID)
          yield* events.publish(Event.Replied, {
            sessionID: existing.info.sessionID,
            requestID: existing.info.id,
            reply: input.reply,
          })

          if (input.reply === "reject") {
            yield* Deferred.fail(
              existing.deferred,
              input.message
                ? new PermissionV1.CorrectedError({ feedback: input.message })
                : new PermissionV1.RejectedError(),
            )

            for (const [id, item] of pending.entries()) {
              if (item.info.sessionID !== existing.info.sessionID) continue
              pending.delete(id)
              yield* events.publish(Event.Replied, {
                sessionID: item.info.sessionID,
                requestID: item.info.id,
                reply: "reject",
              })
              yield* Deferred.fail(item.deferred, new PermissionV1.RejectedError())
            }
            return
          }

          yield* Deferred.succeed(existing.deferred, undefined)
          if (input.reply === "once") return

          for (const pattern of existing.info.always) {
            approved.push({
              permission: existing.info.permission,
              pattern,
              action: "allow",
            })
          }

          for (const [id, item] of pending.entries()) {
            if (item.info.sessionID !== existing.info.sessionID) continue
            const ok = item.info.patterns.every(
              (pattern) => evaluate(item.info.permission, pattern, approved).action === "allow",
            )
            if (!ok) continue
            pending.delete(id)
            yield* events.publish(Event.Replied, {
              sessionID: item.info.sessionID,
              requestID: item.info.id,
              reply: "always",
            })
            yield* Deferred.succeed(item.deferred, undefined)
          }
        }),
      )
      return input.reply === "always"
        ? hostedApproval.withConfigMutation(settle)
        : hostedApproval.withConditionalReply(settle)
    })

    const conditionalReply = Effect.fn("Permission.conditionalReply")((input: ConditionalReplyInput) =>
      Effect.uninterruptible(
        mutex.withPermit(
          Effect.gen(function* () {
            const { pending, sessionIncarnations } = yield* InstanceState.get(state)
            const existing = pending.get(input.requestID)
            if (!existing) return { status: "mismatch" } as const
            if (
              existing.info.sessionID !== input.sessionID ||
              existing.sessionIncarnation !== input.sessionIncarnation ||
              sessionIncarnations.get(input.sessionID) !== input.sessionIncarnation ||
              existing.requestIncarnation !== input.requestIncarnation ||
              !input.matches(existing.info)
            ) {
              return { status: "mismatch" } as const
            }

            pending.delete(input.requestID)
            yield* events.publish(Event.Replied, {
              sessionID: existing.info.sessionID,
              requestID: existing.info.id,
              reply: input.reply,
            })
            if (input.reply === "once") yield* Deferred.succeed(existing.deferred, undefined)
            else {
              yield* Deferred.fail(
                existing.deferred,
                input.message
                  ? new PermissionV1.CorrectedError({ feedback: input.message })
                  : new PermissionV1.RejectedError(),
              )
            }
            return { status: "applied", request: existing.info } as const
          }),
        ),
      ),
    )

    const list = Effect.fn("Permission.list")(() =>
      hostedApproval.withConditionalReply(mutex.withPermit(
        Effect.gen(function* () {
          const pending = (yield* InstanceState.get(state)).pending
          return Array.from(pending.values(), (item) => item.info)
        }),
      )),
    )

    const hostedList = Effect.fn("Permission.hostedList")(() =>
      mutex.withPermit(
        Effect.gen(function* () {
          const pending = (yield* InstanceState.get(state)).pending
          return Array.from(pending.values(), ({ info: request, sessionIncarnation, requestIncarnation }) => ({
            request,
            sessionIncarnation,
            requestIncarnation,
          }))
        }),
      ),
    )

    return Service.of({ ask, reply, conditionalReply, list, hostedList })
  }),
)

function expand(pattern: string): string {
  if (pattern.startsWith("~/")) return os.homedir() + pattern.slice(1)
  if (pattern === "~") return os.homedir()
  if (pattern.startsWith("$HOME/")) return os.homedir() + pattern.slice(5)
  if (pattern.startsWith("$HOME")) return os.homedir() + pattern.slice(5)
  return pattern
}

export function fromConfig(permission: ConfigPermissionV1.Info) {
  const ruleset: PermissionV1.Rule[] = []
  for (const [key, value] of Object.entries(permission)) {
    if (typeof value === "string") {
      ruleset.push({ permission: key, action: value, pattern: "*" })
      continue
    }
    ruleset.push(
      ...Object.entries(value).map(([pattern, action]) => ({ permission: key, pattern: expand(pattern), action })),
    )
  }
  return ruleset
}

export function merge(...rulesets: PermissionV1.Ruleset[]): PermissionV1.Rule[] {
  return rulesets.flat()
}

export function disabled(tools: string[], ruleset: PermissionV1.Ruleset): Set<string> {
  const edits = ["edit", "write", "apply_patch"]
  const reads = ["list_mcp_resources", "list_mcp_resource_templates", "read_mcp_resource"]
  return new Set(
    tools.filter((tool) => {
      const permission = edits.includes(tool) ? "edit" : reads.includes(tool) ? "read" : tool
      const rule = ruleset.findLast((rule) => Wildcard.match(permission, rule.permission))
      return rule?.pattern === "*" && rule.action === "deny"
    }),
  )
}

export function visibleTools<T>(tools: Record<string, T>, ruleset: PermissionV1.Ruleset): Record<string, T> {
  const hidden = disabled(Object.keys(tools), ruleset)
  return Object.fromEntries(Object.entries(tools).filter(([name]) => !hidden.has(name)))
}

export const node = LayerNode.make({
  service: Service,
  layer: layer,
  deps: [EventV2Bridge.node, HostedApprovalCoordinator.node],
})

export * as Permission from "."
