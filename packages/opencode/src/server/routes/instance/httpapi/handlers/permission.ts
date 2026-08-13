import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { Permission } from "@/permission"
import { Effect } from "effect"
import { HttpApiBuilder, HttpApiError } from "effect/unstable/httpapi"
import { InstanceHttpApi } from "../api"
import { PermissionNotFoundError } from "../errors"
import { HostedApprovalCoordinator } from "@/hosted-approval/coordinator"
import { digest, protocol, rawPermission } from "@/hosted-approval/protocol"
import { HostedConflictError, HostedPreconditionError, HostedReplyPayload, HostedUnavailableError } from "../groups/permission"
import { ServerAuth } from "@/server/auth"
import { Schema, Stream } from "effect"
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http"

function rejectDuplicateJsonKeys(text: string): void {
  let offset = 0
  const whitespace = () => { while (/\s/.test(text[offset] ?? "")) offset++ }
  const string = () => {
    const start = offset
    if (text[offset++] !== '"') throw new Error("invalid string")
    while (offset < text.length) {
      const char = text[offset++]
      if (char === '"') return JSON.parse(text.slice(start, offset)) as string
      if (char === "\\") offset++
    }
    throw new Error("unterminated string")
  }
  const value = (): void => {
    whitespace()
    if (text[offset] === "{") {
      offset++
      const keys = new Set<string>()
      whitespace()
      if (text[offset] === "}") { offset++; return }
      while (true) {
        whitespace()
        const key = string()
        if (keys.has(key)) throw new Error("duplicate key")
        keys.add(key)
        whitespace()
        if (text[offset++] !== ":") throw new Error("invalid object")
        value()
        whitespace()
        const delimiter = text[offset++]
        if (delimiter === "}") return
        if (delimiter !== ",") throw new Error("invalid object")
      }
    }
    if (text[offset] === "[") {
      offset++
      whitespace()
      if (text[offset] === "]") { offset++; return }
      while (true) {
        value(); whitespace()
        const delimiter = text[offset++]
        if (delimiter === "]") return
        if (delimiter !== ",") throw new Error("invalid array")
      }
    }
    if (text[offset] === '"') { string(); return }
    const match = text.slice(offset).match(/^(?:true|false|null|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/)
    if (!match) throw new Error("invalid value")
    offset += match[0].length
  }
  value(); whitespace()
  if (offset !== text.length) throw new Error("trailing data")
}

const HOSTED_REPLY_KEYS = [
  "decision",
  "expectedConfigGeneration",
  "expectedPermissionDigest",
  "protocol",
  "requestId",
  "requestIncarnation",
  "runtimeInstanceId",
  "schemaVersion",
  "sessionId",
] as const

export class HostedReplyBodyError extends Error {}

export const readHostedReplyBody = Effect.fn("PermissionHttpApi.readHostedReplyBody")(
  <E>(stream: Stream.Stream<Uint8Array, E>, limit = 16 * 1024) =>
    Stream.runFoldEffect(
      stream,
      () => [] as Uint8Array[],
      (chunks, chunk) => {
        const size = chunks.reduce((total, value) => total + value.byteLength, 0)
        if (size + chunk.byteLength > limit) return Effect.fail(new HostedReplyBodyError("body too large"))
        chunks.push(chunk)
        return Effect.succeed(chunks)
      },
    ).pipe(
      Effect.mapError((error) => error instanceof HostedReplyBodyError ? error : new HostedReplyBodyError("body read failed")),
      Effect.map((chunks) => {
        const size = chunks.reduce((total, value) => total + value.byteLength, 0)
        const bytes = new Uint8Array(size)
        let offset = 0
        for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength }
        return bytes
      }),
    ),
)

export const permissionHandlers = HttpApiBuilder.group(InstanceHttpApi, "permission", (handlers) =>
  Effect.gen(function* () {
    const svc = yield* Permission.Service
    const hostedApproval = yield* HostedApprovalCoordinator.Service
    const serverAuth = yield* ServerAuth.Config

    const requireHosted = Effect.fn("PermissionHttpApi.requireHosted")(function* () {
      if (!ServerAuth.required(serverAuth)) {
        return { unavailable: HttpServerResponse.empty({ status: 404 }) } as const
      }
      return { unavailable: undefined } as const
    })

    const hostedCapability = Effect.fn("PermissionHttpApi.hostedCapability")(function* () {
      const availability = yield* requireHosted()
      if (availability.unavailable) return availability.unavailable
      return yield* hostedApproval.withConditionalReply(
        Effect.sync(() => ({
          schemaVersion: 2 as const,
          protocol,
          ...hostedApproval.snapshot(),
          authentication: "opencode-basic" as const,
        })),
      )
    })

    const hostedObserve = Effect.fn("PermissionHttpApi.hostedObserve")(function* (ctx: {
      params: { sessionID: string }
    }) {
      const availability = yield* requireHosted()
      if (availability.unavailable) return availability.unavailable
      return yield* hostedApproval.withConditionalReply(
        Effect.gen(function* () {
          const snapshot = hostedApproval.snapshot()
          const pending = yield* svc.hostedList()
          const permissions = pending
            .filter(({ request }) => request.sessionID === ctx.params.sessionID)
            .map(({ request, requestIncarnation }) => {
              const raw = rawPermission(request)
              return {
                requestId: request.id,
                sessionId: request.sessionID,
                requestIncarnation,
                permissionDigest: digest(raw),
                rawPermission: raw,
              }
            })
          if (permissions.length > 256) return yield* new HttpApiError.InternalServerError()
          const response = {
            schemaVersion: 2 as const,
            protocol,
            ...snapshot,
            sessionId: ctx.params.sessionID,
            permissions,
          }
          if (Buffer.byteLength(JSON.stringify(response), "utf8") > 1024 * 1024) {
            return yield* new HttpApiError.InternalServerError()
          }
          return response
        }),
      )
    })

    const hostedReply = Effect.fn("PermissionHttpApi.hostedReply")(function* (ctx: {
      params: { sessionID: string; requestID: PermissionV1.ID }
      payload: typeof HostedReplyPayload.Type
    }) {
      const availability = yield* requireHosted()
      if (availability.unavailable) return availability.unavailable
      if (ctx.params.requestID !== ctx.payload.requestId || ctx.params.sessionID !== ctx.payload.sessionId) {
        return yield* new HttpApiError.BadRequest()
      }
      return yield* hostedApproval.withConditionalReply(
        Effect.gen(function* () {
          const snapshot = hostedApproval.snapshot()
          if (
            snapshot.runtimeInstanceId !== ctx.payload.runtimeInstanceId ||
            snapshot.configGeneration !== ctx.payload.expectedConfigGeneration
          ) return yield* new HostedPreconditionError()

          const result = yield* svc.conditionalReply({
            requestID: ctx.params.requestID,
            sessionID: ctx.payload.sessionId as PermissionV1.Request["sessionID"],
            requestIncarnation: ctx.payload.requestIncarnation,
            reply: ctx.payload.decision === "allow_once" ? "once" : "reject",
            matches: (request) => digest(rawPermission(request)) === ctx.payload.expectedPermissionDigest,
          })
          if (result.status === "mismatch") return yield* new HostedConflictError()
          return {
            schemaVersion: 2 as const,
            protocol,
            status: "applied" as const,
            runtimeInstanceId: snapshot.runtimeInstanceId,
            configGeneration: snapshot.configGeneration,
            requestId: ctx.payload.requestId,
            sessionId: ctx.payload.sessionId,
            requestIncarnation: ctx.payload.requestIncarnation,
            permissionDigest: ctx.payload.expectedPermissionDigest,
            decision: ctx.payload.decision,
          }
        }),
      )
    })

    const hostedReplyRaw = Effect.fn("PermissionHttpApi.hostedReplyRaw")(function* (ctx: {
      params: { sessionID: string; requestID: PermissionV1.ID }
      request: HttpServerRequest.HttpServerRequest
    }) {
      const availability = yield* requireHosted()
      if (availability.unavailable) return availability.unavailable
      const bytes = yield* readHostedReplyBody(ctx.request.stream).pipe(
        Effect.catch(() => Effect.succeed(undefined)),
      )
      if (!bytes) return HttpServerResponse.empty({ status: 400 })
      let text: string
      try {
        text = new TextDecoder("utf-8", { fatal: true }).decode(bytes)
        rejectDuplicateJsonKeys(text)
      } catch {
        return HttpServerResponse.empty({ status: 400 })
      }
      let untyped: unknown
      try { untyped = JSON.parse(text) } catch { return HttpServerResponse.empty({ status: 400 }) }
      if (!untyped || typeof untyped !== "object" || Array.isArray(untyped)) return HttpServerResponse.empty({ status: 400 })
      const keys = Object.keys(untyped).sort()
      if (keys.length !== HOSTED_REPLY_KEYS.length || keys.some((key, index) => key !== HOSTED_REPLY_KEYS[index])) {
        return HttpServerResponse.empty({ status: 400 })
      }
      const payload = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(HostedReplyPayload))(text, {
        onExcessProperty: "error",
      }).pipe(
        Effect.catch(() => Effect.succeed(undefined)),
      )
      if (!payload) return HttpServerResponse.empty({ status: 400 })
      const result = yield* hostedReply({ params: ctx.params, payload }).pipe(
        Effect.catchTags({
          BadRequest: () => Effect.succeed(HttpServerResponse.empty({ status: 400 })),
          HostedApprovalConflict: () => Effect.succeed(HttpServerResponse.empty({ status: 409 })),
          HostedApprovalPreconditionFailed: () => Effect.succeed(HttpServerResponse.empty({ status: 412 })),
        }),
      )
      if (HttpServerResponse.isHttpServerResponse(result)) return result
      return HttpServerResponse.jsonUnsafe(result)
    })

    const list = Effect.fn("PermissionHttpApi.list")(function* () {
      return yield* svc.list()
    })

    const reply = Effect.fn("PermissionHttpApi.reply")(function* (ctx: {
      params: { requestID: PermissionV1.ID }
      payload: PermissionV1.ReplyBody
    }) {
      yield* svc
        .reply({
          requestID: ctx.params.requestID,
          reply: ctx.payload.reply,
          message: ctx.payload.message,
        })
        .pipe(
          Effect.catchTag("Permission.NotFoundError", (error) =>
            Effect.fail(
              new PermissionNotFoundError({
                requestID: String(error.requestID),
                message: `Permission request not found: ${error.requestID}`,
              }),
            ),
          ),
        )
      return true
    })

    return handlers
      .handle("hostedCapability", hostedCapability)
      .handle("hostedObserve", hostedObserve)
      .handleRaw("hostedReply", hostedReplyRaw)
      .handle("list", list)
      .handle("reply", reply)
  }),
)
