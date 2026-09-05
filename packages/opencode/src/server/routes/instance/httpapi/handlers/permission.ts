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
import { HostedApprovalProvenance, sha256, type Producer } from "@/hosted-approval/provenance"

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
  "sessionIncarnation",
] as const

export class HostedReplyBodyError extends Error {}

export const readHostedReplyBody = Effect.fn("PermissionHttpApi.readHostedReplyBody")(
  <E>(stream: Stream.Stream<Uint8Array, E>, limit = 16 * 1024) =>
    Stream.runFoldEffect(
      stream,
      () => ({ chunks: [] as Uint8Array[], size: 0 }),
      (state, chunk) => {
        if (state.size + chunk.byteLength > limit) return Effect.fail(new HostedReplyBodyError("body too large"))
        state.chunks.push(chunk)
        return Effect.succeed({ chunks: state.chunks, size: state.size + chunk.byteLength })
      },
    ).pipe(
      Effect.mapError((error) => error instanceof HostedReplyBodyError ? error : new HostedReplyBodyError("body read failed")),
      Effect.map((state) => {
        const bytes = new Uint8Array(state.size)
        let offset = 0
        for (const chunk of state.chunks) { bytes.set(chunk, offset); offset += chunk.byteLength }
        return bytes
      }),
    ),
)

function emitRaw(
  provenance: Producer | null,
  operationNonce: string | undefined,
  params: { sessionID: string; requestID: PermissionV1.ID },
  outcome:
    | "unavailable"
    | "body-read-failed"
    | "invalid-json"
    | "invalid-schema"
    | "bad-request"
    | "conflict"
    | "precondition-failed",
  requestBodySha256: string | null,
  status: 400 | 404 | 409 | 412,
) {
  provenance?.emit("openCodeTimeline", {
    recordType: "hosted-reply-raw",
    operationNonce: operationNonce!,
    native: {
      configGeneration: null,
      outcome,
      requestBodySha256,
      requestId: params.requestID,
      requestIncarnation: null,
      responseSha256: sha256(new Uint8Array()),
      runtimeInstanceId: null,
      sessionId: params.sessionID,
      sessionIncarnation: null,
      status,
    },
  })
}

function emitTypedFailure(
  provenance: Producer | null,
  operationNonce: string | undefined,
  payload: typeof HostedReplyPayload.Type,
  outcome: "bad-request" | "conflict" | "precondition-failed",
  status: 400 | 409 | 412,
) {
  provenance?.emit("openCodeTimeline", {
    recordType: "hosted-reply",
    operationNonce: operationNonce!,
    native: {
      configGeneration: null,
      decision: payload.decision,
      outcome,
      permissionDigest: payload.expectedPermissionDigest,
      requestId: payload.requestId,
      requestIncarnation: null,
      runtimeInstanceId: null,
      sessionId: payload.sessionId,
      sessionIncarnation: null,
      status,
    },
  })
}

function requireIdentityEncoding(provenance: Producer | null, request: HttpServerRequest.HttpServerRequest) {
  if (!provenance) return
  if (request.headers["accept-encoding"] === "identity") return
  provenance.poison("producer-provenance-wire-encoding")
}

export const permissionHandlers = HttpApiBuilder.group(InstanceHttpApi, "permission", (handlers) =>
  Effect.gen(function* () {
    const svc = yield* Permission.Service
    const hostedApproval = yield* HostedApprovalCoordinator.Service
    const provenance = (yield* HostedApprovalProvenance.Service).producer
    const serverAuth = yield* ServerAuth.Config

    const requireHosted = Effect.fn("PermissionHttpApi.requireHosted")(function* () {
      if (!ServerAuth.required(serverAuth)) return yield* new HostedUnavailableError()
      return yield* Effect.void
    })

    const hostedCapability = Effect.fn("PermissionHttpApi.hostedCapability")(function* (ctx: {
      request: HttpServerRequest.HttpServerRequest
    }) {
      requireIdentityEncoding(provenance, ctx.request)
      const unavailable = yield* requireHosted().pipe(
        Effect.match({ onFailure: () => true, onSuccess: () => false }),
      )
      if (unavailable) return HttpServerResponse.jsonUnsafe({ _tag: "HostedApprovalUnavailable" }, { status: 404 })
      return yield* hostedApproval.withConditionalReply(
        Effect.sync(() => {
          const body = {
            schemaVersion: 2 as const,
            protocol,
            ...hostedApproval.snapshot(),
            authentication: "opencode-basic" as const,
          }
          const response = HttpServerResponse.jsonUnsafe(body)
          provenance?.emit("openCodeTimeline", {
            recordType: "hosted-capability",
            operationNonce: provenance.operationNonce(),
            native: {
              configGeneration: body.configGeneration,
              outcome: "ok",
              responseSha256: sha256(JSON.stringify(body)),
              runtimeInstanceId: body.runtimeInstanceId,
              status: 200,
            },
          })
          return response
        }),
      )
    })

    const hostedObserve = Effect.fn("PermissionHttpApi.hostedObserve")(function* (ctx: {
      params: { sessionID: string }
      request: HttpServerRequest.HttpServerRequest
    }) {
      requireIdentityEncoding(provenance, ctx.request)
      const unavailable = yield* requireHosted().pipe(
        Effect.match({ onFailure: () => true, onSuccess: () => false }),
      )
      if (unavailable) return HttpServerResponse.jsonUnsafe({ _tag: "HostedApprovalUnavailable" }, { status: 404 })
      return yield* hostedApproval.withConditionalReply(
        Effect.gen(function* () {
          const snapshot = hostedApproval.snapshot()
          const pending = yield* svc.hostedList()
          const permissions = pending
            .filter(({ request }) => request.sessionID === ctx.params.sessionID)
            .map(({ request, sessionIncarnation, requestIncarnation }) => {
              const raw = rawPermission(request)
              return {
                requestId: request.id,
                sessionId: request.sessionID,
                sessionIncarnation,
                requestIncarnation,
                permissionDigest: digest(raw),
                rawPermission: raw,
              }
            })
          const response = {
            schemaVersion: 2 as const,
            protocol,
            ...snapshot,
            sessionId: ctx.params.sessionID,
            permissions,
          }
          const bytes = Buffer.byteLength(JSON.stringify(response), "utf8")
          if (permissions.length > 256 || bytes > 1024 * 1024) {
            yield* Effect.logWarning("hosted approval observe overflow", {
              sessionIdentity: digest(ctx.params.sessionID).slice(0, 16),
              droppedCount: permissions.length,
              droppedBytes: bytes,
            })
            const error = { _tag: "InternalServerError" }
            const finalized = HttpServerResponse.jsonUnsafe(error, { status: 500 })
            provenance?.emit("openCodeTimeline", {
              recordType: "hosted-observe",
              operationNonce: provenance.operationNonce(),
              native: {
                configGeneration: response.configGeneration,
                outcome: "overflow",
                permissionCount: permissions.length,
                responseSha256: sha256(JSON.stringify(error)),
                runtimeInstanceId: response.runtimeInstanceId,
                sessionId: response.sessionId,
                status: 500,
              },
            })
            return finalized
          }
          const finalized = HttpServerResponse.jsonUnsafe(response)
          provenance?.emit("openCodeTimeline", {
            recordType: "hosted-observe",
            operationNonce: provenance.operationNonce(),
            native: {
              configGeneration: response.configGeneration,
              outcome: "ok",
              permissionCount: permissions.length,
              responseSha256: sha256(JSON.stringify(response)),
              runtimeInstanceId: response.runtimeInstanceId,
              sessionId: response.sessionId,
              status: 200,
            },
          })
          return finalized
        }),
      )
    })

    const hostedReply = Effect.fn("PermissionHttpApi.hostedReply")(function* (ctx: {
      params: { sessionID: string; requestID: PermissionV1.ID }
      payload: typeof HostedReplyPayload.Type
      operationNonce?: string
    }) {
      yield* requireHosted()
      if (ctx.params.requestID !== ctx.payload.requestId || ctx.params.sessionID !== ctx.payload.sessionId) {
        return yield* new HttpApiError.BadRequest()
      }
      return yield* Effect.gen(function* () {
          const snapshot = hostedApproval.snapshot()
          if (
            snapshot.runtimeInstanceId !== ctx.payload.runtimeInstanceId ||
            snapshot.configGeneration !== ctx.payload.expectedConfigGeneration
          ) return yield* new HostedPreconditionError()

          const result = yield* svc.conditionalReply({
            requestID: ctx.params.requestID,
            sessionID: ctx.payload.sessionId as PermissionV1.Request["sessionID"],
            sessionIncarnation: ctx.payload.sessionIncarnation,
            requestIncarnation: ctx.payload.requestIncarnation,
            reply: ctx.payload.decision === "allow_once" ? "once" : "reject",
            matches: (request) => digest(rawPermission(request)) === ctx.payload.expectedPermissionDigest,
            provenanceOperationNonce: ctx.operationNonce,
          })
          if (result.status === "mismatch") return yield* new HostedConflictError()
          return {
            schemaVersion: 2 as const,
            protocol,
            status: "applied" as const,
            runtimeInstanceId: result.runtimeInstanceId,
            configGeneration: result.configGeneration,
            requestId: result.request.id,
            sessionId: result.request.sessionID,
            sessionIncarnation: result.sessionIncarnation,
            requestIncarnation: result.requestIncarnation,
            permissionDigest: result.permissionDigest,
            decision: result.reply === "once" ? "allow_once" as const : "reject" as const,
          }
        })
    })

    const hostedReplyRaw = Effect.fn("PermissionHttpApi.hostedReplyRaw")(function* (ctx: {
      params: { sessionID: string; requestID: PermissionV1.ID }
      request: HttpServerRequest.HttpServerRequest
    }) {
      const operationNonce = provenance?.operationNonce()
      requireIdentityEncoding(provenance, ctx.request)
      const unavailable = yield* requireHosted().pipe(
        Effect.match({ onFailure: () => true, onSuccess: () => false }),
      )
      if (unavailable) {
        const response = HttpServerResponse.empty({ status: 404 })
        emitRaw(provenance, operationNonce, ctx.params, "unavailable", null, 404)
        return response
      }
      const bytes = yield* readHostedReplyBody(ctx.request.stream).pipe(
        Effect.catch(() => Effect.succeed(undefined)),
      )
      if (!bytes) {
        const response = HttpServerResponse.empty({ status: 400 })
        emitRaw(provenance, operationNonce, ctx.params, "body-read-failed", null, 400)
        return response
      }
      const requestBodySha256 = sha256(bytes)
      let text: string
      try {
        text = new TextDecoder("utf-8", { fatal: true }).decode(bytes)
        rejectDuplicateJsonKeys(text)
      } catch {
        const response = HttpServerResponse.empty({ status: 400 })
        emitRaw(provenance, operationNonce, ctx.params, "invalid-json", requestBodySha256, 400)
        return response
      }
      let untyped: unknown
      try { untyped = JSON.parse(text) } catch {
        const response = HttpServerResponse.empty({ status: 400 })
        emitRaw(provenance, operationNonce, ctx.params, "invalid-json", requestBodySha256, 400)
        return response
      }
      if (!untyped || typeof untyped !== "object" || Array.isArray(untyped)) {
        const response = HttpServerResponse.empty({ status: 400 })
        emitRaw(provenance, operationNonce, ctx.params, "invalid-schema", requestBodySha256, 400)
        return response
      }
      const keys = Object.keys(untyped).sort()
      if (keys.length !== HOSTED_REPLY_KEYS.length || keys.some((key, index) => key !== HOSTED_REPLY_KEYS[index])) {
        const response = HttpServerResponse.empty({ status: 400 })
        emitRaw(provenance, operationNonce, ctx.params, "invalid-schema", requestBodySha256, 400)
        return response
      }
      const payload = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(HostedReplyPayload))(text, {
        onExcessProperty: "error",
      }).pipe(
        Effect.catch(() => Effect.succeed(undefined)),
      )
      if (!payload) {
        const response = HttpServerResponse.empty({ status: 400 })
        emitRaw(provenance, operationNonce, ctx.params, "invalid-schema", requestBodySha256, 400)
        return response
      }
      return yield* hostedApproval.withConditionalReply(Effect.uninterruptible(Effect.gen(function* () {
        const result = yield* hostedReply({ params: ctx.params, payload, operationNonce }).pipe(
          Effect.map((response) => ({ response } as const)),
          Effect.catchTags({
            BadRequest: () => Effect.succeed({ outcome: "bad-request", status: 400 } as const),
            HostedApprovalConflict: () => Effect.succeed({ outcome: "conflict", status: 409 } as const),
            HostedApprovalPreconditionFailed: () => Effect.succeed({ outcome: "precondition-failed", status: 412 } as const),
          }),
        )
        if (!("response" in result)) {
          const response = HttpServerResponse.empty({ status: result.status })
          emitRaw(provenance, operationNonce, ctx.params, result.outcome, requestBodySha256, result.status)
          emitTypedFailure(provenance, operationNonce, payload, result.outcome, result.status)
          return response
        }
        const responseSha256 = sha256(JSON.stringify(result.response))
        const response = HttpServerResponse.jsonUnsafe(result.response)
        provenance?.emit("openCodeTimeline", {
          recordType: "hosted-reply-raw",
          operationNonce: operationNonce!,
          native: {
            configGeneration: result.response.configGeneration,
            outcome: "applied",
            requestBodySha256,
            requestId: result.response.requestId,
            requestIncarnation: result.response.requestIncarnation,
            responseSha256,
            runtimeInstanceId: result.response.runtimeInstanceId,
            sessionId: result.response.sessionId,
            sessionIncarnation: result.response.sessionIncarnation,
            status: 200,
          },
        })
        provenance?.emit("openCodeTimeline", {
          recordType: "hosted-reply",
          operationNonce: operationNonce!,
          native: {
            configGeneration: result.response.configGeneration,
            decision: result.response.decision,
            outcome: "applied",
            permissionDigest: result.response.permissionDigest,
            requestId: result.response.requestId,
            requestIncarnation: result.response.requestIncarnation,
            responseSha256,
            runtimeInstanceId: result.response.runtimeInstanceId,
            sessionId: result.response.sessionId,
            sessionIncarnation: result.response.sessionIncarnation,
            status: 200,
          },
        })
        return response
      })))
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
      .handleRaw("hostedCapability", hostedCapability)
      .handleRaw("hostedObserve", hostedObserve)
      .handleRaw("hostedReply", hostedReplyRaw)
      .handle("list", list)
      .handle("reply", reply)
  }),
)
