import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { Permission } from "@/permission"
import { Schema } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiError, HttpApiGroup, OpenApi } from "effect/unstable/httpapi"
import { PermissionNotFoundError } from "../errors"
import { Authorization } from "../middleware/authorization"
import { InstanceContextMiddleware } from "../middleware/instance-context"
import { WorkspaceRoutingMiddleware, WorkspaceRoutingQuery } from "../middleware/workspace-routing"
import { described } from "./metadata"

const RuntimeInstanceId = Schema.String.check(Schema.isPattern(/^runtime_instance_[0-9a-f]{32}$/))
const ConfigGeneration = Schema.String.check(Schema.isPattern(/^config_generation_[0-9a-f]{32}$/))
const RequestIncarnation = Schema.String.check(Schema.isPattern(/^request_incarnation_[0-9a-f]{32}$/))
const HostedDigest = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/))
export class HostedConflictError extends Schema.TaggedErrorClass<HostedConflictError>()(
  "HostedApprovalConflict",
  {},
  { httpApiStatus: 409 },
) {}
export class HostedPreconditionError extends Schema.TaggedErrorClass<HostedPreconditionError>()(
  "HostedApprovalPreconditionFailed",
  {},
  { httpApiStatus: 412 },
) {}
export class HostedUnavailableError extends Schema.TaggedErrorClass<HostedUnavailableError>()(
  "HostedApprovalUnavailable",
  {},
  { httpApiStatus: 404 },
) {}
export const HostedReplyPayload = Schema.Struct({
  schemaVersion: Schema.Literal(2),
  protocol: Schema.Literal("agent-teams-hosted-approval-v2"),
  runtimeInstanceId: RuntimeInstanceId,
  expectedConfigGeneration: ConfigGeneration,
  requestId: PermissionV1.ID,
  sessionId: Schema.String,
  requestIncarnation: RequestIncarnation,
  expectedPermissionDigest: HostedDigest,
  decision: Schema.Union([Schema.Literal("allow_once"), Schema.Literal("reject")]),
})
const HostedCapability = Schema.Struct({
  schemaVersion: Schema.Literal(2),
  protocol: Schema.Literal("agent-teams-hosted-approval-v2"),
  runtimeInstanceId: RuntimeInstanceId,
  configGeneration: ConfigGeneration,
  authentication: Schema.Literal("opencode-basic"),
})
const HostedObserve = Schema.Struct({
  schemaVersion: Schema.Literal(2),
  protocol: Schema.Literal("agent-teams-hosted-approval-v2"),
  runtimeInstanceId: RuntimeInstanceId,
  configGeneration: ConfigGeneration,
  sessionId: Schema.String,
  permissions: Schema.Array(Schema.Struct({
    requestId: PermissionV1.ID,
    sessionId: Schema.String,
    requestIncarnation: RequestIncarnation,
    permissionDigest: HostedDigest,
    rawPermission: PermissionV1.Request,
  })),
})
const HostedReplySuccess = Schema.Struct({
  schemaVersion: Schema.Literal(2),
  protocol: Schema.Literal("agent-teams-hosted-approval-v2"),
  status: Schema.Literal("applied"),
  runtimeInstanceId: RuntimeInstanceId,
  configGeneration: ConfigGeneration,
  requestId: PermissionV1.ID,
  sessionId: Schema.String,
  requestIncarnation: RequestIncarnation,
  permissionDigest: HostedDigest,
  decision: Schema.Union([Schema.Literal("allow_once"), Schema.Literal("reject")]),
})

const root = "/permission"
const ReplyPayload = Schema.Struct({
  reply: PermissionV1.Reply,
  message: Schema.optional(Schema.String),
})

export const PermissionApi = HttpApi.make("permission")
  .add(
    HttpApiGroup.make("permission")
      .add(
        HttpApiEndpoint.get("hostedCapability", "/experimental/agent-teams/hosted-approval-capability", {
          query: WorkspaceRoutingQuery,
          success: HostedCapability,
          error: HostedUnavailableError,
        }),
        HttpApiEndpoint.get("hostedObserve", "/experimental/agent-teams/hosted-approval/session/:sessionID/permissions", {
          params: { sessionID: Schema.String },
          query: WorkspaceRoutingQuery,
          success: HostedObserve,
          error: Schema.Union([HostedUnavailableError, HttpApiError.InternalServerError]),
        }),
        HttpApiEndpoint.post("hostedReply", "/experimental/agent-teams/hosted-approval/session/:sessionID/permission/:requestID/reply", {
          params: { sessionID: Schema.String, requestID: PermissionV1.ID },
          query: WorkspaceRoutingQuery,
          payload: HostedReplyPayload,
          success: HostedReplySuccess,
          error: Schema.Union([HttpApiError.BadRequest, HostedConflictError, HostedPreconditionError, HostedUnavailableError]),
        }),
        HttpApiEndpoint.get("list", root, {
          query: WorkspaceRoutingQuery,
          success: described(Schema.Array(PermissionV1.Request), "List of pending permissions"),
        }).annotateMerge(
          OpenApi.annotations({
            identifier: "permission.list",
            summary: "List pending permissions",
            description: "Get all pending permission requests across all sessions.",
          }),
        ),
        HttpApiEndpoint.post("reply", `${root}/:requestID/reply`, {
          params: { requestID: PermissionV1.ID },
          query: WorkspaceRoutingQuery,
          payload: ReplyPayload,
          success: described(Schema.Boolean, "Permission processed successfully"),
          error: [HttpApiError.BadRequest, PermissionNotFoundError],
        }).annotateMerge(
          OpenApi.annotations({
            identifier: "permission.reply",
            summary: "Respond to permission request",
            description: "Approve or deny a permission request from the AI assistant.",
          }),
        ),
      )
      .annotateMerge(
        OpenApi.annotations({
          title: "permission",
          description: "Experimental HttpApi permission routes.",
        }),
      )
      .middleware(InstanceContextMiddleware)
      .middleware(WorkspaceRoutingMiddleware)
      .middleware(Authorization),
  )
  .annotateMerge(
    OpenApi.annotations({
      title: "opencode experimental HttpApi",
      version: "0.0.1",
      description: "Experimental HttpApi surface for selected instance routes.",
    }),
  )
