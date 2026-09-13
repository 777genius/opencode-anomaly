import { HttpServerResponse } from "effect/unstable/http"

export const operationNonceHeader = "x-agent-teams-hosted-operation-nonce"

// Call only after every required native fact has synchronously emitted/synced.
// This preserves the finalized body and never allocates an operation.
export function withOperationNonce(response: HttpServerResponse.HttpServerResponse, nonce: string | undefined) {
  if (nonce === undefined) return response
  return HttpServerResponse.setHeader(response, operationNonceHeader, nonce)
}
