import { isIP } from "node:net"
import type { Snapshot } from "./coordinator"

export const readinessFormat = "agent-teams.opencode.hosted-approval-readiness/v1"
export const readinessMaxBytes = 1024

export type Readiness = Readonly<
  Snapshot & {
    endpoint: Readonly<{
      protocol: "http:"
      address: string
      port: number
      baseUrl: string
    }>
  }
>

// A local startup observation, not a native operation or an authentication
// claim. The supervisor binds this output pipe to the child it actually owns.
export function readinessLine(value: Readiness) {
  if (
    typeof value.runtimeInstanceId !== "string" ||
    value.runtimeInstanceId.length !== 49 ||
    !/^runtime_instance_[0-9a-f]{32}$/.test(value.runtimeInstanceId) ||
    typeof value.configGeneration !== "string" ||
    value.configGeneration.length !== 50 ||
    !/^config_generation_[0-9a-f]{32}$/.test(value.configGeneration) ||
    value.endpoint.protocol !== "http:" ||
    !isIP(value.endpoint.address) ||
    !Number.isSafeInteger(value.endpoint.port) ||
    value.endpoint.port < 1 ||
    value.endpoint.port > 65535
  ) {
    throw new Error("hosted-approval-readiness-invalid")
  }
  const host = isIP(value.endpoint.address) === 6 ? `[${value.endpoint.address}]` : value.endpoint.address
  const endpoint = new URL(`http://${host}:${value.endpoint.port}`)
  if (value.endpoint.baseUrl !== endpoint.origin) throw new Error("hosted-approval-readiness-endpoint")
  // Select fields explicitly so no configuration, credentials, or capsule
  // contents can leak through object spreading into the output channel.
  const bytes = Buffer.from(
    `${JSON.stringify({
      format: readinessFormat,
      version: 1,
      runtimeInstanceId: value.runtimeInstanceId,
      configGeneration: value.configGeneration,
      endpoint: {
        protocol: "http:",
        address: value.endpoint.address,
        port: value.endpoint.port,
        baseUrl: value.endpoint.baseUrl,
      },
    })}\n`,
  )
  if (bytes.byteLength > readinessMaxBytes) throw new Error("hosted-approval-readiness-bounded")
  return bytes
}
