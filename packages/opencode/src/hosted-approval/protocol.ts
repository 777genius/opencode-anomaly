import { createHash } from "node:crypto"

export const protocol = "agent-teams-hosted-approval-v2" as const

export function canonicalJson(value: unknown): string {
  if (value === null || typeof value === "string" || typeof value === "boolean" || typeof value === "number") {
    const encoded = JSON.stringify(value)
    if (encoded === undefined) throw new TypeError("Unsupported hosted approval canonical value")
    return encoded
  }
  if (Array.isArray(value)) return `[${Array.from(value, canonicalJson).join(",")}]`
  if (!value || typeof value !== "object") throw new TypeError("Unsupported hosted approval canonical value")
  const record = value as Record<string, unknown>
  return `{${Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
    .join(",")}}`
}

export function digest(value: unknown): string {
  return createHash("sha256").update(canonicalJson(value), "utf8").digest("hex")
}

export function rawPermission<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T
}
