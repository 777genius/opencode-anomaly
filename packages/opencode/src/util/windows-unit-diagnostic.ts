import fs from "node:fs"
import { Cause, Context, Effect } from "effect"

export const unitDiagnosticEnabled =
  process.platform === "win32" && process.env.OPENCODE_WINDOWS_UNIT_DIAGNOSTICS === "1"
export type DiagnosticOwner = { readonly file: string; readonly test: string; readonly id: number }

export const DiagnosticOwner = Context.Reference<DiagnosticOwner | undefined>("~test/DiagnosticOwner", {
  defaultValue: () => undefined,
})

export function diagnosticContext<A, E, R>(effect: Effect.Effect<A, E, R>, owner?: DiagnosticOwner) {
  if (!unitDiagnosticEnabled) return effect
  return Effect.provideService(effect, DiagnosticOwner, owner)
}
const budget = { records: 0, ids: 0 }

// A process-wide cap bounds CI output; no timers, buffered logger, or raw payloads.
const silent = (_phase: string, _childpid: number | null = null) => {}

export function unitDiagnostic(
  kind: "test" | "run" | "serve" | "acp" | "other" | "hosted" | "preload",
  file?: string,
  owner?: DiagnosticOwner,
) {
  if (!unitDiagnosticEnabled || budget.records >= 20000) return silent
  const id = ++budget.ids
  const start = performance.now()
  return (phase: string, childpid: number | null = null) => {
    if (!unitDiagnosticEnabled || budget.records >= 20000) return
    budget.records++
    try {
      fs.writeSync(
        2,
        JSON.stringify({
          marker: "windows-unit",
          id,
          testid: owner?.id ?? null,
          file: (owner?.file ?? file ?? "unattributed").slice(0, 240),
          test: (owner?.test ?? "unattributed cleanup").slice(0, 240),
          parentpid: process.pid,
          childpid,
          kind,
          elapsedMs: Math.round(performance.now() - start),
          phase: budget.records === 20000 ? "diagnostic.limit" : phase.slice(0, 240),
        }) + "\n",
      )
    } catch {
      // Diagnostics must not turn a closed CI output descriptor into a test failure.
    }
  }
}

// Weak identities correlate deferreds/disposers without emitting directories or state.
const identities = new WeakMap<object, number>()
export function diagnosticIdentity(value: object) {
  if (!unitDiagnosticEnabled) return 0
  const existing = identities.get(value)
  if (existing !== undefined) return existing
  const id = ++budget.ids
  identities.set(value, id)
  return id
}

export function diagnosticError(error: unknown, depth = 0): string {
  try {
    if (typeof error !== "object" || error === null || depth > 3) return "unknown"
    const code = "code" in error ? error.code : undefined
    if (
      typeof code === "string" &&
      ["EACCES", "EPERM", "EBUSY", "ENOENT", "EEXIST", "ENOTEMPTY", "EXDEV"].includes(code)
    )
      return code
    if ("cause" in error) {
      const code = diagnosticError(error.cause, depth + 1)
      if (code !== "unknown" && code !== "other") return code
    }
    if ("reason" in error) {
      const code = diagnosticError(error.reason, depth + 1)
      if (code !== "unknown" && code !== "other") return code
    }
    const tag = "_tag" in error ? error._tag : undefined
    if (
      typeof tag === "string" &&
      [
        "NotFound",
        "PermissionDenied",
        "AlreadyExists",
        "Busy",
        "Unknown",
        "StatusCode",
        "Transport",
        "Decode",
      ].includes(tag)
    )
      return tag
    return "other"
  } catch {
    // Error properties and Proxy traps are untrusted diagnostic input.
    return "unknown"
  }
}

export function diagnosticPhase<A, E, R>(effect: Effect.Effect<A, E, R>, phase: string, scope?: object) {
  if (!unitDiagnosticEnabled) return effect
  return Effect.gen(function* () {
    const owner = yield* DiagnosticOwner
    const mark = unitDiagnostic("other", "measurement", owner)
    const suffix = scope ? `.${diagnosticIdentity(scope)}` : ""
    mark(`${phase}${suffix}.start`)
    return yield* effect.pipe(
      Effect.onExit((exit) =>
        Effect.sync(() => {
          mark(`${phase}${suffix}.${exit._tag === "Success" ? "success" : "failure"}`)
          if (exit._tag === "Failure") {
            try {
              mark(`${phase}${suffix}.error.${diagnosticError(Cause.squash(exit.cause))}`)
            } catch {
              // Squashing itself can throw; observation must preserve the original cause.
              mark(`${phase}${suffix}.error.unknown`)
            }
          }
        }),
      ),
    )
  })
}

export function diagnosticObservation(observation: {
  version: "1" | "3" | "other" | "read-error"
  native: "old" | "new" | "other" | "read-error"
  bun: "old" | "new" | "other" | "read-error"
  downloads: number
  nativeSize: number | null
  nativeMtimeMs: number | null
  bunSize: number
  bunLastModified: number
  errors: string[]
}) {
  if (!unitDiagnosticEnabled || budget.records >= 20000) return
  budget.records++
  try {
    fs.writeSync(2, JSON.stringify({ marker: "mutable-skill-v3", ...observation }) + "\n")
  } catch {
    // Preserve the original assertion if the diagnostic output descriptor is closed.
  }
}
