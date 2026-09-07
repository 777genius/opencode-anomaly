import { writeSync } from "node:fs"
import { Context, Effect } from "effect"

export const unitDiagnosticEnabled = process.platform === "win32" && process.env.OPENCODE_WINDOWS_UNIT_DIAGNOSTICS === "1"
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
      writeSync(2, JSON.stringify({
        marker: "windows-unit", id, testid: owner?.id ?? null,
        file: (owner?.file ?? file ?? "unattributed").slice(0, 240),
        test: (owner?.test ?? "unattributed cleanup").slice(0, 240),
        parentpid: process.pid, childpid, kind,
        elapsedMs: Math.round(performance.now() - start),
        phase: budget.records === 20000 ? "diagnostic.limit" : phase,
      }) + "\n")
    } catch {
      // Diagnostics must not turn a closed CI output descriptor into a test failure.
    }
  }
}

export function diagnosticTest<A>(name: string, run: (owner?: DiagnosticOwner) => Promise<A>) {
  if (!unitDiagnosticEnabled) return run
  // Capture only the repository-relative test frame, never emit a raw stack.
  const file = new Error().stack?.replaceAll("\\", "/").match(/(?:packages\/opencode\/)?test\/[^\s():]+\.test\.tsx?/)?.[0] ?? "unattributed"
  return async () => {
    const owner = { file, test: name, id: ++budget.ids }
    const mark = unitDiagnostic("test", undefined, owner)
    mark("test.entry")
    try {
      return await run(owner)
    } finally {
      mark("scope.settled")
    }
  }
}

// Bun may supply a done callback. Keep that argument outside the explicit owner boundary,
// including when diagnosticTest preserves the original callback's disabled identity.
export function diagnosticRegistration<A>(name: string, run: (owner?: DiagnosticOwner) => Promise<A>) {
  const callback = diagnosticTest(name, run)
  return () => callback()
}

export function diagnosticBody<A, E, R>(effect: Effect.Effect<A, E, R>, phase = "body") {
  if (!unitDiagnosticEnabled) return effect
  return Effect.gen(function* () {
    const owner = yield* DiagnosticOwner
    const mark = unitDiagnostic("test", undefined, owner)
    mark(`${phase}.entry`)
    return yield* effect.pipe(Effect.onExit(() => Effect.sync(() => mark(`${phase}.settled`))))
  })
}

// Invoke synchronously at the caller's original boundary. A thrown defect must
// settle the marker too; onExit only covers an Effect successfully returned by fn.
export function diagnosticCallback<A, E, R>(fn: () => Effect.Effect<A, E, R>, owner?: DiagnosticOwner) {
  if (!unitDiagnosticEnabled) return fn()
  const mark = unitDiagnostic("test", undefined, owner)
  mark("callback.entry")
  try {
    return fn().pipe(Effect.onExit(() => Effect.sync(() => mark("callback.settled"))))
  } catch (error) {
    mark("callback.settled")
    throw error
  }
}

export function diagnosticDrain<A, E, R>(
  effect: Effect.Effect<A, E, R>,
  mark: ReturnType<typeof unitDiagnostic>,
  pipe: "stdout" | "stderr",
  pid: number,
) {
  if (!unitDiagnosticEnabled) return effect
  return Effect.suspend(() => {
    mark(`${pipe}.drain.start`, pid)
    return effect.pipe(
      Effect.onExit((exit) =>
        Effect.sync(() => mark(`${pipe}.drain.${exit._tag === "Success" ? "complete" : "interrupted-or-failed"}`, pid)),
      ),
    )
  })
}

export function diagnosticExit(promise: Promise<number>, mark: ReturnType<typeof unitDiagnostic>, pid: number) {
  if (!unitDiagnosticEnabled) return promise
  mark("exit.wait", pid)
  void promise.then(() => mark("exited", pid), () => mark("exit.rejected", pid))
  return promise
}

export function diagnosticAcp(mark: ReturnType<typeof unitDiagnostic>, pid: number) {
  const buffer = { tail: "", oversized: false }
  return (chunk: string) => {
    if (!unitDiagnosticEnabled) return
    // Retain at most 512 characters; discard an oversized line through its newline.
    for (const part of chunk.match(/[^\n]*\n|[^\n]+$/g) ?? []) {
      const complete = part.endsWith("\n")
      if (buffer.tail.length + part.length > 512) buffer.oversized = true
      if (!buffer.oversized) buffer.tail += part
      if (!complete) continue
      const match = buffer.oversized ? null : buffer.tail.match(
        /^\[acp-profile\] (cli\.acp\.(?:runtime\.(?:start|ready)|instance\.(?:load|dispose)\.(?:start|complete)|handler|connection\.create|stdin\.(?:end|wait|complete)))\.mark \d+ms(?: (?:ended=(?:true|false)|uptimeMs=\d+))?\r?\n$/,
      )
      buffer.tail = ""
      buffer.oversized = false
      if (match) mark(`profile.${match[1]}`, pid)
    }
  }
}

export function diagnosticText(
  promise: Promise<string>,
  mark: ReturnType<typeof unitDiagnostic>,
  pipe: "stdout" | "stderr",
  pid: number,
) {
  if (!unitDiagnosticEnabled) return
  void promise.then(() => mark(`${pipe}.drain.complete`, pid), () => mark(`${pipe}.drain.failed`, pid))
}
