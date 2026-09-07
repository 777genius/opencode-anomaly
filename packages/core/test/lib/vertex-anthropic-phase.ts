import { writeSync } from "node:fs"

// Scratch-only opt-in. Never serialize test inputs, errors, environment, or SDK values.
export const vertexAnthropicPhases = process.env.OPENCODE_VERTEX_ANTHROPIC_PHASES === "1"
export const vertexAnthropicPreimport = vertexAnthropicPhases && process.env.OPENCODE_VERTEX_ANTHROPIC_PREIMPORT === "1"
const origin = performance.now()
let records = 0

// Fixed source IDs only. Each returned observer keeps its identity even after a Bun timeout.
export function vertexAnthropicPhase(
  test_id:
    | "case-01"
    | "case-02"
    | "case-03"
    | "case-04"
    | "case-05"
    | "case-06"
    | "case-07"
    | "case-08"
    | "case-09"
    | "non-test:preimport",
) {
  return (phase: string) => {
    if (!vertexAnthropicPhases || records++ >= 256) return
    try {
      writeSync(2, JSON.stringify({ test_id, phase, elapsed_ms: performance.now() - origin }) + "\n")
    } catch {
      // A closed diagnostic stream must not replace a test result or interrupt cleanup.
    }
  }
}
