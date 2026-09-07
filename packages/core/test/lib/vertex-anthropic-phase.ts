import { writeSync } from "node:fs"

// Scratch-only opt-in. Never serialize test inputs, errors, environment, or SDK values.
export const vertexAnthropicPhases = process.env.OPENCODE_VERTEX_ANTHROPIC_PHASES === "1"
export const vertexAnthropicPreimport = vertexAnthropicPhases && process.env.OPENCODE_VERTEX_ANTHROPIC_PREIMPORT === "1"
const origin = performance.now()
let records = 0

export function vertexAnthropicPhase(phase: string) {
  if (!vertexAnthropicPhases || records++ >= 256) return
  try {
    writeSync(2, JSON.stringify({ phase, elapsed_ms: performance.now() - origin }) + "\n")
  } catch {
    // A closed diagnostic stream must not replace a test result or interrupt cleanup.
  }
}
