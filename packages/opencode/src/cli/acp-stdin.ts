import { Effect } from "effect"
import type { Readable } from "node:stream"

export function waitForStdinEnd(stdin: Readable) {
  return Effect.callback<void>((resume) => {
    const cleanup = () => {
      stdin.off("end", end)
      stdin.off("error", error)
    }
    const end = () => {
      cleanup()
      resume(Effect.void)
    }
    const error = (cause: Error) => {
      cleanup()
      resume(Effect.die(cause))
    }
    stdin.once("end", end)
    stdin.once("error", error)
    // The transport can consume EOF before the command starts waiting.
    if (stdin.errored) error(stdin.errored)
    if (!stdin.errored && stdin.readableEnded) end()
    return Effect.sync(cleanup)
  })
}
