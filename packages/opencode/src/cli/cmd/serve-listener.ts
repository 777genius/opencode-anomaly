import { Effect, Exit } from "effect"
import { writeSync } from "node:fs"
import { readinessLine } from "@/hosted-approval/readiness"
import type { Listener } from "@/server/server"

// Resource ownership begins before any startup output. A broken supervisor
// pipe must tear down the listener even when the shutdown signal never comes.
export function serveListener(
  listen: () => Promise<Listener>,
  shutdown: Promise<void>,
  write: (bytes: Uint8Array) => number = (bytes) => writeSync(process.stdout.fd, bytes),
) {
  return Effect.acquireUseRelease(
    Effect.promise(listen),
    (listener) =>
      Effect.gen(function* () {
        yield* Effect.sync(() => {
          console.log(`opencode server listening on http://${listener.hostname}:${listener.port}`)
          if (listener.hostedApprovalReadiness === undefined) return
          const bytes = readinessLine(listener.hostedApprovalReadiness)
          // One bounded write on the existing stdout pipe. A short write or
          // EAGAIN is a failed publication; never print a replacement record.
          if (write(bytes) !== bytes.byteLength) throw new Error("hosted-approval-readiness-write")
        })
        yield* Effect.promise(() => shutdown)
      }),
    (server, exit) => Effect.promise(() => server.stop(Exit.isFailure(exit))),
  )
}
