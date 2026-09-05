import { Effect } from "effect"
import { effectCmd } from "../effect-cmd"
import { withNetworkOptions, resolveNetworkOptions } from "../network"
import { Flag } from "@opencode-ai/core/flag/flag"
import { HostedApprovalProvenance } from "@/hosted-approval/provenance"

export const ServeCommand = effectCmd({
  command: "serve",
  builder: (yargs) => withNetworkOptions(yargs),
  describe: "starts a headless opencode server",
  // Server loads instances per-request via x-opencode-directory header — no
  // need for an ambient project InstanceContext at startup.
  instance: false,
  handler: Effect.fn("Cli.serve")(function* (args) {
    const signal = shutdownSignal(HostedApprovalProvenance.current() !== null)
    yield* Effect.gen(function* () {
      const { Server } = yield* Effect.promise(() => import("../../server/server"))
      if (!Flag.OPENCODE_SERVER_PASSWORD) {
        console.log("Warning: OPENCODE_SERVER_PASSWORD is not set; server is unsecured.")
      }
      const opts = yield* resolveNetworkOptions(args)
      const server = yield* Effect.promise(() => Server.listen(opts))
      console.log(`opencode server listening on http://${server.hostname}:${server.port}`)

      yield* Effect.promise(() => signal.wait).pipe(Effect.ensuring(Effect.promise(() => server.stop())))
    }).pipe(Effect.ensuring(Effect.sync(signal.close)))
  }),
})

function shutdownSignal(active: boolean) {
  if (!active) return { wait: new Promise<void>(() => {}), close: () => {} }
  let complete = () => {}
  const wait = new Promise<void>((resolve) => {
    complete = resolve
  })
  const shutdown = () => complete()
  process.on("SIGINT", shutdown)
  process.on("SIGTERM", shutdown)
  return {
    wait,
    close: () => {
      process.off("SIGINT", shutdown)
      process.off("SIGTERM", shutdown)
    },
  }
}
