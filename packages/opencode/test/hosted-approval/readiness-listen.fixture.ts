// Isolated real singleton/listener fixture. The parent supplies temporary
// append-only native files and a clean environment; no coordinator is faked.
import assert from "node:assert/strict"
import { fstatSync } from "node:fs"
import { createServer } from "node:net"
import { Effect } from "effect"
import { Server } from "../../src/server/server"
import { HostedApprovalCoordinator } from "../../src/hosted-approval/coordinator"
import { serveListener } from "../../src/cli/cmd/serve-listener"
import {
  canonicalJson, close, contract, contractSha256, createNodeOperations,
  environmentKey, implementationId, initialize,
} from "../../src/hosted-approval/provenance"
import { operationNonceHeader } from "../../src/hosted-approval/operation-header"

const configured = process.argv[2] === "configured"
if (configured) {
  const identity = createNodeOperations().deriveIdentity(import.meta.path)
  const descriptor = (fd: number) => {
    const stat = fstatSync(fd, { bigint: true })
    return { fd, device: stat.dev.toString(), inode: stat.ino.toString() }
  }
  await initialize({
    [environmentKey]: canonicalJson({
      activation: { controllerNonce: "a".repeat(64), runId: "run_readiness_fixture", stackManifestSha256: "b".repeat(64) },
      contract, contractSha256,
      expectedProducer: {
        artifactManifestSha256: "c".repeat(64), executableSha256: identity.exeSha256,
        implementationId, moduleSha256: identity.moduleSha256,
      },
      producerRole: "opencode",
      streams: { openCodeTimeline: descriptor(9), protectedEffectLedger: descriptor(10) }, version: 2,
    }),
  }, import.meta.path)
}

const coordinator = await Effect.runPromise(HostedApprovalCoordinator.Service.pipe(Effect.provide(HostedApprovalCoordinator.layer)))
const before = coordinator.snapshot()
await Effect.runPromise(coordinator.withConfigMutation(Effect.void))
assert.notEqual(coordinator.snapshot().configGeneration, before.configGeneration)

// Reserve the preferred port if free, proving readiness reports the actual
// fallback endpoint. An already occupied port proves the same fallback.
const blocker = createServer()
const occupied = await new Promise<boolean>((resolve) => {
  blocker.once("error", () => resolve(false))
  blocker.listen(4096, "127.0.0.1", () => resolve(true))
})
try {
  const listener = await Server.listen({ hostname: "127.0.0.1", port: 0 })
  assert.notEqual(listener.port, 4096)
  if (!configured) {
    assert.equal(Object.hasOwn(listener, "hostedApprovalReadiness"), false)
    await Effect.runPromise(serveListener(() => Promise.resolve(listener), Promise.resolve(), () => { throw new Error("ordinary serve wrote readiness") }))
    assert.equal(Server.url, undefined)
  }
  if (configured) {
    const snapshot = listener.hostedApprovalReadiness
    assert(snapshot)
    assert(Object.isFrozen(snapshot))
    assert(Object.isFrozen(snapshot.endpoint))
    assert.equal(snapshot.runtimeInstanceId, coordinator.snapshot().runtimeInstanceId)
    assert.equal(snapshot.configGeneration, coordinator.snapshot().configGeneration)
    assert.deepEqual(snapshot.endpoint, { protocol: "http:", address: "127.0.0.1", port: listener.port, baseUrl: listener.url.origin })
    const shutdown = Promise.withResolvers<void>()
    const serving = Effect.runPromise(serveListener(() => Promise.resolve(listener), shutdown.promise))
    try {
      const response = await fetch(new URL("/experimental/agent-teams/hosted-approval-capability", listener.url), {
        headers: {
          authorization: `Basic ${btoa("opencode:readiness-fixture-secret")}`,
          "accept-encoding": "identity", "x-opencode-directory": process.env.OPENCODE_TEST_HOME!,
        },
      })
      assert.equal(response.status, 200)
      assert.match(response.headers.get(operationNonceHeader)!, /^[0-9a-f]{64}$/)
      assert.deepEqual(await response.json(), {
        schemaVersion: 2, protocol: "agent-teams-hosted-approval-v2", authentication: "opencode-basic",
        runtimeInstanceId: snapshot.runtimeInstanceId, configGeneration: snapshot.configGeneration,
      })
    } finally {
      shutdown.resolve()
      await serving
    }
    assert.equal(Server.url, undefined)

    for (const failure of ["throw", "short", "invalid"] as const) {
      const owned = await Server.listen({ hostname: "127.0.0.1", port: 0 })
      const writes: number[] = []
      const selected = failure === "invalid" ? {
        ...owned,
        hostedApprovalReadiness: { ...owned.hostedApprovalReadiness!, configGeneration: "not-a-generation" },
      } : owned
      const exit = await Effect.runPromiseExit(serveListener(() => Promise.resolve(selected), new Promise<void>(() => {}), (bytes) => {
        writes.push(bytes.byteLength)
        if (failure === "throw") throw new Error("closed supervisor pipe")
        return bytes.byteLength - 1
      }))
      assert.equal(exit._tag, "Failure")
      assert.equal(writes.length, failure === "invalid" ? 0 : 1)
      assert.equal(Server.url, undefined)
      await assert.rejects(fetch(new URL("/global/health", owned.url)))
    }
    close()
  }
} finally {
  if (occupied) await new Promise<void>((resolve) => blocker.close(() => resolve()))
}
console.log(`readiness-${configured ? "configured" : "ordinary"}-ok`)
process.exit(0)
