// Isolated process fixture: exercises the real singleton and Server.listen guard
// without resetting either module or injecting identity operations. FDs 9/10 are
// inherited temporary files; there are no provider credentials or project data.
import assert from "node:assert/strict"
import { fstatSync } from "node:fs"
import { Server } from "../../src/server/server"
import {
  canonicalJson,
  contract,
  contractSha256,
  current,
  environmentKey,
  implementationId,
  initialize,
} from "../../src/hosted-approval/provenance"

const descriptor = (fd: number) => {
  const stat = fstatSync(fd, { bigint: true })
  return { fd, device: stat.dev.toString(), inode: stat.ino.toString() }
}
const pending = initialize(
  {
    [environmentKey]: canonicalJson({
      activation: {
        controllerNonce: "a".repeat(64),
        runId: "run_compiled_listen_test",
        stackManifestSha256: "b".repeat(64),
      },
      contract,
      contractSha256,
      expectedProducer: {
        artifactManifestSha256: "c".repeat(64),
        executableSha256: "d".repeat(64),
        implementationId,
        moduleSha256: "e".repeat(64),
      },
      producerRole: "opencode",
      streams: { openCodeTimeline: descriptor(9), protectedEffectLedger: descriptor(10) },
      version: 2,
    }),
  },
  `/$bunfs/root/compiled-identity-missing-${process.pid}`,
)
const failure = pending.then(
  () => {
    throw new Error("missing virtual module unexpectedly initialized")
  },
  (error: unknown) => error,
)

assert.throws(current, /process-initializing/)
await assert.rejects(Server.listen({ hostname: "127.0.0.1", port: 0 }), /process-initializing/)
const error = await failure
assert(error instanceof Error)
assert.match(error.message, /producer-provenance|ENOENT/)
assert.throws(current, (observed) => observed === error)
// process.env intentionally has no capsule: terminal failure still gates listen.
await assert.rejects(Server.listen({ hostname: "127.0.0.1", port: 0 }), (observed) => observed === error)
assert.equal(Server.url, undefined)
assert.throws(() => fstatSync(9), { code: "EBADF" })
assert.throws(() => fstatSync(10), { code: "EBADF" })
await Bun.write(Bun.stdout, "compiled-listen-guard-ok\n")
process.exit(0)
