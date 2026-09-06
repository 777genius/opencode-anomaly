import { ConfigProvider, Effect, Layer, ManagedRuntime } from "effect"
import { HttpRouter } from "effect/unstable/http"
import { AppLayer } from "../../src/effect/app-runtime"
import { attach } from "../../src/effect/run-service"
import { HttpApiApp } from "../../src/server/routes/instance/httpapi/server"
import { ServerAuth } from "../../src/server/auth"
import {
  canonicalJson,
  contract,
  contractSha256,
  createFromEnvironment,
  environmentKey,
  HostedApprovalProvenance,
  implementationId,
  type NativeRecord,
  type Operations,
  type Producer,
  type Stream,
} from "../../src/hosted-approval/provenance"

const apps = new Set<{ dispose: () => Promise<void> }>()

export function app(password?: string, producer?: Producer) {
  const memoMap = Layer.makeMemoMapUnsafe()
  const runtime = ManagedRuntime.make(AppLayer, { memoMap })
  const web = HttpRouter.toWebHandler(
    (producer
      ? HttpApiApp.createRoutes(undefined, HostedApprovalProvenance.makeLayer(producer))
      : HttpApiApp.routes).pipe(
      Layer.provide(
        ConfigProvider.layer(
          ConfigProvider.fromUnknown({
            OPENCODE_DISABLE_DEFAULT_PLUGINS: "true",
            OPENCODE_SERVER_PASSWORD: password,
          }),
        ),
      ),
    ),
    { disableLogger: true, memoMap },
  )
  const entry = {
    dispose: async () => {
      apps.delete(entry)
      await web.dispose()
      await runtime.dispose()
    },
  }
  apps.add(entry)
  return Object.assign(
    (path: string, init?: RequestInit) =>
      web.handler(new Request(new URL(path, "http://localhost"), init), HttpApiApp.context),
    {
      runFork: <A, E>(effect: Effect.Effect<A, E, ManagedRuntime.ManagedRuntime.Services<typeof runtime>>) =>
        runtime.runFork(attach(effect)),
      dispose: entry.dispose,
    },
  )
}

// Real producer and handlers; only native descriptor I/O is replaced here.
export function capture(options: {
  failRecord?: NativeRecord["recordType"]
  failPhase?: "write" | "sync"
} = {}) {
  const records: Array<{ stream: Stream; record: NativeRecord }> = []
  const chunks = new Map<number, Uint8Array[]>([[9, []], [10, []]])
  const consumed = new Map<number, number>([[9, 0], [10, 0]])
  let nonce = 0
  let nextAllocation: string | Error | undefined
  let failEffects = false
  const executableSha256 = HostedApprovalProvenance.sha256("http test executable")
  const moduleSha256 = HostedApprovalProvenance.sha256("http test module")
  const operations: Operations = {
    deriveIdentity: () => ({
      pid: process.pid,
      startTicks: "1234",
      exeDevice: "31",
      exeInode: "41",
      exeSha256: executableSha256,
      moduleDevice: "32",
      moduleInode: "42",
      moduleSha256,
    }),
    descriptorIdentity: (fd) => ({
      device: fd === 9 ? "71" : "72",
      inode: fd === 9 ? "91" : "92",
      regularFile: true,
      append: true,
      writeOnly: true,
      mode: 0o600,
      nlink: "1",
      size: "0",
    }),
    randomNonce: () => {
      const selected = nextAllocation
      nextAllocation = undefined
      if (selected instanceof Error) throw selected
      return selected ?? (++nonce).toString(16).padStart(64, "0")
    },
    write: (fd, bytes, offset) => {
      const record = JSON.parse(Buffer.from(bytes).toString("utf8")) as { recordType: string }
      if (record.recordType === options.failRecord && options.failPhase === "write") throw new Error("capture write failed")
      chunks.get(fd)!.push(bytes.slice(offset))
      return bytes.byteLength - offset
    },
    sync: (fd) => {
      const bytes = Buffer.concat(chunks.get(fd)!.map((chunk) => Buffer.from(chunk)))
      const offset = consumed.get(fd)!
      const lines = bytes.subarray(offset).toString("utf8").trimEnd().split("\n").filter(Boolean)
      for (const line of lines) {
        const record = JSON.parse(line) as { recordType: string }
        if (record.recordType === options.failRecord && options.failPhase === "sync") throw new Error("capture sync failed")
        if (record.recordType !== "producer-open" && record.recordType !== "producer-close") {
          records.push({ stream: fd === 9 ? "openCodeTimeline" : "protectedEffectLedger", record: record as NativeRecord })
        }
      }
      consumed.set(fd, bytes.byteLength)
      if (failEffects && fd === 10) throw new Error("effect capture failed")
    },
    close: () => {},
  }
  const producer = createFromEnvironment({
    [environmentKey]: canonicalJson({
      activation: {
        controllerNonce: "a".repeat(64),
        runId: "run_http_capture",
        stackManifestSha256: "b".repeat(64),
      },
      contract,
      contractSha256,
      expectedProducer: {
        artifactManifestSha256: "c".repeat(64),
        executableSha256,
        implementationId,
        moduleSha256,
      },
      producerRole: "opencode",
      streams: {
        openCodeTimeline: { device: "71", fd: 9, inode: "91" },
        protectedEffectLedger: { device: "72", fd: 10, inode: "92" },
      },
      version: 2,
    }),
  }, { modulePath: "/admitted/opencode", operations })!
  return {
    producer,
    records,
    failEffects: () => { failEffects = true },
    failNextAllocation: (value: string | Error) => { nextAllocation = value },
    nonces: () => nonce,
  }
}

export function auth(password = "secret") {
  return ServerAuth.header({ username: "opencode", password }) ?? ""
}

export async function disposeApps() {
  await Promise.all(Array.from(apps, (web) => web.dispose()))
  apps.clear()
}
