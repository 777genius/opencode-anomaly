import { createHash, randomBytes } from "node:crypto"
import {
  closeSync,
  constants,
  fdatasyncSync,
  fstatSync,
  openSync,
  readFileSync,
  readSync,
  writeSync,
} from "node:fs"
import { Context, Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"

export const environmentKey = "CLAUDE_TEAM_PRODUCER_PROVENANCE_V2"
export const contract = "claude-team/hosted-producer-provenance"
export const contractSha256 = "acde43e62b8ab42cc5fd2bbecc22f1b96d68f456bfa188b8c63730751222f498"
export const implementationId = "agent-teams.opencode.hosted-approval.v1"

const HEX = /^[0-9a-f]{64}$/
const DECIMAL = /^(?:0|[1-9]\d*)$/
const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/
const MAX_CONTRACT_BYTES = 128 * 1024
const MAX_LINE_BYTES = 64 * 1024

export type Stream = "openCodeTimeline" | "protectedEffectLedger"

interface Descriptor {
  readonly fd: 9 | 10
  readonly device: string
  readonly inode: string
}

interface Capsule {
  readonly activation: {
    readonly controllerNonce: string
    readonly runId: string
    readonly stackManifestSha256: string
  }
  readonly expectedProducer: {
    readonly artifactManifestSha256: string
    readonly executableSha256: string
    readonly implementationId: typeof implementationId
    readonly moduleSha256: string
  }
  readonly streams: {
    readonly openCodeTimeline: Descriptor
    readonly protectedEffectLedger: Descriptor
  }
}

export interface DerivedIdentity {
  readonly pid: number
  readonly startTicks: string
  readonly exeDevice: string
  readonly exeInode: string
  readonly exeSha256: string
  readonly moduleDevice: string
  readonly moduleInode: string
  readonly moduleSha256: string
}

interface DescriptorIdentity {
  readonly device: string
  readonly inode: string
  readonly regularFile: boolean
  readonly append: boolean
  readonly writeOnly: boolean
  readonly mode: number
  readonly nlink: string
  readonly size: string
}

export interface Operations {
  readonly deriveIdentity: (modulePath: string) => DerivedIdentity
  readonly descriptorIdentity: (fd: number) => DescriptorIdentity
  readonly randomNonce: () => string
  readonly write: (fd: number, bytes: Uint8Array, offset: number) => number
  readonly sync: (fd: number) => void
  readonly close: (fd: number) => void
}

interface CommonReply {
  readonly configGeneration: string | null
  readonly requestId: string
  readonly requestIncarnation: string | null
  readonly runtimeInstanceId: string | null
  readonly sessionId: string
  readonly sessionIncarnation: string | null
}

export type NativeRecord =
  | {
      readonly recordType: "hosted-capability"
      readonly operationNonce: string
      readonly native: {
        readonly configGeneration: string
        readonly outcome: "ok"
        readonly responseSha256: string
        readonly runtimeInstanceId: string
        readonly status: 200
      }
    }
  | {
      readonly recordType: "hosted-observe"
      readonly operationNonce: string
      readonly native: {
        readonly configGeneration: string
        readonly outcome: "ok" | "overflow"
        readonly permissionCount: number
        readonly responseSha256: string
        readonly runtimeInstanceId: string
        readonly sessionId: string
        readonly status: 200 | 500
      }
    }
  | {
      readonly recordType: "hosted-reply-raw"
      readonly operationNonce: string
      readonly native: CommonReply & {
        readonly outcome:
          | "unavailable"
          | "body-read-failed"
          | "invalid-json"
          | "invalid-schema"
          | "bad-request"
          | "conflict"
          | "precondition-failed"
          | "applied"
        readonly requestBodySha256: string | null
        readonly responseSha256: string
        readonly status: 200 | 400 | 404 | 409 | 412
      }
    }
  | {
      readonly recordType: "hosted-reply"
      readonly operationNonce: string
      readonly native: CommonReply & {
        readonly decision: "allow_once" | "reject"
        readonly outcome: "bad-request" | "conflict" | "precondition-failed" | "applied"
        readonly permissionDigest: string
        readonly responseSha256?: string
        readonly status: 200 | 400 | 409 | 412
      }
    }
  | {
      readonly recordType: "conditional-reply-effect"
      readonly operationNonce: string
      readonly native: {
        readonly configGeneration: string
        readonly decision: "once" | "reject"
        readonly outcome: "applied"
        readonly permissionDigest: string
        readonly requestId: string
        readonly requestIncarnation: string
        readonly runtimeInstanceId: string
        readonly sessionId: string
        readonly sessionIncarnation: string
      }
    }

export class FatalError extends Error {
  constructor(message = "producer-provenance-fatal", options?: ErrorOptions) {
    super(message, options)
    this.name = "HostedProducerProvenanceFatalError"
  }
}

export interface Producer {
  readonly controllerNonce: string
  readonly runId: string
  readonly assertHealthy: () => void
  readonly operationNonce: () => string
  readonly emit: (stream: Stream, record: NativeRecord) => void
  readonly poison: (reason: string) => never
  readonly close: () => void
}

let processProducer: Producer | null = null
let processInitialized = false

export function initialize(environment: Readonly<Record<string, string | undefined>>, modulePath: string) {
  if (processInitialized) throw new FatalError("producer-provenance-process-already-initialized")
  processInitialized = true
  processProducer = createFromEnvironment(environment, { modulePath })
  return processProducer
}

export function current() {
  return processProducer
}

export function assertInitialized(environment: Readonly<Record<string, string | undefined>>) {
  if (environment[environmentKey] !== undefined && (!processInitialized || processProducer === null)) {
    throw new FatalError("producer-provenance-process-not-initialized")
  }
}

export function close() {
  processProducer?.close()
  processProducer = null
}

export interface Interface {
  readonly producer: Producer | null
}

export class Service extends Context.Service<Service, Interface>()("@opencode/HostedApprovalProvenance") {}

export const makeLayer = (producer: Producer | null) => Layer.succeed(Service, Service.of({ producer }))
export const layer = Layer.sync(Service, () => Service.of({ producer: current() }))
export const node = LayerNode.make({ service: Service, layer, deps: [] })

export function createFromEnvironment(
  environment: Readonly<Record<string, string | undefined>>,
  options: { readonly modulePath: string; readonly operations?: Operations },
): Producer | null {
  const source = environment[environmentKey]
  if (source === undefined) return null
  const capsule = parseCapsule(source)
  const operations = options.operations ?? createNodeOperations()
  try {
    const identity = operations.deriveIdentity(options.modulePath)
    if (
      identity.pid !== process.pid ||
      !Number.isSafeInteger(identity.pid) ||
      identity.pid < 1 ||
      !DECIMAL.test(identity.startTicks) ||
      !DECIMAL.test(identity.exeDevice) ||
      !DECIMAL.test(identity.exeInode) ||
      !DECIMAL.test(identity.moduleDevice) ||
      !DECIMAL.test(identity.moduleInode) ||
      !HEX.test(identity.exeSha256) ||
      identity.exeSha256 !== capsule.expectedProducer.executableSha256 ||
      identity.moduleSha256 !== capsule.expectedProducer.moduleSha256
    ) throw new TypeError("producer-provenance-producer-identity")

    for (const descriptor of Object.values(capsule.streams)) {
      const observed = operations.descriptorIdentity(descriptor.fd)
      if (
        !observed.regularFile ||
        !observed.append ||
        !observed.writeOnly ||
        observed.mode !== 0o600 ||
        observed.nlink !== "1" ||
        observed.size !== "0" ||
        observed.device !== descriptor.device ||
        observed.inode !== descriptor.inode
      ) throw new TypeError("producer-provenance-descriptor-identity")
    }
    return makeProducer(capsule, identity, operations)
  } catch (error) {
    for (const descriptor of Object.values(capsule.streams)) {
      try {
        operations.close(descriptor.fd)
      } catch {}
    }
    throw error
  }
}

export function parseCapsule(source: string): Capsule {
  if (Buffer.byteLength(source) > MAX_CONTRACT_BYTES) throw new TypeError("producer-provenance-contract-bounded")
  const parsed: unknown = JSON.parse(source)
  if (canonicalJson(parsed) !== source) throw new TypeError("producer-provenance-contract-canonical")
  const item = exactObject(parsed, [
    "activation",
    "contract",
    "contractSha256",
    "expectedProducer",
    "producerRole",
    "streams",
    "version",
  ], "producer-provenance-contract")
  if (item.contract !== contract || item.contractSha256 !== contractSha256 || item.producerRole !== "opencode" || item.version !== 2) {
    throw new TypeError("producer-provenance-contract")
  }
  const activation = exactObject(item.activation, ["controllerNonce", "runId", "stackManifestSha256"], "producer-provenance-activation")
  const expected = exactObject(item.expectedProducer, ["artifactManifestSha256", "executableSha256", "implementationId", "moduleSha256"], "producer-provenance-expected-producer")
  const streams = exactObject(item.streams, ["openCodeTimeline", "protectedEffectLedger"], "producer-provenance-streams")
  if (
    typeof activation.controllerNonce !== "string" || !HEX.test(activation.controllerNonce) ||
    typeof activation.runId !== "string" || !SAFE_ID.test(activation.runId) ||
    typeof activation.stackManifestSha256 !== "string" || !HEX.test(activation.stackManifestSha256) ||
    typeof expected.artifactManifestSha256 !== "string" || !HEX.test(expected.artifactManifestSha256) ||
    typeof expected.executableSha256 !== "string" || !HEX.test(expected.executableSha256) ||
    expected.implementationId !== implementationId ||
    typeof expected.moduleSha256 !== "string" || !HEX.test(expected.moduleSha256)
  ) throw new TypeError("producer-provenance-contract")
  const openCodeTimeline = parseDescriptor(streams.openCodeTimeline, 9)
  const protectedEffectLedger = parseDescriptor(streams.protectedEffectLedger, 10)
  if (`${openCodeTimeline.device}:${openCodeTimeline.inode}` === `${protectedEffectLedger.device}:${protectedEffectLedger.inode}`) {
    throw new TypeError("producer-provenance-descriptor-alias")
  }
  return Object.freeze({
    activation: Object.freeze({
      controllerNonce: activation.controllerNonce,
      runId: activation.runId,
      stackManifestSha256: activation.stackManifestSha256,
    }),
    expectedProducer: Object.freeze({
      artifactManifestSha256: expected.artifactManifestSha256,
      executableSha256: expected.executableSha256,
      implementationId,
      moduleSha256: expected.moduleSha256,
    }),
    streams: Object.freeze({ openCodeTimeline, protectedEffectLedger }),
  })
}

function makeProducer(capsule: Capsule, identity: DerivedIdentity, operations: Operations): Producer {
  const previous = new Map<Stream, string>()
  const sequences = new Map<Stream, number>()
  const emissionNonces = new Set<string>()
  let fatal: FatalError | null = null
  let closing = false
  let closed = false

  const fail = (error: unknown): never => {
    fatal ??= error instanceof FatalError ? error : new FatalError("producer-provenance-fatal", {
      cause: error instanceof Error ? error : new Error("producer-provenance-failure"),
    })
    throw fatal
  }
  const writeRecord = (stream: Stream, recordType: string, operationNonce: string | null, native: object) => {
    if (fatal) throw fatal
    if (closed) throw new FatalError("producer-provenance-writer-closed")
    try {
      const emissionNonce = operations.randomNonce()
      if (!HEX.test(emissionNonce) || emissionNonces.has(emissionNonce)) throw new TypeError("producer-provenance-emission-nonce")
      emissionNonces.add(emissionNonce)
      const sequence = sequences.get(stream) ?? 0
      const line = Buffer.from(`${canonicalJson({
        activation: capsule.activation,
        contract,
        contractSha256,
        emissionNonce,
        native,
        operationNonce,
        previousRecordSha256: previous.get(stream) ?? null,
        producer: {
          artifactManifestSha256: capsule.expectedProducer.artifactManifestSha256,
          exeDev: identity.exeDevice,
          exeIno: identity.exeInode,
          exeSha256: identity.exeSha256,
          implementationId,
          moduleSha256: identity.moduleSha256,
          pid: identity.pid,
          role: "opencode",
          startTicks: identity.startTicks,
        },
        recordType,
        sequence,
        stream,
        version: 2,
      })}\n`)
      if (line.byteLength > MAX_LINE_BYTES) throw new RangeError("producer-provenance-line-bounded")
      const descriptor = capsule.streams[stream]
      for (let offset = 0; offset < line.byteLength;) {
        const written = operations.write(descriptor.fd, line, offset)
        if (!Number.isSafeInteger(written) || written < 1 || offset + written > line.byteLength) {
          throw new Error("producer-provenance-short-write")
        }
        offset += written
      }
      operations.sync(descriptor.fd)
      previous.set(stream, sha256(line))
      sequences.set(stream, sequence + 1)
    } catch (error) {
      fail(error)
    }
  }

  for (const stream of ["openCodeTimeline", "protectedEffectLedger"] as const) {
    const descriptor = capsule.streams[stream]
    writeRecord(stream, "producer-open", null, { descriptor })
  }

  const producer: Producer = {
    controllerNonce: capsule.activation.controllerNonce,
    runId: capsule.activation.runId,
    assertHealthy() {
      if (fatal) throw fatal
      if (closed || closing) throw new FatalError("producer-provenance-writer-closed")
    },
    operationNonce: () => {
      if (fatal) throw fatal
      if (closed || closing) throw new FatalError("producer-provenance-writer-closed")
      const nonce = operations.randomNonce()
      if (!HEX.test(nonce)) return fail(new TypeError("producer-provenance-operation-nonce"))
      return nonce
    },
    emit(stream, record) {
      try {
        if (!HEX.test(record.operationNonce)) throw new TypeError("producer-provenance-operation-nonce")
        if (stream === "protectedEffectLedger" && record.recordType !== "conditional-reply-effect") {
          throw new TypeError("producer-provenance-stream-not-owned")
        }
        if (stream === "openCodeTimeline" && record.recordType === "conditional-reply-effect") {
          throw new TypeError("producer-provenance-stream-not-owned")
        }
        validateRecord(record)
        writeRecord(stream, record.recordType, record.operationNonce, record.native)
      } catch (error) {
        fail(error)
      }
    },
    poison: (reason) => fail(new TypeError(reason)),
    close() {
      if (closed || closing) return
      closing = true
      let first: unknown
      for (const stream of ["openCodeTimeline", "protectedEffectLedger"] as const) {
        const descriptor = capsule.streams[stream]
        if (!fatal) {
          try {
            writeRecord(stream, "producer-close", null, {})
          } catch (error) {
            first ??= error
          }
        }
        try {
          operations.sync(descriptor.fd)
        } catch (error) {
          first ??= error
        }
        try {
          operations.close(descriptor.fd)
        } catch (error) {
          first ??= error
        }
      }
      closed = true
      closing = false
      if (first) fail(first)
    },
  }
  return Object.freeze(producer)
}

function validateRecord(record: NativeRecord) {
  if (record.recordType === "hosted-capability") {
    const native = record.native
    exactObject(native, ["configGeneration", "outcome", "responseSha256", "runtimeInstanceId", "status"], "producer-provenance-native-capability")
    requireIds(native.configGeneration, native.runtimeInstanceId)
    requireHashes(native.responseSha256)
    if (native.outcome !== "ok" || native.status !== 200) throw new TypeError("producer-provenance-native-capability")
    return
  }
  if (record.recordType === "hosted-observe") {
    const native = record.native
    exactObject(native, ["configGeneration", "outcome", "permissionCount", "responseSha256", "runtimeInstanceId", "sessionId", "status"], "producer-provenance-native-observe")
    requireIds(native.configGeneration, native.runtimeInstanceId, native.sessionId)
    requireHashes(native.responseSha256)
    if (
      !Number.isSafeInteger(native.permissionCount) ||
      native.permissionCount < 0 ||
      !["ok", "overflow"].includes(native.outcome) ||
      ![200, 500].includes(native.status) ||
      (native.outcome === "ok") !== (native.status === 200)
    ) {
      throw new TypeError("producer-provenance-native-observe")
    }
    return
  }
  if (record.recordType === "conditional-reply-effect") {
    const native = record.native
    exactObject(native, ["configGeneration", "decision", "outcome", "permissionDigest", "requestId", "requestIncarnation", "runtimeInstanceId", "sessionId", "sessionIncarnation"], "producer-provenance-native-effect")
    requireIds(native.configGeneration, native.requestId, native.requestIncarnation, native.runtimeInstanceId, native.sessionId, native.sessionIncarnation)
    requireHashes(native.permissionDigest)
    if (native.outcome !== "applied" || !["once", "reject"].includes(native.decision)) throw new TypeError("producer-provenance-native-effect")
    return
  }
  const commonKeys = ["configGeneration", "requestId", "requestIncarnation", "runtimeInstanceId", "sessionId", "sessionIncarnation"]
  if (record.recordType === "hosted-reply-raw") {
    const native = record.native
    exactObject(native, [...commonKeys, "outcome", "requestBodySha256", "responseSha256", "status"], "producer-provenance-native-raw-reply")
    requireIds(native.requestId, native.sessionId)
    requireHashes(native.responseSha256)
    const early = native.outcome === "unavailable" || native.outcome === "body-read-failed"
    const failed = native.outcome !== "applied"
    if (![
      "unavailable",
      "body-read-failed",
      "invalid-json",
      "invalid-schema",
      "bad-request",
      "conflict",
      "precondition-failed",
      "applied",
    ].includes(native.outcome)) throw new TypeError("producer-provenance-native-raw-reply")
    const expectedStatus = native.outcome === "unavailable" ? 404
      : native.outcome === "conflict" ? 409
      : native.outcome === "precondition-failed" ? 412
      : native.outcome === "applied" ? 200
      : 400
    if (
      native.status !== expectedStatus ||
      (early ? native.requestBodySha256 !== null : typeof native.requestBodySha256 !== "string" || !HEX.test(native.requestBodySha256)) ||
      (failed
        ? native.configGeneration !== null || native.requestIncarnation !== null || native.runtimeInstanceId !== null || native.sessionIncarnation !== null
        : !validIds(native.configGeneration, native.requestIncarnation, native.runtimeInstanceId, native.sessionIncarnation))
    ) throw new TypeError("producer-provenance-native-raw-reply")
    return
  }
  const native = record.native
  if (!["bad-request", "conflict", "precondition-failed", "applied"].includes(native.outcome)) {
    throw new TypeError("producer-provenance-native-reply")
  }
  const applied = native.outcome === "applied"
  exactObject(
    native,
    [...commonKeys, "decision", "outcome", "permissionDigest", ...(applied ? ["responseSha256"] : []), "status"],
    "producer-provenance-native-reply",
  )
  requireIds(native.requestId, native.sessionId)
  requireHashes(native.permissionDigest)
  const expectedStatus = native.outcome === "conflict" ? 409 : native.outcome === "precondition-failed" ? 412 : applied ? 200 : 400
  if (
    native.status !== expectedStatus ||
    !["allow_once", "reject"].includes(native.decision) ||
    (applied
      ? !validIds(native.configGeneration, native.requestIncarnation, native.runtimeInstanceId, native.sessionIncarnation) || typeof native.responseSha256 !== "string" || !HEX.test(native.responseSha256)
      : native.configGeneration !== null || native.requestIncarnation !== null || native.runtimeInstanceId !== null || native.sessionIncarnation !== null || "responseSha256" in native)
  ) throw new TypeError("producer-provenance-native-reply")
}

function validIds(...values: unknown[]) {
  return values.every((value) => typeof value === "string" && SAFE_ID.test(value))
}

function requireIds(...values: unknown[]) {
  if (!validIds(...values)) throw new TypeError("producer-provenance-native-id")
}

function requireHashes(...values: unknown[]) {
  if (values.some((value) => typeof value !== "string" || !HEX.test(value))) throw new TypeError("producer-provenance-native-hash")
}

function parseDescriptor(value: unknown, fd: 9 | 10): Descriptor {
  const item = exactObject(value, ["device", "fd", "inode"], "producer-provenance-descriptor")
  if (item.fd !== fd || typeof item.device !== "string" || !DECIMAL.test(item.device) || typeof item.inode !== "string" || !DECIMAL.test(item.inode)) {
    throw new TypeError("producer-provenance-descriptor")
  }
  return Object.freeze({ fd, device: item.device, inode: item.inode })
}

function exactObject(value: unknown, keys: readonly string[], reason: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new TypeError(reason)
  const item = value as Record<string, unknown>
  const actual = Reflect.ownKeys(item)
  if (actual.some((key) => typeof key !== "string") || (actual as string[]).sort(compareUtf8).join("\0") !== [...keys].sort(compareUtf8).join("\0")) {
    throw new TypeError(reason)
  }
  return item
}

export function canonicalJson(value: unknown): string {
  if (value === null || typeof value === "boolean" || typeof value === "string") return JSON.stringify(value)
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) throw new TypeError("producer-provenance-native-json")
    return JSON.stringify(value)
  }
  if (typeof value !== "object") throw new TypeError("producer-provenance-native-json")
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`
  const prototype = Object.getPrototypeOf(value)
  if (prototype !== Object.prototype && prototype !== null) throw new TypeError("producer-provenance-native-json")
  return `{${Object.entries(value as Record<string, unknown>)
    .sort(([left], [right]) => compareUtf8(left, right))
    .map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`)
    .join(",")}}`
}

function compareUtf8(left: string, right: string) {
  return Buffer.from(left).compare(Buffer.from(right))
}

export function sha256(bytes: string | Uint8Array) {
  return createHash("sha256").update(bytes).digest("hex")
}

export function createNodeOperations(): Operations {
  const hashOpenFile = (path: string) => {
    const fd = openSync(path, "r")
    try {
      const identity = fstatSync(fd, { bigint: true })
      if (!identity.isFile()) throw new TypeError("producer-provenance-identity-not-file")
      const digest = createHash("sha256")
      const buffer = Buffer.allocUnsafe(64 * 1024)
      for (;;) {
        const count = readSync(fd, buffer, 0, buffer.length, null)
        if (count === 0) break
        digest.update(buffer.subarray(0, count))
      }
      return { device: identity.dev.toString(), inode: identity.ino.toString(), sha256: digest.digest("hex") }
    } finally {
      closeSync(fd)
    }
  }
  const operations: Operations = {
    deriveIdentity(modulePath) {
      const executable = hashOpenFile("/proc/self/exe")
      const module = hashOpenFile(modulePath)
      const stat = readFileSync("/proc/self/stat", "utf8")
      const end = stat.lastIndexOf(") ")
      const startTicks = end < 0 ? undefined : stat.slice(end + 2).trim().split(/\s+/)[19]
      if (startTicks === undefined || !DECIMAL.test(startTicks)) throw new TypeError("producer-provenance-process-stat")
      return {
        pid: process.pid,
        startTicks,
        exeDevice: executable.device,
        exeInode: executable.inode,
        exeSha256: executable.sha256,
        moduleDevice: module.device,
        moduleInode: module.inode,
        moduleSha256: module.sha256,
      }
    },
    descriptorIdentity(fd) {
      if (process.platform !== "linux") throw new TypeError("producer-provenance-descriptor-flags-unavailable")
      const identity = fstatSync(fd, { bigint: true })
      const match = /^flags:\s+([0-7]+)$/m.exec(readFileSync(`/proc/self/fdinfo/${fd}`, "utf8"))
      if (!match) throw new TypeError("producer-provenance-descriptor-flags")
      const flags = Number.parseInt(match[1], 8)
      return {
        device: identity.dev.toString(),
        inode: identity.ino.toString(),
        regularFile: identity.isFile(),
        append: (flags & constants.O_APPEND) !== 0,
        writeOnly: (flags & 3) === constants.O_WRONLY,
        mode: Number(identity.mode & 0o7777n),
        nlink: identity.nlink.toString(),
        size: identity.size.toString(),
      }
    },
    randomNonce: () => randomBytes(32).toString("hex"),
    write: (fd, bytes, offset) => writeSync(fd, bytes, offset, bytes.byteLength - offset, null),
    sync: fdatasyncSync,
    close: closeSync,
  }
  return Object.freeze(operations)
}

export const HostedApprovalProvenance = {
  Service,
  assertInitialized,
  close,
  current,
  initialize,
  layer,
  makeLayer,
  node,
  sha256,
}
