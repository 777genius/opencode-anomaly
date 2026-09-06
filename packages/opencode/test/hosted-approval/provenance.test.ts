import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import {
  canonicalJson,
  contract,
  contractSha256,
  createFromEnvironment,
  environmentKey,
  implementationId,
  HostedApprovalProvenance,
  sha256,
  type Operations,
} from "../../src/hosted-approval/provenance"

const exeSha256 = sha256("test executable bytes")
const moduleSha256 = sha256("test module bytes")

function capsule(overrides: Record<string, unknown> = {}) {
  return canonicalJson({
    activation: {
      controllerNonce: "a".repeat(64),
      runId: "run_provenance_test",
      stackManifestSha256: "b".repeat(64),
    },
    contract,
    contractSha256,
    expectedProducer: {
      artifactManifestSha256: "c".repeat(64),
      executableSha256: exeSha256,
      implementationId,
      moduleSha256,
    },
    producerRole: "opencode",
    streams: {
      openCodeTimeline: { device: "71", fd: 9, inode: "901" },
      protectedEffectLedger: { device: "72", fd: 10, inode: "902" },
    },
    version: 2,
    ...overrides,
  })
}

function harness(options: {
  writeLimit?: number
  failSyncAt?: number
  failCloseFd?: 9 | 10
  repeatedNonce?: boolean
} = {}) {
  const chunks = new Map<number, Uint8Array[]>([[9, []], [10, []]])
  const closed: number[] = []
  let nonce = 0
  let syncs = 0
  const operations: Operations = {
    deriveIdentity: () => ({
      pid: process.pid,
      startTicks: "456789",
      exeDevice: "31",
      exeInode: "401",
      exeSha256,
      moduleDevice: "32",
      moduleInode: "402",
      moduleSha256,
    }),
    descriptorIdentity: (fd) => ({
      device: fd === 9 ? "71" : "72",
      inode: fd === 9 ? "901" : "902",
      regularFile: true,
      append: true,
      writeOnly: true,
      mode: 0o600,
      nlink: "1",
      size: "0",
    }),
    randomNonce: () => (options.repeatedNonce ? "d".repeat(64) : (++nonce).toString(16).padStart(64, "0")),
    write: (fd, bytes, offset) => {
      const count = Math.min(options.writeLimit ?? bytes.byteLength, bytes.byteLength - offset)
      chunks.get(fd)!.push(bytes.slice(offset, offset + count))
      return count
    },
    sync: () => {
      syncs++
      if (syncs === options.failSyncAt) throw new Error("test fsync failure")
    },
    close: (fd) => {
      closed.push(fd)
      if (fd === options.failCloseFd) throw new Error("test close failure")
    },
  }
  const text = (fd: 9 | 10) => Buffer.concat(chunks.get(fd)!.map((value) => Buffer.from(value))).toString("utf8")
  const records = (fd: 9 | 10) => text(fd).trimEnd().split("\n").filter(Boolean).map((line) => JSON.parse(line))
  return { closed, operations, records, text, syncs: () => syncs }
}

function create(operations: Operations) {
  return createFromEnvironment({ [environmentKey]: capsule() }, { modulePath: "/admitted/opencode", operations })!
}

describe("native hosted approval provenance producer", () => {
  test("is inert without an authorized capsule", () => {
    const fixture = harness()
    expect(createFromEnvironment({}, { modulePath: "/admitted/opencode", operations: fixture.operations })).toBeNull()
    expect(fixture.closed).toEqual([])
  })

  test("shares one genuine factory producer through independently built layers", async () => {
    const fixture = harness()
    const producer = create(fixture.operations)
    const read = Effect.gen(function* () {
      return (yield* HostedApprovalProvenance.Service).producer
    })
    const observed = await Promise.all([
      Effect.runPromise(read.pipe(Effect.provide(HostedApprovalProvenance.makeLayer(producer)))),
      Effect.runPromise(read.pipe(Effect.provide(HostedApprovalProvenance.makeLayer(producer)))),
    ])
    expect(observed).toEqual([producer, producer])
    producer.close()
    expect(fixture.records(9).filter((record) => record.recordType === "producer-open")).toHaveLength(1)
    expect(fixture.records(10).filter((record) => record.recordType === "producer-open")).toHaveLength(1)
  })

  test("rejects noncanonical and wrong-role capsules before descriptor use", () => {
    const fixture = harness()
    expect(() => createFromEnvironment({ [environmentKey]: ` ${capsule()}` }, {
      modulePath: "/admitted/opencode",
      operations: fixture.operations,
    })).toThrow("producer-provenance-contract-canonical")
    expect(() => createFromEnvironment({ [environmentKey]: capsule({ producerRole: "owner" }) }, {
      modulePath: "/admitted/opencode",
      operations: fixture.operations,
    })).toThrow("producer-provenance-contract")
    expect(fixture.closed).toEqual([])
  })

  test("rejects aliased descriptors and exact contract corruption", () => {
    const fixture = harness()
    expect(() => createFromEnvironment({
      [environmentKey]: capsule({
        streams: {
          openCodeTimeline: { device: "71", fd: 9, inode: "901" },
          protectedEffectLedger: { device: "71", fd: 10, inode: "901" },
        },
      }),
    }, { modulePath: "/admitted/opencode", operations: fixture.operations })).toThrow("producer-provenance-descriptor-alias")
    expect(() => createFromEnvironment({ [environmentKey]: capsule({ contractSha256: "e".repeat(64) }) }, {
      modulePath: "/admitted/opencode",
      operations: fixture.operations,
    })).toThrow("producer-provenance-contract")
  })

  test("rejects descriptor flags, identity drift, and closes both descriptors", () => {
    const fixture = harness()
    const operations: Operations = {
      ...fixture.operations,
      descriptorIdentity: (fd) => ({ ...fixture.operations.descriptorIdentity(fd), append: fd !== 9 }),
    }
    expect(() => create(operations)).toThrow("producer-provenance-descriptor-identity")
    expect(fixture.closed).toEqual([9, 10])

    const identityFixture = harness()
    expect(() => create({
      ...identityFixture.operations,
      deriveIdentity: (path) => ({ ...identityFixture.operations.deriveIdentity(path), moduleSha256: "f".repeat(64) }),
    })).toThrow("producer-provenance-producer-identity")
    expect(identityFixture.closed).toEqual([9, 10])
  })

  test.each([0o1600, 0o2600, 0o4600])("rejects descriptor mode %# while checking file type separately", (mode) => {
    const fixture = harness()
    expect(() => create({
      ...fixture.operations,
      descriptorIdentity: (fd) => ({ ...fixture.operations.descriptorIdentity(fd), mode }),
    })).toThrow("producer-provenance-descriptor-identity")
    expect(fixture.closed).toEqual([9, 10])
  })

  test("rejects a non-regular descriptor even with exact mode 0600", () => {
    const fixture = harness()
    expect(() => create({
      ...fixture.operations,
      descriptorIdentity: (fd) => ({ ...fixture.operations.descriptorIdentity(fd), regularFile: false }),
    })).toThrow("producer-provenance-descriptor-identity")
    expect(fixture.closed).toEqual([9, 10])
  })

  test("writes partial chunks into exact independent canonical chains", () => {
    const fixture = harness({ writeLimit: 7 })
    const producer = create(fixture.operations)
    const operationNonce = producer.operationNonce()
    producer.emit("openCodeTimeline", {
      recordType: "hosted-reply-raw",
      operationNonce,
      native: {
        configGeneration: null,
        outcome: "invalid-json",
        requestBodySha256: sha256("{ bad json"),
        requestId: "permission_1",
        requestIncarnation: null,
        responseSha256: sha256(new Uint8Array()),
        runtimeInstanceId: null,
        sessionId: "session_1",
        sessionIncarnation: null,
        status: 400,
      },
    })
    producer.emit("protectedEffectLedger", {
      recordType: "conditional-reply-effect",
      operationNonce,
      native: {
        configGeneration: "config_1",
        decision: "reject",
        outcome: "applied",
        permissionDigest: "8".repeat(64),
        requestId: "permission_1",
        requestIncarnation: "request_incarnation_1",
        runtimeInstanceId: "runtime_1",
        sessionId: "session_1",
        sessionIncarnation: "session_incarnation_1",
      },
    })
    producer.close()

    for (const fd of [9, 10] as const) {
      const records = fixture.records(fd)
      expect(records.map((record) => record.sequence)).toEqual([0, 1, 2])
      expect(records[0].previousRecordSha256).toBeNull()
      const lines = fixture.text(fd).trimEnd().split("\n")
      expect(records[1].previousRecordSha256).toBe(sha256(`${lines[0]}\n`))
      expect(records[2].previousRecordSha256).toBe(sha256(`${lines[1]}\n`))
      expect(new Set(records.map((record) => record.emissionNonce)).size).toBe(3)
      expect(records[0].operationNonce).toBeNull()
      expect(records[2].operationNonce).toBeNull()
    }
    const timeline = fixture.records(9)
    const effects = fixture.records(10)
    expect(timeline[0].producer).toEqual(effects[0].producer)
    expect(timeline[0].activation).toEqual(effects[0].activation)
    expect(new Set([...timeline, ...effects].map((record) => record.emissionNonce)).size).toBe(6)
    expect(fixture.records(9)[1].native.requestBodySha256).toBe(sha256("{ bad json"))
    expect(fixture.records(10)[1].operationNonce).toBe(operationNonce)
    expect(fixture.closed).toEqual([9, 10])
    expect(fixture.syncs()).toBe(8)
  })

  test("poisons on fsync failure and never emits a later semantic fact", () => {
    const fixture = harness({ failSyncAt: 3 })
    const producer = create(fixture.operations)
    const operationNonce = producer.operationNonce()
    expect(() => producer.emit("openCodeTimeline", {
      recordType: "hosted-capability",
      operationNonce,
      native: {
        configGeneration: "config_1",
        outcome: "ok",
        responseSha256: "3".repeat(64),
        runtimeInstanceId: "runtime_1",
        status: 200,
      },
    })).toThrow("producer-provenance-fatal")
    expect(() => producer.assertHealthy()).toThrow("producer-provenance-fatal")
    expect(() => producer.operationNonce()).toThrow("producer-provenance-fatal")
    expect(() => producer.emit("openCodeTimeline", {
      recordType: "hosted-capability",
      operationNonce,
      native: {
        configGeneration: "config_1",
        outcome: "ok",
        responseSha256: "3".repeat(64),
        runtimeInstanceId: "runtime_1",
        status: 200,
      },
    })).toThrow("producer-provenance-fatal")
    producer.close()
    expect(fixture.records(9).filter((record) => record.recordType === "hosted-capability")).toHaveLength(1)
    expect(fixture.records(9).some((record) => record.recordType === "producer-close")).toBe(false)
    expect(fixture.closed).toEqual([9, 10])
  })

  test("attempts both descriptor closes after a close failure and poisons the producer", () => {
    const fixture = harness({ failCloseFd: 9 })
    const producer = create(fixture.operations)
    expect(() => producer.close()).toThrow("producer-provenance-fatal")
    expect(fixture.closed).toEqual([9, 10])
    expect(fixture.records(9).at(-1)?.recordType).toBe("producer-close")
    expect(fixture.records(10).at(-1)?.recordType).toBe("producer-close")
    expect(() => producer.emit("openCodeTimeline", {
      recordType: "hosted-capability",
      operationNonce: "7".repeat(64),
      native: {
        configGeneration: "config_1",
        outcome: "ok",
        responseSha256: "3".repeat(64),
        runtimeInstanceId: "runtime_1",
        status: 200,
      },
    })).toThrow("producer-provenance-fatal")
  })

  test("duplicate emission nonces poison the shared producer", () => {
    const fixture = harness({ repeatedNonce: true })
    expect(() => create(fixture.operations)).toThrow("producer-provenance-fatal")
    expect(fixture.closed).toEqual([9, 10])
  })

  test("rejects optional mismatch effects and malformed native identities", () => {
    const fixture = harness()
    const producer = create(fixture.operations)
    expect(() => producer.emit("protectedEffectLedger", {
      recordType: "conditional-reply-effect",
      operationNonce: producer.operationNonce(),
      native: {
        configGeneration: "config_1",
        decision: "once",
        outcome: "mismatch",
        permissionDigest: null,
        requestId: "permission_1",
        requestIncarnation: "request_incarnation_1",
        runtimeInstanceId: "runtime_1",
        sessionId: "session_1",
        sessionIncarnation: "session_incarnation_1",
      },
    } as never)).toThrow("producer-provenance-fatal")
  })
})
