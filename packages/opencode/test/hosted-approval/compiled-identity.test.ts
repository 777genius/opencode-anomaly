import { describe, expect, test } from "bun:test"
import { readFileSync, statSync } from "node:fs"
import {
  deriveCompiledIdentity,
  MAX_COMPILED_MODULE_BYTES,
  readCompiledModuleBytes,
  type ExecutableIdentity,
} from "../../src/hosted-approval/compiled-identity"
import {
  canonicalJson,
  contract,
  contractSha256,
  createFromEnvironment,
  createNodeOperations,
  createTrustedBootstrap,
  environmentKey,
  implementationId,
  sha256,
  type Operations,
} from "../../src/hosted-approval/provenance"

const modulePath = "/$bunfs/root/opencode"
const moduleBytes = new TextEncoder().encode("hello")
const moduleSha256 = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
const executableSha256 = sha256("independent executable bytes")

function capsule(expected: Record<string, unknown> = {}) {
  return canonicalJson({
    activation: {
      controllerNonce: "a".repeat(64),
      runId: "run_compiled_bootstrap_test",
      stackManifestSha256: "b".repeat(64),
    },
    contract,
    contractSha256,
    expectedProducer: {
      artifactManifestSha256: "c".repeat(64),
      executableSha256,
      implementationId,
      moduleSha256,
      ...expected,
    },
    producerRole: "opencode",
    streams: {
      openCodeTimeline: { device: "71", fd: 9, inode: "901" },
      protectedEffectLedger: { device: "72", fd: 10, inode: "902" },
    },
    version: 2,
  })
}

// All injected readers and FDs below are explicitly trusted in-process seams.
// No test creates a capsule-selected module path or changes global runtime APIs.
function harness(options: { failClose?: boolean; failSync?: boolean } = {}) {
  const read = Promise.withResolvers<Uint8Array>()
  const entered = Promise.withResolvers<void>()
  const calls: string[] = []
  const closed: number[] = []
  const lines = new Map<number, string[]>([
    [9, []],
    [10, []],
  ])
  const identity = {
    pid: process.pid,
    startTicks: "456789",
    exeDevice: "31",
    exeInode: "401",
    exeSha256: executableSha256,
  }
  const descriptor = { append: true }
  let nonce = 0
  const compiled = {
    executableIdentity: () => {
      calls.push("executable")
      return identity
    },
    moduleBytes: (path: string) => {
      calls.push(`module:${path}`)
      entered.resolve()
      return read.promise
    },
  }
  const operations: Operations = {
    deriveIdentity: (path) => {
      calls.push(`source:${path}`)
      return { ...identity, moduleDevice: "32", moduleInode: "402", moduleSha256 }
    },
    deriveCompiledIdentity: (path) => deriveCompiledIdentity(path, compiled),
    descriptorIdentity: (fd) => {
      calls.push(`descriptor:${fd}`)
      return {
        device: fd === 9 ? "71" : "72",
        inode: fd === 9 ? "901" : "902",
        regularFile: true,
        append: descriptor.append,
        writeOnly: true,
        mode: 0o600,
        nlink: "1",
        size: "0",
      }
    },
    randomNonce: () => (++nonce).toString(16).padStart(64, "0"),
    write: (fd, bytes, offset) => {
      lines.get(fd)!.push(Buffer.from(bytes.subarray(offset)).toString("utf8"))
      return bytes.byteLength - offset
    },
    sync: () => {
      if (options.failSync) throw new Error("test fsync failure")
    },
    close: (fd) => {
      closed.push(fd)
      if (options.failClose && fd === 9) throw new Error("test close failure")
    },
  }
  const environment = { [environmentKey]: capsule() }
  const records = (fd: number) =>
    lines
      .get(fd)!
      .join("")
      .trimEnd()
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line))
  return {
    bootstrap: createTrustedBootstrap(operations),
    calls,
    closed,
    compiled,
    descriptor,
    entered: entered.promise,
    environment,
    identity,
    operations,
    read,
    records,
  }
}

describe("compiled module identity", () => {
  test("observes executable identity on both sides of the read and hashes only module bytes", async () => {
    const fixture = harness()
    const pending = deriveCompiledIdentity(modulePath, fixture.compiled)
    await fixture.entered
    expect(fixture.calls).toEqual(["executable", `module:${modulePath}`])
    fixture.read.resolve(moduleBytes)
    const identity = await pending
    expect(fixture.calls).toEqual(["executable", `module:${modulePath}`, "executable"])
    expect(identity).toEqual({
      ...fixture.identity,
      moduleDevice: fixture.identity.exeDevice,
      moduleInode: fixture.identity.exeInode,
      moduleSha256,
    })
    expect(identity.moduleSha256).not.toBe(identity.exeSha256)
  })

  test.each([
    { pid: process.pid + 1 },
    { startTicks: "456790" },
    { exeDevice: "33" },
    { exeInode: "403" },
    { exeSha256: "f".repeat(64) },
  ] satisfies Partial<ExecutableIdentity>[])("rejects identity drift during await: %j", async (change) => {
    const fixture = harness()
    const pending = fixture.bootstrap.initialize(fixture.environment, modulePath)
    await fixture.entered
    // Mutating the same object also checks that the first observation was copied.
    Object.assign(fixture.identity, change)
    fixture.read.resolve(moduleBytes)
    await expect(pending).rejects.toThrow("compiled-identity-changed")
    expect(fixture.calls).toEqual(["executable", `module:${modulePath}`, "executable"])
    expect(fixture.records(9)).toEqual([])
    expect(fixture.records(10)).toEqual([])
    expect(fixture.closed).toEqual([9, 10])
    expect(() => fixture.bootstrap.current()).toThrow("compiled-identity-changed")
    expect(() => fixture.bootstrap.assertInitialized({})).toThrow("compiled-identity-changed")
    await expect(fixture.bootstrap.initialize(fixture.environment, modulePath)).rejects.toThrow("already-initialized")
  })

  test.each([
    "/$bunfs/root/",
    "/$bunfs/root/../opencode",
    "/$bunfs/root/./opencode",
    "/$bunfs/root//opencode",
    "/$bunfs/root/opencode/",
    "/$bunfs/root/opencode?entry",
    "/$bunfs/root/opencode#entry",
    "/$bunfs/root/open\u0000code",
    "/$bunfs/other/opencode",
    "/$bunfs/root\\opencode",
    "B:/~BUN/root/opencode",
    "file:///$bunfs/root/opencode",
    "/ordinary/source.ts",
  ])("rejects unsupported virtual path %s before identity or module I/O", async (path) => {
    const fixture = harness()
    await expect(deriveCompiledIdentity(path, fixture.compiled)).rejects.toThrow(
      "producer-provenance-compiled-module-path",
    )
    await expect(
      readCompiledModuleBytes(path, () => {
        throw new Error("must not select a file")
      }),
    ).rejects.toThrow("producer-provenance-compiled-module-path")
    expect(fixture.calls).toEqual([])
  })

  test.skipIf(process.platform !== "linux")("keeps the ordinary source identity reader on Node fs", () => {
    const identity = createNodeOperations().deriveIdentity(import.meta.path)
    const module = statSync(import.meta.path, { bigint: true })
    const executable = statSync("/proc/self/exe", { bigint: true })
    expect(identity.pid).toBe(process.pid)
    expect(identity.moduleDevice).toBe(module.dev.toString())
    expect(identity.moduleInode).toBe(module.ino.toString())
    expect(identity.moduleSha256).toBe(sha256(readFileSync(import.meta.path)))
    expect(identity.exeDevice).toBe(executable.dev.toString())
    expect(identity.exeInode).toBe(executable.ino.toString())
    expect(identity.exeSha256).toBe(sha256(readFileSync("/proc/self/exe")))
    expect(identity.moduleSha256).not.toBe(identity.exeSha256)
  })
})

describe.skipIf(process.platform !== "linux")("bounded Bun module reader", () => {
  test("bounds the byte allocation and preserves the complete immutable module", async () => {
    const reads: number[][] = []
    const bytes = await readCompiledModuleBytes(modulePath, (path) => {
      expect(path).toBe(modulePath)
      return {
        size: moduleBytes.byteLength,
        slice: (start, end) => {
          reads.push([start, end])
          return { bytes: async () => moduleBytes }
        },
      }
    })
    expect(reads).toEqual([[0, MAX_COMPILED_MODULE_BYTES + 1]])
    expect(bytes).toEqual(moduleBytes)
    expect(sha256(bytes)).toBe(moduleSha256)
  })

  test.each([0, -1, 1.5, NaN, Infinity, MAX_COMPILED_MODULE_BYTES + 1])(
    "rejects size %s before starting bytes()",
    async (size) => {
      const reads: number[] = []
      await expect(
        readCompiledModuleBytes(modulePath, () => ({
          size,
          slice: () => {
            reads.push(1)
            throw new Error("must not read bytes")
          },
        })),
      ).rejects.toThrow("producer-provenance-compiled-module-bounded")
      expect(reads).toEqual([])
    },
  )

  test.each([0, 4, 6])("rejects a read with unexpected byte count %s", async (size) => {
    await expect(
      readCompiledModuleBytes(modulePath, () => ({
        size: 5,
        slice: () => ({ bytes: async () => new Uint8Array(size) }),
      })),
    ).rejects.toThrow("producer-provenance-compiled-module-bounded")
  })

  test("propagates a rejected Bun read without a fallback", async () => {
    const error = new Error("test Bun read failure")
    await expect(
      readCompiledModuleBytes(modulePath, () => ({
        size: 5,
        slice: () => ({
          bytes: async () => {
            throw error
          },
        }),
      })),
    ).rejects.toBe(error)
  })
})

describe("trusted compiled process bootstrap", () => {
  test("is inert without configuration even for an unsupported module path", async () => {
    const fixture = harness()
    expect(fixture.bootstrap.current()).toBeNull()
    expect(fixture.bootstrap.assertInitialized({})).toBeUndefined()
    expect(await fixture.bootstrap.initialize({}, "/$bunfs/unsupported")).toBeNull()
    expect(fixture.calls).toEqual([])
    expect(fixture.bootstrap.current()).toBeNull()
    fixture.bootstrap.close()
    expect(fixture.closed).toEqual([])
    await expect(fixture.bootstrap.initialize(fixture.environment, modulePath)).rejects.toThrow("already-initialized")
  })

  test("retains synchronous source factory behavior and uses its source reader in bootstrap", async () => {
    const source = harness()
    const direct = createFromEnvironment(source.environment, {
      modulePath: "/source/index.ts",
      operations: source.operations,
    })
    expect(direct).not.toBeNull()
    expect(source.calls).toEqual(["source:/source/index.ts", "descriptor:9", "descriptor:10"])
    direct!.close()
    const boot = harness()
    const producer = await boot.bootstrap.initialize(boot.environment, "/source/index.ts")
    expect(boot.calls).toEqual(source.calls)
    expect(boot.records(9)[0]).toEqual(source.records(9)[0])
    expect(boot.records(10)[0]).toEqual(source.records(10)[0])
    expect(boot.bootstrap.current()).toBe(producer)
    boot.bootstrap.close()
    expect(boot.closed).toEqual([9, 10])
  })

  test("blocks publication, readiness and concurrent initialization until both identities validate", async () => {
    const fixture = harness()
    const pending = fixture.bootstrap.initialize(fixture.environment, modulePath)
    await fixture.entered
    expect(() => fixture.bootstrap.current()).toThrow("process-initializing")
    expect(() => fixture.bootstrap.assertInitialized(fixture.environment)).toThrow("process-initializing")
    expect(() => fixture.bootstrap.assertInitialized({})).toThrow("process-initializing")
    expect(fixture.records(9)).toEqual([])
    expect(fixture.records(10)).toEqual([])
    await expect(fixture.bootstrap.initialize({}, "/unrelated/source")).rejects.toThrow("already-initialized")
    expect(fixture.calls).toEqual(["executable", `module:${modulePath}`])
    fixture.read.resolve(moduleBytes)
    const producer = await pending
    expect(fixture.bootstrap.current()).toBe(producer)
    expect(fixture.bootstrap.assertInitialized(fixture.environment)).toBeUndefined()
    expect(fixture.calls).toEqual(["executable", `module:${modulePath}`, "executable", "descriptor:9", "descriptor:10"])
    for (const fd of [9, 10]) {
      const records = fixture.records(fd)
      expect(records).toHaveLength(1)
      expect(records[0].recordType).toBe("producer-open")
      expect(records[0].contractSha256).toBe(contractSha256)
      expect(records[0].activation.stackManifestSha256).toBe("b".repeat(64))
      expect(records[0].producer).toEqual({
        artifactManifestSha256: "c".repeat(64),
        exeDev: "31",
        exeIno: "401",
        exeSha256: executableSha256,
        implementationId,
        moduleSha256,
        pid: process.pid,
        role: "opencode",
        startTicks: "456789",
      })
    }
    await expect(fixture.bootstrap.initialize(fixture.environment, modulePath)).rejects.toThrow("already-initialized")
    fixture.bootstrap.close()
    fixture.bootstrap.close()
    expect(fixture.closed).toEqual([9, 10])
    expect(fixture.bootstrap.current()).toBeNull()
    expect(() => fixture.bootstrap.assertInitialized(fixture.environment)).toThrow("not-initialized")
    await expect(fixture.bootstrap.initialize(fixture.environment, modulePath)).rejects.toThrow("already-initialized")
  })

  test.each([{ executableSha256: "f".repeat(64) }, { moduleSha256: executableSha256 }])(
    "checks independent capsule digest expectations %j",
    async (expected) => {
      const fixture = harness()
      const pending = fixture.bootstrap.initialize({ [environmentKey]: capsule(expected) }, modulePath)
      await fixture.entered
      fixture.read.resolve(moduleBytes)
      await expect(pending).rejects.toThrow("producer-provenance-producer-identity")
      expect(fixture.records(9)).toEqual([])
      expect(fixture.records(10)).toEqual([])
      expect(fixture.closed).toEqual([9, 10])
      expect(() => fixture.bootstrap.current()).toThrow("producer-identity")
      expect(() => fixture.bootstrap.assertInitialized({})).toThrow("producer-identity")
      await expect(fixture.bootstrap.initialize(fixture.environment, modulePath)).rejects.toThrow("already-initialized")
    },
  )

  test("retains a rejected read as terminal failure and attempts both closes", async () => {
    const fixture = harness({ failClose: true })
    const error = new Error("test virtual module unavailable")
    const pending = fixture.bootstrap.initialize(fixture.environment, modulePath)
    await fixture.entered
    fixture.read.reject(error)
    await expect(pending).rejects.toBe(error)
    expect(fixture.closed).toEqual([9, 10])
    expect(fixture.records(9)).toEqual([])
    expect(fixture.records(10)).toEqual([])
    expect(fixture.calls).toEqual(["executable", `module:${modulePath}`])
    expect(() => fixture.bootstrap.current()).toThrow(error)
    expect(() => fixture.bootstrap.assertInitialized({})).toThrow(error)
    fixture.bootstrap.close()
    await expect(fixture.bootstrap.initialize(fixture.environment, modulePath)).rejects.toThrow("already-initialized")
    expect(fixture.closed).toEqual([9, 10])
  })

  test.each([0, MAX_COMPILED_MODULE_BYTES + 1])("rejects %s module bytes before publication", async (size) => {
    const fixture = harness()
    const pending = fixture.bootstrap.initialize(fixture.environment, modulePath)
    await fixture.entered
    fixture.read.resolve(new Uint8Array(size))
    await expect(pending).rejects.toThrow("compiled-module-bounded")
    expect(fixture.records(9)).toEqual([])
    expect(fixture.records(10)).toEqual([])
    expect(fixture.closed).toEqual([9, 10])
  })

  test("does not publish after close during the read", async () => {
    const fixture = harness()
    const pending = fixture.bootstrap.initialize(fixture.environment, modulePath)
    await fixture.entered
    fixture.bootstrap.close()
    expect(() => fixture.bootstrap.current()).toThrow("closed-during-initialization")
    expect(() => fixture.bootstrap.assertInitialized({})).toThrow("closed-during-initialization")
    expect(fixture.closed).toEqual([])
    fixture.read.resolve(moduleBytes)
    await expect(pending).rejects.toThrow("closed-during-initialization")
    expect(fixture.records(9)).toEqual([])
    expect(fixture.records(10)).toEqual([])
    expect(fixture.closed).toEqual([9, 10])
    await expect(fixture.bootstrap.initialize(fixture.environment, modulePath)).rejects.toThrow("already-initialized")
  })

  test("retains the first terminal failure when a read rejects after close", async () => {
    const fixture = harness()
    const pending = fixture.bootstrap.initialize(fixture.environment, modulePath)
    await fixture.entered
    fixture.bootstrap.close()
    fixture.read.reject(new Error("later read failure"))
    await expect(pending).rejects.toThrow("closed-during-initialization")
    expect(() => fixture.bootstrap.current()).toThrow("closed-during-initialization")
    expect(fixture.records(9)).toEqual([])
    expect(fixture.records(10)).toEqual([])
    expect(fixture.closed).toEqual([9, 10])
  })

  test("validates descriptor ownership after the async read", async () => {
    const fixture = harness()
    const pending = fixture.bootstrap.initialize(fixture.environment, modulePath)
    await fixture.entered
    fixture.descriptor.append = false
    fixture.read.resolve(moduleBytes)
    await expect(pending).rejects.toThrow("descriptor-identity")
    expect(fixture.records(9)).toEqual([])
    expect(fixture.records(10)).toEqual([])
    expect(fixture.closed).toEqual([9, 10])
  })

  test("preserves poison and cleanup when opening the producer fails", async () => {
    const fixture = harness({ failSync: true })
    const pending = fixture.bootstrap.initialize(fixture.environment, modulePath)
    await fixture.entered
    fixture.read.resolve(moduleBytes)
    await expect(pending).rejects.toThrow("producer-provenance-fatal")
    expect(() => fixture.bootstrap.current()).toThrow("producer-provenance-fatal")
    expect(() => fixture.bootstrap.assertInitialized({})).toThrow("producer-provenance-fatal")
    expect(fixture.records(9).map((record) => record.recordType)).toEqual(["producer-open"])
    expect(fixture.records(10)).toEqual([])
    expect(fixture.closed).toEqual([9, 10])
    fixture.bootstrap.close()
    expect(fixture.closed).toEqual([9, 10])
  })

  test("retains close failures without closing owned descriptors twice", async () => {
    const fixture = harness({ failClose: true })
    const pending = fixture.bootstrap.initialize(fixture.environment, modulePath)
    await fixture.entered
    fixture.read.resolve(moduleBytes)
    await pending
    expect(() => fixture.bootstrap.close()).toThrow("producer-provenance-fatal")
    expect(() => fixture.bootstrap.current()).toThrow("producer-provenance-fatal")
    expect(() => fixture.bootstrap.assertInitialized({})).toThrow("producer-provenance-fatal")
    fixture.bootstrap.close()
    expect(fixture.closed).toEqual([9, 10])
    await expect(fixture.bootstrap.initialize(fixture.environment, modulePath)).rejects.toThrow("already-initialized")
  })

  test("fails closed without a compiled reader, never falling back to a source reader", async () => {
    const fixture = harness()
    const bootstrap = createTrustedBootstrap({ ...fixture.operations, deriveCompiledIdentity: undefined })
    await expect(bootstrap.initialize(fixture.environment, modulePath)).rejects.toThrow("compiled-unavailable")
    expect(fixture.calls).toEqual([])
    expect(fixture.closed).toEqual([9, 10])
    expect(() => bootstrap.assertInitialized({})).toThrow("compiled-unavailable")
  })

  test("rejects an unsupported virtual namespace and cleans up without a fallback", async () => {
    const fixture = harness()
    await expect(fixture.bootstrap.initialize(fixture.environment, "/$bunfs/other/entry")).rejects.toThrow(
      "compiled-module-path",
    )
    expect(fixture.calls).toEqual([])
    expect(fixture.closed).toEqual([9, 10])
    expect(fixture.records(9)).toEqual([])
    expect(fixture.records(10)).toEqual([])
  })

  test("rejects capsule-selected module paths before claiming descriptors", async () => {
    const fixture = harness()
    await expect(
      fixture.bootstrap.initialize(
        {
          [environmentKey]: capsule({ modulePath: "/fallback/source.ts" }),
        },
        modulePath,
      ),
    ).rejects.toThrow("expected-producer")
    expect(fixture.calls).toEqual([])
    expect(fixture.closed).toEqual([])
    expect(() => fixture.bootstrap.current()).toThrow("expected-producer")
    await expect(fixture.bootstrap.initialize(fixture.environment, modulePath)).rejects.toThrow("already-initialized")
  })

  test("snapshots the admitted capsule across the asynchronous boundary", async () => {
    const fixture = harness()
    const pending = fixture.bootstrap.initialize(fixture.environment, modulePath)
    await fixture.entered
    fixture.environment[environmentKey] = capsule({ moduleSha256: "f".repeat(64) })
    fixture.read.resolve(moduleBytes)
    await pending
    expect(fixture.records(9)[0].producer.moduleSha256).toBe(moduleSha256)
    fixture.bootstrap.close()
  })

  test("the real CLI awaits its own import.meta.path before dispatch, and listen retains its readiness guard", () => {
    // Source-level wiring check. The later exact-artifact checkpoint must also
    // demonstrate this barrier in a compiled executable; this does not qualify it.
    const index = readFileSync(new URL("../../src/index.ts", import.meta.url), "utf8")
    const call = "await HostedApprovalProvenance.initialize(process.env, import.meta.path)"
    expect(index.split(call)).toHaveLength(2)
    expect(index.indexOf(call)).toBeLessThan(index.indexOf("const cli = yargs(args)"))
    expect(index.indexOf(call)).toBeLessThan(index.indexOf("await cli.parse"))
    const server = readFileSync(new URL("../../src/server/server.ts", import.meta.url), "utf8")
    expect(server).toContain(
      "HostedApprovalProvenance.assertInitialized(process.env)\n  const listener = await Effect.runPromise(listenEffect(opts))",
    )
  })

  test.skipIf(process.platform !== "linux")(
    "the real Server.listen rejects in-flight and failed native bootstrap before opening a socket",
    async () => {
      const { spawnSync } = await import("node:child_process")
      const { openSync, closeSync, mkdtempSync, rmSync } = await import("node:fs")
      const { tmpdir } = await import("node:os")
      const { join } = await import("node:path")
      using tmp = {
        path: mkdtempSync(join(tmpdir(), "opencode-compiled-listen-")),
        [Symbol.dispose]() {
          rmSync(this.path, { recursive: true, force: true })
        },
      }
      const timeline = openSync(`${tmp.path}/timeline.jsonl`, "ax", 0o600)
      try {
        const effects = openSync(`${tmp.path}/effects.jsonl`, "ax", 0o600)
        try {
          const result = spawnSync(process.execPath, [`${import.meta.dir}/compiled-identity-listen.fixture.ts`], {
            cwd: `${import.meta.dir}/../..`,
            // Do not inherit credentials, real home directories, or activation.
            env: {
              PATH: process.env.PATH,
              HOME: tmp.path,
              XDG_DATA_HOME: `${tmp.path}/data`,
              XDG_CACHE_HOME: `${tmp.path}/cache`,
              XDG_CONFIG_HOME: `${tmp.path}/config`,
              XDG_STATE_HOME: `${tmp.path}/state`,
              OPENCODE_TEST_HOME: tmp.path,
              OPENCODE_TEST_MANAGED_CONFIG_DIR: `${tmp.path}/managed`,
              OPENCODE_DB: ":memory:",
              OPENCODE_DISABLE_AUTOUPDATE: "1",
              OPENCODE_DISABLE_MODELS_FETCH: "1",
            },
            stdio: [
              "ignore",
              "pipe",
              "pipe",
              "ignore",
              "ignore",
              "ignore",
              "ignore",
              "ignore",
              "ignore",
              timeline,
              effects,
            ],
            encoding: "utf8",
            timeout: 15000,
          })
          expect(result.error).toBeUndefined()
          expect({ status: result.status, stderr: result.stderr }).toEqual({ status: 0, stderr: "" })
          expect(result.stdout).toContain("compiled-listen-guard-ok")
          expect(readFileSync(`${tmp.path}/timeline.jsonl`, "utf8")).toBe("")
          expect(readFileSync(`${tmp.path}/effects.jsonl`, "utf8")).toBe("")
        } finally {
          closeSync(effects)
        }
      } finally {
        closeSync(timeline)
      }
    },
    30_000,
  )
})
