import {
  diagnosticAcp,
  DiagnosticOwner,
  diagnosticBody,
  diagnosticCallback,
  diagnosticContext,
  diagnosticDrain,
  diagnosticExit,
  diagnosticRegistration,
  diagnosticText,
  unitDiagnostic,
} from "./unit-diagnostic"
// Subprocess test harness for the opencode CLI. Spawns the real binary against
// a TestLLMServer running in-process at a random port, with full env isolation.
//
// This is the missing test tier: in-process tests can't catch bugs that span
// argv parsing → server boot → SDK call → event consumption → exit code (like
// the original /event race or #27371's invalid-model hang).
//
// Configuration flows through opencode's built-in test affordances:
//   - OPENCODE_CONFIG_CONTENT      : provider config inline, no files to find
//   - OPENCODE_TEST_HOME           : pins os.homedir() → tmpdir
//   - OPENCODE_DISABLE_PROJECT_CONFIG : skip walking up for opencode.json
//   - OPENCODE_PURE                : skip external plugin discovery + install
//   - OPENCODE_DISABLE_AUTOUPDATE / AUTOCOMPACT / MODELS_FETCH : no background work
// Plus HOME / XDG_* pointing at the tmpdir for belt-and-suspenders isolation.
//
// Today only `opencode.run` is fully wired. The shape supports adding more
// builders (`opencode.serve(opts)`, `opencode.acp(opts)`, `opencode.auth(...)`)
// without changing the fixture. Long-lived commands like `serve` will need a
// different return shape — see the TODO at the bottom of OpencodeCli.
import { test, type TestOptions } from "bun:test"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Clock, Deferred, Duration, Effect, Layer, Queue, Schedule, Scope, Semaphore, Stream } from "effect"
import { FetchHttpClient, HttpClient } from "effect/unstable/http"
import {
  accessSync,
  constants,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  readlinkSync,
  rmdirSync,
  unlinkSync,
  writeFileSync,
} from "node:fs"
import os from "node:os"
import path from "node:path"
import { TestLLMServer } from "./llm-server"
import { testProviderConfig } from "./test-provider"
import { trackAcpStartup } from "./acp-startup"

const opencodeRoot = path.resolve(import.meta.dir, "../../")
const cliEntry = path.join(opencodeRoot, "src/index.ts")
export const testModelID = "test/test-model"

// Wrap a Bun subprocess pipe (or any ReadableStream<Uint8Array>) as a Stream.
// Centralizes the `evaluate` + `onError` boilerplate and tags errors with the
// stream name so a stderr/stdout failure is greppable in logs.
function fromBunStream(name: string, get: () => ReadableStream<Uint8Array>) {
  return Stream.fromReadableStream({
    evaluate: get,
    onError: (cause) => new Error(`${name} stream error: ${String(cause)}`),
  })
}

// Long-lived processes (serve, acp) all want the same stderr drain: read every
// chunk, push to a tail buffer, swallow stream errors (the child closing the
// pipe is normal). `log: true` surfaces a real protocol error to logs so a
// regression doesn't silently disappear.
function forkStderrDrain(
  stream: ReadableStream<Uint8Array>,
  into: string[],
  mark: ReturnType<typeof unitDiagnostic>,
  pid: number,
) {
  return Effect.forkScoped(
    fromBunStream("stderr", () => stream).pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) => Effect.sync(() => into.push(chunk))),
      (effect) => diagnosticDrain(effect, mark, "stderr", pid),
      Effect.ignore({ log: true }),
    ),
  )
}

export type ProcessIdentity = {
  readonly pid: number
  readonly started: string
  readonly executable: string
  readonly containment: string
}

type ProcessTracker = {
  readonly scan: (deadline?: number) => Promise<readonly ProcessIdentity[]>
  // A bounded Darwin PID slice is not evidence that the nonce set is empty.
  // Finalization must retain this distinction while a cursor is in progress.
  readonly scanComplete?: () => boolean
  readonly hasKnown?: () => boolean
  readonly broadDiscovery?: boolean
  readonly stop: () => Promise<void>
  readonly signal: (
    signal: "SIGINT" | "SIGTERM" | "SIGKILL",
    deadline: number,
    mark: ReturnType<typeof unitDiagnostic>,
  ) => Promise<void>
  readonly signalKnown?: (
    signal: "SIGINT" | "SIGTERM" | "SIGKILL",
    deadline: number,
    mark: ReturnType<typeof unitDiagnostic>,
  ) => Promise<void>
  readonly errors: readonly Error[]
  // The serialized facade exposes its raw implementation only to one owned
  // cleanup operation. Public discovery and signalling still enter the same
  // queue, so they cannot interleave with finalization.
  readonly run?: (operation: () => Promise<void>) => Promise<void>
  readonly unlocked?: ProcessTracker
}

type TrackedProcess = {
  readonly child: ReturnType<typeof Bun.spawn>
  readonly tracker: ProcessTracker
  // Windows captures these getters while spawnTracked still owns the native
  // handle. Bun can throw synchronously while exposing a pipe or `exited`;
  // keeping the values also prevents a later consumer from reopening that
  // acquisition boundary outside cleanup ownership.
  readonly rawExited?: Promise<number>
  readonly stdout?: ReadableStream<Uint8Array>
  readonly stderr?: ReadableStream<Uint8Array>
  // This is the diagnostic-wrapped exit promise. Unlike `child.exited`, a
  // rejection has already initiated containment cleanup before it reaches a
  // builder's normal result path.
  readonly exited: Promise<number>
}

const quiescenceScans = 3
const quiescenceDelayMs = 50
const linuxBootIdentity =
  process.platform === "linux" ? readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim() : ""
const containmentRoot = "/sys/fs/cgroup"

type ProcessSnapshot = ProcessIdentity & {
  readonly parent: number
  readonly group: number
  readonly nonce: boolean
}

type PortableSnapshots = {
  readonly snapshots: readonly ProcessSnapshot[]
  readonly errors: readonly Error[]
  readonly complete: boolean
}

type DarwinScanCursor = {
  pids?: readonly number[]
  next: number
}

type DarwinNative = {
  readonly pidinfo: (pid: number, info: Uint8Array) => number
  readonly pidpath: (pid: number, executable: Uint8Array) => number
  readonly exists: (pid: number) => void
}

function exitedWithin(
  child: Pick<ReturnType<typeof Bun.spawn>, "pid" | "kill" | "exited">,
  milliseconds: number,
  stage: string,
  errors: Error[],
  observed?: Promise<number>,
) {
  return new Promise<boolean>((resolve) => {
    let settled = false
    const finish = (exited: boolean) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      resolve(exited)
    }
    const timer = setTimeout(() => finish(false), milliseconds)
    try {
      const exited = observed ?? child.exited
      exited.then(
        () => finish(true),
        (cause) => {
          recordError(errors, `failed to observe process ${child.pid} exit ${stage}`, cause)
          // Keep the rejection observed even when it arrives after the
          // bounded reap wait. The eventual cleanup diagnostic must retain
          // that fact rather than turning it into an unhandled rejection.
          if (settled) return
          finish(false)
        },
      )
    } catch (cause) {
      recordError(errors, `failed to observe process ${child.pid} exit ${stage}`, cause)
      if (settled) return
      finish(false)
    }
  })
}

function isMissingProcess(cause: unknown) {
  return (
    typeof cause === "object" &&
    cause !== null &&
    "code" in cause &&
    (cause.code === "ENOENT" || cause.code === "ESRCH")
  )
}

function recordError(errors: Error[], message: string, cause: unknown) {
  const error = new Error(message, { cause })
  if (!errors.some((previous) => previous.message === error.message)) errors.push(error)
}

function linuxCgroup(pid: number) {
  const line = readFileSync(`/proc/${pid}/cgroup`, "utf8")
    .split("\n")
    .find((line) => line.startsWith("0::"))
  if (!line) throw new Error(`process ${pid} has no unified cgroup membership`)
  const containment = line.slice(3)
  if (!containment.startsWith("/") || containment.includes(".."))
    throw new Error(`process ${pid} reported an unsafe cgroup path`)
  return containment
}

function linuxSnapshot(pid: number): ProcessSnapshot | undefined {
  let stat: string
  try {
    stat = readFileSync(`/proc/${pid}/stat`, "utf8")
  } catch (cause) {
    if (isMissingProcess(cause)) return
    throw new Error(`failed to read stat for process ${pid}`, { cause })
  }
  const close = stat.lastIndexOf(")")
  const fields =
    close < 0
      ? []
      : stat
          .slice(close + 2)
          .trim()
          .split(/\s+/)
  if (fields.length < 20 || !fields[1] || !fields[2] || !fields[19])
    throw new Error(`process ${pid} has an incomplete stat record`)
  // /proc keeps a zombie's stat file after its executable link is gone. Both
  // states mean the process is no longer signalable; do not turn ordinary
  // reaping into an identity-corruption error that prevents other cleanup.
  if (fields[0] === "Z") return
  let executable: string
  try {
    executable = readlinkSync(`/proc/${pid}/exe`)
  } catch (cause) {
    if (isMissingProcess(cause)) return
    throw new Error(`failed to read executable for process ${pid}`, { cause })
  }
  return {
    pid,
    parent: Number(fields[1]),
    group: Number(fields[2]),
    started: `${linuxBootIdentity}:${fields[19]}`,
    executable,
    containment: "",
    nonce: true,
  }
}

// Looking at environ is deliberately the first operation in portable discovery.
// In particular, do not let an unrelated zombie or an inaccessible process make
// a launch fail before it has proved that it carries this launch's nonce.
function linuxHasNonce(pid: number, nonce: string) {
  try {
    return readFileSync(`/proc/${pid}/environ`, "utf8").split("\0").includes(`OPENCODE_TEST_PROCESS_NONCE=${nonce}`)
  } catch {
    return false
  }
}

function sameIdentity(left: ProcessIdentity, right: ProcessIdentity | undefined) {
  return (
    right !== undefined &&
    left.started === right.started &&
    left.executable === right.executable &&
    left.containment === right.containment
  )
}

function updateContainedIdentity(previous: ProcessIdentity, current: ProcessIdentity) {
  // exec(2) preserves both the PID and its start token. The cgroup check in
  // `inspect` has already proved that this remains inside this launch's
  // containment, so retain the owned process under its new executable.
  if (
    previous.pid === current.pid &&
    previous.started === current.started &&
    previous.containment === current.containment
  )
    return current
  throw new Error(`process ${previous.pid} changed identity inside containment`)
}

function containedPids(cgroupPath: string) {
  return readFileSync(path.join(cgroupPath, "cgroup.procs"), "utf8")
    .split("\n")
    .filter(Boolean)
    .map((value) => {
      const pid = Number(value)
      if (!Number.isSafeInteger(pid)) throw new Error(`containment ${cgroupPath} returned an invalid PID`)
      return pid
    })
}

function signalExact(
  identity: ProcessIdentity,
  inspect: (pid: number) => ProcessIdentity | undefined,
  signal: "SIGINT" | "SIGTERM" | "SIGKILL",
  deadline: number,
  mark: ReturnType<typeof unitDiagnostic>,
  errors: Error[],
  send: (pid: number, signal: "SIGINT" | "SIGTERM" | "SIGKILL") => void = (pid, signal) => process.kill(pid, signal),
) {
  if (Date.now() >= deadline) {
    recordError(errors, `deadline elapsed before ${signal} for process ${identity.pid}`, undefined)
    return
  }
  let current: ProcessIdentity | undefined
  try {
    current = inspect(identity.pid)
  } catch (cause) {
    recordError(errors, `failed to inspect process ${identity.pid} before ${signal}`, cause)
    return
  }
  if (!current) return
  if (!sameIdentity(identity, current)) {
    recordError(errors, `process ${identity.pid} identity changed before ${signal}`, undefined)
    return
  }
  // Inspection can itself consume the deadline. Never signal after it.
  if (Date.now() >= deadline) {
    recordError(errors, `deadline elapsed after inspecting process ${identity.pid} before ${signal}`, undefined)
    return
  }
  try {
    send(identity.pid, signal)
    mark(
      signal === "SIGINT" ? "kill.sigint.request" : signal === "SIGTERM" ? "kill.request" : "kill.sigkill.request",
      identity.pid,
    )
  } catch (cause) {
    if (isMissingProcess(cause)) {
      try {
        if (!inspect(identity.pid)) return
      } catch (inspection) {
        recordError(errors, `failed to confirm process ${identity.pid} absence after ${signal}`, inspection)
        return
      }
    }
    recordError(errors, `failed to send ${signal} to process ${identity.pid}`, cause)
  }
}

function trackContainedProcess(
  child: Pick<ReturnType<typeof Bun.spawn>, "pid">,
  containment: string,
  cgroupPath: string,
): ProcessTracker {
  const tracked = new Map<number, ProcessIdentity>()
  const errors: Error[] = []
  let emptyScans = 0
  const inspect = (pid: number) => {
    const snapshot = linuxSnapshot(pid)
    if (!snapshot) return
    const currentContainment = linuxCgroup(pid)
    if (currentContainment !== containment) throw new Error(`process ${pid} escaped containment ${containment}`)
    return { ...snapshot, containment }
  }
  const scan = async () => {
    try {
      let pids: number[]
      try {
        pids = containedPids(cgroupPath)
      } catch (cause) {
        // A cgroup can disappear after its last process exits (for example
        // when a parent cleanup races ours). That is already quiescent.
        if (isMissingProcess(cause)) {
          tracked.clear()
          emptyScans += 1
          return
        }
        throw cause
      }
      const current = pids.flatMap((pid) => {
        let identity: ProcessIdentity | undefined
        try {
          identity = inspect(pid)
        } catch (cause) {
          // A single vanishing or inaccessible member must not hide later
          // members. Keep every healthy identity available for escalation.
          if (!isMissingProcess(cause)) recordError(errors, `failed to inspect containment member ${pid}`, cause)
          return []
        }
        // cgroup.procs can briefly report a zombie while /proc/<pid>/exe has
        // already disappeared. It is normal termination, not containment
        // corruption, and other live members still need cleanup.
        return identity ? [identity] : []
      })
      const members = new Set(current.map((identity) => identity.pid))
      current.forEach((identity) => {
        const previous = tracked.get(identity.pid)
        if (previous && !sameIdentity(previous, identity)) {
          tracked.set(identity.pid, updateContainedIdentity(previous, identity))
          return
        }
        if (!previous) {
          tracked.set(identity.pid, identity)
        }
      })
      Array.from(tracked.values())
        .filter((identity) => !members.has(identity.pid))
        .forEach((identity) => {
          try {
            const live = linuxSnapshot(identity.pid)
            if (live)
              recordError(errors, `recorded process ${identity.pid} left containment while still live`, undefined)
            if (!live) tracked.delete(identity.pid)
          } catch (cause) {
            if (!isMissingProcess(cause))
              recordError(errors, `failed to confirm containment member ${identity.pid} absence`, cause)
          }
        })
      emptyScans = tracked.size === 0 ? emptyScans + 1 : 0
    } catch (cause) {
      emptyScans = 0
      recordError(errors, `failed to inspect containment for process ${child.pid}`, cause)
    }
  }
  return serializeTracker({
    scan: async () => {
      await scan()
      return Array.from(tracked.values())
    },
    stop: async () => {
      await scan()
      if (errors.length || tracked.size || emptyScans < quiescenceScans) {
        recordError(errors, `containment ${containment} was not quiescent before removal`, undefined)
        return
      }
      try {
        // cgroupfs control files are virtual. rmdir is the only valid removal
        // operation and is only attempted after a clean, empty membership scan.
        rmdirSync(cgroupPath)
      } catch (cause) {
        recordError(errors, `failed to remove containment ${containment}`, cause)
      }
    },
    signal: async (signal, deadline, mark) => {
      await scan()
      Array.from(tracked.values()).forEach((identity) => signalExact(identity, inspect, signal, deadline, mark, errors))
    },
    errors,
  })
}

function trackPortableProcess(
  child: Pick<ReturnType<typeof Bun.spawn>, "pid">,
  nonce: string,
  gateRoot: ProcessIdentity,
  options?: {
    readonly snapshots?: (deadline: number) => Promise<PortableSnapshots>
    readonly inspect?: (pid: number, deadline: number) => Promise<ProcessIdentity | undefined>
    readonly inspectKnown?: (pid: number, deadline: number) => Promise<ProcessIdentity | undefined>
    readonly exists?: (pid: number, deadline: number) => Promise<boolean>
    readonly broadDiscovery?: boolean
    readonly send?: (pid: number, signal: "SIGINT" | "SIGTERM" | "SIGKILL") => void
  },
): ProcessTracker {
  const tracked = new Map<number, ProcessIdentity>()
  const errors: Error[] = []
  const cursor: DarwinScanCursor = { next: 0 }
  let awaitingGateExec = true
  let scanComplete = true
  const snapshots = async (deadline: number) => {
    const result = await (options?.snapshots?.(deadline) ?? portableSnapshots(nonce, deadline, child.pid, cursor))
    scanComplete = result.complete
    result.errors.forEach((error) => {
      if (!errors.includes(error)) errors.push(error)
    })
    return result.snapshots
  }
  const inspect = async (pid: number, deadline: number) => {
    // Discovery is intentionally broad so it can find descendants which have
    // left the launcher's group. Signalling is not discovery: re-enumerating
    // here creates a long PID-reuse window. Re-observe only this target with
    // its platform-native creation identity immediately before each signal.
    return options?.inspect?.(pid, deadline) ?? portableSnapshot(pid, nonce, deadline, true)
  }
  const inspectKnown = async (pid: number, deadline: number) => {
    // A known target already crossed the nonce ownership boundary. On Darwin
    // use its native creation identity directly so the pre-discovery
    // TERM/KILL/reap sequence never starts a sysctl helper or its cleanup.
    if (options?.inspectKnown) return options.inspectKnown(pid, deadline)
    if (process.platform === "darwin") {
      const snapshot = await darwinNativeSnapshot(pid)
      return snapshot ? { ...snapshot, containment: `nonce:${nonce}` } : undefined
    }
    return inspect(pid, deadline)
  }
  const scan = async (deadline = Date.now() + 4_000) => {
    try {
      const currentSnapshots = await snapshots(deadline)
      // The nonce is the portable ownership boundary. A normal child can
      // setsid() or be reparented, so ancestry/group are only supplementary
      // evidence and never exclude an exact-nonce process. Deliberately
      // clearing the nonce is outside this portable contract.
      const current = new Map<number, ProcessSnapshot>(currentSnapshots.map((snapshot) => [snapshot.pid, snapshot]))
      // Preserve a previously verified member until this scan has positively
      // proved it absent. A partial ps/proc failure must not erase a healthy
      // target from a later escalation.
      const next = new Map(tracked)
      const root = current.get(child.pid)
      if (awaitingGateExec && root) {
        if (root.started !== gateRoot.started) throw new Error(`portable gate PID ${child.pid} was reused before exec`)
        if (root.executable !== gateRoot.executable) awaitingGateExec = false
      }
      for (const identity of tracked.values()) {
        try {
          const live = await inspect(identity.pid, deadline)
          if (!live) {
            const stillLive = await (options?.exists?.(identity.pid, deadline) ?? portablePidExists(identity.pid, deadline))
            if (stillLive) throw new Error(`observed portable process ${identity.pid} lost its nonce while still live`)
            next.delete(identity.pid)
            continue
          }
          if (!sameIdentity(identity, live)) {
            if (identity.started !== live.started || identity.containment !== live.containment) {
              throw new Error(`process ${identity.pid} changed identity in portable containment`)
            }
          }
          next.set(identity.pid, live)
        } catch (cause) {
          recordError(errors, `failed to inspect portable process ${identity.pid}`, cause)
        }
      }
      current.forEach((identity) => {
        const previous = tracked.get(identity.pid)
        if (!previous) {
          next.set(identity.pid, identity)
          return
        }
        if (previous.started === identity.started && previous.containment === identity.containment) return
        recordError(errors, `process ${identity.pid} changed identity in portable containment`, undefined)
      })
      tracked.clear()
      next.forEach((identity) => tracked.set(identity.pid, identity))
    } catch (cause) {
      recordError(errors, `failed to scan portable containment for process ${child.pid}`, cause)
    }
  }
  tracked.set(gateRoot.pid, gateRoot)
  return serializeTracker({
    scan: async (deadline) => {
      await scan(deadline)
      return Array.from(tracked.values())
    },
    scanComplete: () => scanComplete,
    hasKnown: () => tracked.size > 0,
    broadDiscovery: options?.broadDiscovery ?? process.platform === "darwin",
    stop: async () => {},
    signalKnown: async (signal, deadline, mark) => {
      for (const identity of tracked.values()) {
        await signalPortableExact(identity, inspectKnown, signal, deadline, mark, errors, options?.send)
      }
    },
    signal: async (signal, deadline, mark) => {
      await signalPortableTargets(
        Array.from(tracked.values()),
        async (discoveryDeadline) => {
          await scan(discoveryDeadline)
          return Array.from(tracked.values())
        },
        inspect,
        signal,
        deadline,
        mark,
        errors,
        options?.send,
      )
    },
    errors,
  })
}

async function signalPortableTargets(
  known: readonly ProcessIdentity[],
  discover: (deadline: number) => Promise<readonly ProcessIdentity[]>,
  inspect: (pid: number, deadline: number) => Promise<ProcessIdentity | undefined>,
  signal: "SIGINT" | "SIGTERM" | "SIGKILL",
  deadline: number,
  mark: ReturnType<typeof unitDiagnostic>,
  errors: Error[],
  send?: (pid: number, signal: "SIGINT" | "SIGTERM" | "SIGKILL") => void,
) {
  // A target already admitted into containment is more important than broad
  // discovery. In particular, do not await a slow Darwin enumeration before
  // sending TERM/KILL through its exact, revalidated identity.
  for (const identity of known) {
    await signalPortableExact(identity, inspect, signal, deadline, mark, errors, send)
  }
  const discoveryDeadline = darwinSignalDiscoveryDeadline(deadline)
  if (Date.now() >= discoveryDeadline) return
  const knownPIDs = new Set(known.map((identity) => identity.pid))
  const discovered = await discover(discoveryDeadline)
  for (const identity of discovered) {
    if (knownPIDs.has(identity.pid)) continue
    await signalPortableExact(identity, inspect, signal, deadline, mark, errors, send)
  }
}

async function signalPortableExact(
  identity: ProcessIdentity,
  inspect: (pid: number, deadline: number) => Promise<ProcessIdentity | undefined>,
  signal: "SIGINT" | "SIGTERM" | "SIGKILL",
  deadline: number,
  mark: ReturnType<typeof unitDiagnostic>,
  errors: Error[],
  send: (pid: number, signal: "SIGINT" | "SIGTERM" | "SIGKILL") => void = (pid, signal) => process.kill(pid, signal),
) {
  if (Date.now() >= deadline) {
    recordError(errors, `deadline elapsed before ${signal} for process ${identity.pid}`, undefined)
    return
  }
  try {
    // Do not reuse a discovery snapshot or a process-group identity here. The
    // final native observation is for this PID alone and includes its creation
    // token, so a PID recycled between scans is never signalled.
    const current = await inspect(identity.pid, deadline)
    if (!current) return
    if (!sameIdentity(identity, current))
      throw new Error(`process ${identity.pid} identity changed before ${signal}`)
    if (Date.now() >= deadline) throw new Error(`deadline elapsed after inspecting process ${identity.pid}`)
    send(identity.pid, signal)
    mark(
      signal === "SIGINT" ? "kill.sigint.request" : signal === "SIGTERM" ? "kill.request" : "kill.sigkill.request",
      identity.pid,
    )
  } catch (cause) {
    if (isMissingProcess(cause)) {
      try {
        // ESRCH is only benign after a second, immediate observation proves
        // absence. A recycled or still-live PID remains an error.
        if (!(await inspect(identity.pid, deadline))) return
      } catch (inspection) {
        recordError(errors, `failed to confirm portable process ${identity.pid} absence after ${signal}`, inspection)
        return
      }
    }
    recordError(errors, `failed to send ${signal} to process ${identity.pid}`, cause)
  }
}

export async function portableSignalRevalidationForTest(
  identity: ProcessIdentity,
  current: ProcessIdentity | undefined,
) {
  const errors: Error[] = []
  let signalled = false
  await signalPortableExact(
    identity,
    async () => current,
    "SIGTERM",
    Date.now() + 1_000,
    () => {},
    errors,
    () => {
      signalled = true
    },
  )
  return { signalled, errors }
}

export async function portableSignalDiscoveryBudgetForTest() {
  const events: string[] = []
  const identity = { pid: 42, started: "started", executable: "opencode", containment: "test" }
  const discover = async (deadline: number) => {
    events.push("discovery")
    await Promise.all(
      Array.from(
        { length: 32 },
        () => new Promise<void>((resolve) => setTimeout(resolve, Math.max(Math.min(deadline - Date.now(), 250), 0))),
      ),
    )
    return []
  }
  for (const signal of ["SIGTERM", "SIGKILL"] as const) {
    await signalPortableTargets(
      [identity],
      discover,
      async () => identity,
      signal,
      Date.now() + 4_000,
      () => {},
      [],
      (_pid, sent) => events.push(sent),
    )
  }
  return events
}

// This drives the real finalizer with a Darwin-shaped tracker. It keeps the
// discovery boundary in the finalizer test: a slow broad scan must be unable
// to get ahead of either exact-target escalation, and an incomplete scan must
// keep running until its later nonce member is killed.
type DarwinFinalizerSyntheticResult = {
  readonly events: string[]
  readonly scans: number
  readonly cause: unknown
}

type DarwinFinalizerRealChildResult = {
  readonly scans: number
  readonly cause: unknown
  readonly exitCode: number
  readonly rootPresentAtAdmission: boolean
  readonly processAdmitted: boolean
  readonly processRetainedAfterUncertainty: boolean
  readonly uncertaintyObserved: boolean
}

export function darwinFinalizerDiscoveryForTest(
  kind: "slow-probes" | "partial-prefix" | "long-prefix",
): Promise<DarwinFinalizerSyntheticResult>
export function darwinFinalizerDiscoveryForTest(kind: "initial-observation-failure"): Promise<DarwinFinalizerRealChildResult>
export async function darwinFinalizerDiscoveryForTest(
  kind: "slow-probes" | "partial-prefix" | "long-prefix" | "initial-observation-failure",
): Promise<DarwinFinalizerSyntheticResult | DarwinFinalizerRealChildResult> {
  const root = {
    pid: 42,
    parent: 1,
    group: 42,
    started: "root",
    executable: "opencode",
    containment: "nonce:test",
    nonce: true,
  } satisfies ProcessSnapshot
  const descendant = {
    pid: 77,
    parent: root.pid,
    group: root.group,
    started: "descendant",
    executable: "sh",
    containment: "nonce:test",
    nonce: true,
  } satisfies ProcessSnapshot
  if (kind === "initial-observation-failure") {
    const child = Bun.spawn([process.execPath, "-e", "setInterval(() => {}, 1_000)"], {
      stdout: "ignore",
      stderr: "ignore",
    })
    let scans = 0
    let injectUncertainty = false
    const processPresent = () => {
      try {
        process.kill(child.pid, 0)
        return true
      } catch (cause) {
        if (isMissingProcess(cause)) return false
        throw cause
      }
    }
    const actualRoot = { ...root, pid: child.pid }
    const tracker = trackPortableProcess(child, "test", actualRoot, {
      broadDiscovery: true,
      snapshots: async () => {
        scans += 1
        return { snapshots: processPresent() ? [actualRoot] : [], errors: [], complete: true }
      },
      inspect: async (pid) => {
        if (pid !== child.pid || !processPresent()) return
        if (!injectUncertainty) return actualRoot
        // Exercise the production tracker after it has admitted a real child:
        // a live native observation failure is ownership uncertainty, never a
        // disappearance.
        return darwinSnapshot(child.pid, "test", Date.now() + 100, true, async () =>
          darwinNativeSnapshot(child.pid, false, {
            pidinfo: () => 0,
            pidpath: () => 0,
            exists: () => {},
          }),
        )
      },
      inspectKnown: async (pid) => {
        if (pid === child.pid && processPresent()) return actualRoot
      },
      exists: async (pid) => pid === child.pid && processPresent(),
    })
    // The production tracker starts with the direct child in its known set.
    // Admit it successfully before making its next native observation opaque.
    await tracker.scan(Date.now() + 100)
    const admitted = await tracker.scan(Date.now() + 100)
    const rootPresentAtAdmission = admitted.some((identity) => identity.pid === child.pid)
    injectUncertainty = true
    const retained = await tracker.scan(Date.now() + 100)
    const processRetainedAfterUncertainty = retained.some((identity) => identity.pid === child.pid)
    const cause = await terminateProcessPromise(
      child,
      () => {},
      tracker,
      Date.now() + 1_000,
    ).catch((cause) => cause)
    // This is intentionally independent from finalizer diagnostics. Awaiting
    // Bun's real child handle proves the OS child actually exited and reaped.
    const exitCode = await child.exited
    return {
      scans,
      cause,
      exitCode,
      rootPresentAtAdmission,
      processAdmitted: rootPresentAtAdmission,
      processRetainedAfterUncertainty,
      uncertaintyObserved: tracker.errors.some((error) =>
        error.message.includes(`failed to inspect portable process ${child.pid}`),
      ),
    }
  }
  const errors: Error[] = []
  const events: string[] = []
  let complete = true
  let scans = 0
  let descendantSeen = false
  let known: ProcessIdentity[] = [root]
  let resolveExit: (code: number) => void = () => {}
  const exited = new Promise<number>((resolve) => {
    resolveExit = resolve
  })
  const reaped = exited.then((code) => {
    events.push("child.reaped")
    return code
  })
  const tracker: ProcessTracker = {
    scan: async () => {
      scans += 1
      if (kind === "slow-probes") {
        complete = true
        events.push(`probe:${scans}`)
        await Promise.all(Array.from({ length: 64 }, () => new Promise<void>((resolve) => setTimeout(resolve, 40))))
        return known
      }
      // These are successive slices of one PID list. The long prefix takes
      // more than the old 1.5s KILL-phase cap, so it proves that finalization
      // resumes the preserved cursor through the overall cleanup deadline.
      if (kind === "long-prefix") await new Promise<void>((resolve) => setTimeout(resolve, 250))
      complete = scans >= (kind === "long-prefix" ? 8 : 5)
      events.push(`slice:${scans}`)
      if (complete && !descendantSeen) {
        descendantSeen = true
        known = [descendant]
      }
      return known
    },
    scanComplete: () => complete,
    hasKnown: () => known.length > 0,
    broadDiscovery: true,
    signalKnown: async (signal) => {
      known.forEach((identity) => {
        events.push(`${signal}:${identity.pid}`)
        if (signal !== "SIGKILL") return
        if (identity.pid === root.pid) resolveExit(0)
        known = known.filter((candidate) => candidate.pid !== identity.pid)
      })
    },
    signal: async () => {},
    stop: async () => {
      events.push("stop")
    },
    errors,
  }
  const cause = await terminateProcessPromise(
    { pid: root.pid, kill: () => {}, exited: reaped } as Pick<ReturnType<typeof Bun.spawn>, "pid" | "kill" | "exited">,
    () => {},
    tracker,
    Date.now() + 4_000,
  ).catch((cause) => cause)
  return { events, scans, cause }
}

async function portablePidExists(pid: number, deadline: number) {
  if (process.platform === "linux") return linuxSnapshot(pid) !== undefined
  if (process.platform === "darwin") return (await darwinNativeSnapshot(pid)) !== undefined
  const result = await commandResult(["ps", "-p", String(pid), "-o", "pid="], deadline)
  // POSIX ps uses status 1 for an absent selected PID. That is ordinary
  // disappearance, not a tracker failure.
  if (result.exitCode === 1) return false
  if (result.exitCode !== 0) throw new Error(`ps exited with ${result.exitCode}`)
  return result.stdout.trim().length > 0
}

async function portableSnapshot(
  pid: number,
  nonce: string,
  deadline: number,
  confirmedNonce = false,
): Promise<ProcessSnapshot | undefined> {
  if (process.platform === "darwin") return darwinSnapshot(pid, nonce, deadline, confirmedNonce)
  if (process.platform !== "linux")
    throw new Error(`portable process containment is unsupported on ${process.platform}`)
  while (Date.now() < deadline) {
    const before = linuxSnapshot(pid)
    if (!before) return
    if (!linuxHasNonce(pid, nonce)) return
    const after = linuxSnapshot(pid)
    if (!after) return
    if (before.started !== after.started || before.executable !== after.executable) continue
    return { ...after, containment: `nonce:${nonce}`, nonce: true }
  }
  throw new Error(`Linux nonce-authorized process ${pid} could not be reconciled before deadline`)
}

async function portableSnapshots(
  nonce: string,
  deadline = Date.now() + 4_000,
  preferredPID?: number,
  cursor?: DarwinScanCursor,
): Promise<PortableSnapshots> {
  if (process.platform === "linux") {
    const errors: Error[] = []
    const snapshots = readdirSync("/proc")
      .filter((entry) => /^\d+$/.test(entry))
      .flatMap((entry) => {
        const pid = Number(entry)
        try {
          if (!linuxHasNonce(pid, nonce)) return []
          const snapshot = linuxSnapshot(pid)
          return snapshot ? [{ ...snapshot, containment: `nonce:${nonce}`, nonce: true }] : []
        } catch (cause) {
          // A failed read after the environment proves this is ours is
          // material: retain every other healthy member, but never certify
          // the launch quiescent while a nonce-authorized snapshot is opaque.
          recordError(errors, `failed to snapshot nonce-authorized process ${pid}`, cause)
          return []
        }
      })
    return { snapshots, errors, complete: true }
  }
  if (process.platform !== "darwin")
    throw new Error(`portable process containment is unsupported on ${process.platform}`)
  const errors: Error[] = []
  try {
    // Enumeration is broad and can encounter arbitrary processes. Its own
    // short deadline prevents unrelated sysctl probes from consuming a
    // containment operation's TERM/KILL/reap reserve.
    const discoveryDeadline = darwinProbeDeadline(deadline)
    if (Date.now() >= discoveryDeadline) return { snapshots: [], errors, complete: false }
    const pids =
      cursor?.pids ??
      (await commandOutput(["ps", "-axo", "pid="], discoveryDeadline))
        .split("\n")
        .map((value) => Number(value.trim()))
        .filter((pid) => Number.isSafeInteger(pid) && pid > 0)
        .sort((left, right) => Number(right === preferredPID) - Number(left === preferredPID))
    if (cursor && !cursor.pids) {
      cursor.pids = pids
      cursor.next = 0
    }
    const snapshots: ProcessSnapshot[] = []
    let next = cursor?.next ?? 0
    const workers = Array.from({ length: Math.min(16, Math.max(pids.length - next, 0)) }, async () => {
      while (Date.now() < discoveryDeadline) {
        const pid = pids[next]
        if (pid === undefined) return
        // Advance before probing. A slow unrelated procargs read can consume
        // this slice, but it must never make later slices start at its prefix.
        next += 1
        if (cursor) cursor.next = next
        try {
          const snapshot = await darwinSnapshot(pid, nonce, discoveryDeadline)
          if (snapshot) snapshots.push(snapshot)
        } catch (cause) {
          // A process which appeared in the enumeration is permitted to exit
          // before either native observation. A nonce-bearing process which
          // keeps changing underneath us is not: darwinSnapshot retries it
          // until the deadline and reports ownership uncertainty below.
          recordError(errors, `failed to snapshot macOS process ${pid}`, cause)
        }
      }
    })
    await Promise.all(workers)
    const complete = next >= pids.length
    if (complete && cursor) {
      cursor.pids = undefined
      cursor.next = 0
    }
    return { snapshots, errors, complete }
  } catch (cause) {
    return { snapshots: [], errors: [new Error("failed to enumerate macOS processes", { cause })], complete: false }
  }
}

function readNulString(bytes: Uint8Array, position: number) {
  const end = bytes.indexOf(0, position)
  if (end < 0) throw new Error("macOS process arguments are not NUL terminated")
  return { value: bytes.subarray(position, end), next: end + 1 }
}

function sameBytes(left: Uint8Array, right: Uint8Array) {
  return left.byteLength === right.byteLength && left.every((value, index) => value === right[index])
}

function darwinEnvironment(bytes: Uint8Array) {
  if (bytes.byteLength < 5) throw new Error("macOS process arguments are incomplete")
  const argc = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0, true)
  let position = readNulString(bytes, 4).next
  while (position < bytes.byteLength && bytes[position] === 0) position += 1
  Array.from({ length: argc }).forEach(() => {
    position = readNulString(bytes, position).next
  })
  const environment: Uint8Array[] = []
  while (position < bytes.byteLength) {
    const entry = readNulString(bytes, position)
    position = entry.next
    if (entry.value.byteLength) environment.push(entry.value)
  }
  return environment
}

export function darwinEnvironmentForTest(bytes: Uint8Array) {
  return darwinEnvironment(bytes).map((entry) => new TextDecoder("utf-8", { fatal: true }).decode(entry))
}

function darwinHasNonce(bytes: Uint8Array, nonce: string) {
  return darwinEnvironment(bytes).some((entry) => sameBytes(entry, new TextEncoder().encode(`OPENCODE_TEST_PROCESS_NONCE=${nonce}`)))
}

function darwinProcArgsCommand(pid: number) {
  return ["sysctl", "-b", `kern.procargs2.${pid}`]
}

export function darwinProcArgsCommandForTest(pid: number) {
  return darwinProcArgsCommand(pid)
}

const darwinProbeTimeoutMs = 250
const darwinSignalReserveMs = 3_000

function darwinProbeDeadline(deadline: number) {
  return Math.min(deadline, Date.now() + darwinProbeTimeoutMs)
}

function darwinSignalDiscoveryDeadline(deadline: number) {
  return Math.min(darwinProbeDeadline(deadline), deadline - darwinSignalReserveMs)
}

export function darwinProbeDeadlineForTest(deadline: number) {
  return darwinProbeDeadline(deadline)
}

async function darwinProcArgs(pid: number, deadline: number, onSpawn?: (pid: number) => void) {
  let child: ReturnType<typeof Bun.spawn>
  let identity: Omit<ProcessSnapshot, "nonce" | "containment"> | undefined
  try {
    // -b is essential. kern.procargs2 starts with a native argc integer and
    // contains embedded NUL bytes, both of which the textual sysctl output
    // path can alter or truncate.
    child = Bun.spawn(darwinProcArgsCommand(pid), { stdout: "pipe", stderr: "pipe" })
    onSpawn?.(child.pid)
  } catch (cause) {
    throw new Error(`failed to start macOS process ${pid} argument probe`, { cause })
  }
  // The probe is owned immediately after spawn. Getter and then failures are
  // acquisition failures too, so retain a separate cleanup reserve for them.
  let exited: Promise<number> | undefined
  let stdout: Promise<Uint8Array> | undefined
  let stderr: Promise<Uint8Array> | undefined
  try {
    identity = await darwinNativeSnapshot(child.pid)
    exited = child.exited
    stdout = drainBytes(child.stdout)
    stderr = drainBytes(child.stderr)
    void Promise.resolve(exited).catch(() => {})
    void Promise.resolve(stdout).catch(() => {})
    void Promise.resolve(stderr).catch(() => {})
  } catch (cause) {
    return cleanupDarwinProcArgs(
      child,
      identity,
      stdout,
      stderr,
      cause instanceof Error ? cause : new Error(`failed to set up macOS process ${pid} argument observation`, { cause }),
    )
  }
  // A single inaccessible unrelated PID must not consume the launch-wide
  // discovery budget. A nonce target that was already observed is handled by
  // darwinSnapshot's fail-closed path instead of being retried indefinitely.
  const probeDeadline = darwinProbeDeadline(deadline)
  const remaining = probeDeadline - Date.now()
  if (remaining <= 0)
    return cleanupDarwinProcArgs(
      child,
      identity,
      stdout,
      stderr,
      new Error("macOS process acquisition deadline elapsed before reading environment"),
    )
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    const [code, output] = await Promise.race([
      Promise.all([exited, stdout, stderr]).then(([exitCode, bytes]) => [exitCode, bytes] as const),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`macOS process ${pid} environment read exceeded acquisition deadline`)), remaining)
      }),
    ])
    if (code !== 0) return
    return output
  } catch (cause) {
    return cleanupDarwinProcArgs(
      child,
      identity,
      stdout,
      stderr,
      cause instanceof Error ? cause : new Error(`failed to read macOS process ${pid} arguments`, { cause }),
    )
  } finally {
    if (timer) clearTimeout(timer)
  }
}

// Kept narrow so the Darwin-gated regression test uses the real binary sysctl
// probe and its ownership cleanup rather than duplicating its deadline logic.
export function darwinProcArgsForTest(pid: number, deadline: number, onSpawn?: (pid: number) => void) {
  return darwinProcArgs(pid, deadline, onSpawn)
}

async function drainBytes(stream: ReadableStream<Uint8Array>) {
  const reader = stream.getReader()
  const chunks: Uint8Array[] = []
  try {
    while (true) {
      const next = await reader.read()
      if (next.done) break
      chunks.push(next.value)
    }
    const output = new Uint8Array(chunks.reduce((total, chunk) => total + chunk.byteLength, 0))
    chunks.reduce((offset, chunk) => {
      output.set(chunk, offset)
      return offset + chunk.byteLength
    }, 0)
    return output
  } finally {
    reader.releaseLock()
  }
}

async function cleanupDarwinProcArgs(
  child: ReturnType<typeof Bun.spawn>,
  identity: Omit<ProcessSnapshot, "nonce" | "containment"> | undefined,
  stdout: Promise<Uint8Array> | undefined,
  stderr: Promise<Uint8Array> | undefined,
  operationError: Error,
): Promise<never> {
  const cleanupErrors: Error[] = []
  const reserveDeadline = Date.now() + 4_000
  const signal = async (name: "SIGTERM" | "SIGKILL") => {
    try {
      // The Bun handle belongs to this helper even if native identity capture
      // failed. It is the only safe pre-identity escape hatch and keeps a
      // synchronous getter failure from leaking the probe.
      if (!identity) {
        child.kill(name)
        return
      }
      const current = await darwinNativeSnapshot(child.pid)
      if (!current) return
      if (!sameDarwinInstance(identity, current))
        throw new Error(`macOS argument probe ${child.pid} identity changed before ${name}`)
      if (Date.now() >= reserveDeadline)
        throw new Error(`cleanup deadline elapsed after inspecting macOS argument probe ${child.pid}`)
      process.kill(child.pid, name)
    } catch (cause) {
      if (!isMissingProcess(cause))
        recordError(cleanupErrors, `failed to send ${name} to macOS argument probe ${child.pid}`, cause)
    }
  }
  await signal("SIGTERM")
  const termWait = Math.min(2_000, Math.max(reserveDeadline - Date.now(), 0))
  if (!(await exitedWithin(child, termWait, "after macOS argument probe SIGTERM", cleanupErrors))) {
    await signal("SIGKILL")
    if (
      !(await exitedWithin(
        child,
        Math.max(reserveDeadline - Date.now(), 0),
        "after macOS argument probe SIGKILL",
        cleanupErrors,
      ))
    )
      cleanupErrors.push(new Error(`macOS argument probe ${child.pid} did not exit during cleanup`))
  }
  const reserve = reserveDeadline - Date.now()
  if (reserve > 0 && stdout && stderr) {
    try {
      await Promise.race([
        Promise.all([stdout, stderr]),
        new Promise<never>((_, reject) => setTimeout(() => reject(new Error("macOS argument probe drains exceeded cleanup reserve")), reserve)),
      ])
    } catch (cause) {
      recordError(cleanupErrors, "failed to drain macOS argument probe during cleanup", cause)
    }
  } else if (!stdout || !stderr) {
    cleanupErrors.push(new Error("macOS argument probe streams could not be acquired before cleanup"))
  } else {
    cleanupErrors.push(new Error("macOS argument probe cleanup reserve elapsed before drains completed"))
  }
  throw new AggregateError(
    cleanupErrors.length
      ? [operationError, new AggregateError(cleanupErrors, `failed to clean up macOS argument probe ${child.pid}`)]
      : [operationError],
    `failed to read macOS process ${child.pid} arguments`,
  )
}

async function darwinNativeSnapshot(
  pid: number,
  allowPreNonceReincarnation = false,
  native?: DarwinNative,
): Promise<Omit<ProcessSnapshot, "nonce" | "containment"> | undefined> {
  if (native) return darwinNativeSnapshotFrom(pid, allowPreNonceReincarnation, native)
  const { dlopen, ptr } = await import("bun:ffi")
  const library = dlopen("/usr/lib/libproc.dylib", {
    proc_pidinfo: { args: ["i32", "i32", "u64", "ptr", "i32"], returns: "i32" },
    proc_pidpath: { args: ["i32", "ptr", "u32"], returns: "i32" },
  })
  try {
    return darwinNativeSnapshotFrom(pid, allowPreNonceReincarnation, {
      pidinfo: (target, info) => library.symbols.proc_pidinfo(target, 3, 0, ptr(info), info.byteLength),
      pidpath: (target, executable) => library.symbols.proc_pidpath(target, ptr(executable), executable.byteLength),
      exists: (target) => process.kill(target, 0),
    })
  } finally {
    library.close()
  }
}

function darwinNativeSnapshotFrom(
  pid: number,
  allowPreNonceReincarnation: boolean,
  native: DarwinNative,
): Omit<ProcessSnapshot, "nonce" | "containment"> | undefined {
  // proc_bsdinfo has a fixed 136-byte ABI layout on both supported macOS
  // architectures. pbi_start_tvsec/usec at 120/128 are a native,
  // microsecond creation identity; lstart's whole seconds are never used.
  const info = new Uint8Array(136)
  if (native.pidinfo(pid, info) !== info.byteLength)
    return darwinConfirmedMissing(pid, "initial native observation", native.exists)
  const executableBytes = new Uint8Array(4096)
  const executableLength = native.pidpath(pid, executableBytes)
  if (executableLength <= 0 || executableLength >= executableBytes.byteLength) {
    // proc_pidinfo and proc_pidpath are separate observations. A process in
    // a broad pre-nonce scan can exit (or have its PID reused) between them.
    // Once the nonce has authorized this PID, though, a failed recheck is
    // uncertainty, never proof that the tracked descendant disappeared.
    const recheck = new Uint8Array(136)
    if (native.pidinfo(pid, recheck) !== recheck.byteLength)
      return darwinConfirmedMissing(pid, "executable observation", native.exists)
    if (!sameDarwinBsdInstance(info, recheck)) {
      if (allowPreNonceReincarnation) return
      throw new Error(`macOS process ${pid} changed identity during executable observation`)
    }
    throw new Error(`failed to read macOS executable for process ${pid}`)
  }
  return darwinBsdIdentity(
    pid,
    info,
    new TextDecoder("utf-8", { fatal: true }).decode(executableBytes.subarray(0, executableLength)),
  )
}

function darwinConfirmedMissing(pid: number, observation: string, exists: (pid: number) => void) {
  try {
    exists(pid)
  } catch (cause) {
    if (typeof cause === "object" && cause !== null && "code" in cause && cause.code === "ESRCH") return
    throw new Error(`failed to confirm macOS process ${pid} disappearance after ${observation}`, { cause })
  }
  throw new Error(`failed to recheck macOS process ${pid} after ${observation}`)
}

function sameDarwinBsdInstance(left: Uint8Array, right: Uint8Array) {
  const leftView = new DataView(left.buffer, left.byteOffset, left.byteLength)
  const rightView = new DataView(right.buffer, right.byteOffset, right.byteLength)
  return (
    leftView.getUint32(12, true) === rightView.getUint32(12, true) &&
    leftView.getBigUint64(120, true) === rightView.getBigUint64(120, true) &&
    leftView.getBigUint64(128, true) === rightView.getBigUint64(128, true)
  )
}

function darwinBsdIdentity(pid: number, info: Uint8Array, executable: string): Omit<ProcessSnapshot, "nonce" | "containment"> {
  if (info.byteLength !== 136) throw new Error(`macOS process ${pid} returned an incomplete native record`)
  const view = new DataView(info.buffer, info.byteOffset, info.byteLength)
  if (view.getUint32(12, true) !== pid) throw new Error(`macOS native process record did not match PID ${pid}`)
  const seconds = view.getBigUint64(120, true)
  const microseconds = view.getBigUint64(128, true)
  if (seconds === 0n || microseconds > 999_999n) throw new Error(`macOS process ${pid} has an invalid creation identity`)
  return {
    pid,
    parent: view.getUint32(16, true),
    group: view.getUint32(100, true),
    started: `darwin:${seconds}:${microseconds}`,
    executable,
  }
}

function sameDarwinInstance(left: Omit<ProcessSnapshot, "nonce" | "containment">, right: Omit<ProcessSnapshot, "nonce" | "containment">) {
  return left.pid === right.pid && left.started === right.started
}

async function darwinSnapshot(
  pid: number,
  nonce: string,
  deadline: number,
  confirmedNonce = false,
  nativeSnapshot: (pid: number, allowPreNonceReincarnation?: boolean) => Promise<Omit<ProcessSnapshot, "nonce" | "containment"> | undefined> =
    darwinNativeSnapshot,
  procArgs: (pid: number, deadline: number) => Promise<Uint8Array | undefined> = darwinProcArgs,
): Promise<ProcessSnapshot | undefined> {
  let sawNonce = false
  while (Date.now() < deadline) {
    const observe = async () => {
      try {
        return await nativeSnapshot(pid, !confirmedNonce && !sawNonce)
      } catch (cause) {
        // A broad PID walk has no ownership evidence for this target yet. An
        // opaque native record can therefore belong to any unrelated process
        // and is skipped. Once admission or a nonce observation crossed the
        // ownership boundary, the identical uncertainty must fail closed.
        if (!confirmedNonce && !sawNonce) return
        throw cause
      }
    }
    const before = await observe()
    if (!before) return
    let argumentsRecord: Uint8Array | undefined
    try {
      argumentsRecord = await procArgs(pid, deadline)
    } catch (cause) {
      // `sysctl kern.procargs2` is routinely inaccessible for processes which
      // do not belong to this test. Its own short deadline must not turn a
      // broad discovery scan into a launch-wide failure. Once a previous
      // observation proved the nonce, however, uncertainty is ownership
      // uncertainty and must remain fail-closed.
      // An inaccessible foreign target returns undefined above. A failed
      // helper cleanup is separately nested in the acquisition AggregateError
      // and is never evidence that the target is harmless.
      if (darwinProcArgsCleanupFailed(cause)) throw cause
      if (confirmedNonce || sawNonce)
        throw new Error(`macOS nonce-authorized process ${pid} could not be observed`, { cause })
      return
    }
    if (!argumentsRecord) {
      if (!(await observe())) return
      // sysctl can reject an unrelated process for ordinary permission
      // reasons. Its independent probe deadline is sufficient evidence to
      // classify that PID as non-owned; only a previously nonce-authorized
      // target is uncertain enough to fail closed.
      if (confirmedNonce || sawNonce) throw new Error(`macOS nonce-authorized process ${pid} could not be observed`)
      return
    }
    const ownsProcess = darwinHasNonce(argumentsRecord, nonce)
    sawNonce ||= ownsProcess
    const after = await observe()
    if (!after) return
    if (!sameDarwinInstance(before, after)) {
      await Bun.sleep(5)
      continue
    }
    if (!ownsProcess) return
    return { ...after, containment: `nonce:${nonce}`, nonce: true }
  }
  if (confirmedNonce || sawNonce)
    throw new Error(`macOS nonce-authorized process ${pid} could not be reconciled before deadline`)
}

export function darwinBroadPreOwnershipObservationForTest(
  confirmedNonce = false,
  observation: "initial" | "arguments-recheck" | "final" = "initial",
) {
  if (observation === "initial")
    return darwinSnapshot(42, "test", Date.now() + 100, confirmedNonce, async () =>
      darwinNativeSnapshot(42, false, {
        pidinfo: () => 0,
        pidpath: () => 0,
        exists: () => {},
      }),
    )
  let calls = 0
  const identity = { pid: 42, parent: 1, group: 42, started: "darwin:1:0", executable: "opencode" }
  const nativeSnapshot = async () => {
    calls += 1
    if ((observation === "arguments-recheck" && calls === 2) || (observation === "final" && calls === 2))
      throw new Error(`opaque ${observation} observation`)
    return identity
  }
  const procArgs = async () => {
    if (observation !== "final") return
    return new Uint8Array([
      2,
      0,
      0,
      0,
      ...new TextEncoder().encode("/usr/local/bin/opencode\0\0opencode\0--serve\0OPENCODE_TEST_PROCESS_NONCE=test\0"),
    ])
  }
  return darwinSnapshot(42, "test", Date.now() + 100, confirmedNonce, nativeSnapshot, procArgs)
}

function darwinProcArgsCleanupFailed(cause: unknown) {
  return (
    cause instanceof AggregateError &&
    Array.from(cause.errors).some(
      (error) => error instanceof AggregateError && error.message.startsWith("failed to clean up macOS argument probe "),
    )
  )
}

async function commandOutput(command: string[], deadline: number) {
  const result = await commandResult(command, deadline)
  if (result.exitCode !== 0) throw new Error(`${command[0]} exited with ${result.exitCode}`)
  return result.stdout
}

async function commandResult(command: string[], deadline: number) {
  if (Date.now() >= deadline) throw new Error(`cleanup deadline elapsed before ${command[0]}`)
  const nonce = crypto.randomUUID()
  const status = path.join(os.tmpdir(), `opencode-command-status-${nonce}`)
  const pendingStatus = `${status}.pending`
  const release = path.join(os.tmpdir(), `opencode-command-release-${nonce}`)
  const abort = path.join(os.tmpdir(), `opencode-command-abort-${nonce}`)
  // On POSIX the directly spawned shell remains the leader of its dedicated
  // session until this caller releases it. Its watcher is therefore an
  // ownership capability established at spawn: it can kill its own live
  // process group without this process ever reusing a saved numeric PGID.
  // This deliberately avoids a follow-up `ps` probe in the timeout path.
  const child = Bun.spawn(
    process.platform === "win32"
      ? command
      : [
          "/bin/sh",
          "-c",
          'status="$1" pending="$2" release="$3" abort="$4"; shift 4; (while [ ! -e "$release" ] && [ ! -e "$abort" ]; do sleep 0.01; done; [ -e "$abort" ] && kill -KILL -$$) >/dev/null 2>&1 & watcher=$!; "$@" & command=$!; wait "$command"; code=$?; printf "%s\\n" "$code" > "$pending" && mv -f "$pending" "$status"; exec 1>&-; while [ ! -e "$release" ] && [ ! -e "$abort" ]; do sleep 0.01; done; kill "$watcher" 2>/dev/null || :; [ -e "$abort" ] && kill -KILL -$$; exit "$code"',
          "--",
          status,
          pendingStatus,
          release,
          abort,
          ...command,
        ],
    { stdout: "pipe", stderr: "ignore", detached: process.platform !== "win32" },
  )
  // The child is owned as soon as spawn returns. Keep every subsequent setup
  // step inside the finalizer boundary: even a hostile synchronous getter or
  // then accessor must fall through to the abort-and-reap reserve below.
  let exited: Promise<number> | undefined
  let stdout: Promise<string> | undefined
  let completed = false
  let aborted = false
  let timer: ReturnType<typeof setTimeout> | undefined
  let operationError: Error | undefined
  try {
    exited = child.exited
    // Do not wait for the status file before reading this pipe. On macOS, `ps`
    // can easily fill a pipe before its parent can publish a status; every
    // probe uses this same bounded drain so a noisy helper cannot wedge cleanup.
    stdout = drainCommandStdout(child.stdout)
    // Promise.resolve safely converts a throwing then accessor to a rejected
    // promise, which remains handled while finalization owns the child.
    void Promise.resolve(exited).catch(() => {})
    void Promise.resolve(stdout).catch(() => {})
    const remaining = deadline - Date.now()
    if (remaining <= 0) throw new Error(`cleanup deadline elapsed before ${command[0]}`)
    const ownedExited = exited
    const ownedStdout = stdout
    if (!ownedExited || !ownedStdout) throw new Error(`failed to establish ${command[0]} helper observations`)
    const result = async () => {
      if (process.platform === "win32") {
        const [exitCode, output] = await Promise.all([ownedExited, ownedStdout])
        return { exitCode, stdout: output }
      }
      while (!existsSync(status)) {
        if (Date.now() >= deadline) throw new Error(`${command[0]} did not exit before the cleanup deadline`)
        await new Promise<void>((resolve) => setTimeout(resolve, 5))
      }
      const exitCode = commandStatus(readFileSync(status, "utf8"))
      if (exitCode === undefined) throw new Error(`${command[0]} returned an invalid exit status`)
      // Keep the helper watchdog armed until every inherited stdout writer
      // closes. The drain is already concurrent, so this cannot deadlock on a
      // full pipe; it does let a deadline abort an orphaned descendant which
      // inherited that pipe after its command root exited.
      const output = await ownedStdout
      if (aborted) throw new Error(`${command[0]} helper operation was aborted during cleanup`)
      writeFileSync(release, "release")
      await ownedExited
      return { exitCode, stdout: output }
    }
    const outcome = await Promise.race([
      result(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${command[0]} did not exit before the cleanup deadline`)), remaining)
      }),
    ])
    completed = true
    return outcome
  } catch (cause) {
    operationError = cause instanceof Error ? cause : new Error(`failed to run ${command[0]}`, { cause })
    throw operationError
  } finally {
    if (timer) clearTimeout(timer)
    const cleanupErrors: Error[] = []
    if (!completed) {
      // Promise.race does not cancel `result()`. Prevent its delayed stdout
      // continuation from publishing a release after this finalizer begins.
      aborted = true
      // The operation deadline bounds only the helper operation. Reaping a
      // timed-out helper gets its own reserve so an elapsed read deadline
      // cannot turn cleanup into a no-op.
      const cleanupDeadline = Date.now() + 4_000
      try {
        await stopCommandChild(child, abort, cleanupDeadline)
      } catch (cause) {
        collectCleanupErrors(cleanupErrors, cause)
      }
      if (
        !(await exitedWithin(
          child,
          Math.max(cleanupDeadline - Date.now(), 0),
          `after failed ${command[0]}`,
          cleanupErrors,
        ))
      ) {
        cleanupErrors.push(new Error(`${command[0]} helper did not exit before the cleanup deadline`))
      }
    }
    ;[status, pendingStatus, release, abort].forEach((file) => {
      try {
        if (existsSync(file)) unlinkSync(file)
      } catch (cause) {
        recordError(cleanupErrors, `failed to remove command helper control ${file}`, cause)
      }
    })
    if (cleanupErrors.length) {
      throw new AggregateError(
        operationError ? [operationError, ...cleanupErrors] : cleanupErrors,
        `${command[0]} helper cleanup failed after an operation failure`,
      )
    }
  }
}

function commandStatus(record: string) {
  // A status is trustworthy only after the helper's same-directory rename.
  // Reject a pre-rename empty/truncated file rather than coercing it to zero.
  const match = /^(?:0|[1-9]\d*)\n/.exec(record)
  if (!match || match[0] !== record) return
  const exitCode = Number(record.slice(0, -1))
  return Number.isSafeInteger(exitCode) ? exitCode : undefined
}

export function commandStatusForTest(record: string) {
  return commandStatus(record)
}

async function drainCommandStdout(stream: ReadableStream<Uint8Array>) {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  const chunks: string[] = []
  const limit = 1_000_000
  let size = 0
  let overflowed = false
  try {
    while (true) {
      const next = await reader.read()
      if (next.done) break
      if (overflowed) continue
      const remaining = limit - size
      if (next.value.byteLength > remaining) {
        if (remaining > 0) chunks.push(decoder.decode(next.value.subarray(0, remaining), { stream: true }))
        overflowed = true
        continue
      }
      size += next.value.byteLength
      chunks.push(decoder.decode(next.value, { stream: true }))
    }
    if (overflowed) throw new Error(`command helper stdout exceeded ${limit} bytes`)
    chunks.push(decoder.decode())
    return chunks.join("")
  } finally {
    reader.releaseLock()
  }
}

// Kept narrow so the regression test exercises the production helper deadline
// and its finalizer without exposing the helper implementation to fixtures.
export function commandResultForTest(command: string[], timeoutMs: number) {
  return commandResult(command, Date.now() + timeoutMs)
}

async function stopCommandChild(
  child: Pick<ReturnType<typeof Bun.spawn>, "pid" | "kill">,
  abort: string,
  deadline: number,
) {
  if (process.platform === "win32") {
    // Bun retains the native process handle for a helper it spawned, unlike a
    // PID-only taskkill lookup. It is therefore safe to close that helper.
    try {
      child.kill()
    } catch (cause) {
      if (!isMissingProcess(cause)) throw cause
    }
    return
  }
  const errors: Error[] = []
  try {
    if (Date.now() >= deadline) throw new Error(`cleanup deadline elapsed before aborting helper ${child.pid}`)
    // The spawned group leader's watcher evaluates `$$` in its own original
    // session. A stale numeric group is never sent from this process.
    writeFileSync(abort, "abort")
  } catch (cause) {
    errors.push(new Error(`failed to abort helper process group ${child.pid}`, { cause }))
  }
  if (errors.length) throw new AggregateError(errors, `failed to stop helper process tree ${child.pid}`)
}

function serializeTracker(tracker: ProcessTracker): ProcessTracker {
  let tail = Promise.resolve()
  const run = <A>(operation: () => Promise<A>) => {
    const next = tail.then(operation, operation)
    tail = next.then(
      () => undefined,
      () => undefined,
    )
    return next
  }
  return {
    scan: (deadline) => run(() => tracker.scan(deadline)),
    signal: (signal, deadline, mark) => run(() => tracker.signal(signal, deadline, mark)),
    stop: () => run(() => tracker.stop()),
    errors: tracker.errors,
    run: (operation) => run(operation),
    unlocked: tracker,
  }
}

async function windowsIdentity(pid: number, deadline = Date.now() + 4_000) {
  const output = await commandOutput(
    [
      "powershell.exe",
      "-NoProfile",
      "-Command",
      `Get-CimInstance Win32_Process -Filter \"ProcessId = ${pid}\" | ForEach-Object { $process = Get-Process -Id $_.ProcessId -ErrorAction Stop; \"$($_.ProcessId)\t$($_.ParentProcessId)\t$($process.StartTime.ToFileTimeUtc())\t$($_.ExecutablePath)\" }`,
    ],
    deadline,
  )
  return windowsIdentityObservationForTest(pid, new TextDecoder().decode(new TextEncoder().encode(output))).map(
    (identity) => ({
      ...identity,
      group: 0,
      // FILETIME is a 64-bit value. Keep its validated decimal spelling intact:
      // converting it to Number would collapse distinct identities.
      started: `native:${identity.created}`,
      executable: identity.executable,
      containment: "job",
      // CIM cannot attest an environment nonce. Windows ownership comes solely
      // from the Job Object established by the supervisor.
      nonce: false,
    }),
  )
}

export function windowsIdentityObservationForTest(pid: number, output: string) {
  if (output.length === 0) return []
  const record = output.endsWith("\r\n") ? output.slice(0, -2) : output.endsWith("\n") ? output.slice(0, -1) : output
  const [observedPID, parent, created, executable, extra] = record.split("\t")
  if (
    extra !== undefined ||
    !observedPID ||
    !parent ||
    !created ||
    !executable ||
    [observedPID, parent, created, executable].some((value) => value.includes("\r") || value.includes("\n")) ||
    !/^[1-9]\d*$/.test(observedPID) ||
    !/^\d+$/.test(parent) ||
    !/^[1-9]\d*$/.test(created)
  )
    throw new Error(`Windows process ${pid} returned a malformed identity observation`)
  const parsedPID = Number(observedPID)
  const parsedParent = Number(parent)
  if (!Number.isSafeInteger(parsedPID) || parsedPID <= 0 || !Number.isSafeInteger(parsedParent)) {
    throw new Error(`Windows process ${pid} returned a malformed identity observation`)
  }
  return [{ pid: parsedPID, parent: parsedParent, created, executable }]
}

async function signalWindowsJob(
  child: Pick<ReturnType<typeof Bun.spawn>, "pid" | "kill">,
  root: ProcessIdentity,
  deadline: number,
  mark: ReturnType<typeof unitDiagnostic>,
  errors: Error[],
  inspect: (deadline: number) => Promise<ProcessIdentity | undefined>,
  abort: () => void,
) {
  let current: ProcessIdentity | undefined
  try {
    // A Job Object owns every descendant from creation, including a root
    // which exits between scans. Revalidate this supervisor immediately
    // before requesting its handle be closed; never use taskkill /t.
    current = await inspect(deadline)
    if (!current) return
    if (!sameIdentity(root, current)) {
      recordError(errors, `Windows job supervisor ${child.pid} identity changed before close`, undefined)
    }
    if (Date.now() >= deadline) {
      recordError(errors, `cleanup deadline elapsed before closing Windows job ${child.pid}`, undefined)
      return
    }
  } catch (cause) {
    // A failed CIM/status read is not authority to leave a known native
    // child alive. Bun's captured handle targets exactly this supervisor.
    recordError(errors, `failed to inspect Windows job for process ${child.pid}`, cause)
  }
  try {
    abort()
  } catch (abortCause) {
    recordError(errors, `failed to write Windows job abort for ${child.pid}`, abortCause)
  }
  try {
    child.kill()
    mark("kill.request", child.pid)
  } catch (termination) {
    if (!isMissingProcess(termination))
      recordError(errors, `failed to terminate Windows job supervisor ${child.pid}`, termination)
  }
}

export async function windowsCapturedHandleCleanupForTest(root: ProcessIdentity, current: ProcessIdentity) {
  const errors: Error[] = []
  let killed = false
  await signalWindowsJob(
    {
      pid: root.pid,
      kill: () => {
        killed = true
      },
    },
    root,
    Date.now() + 1_000,
    () => {},
    errors,
    async () => current,
    () => {},
  )
  return { killed, errors }
}

function trackWindowsProcess(
  child: Pick<ReturnType<typeof Bun.spawn>, "pid" | "kill">,
  root: ProcessIdentity,
  release: string,
  abort: string,
  status: string,
  supervisor: string,
): ProcessTracker {
  const errors: Error[] = []
  const scan = async (deadline = Date.now() + 4_000) => {
    try {
      const current = (await windowsIdentity(child.pid, deadline))[0]
      if (!current) return []
      if (!sameIdentity(root, current))
        recordError(errors, `Windows job supervisor ${child.pid} identity changed`, undefined)
      return [root]
    } catch (cause) {
      recordError(errors, `failed to scan Windows containment for process ${child.pid}`, cause)
      return [root]
    }
  }
  return serializeTracker({
    scan,
    stop: async () => {
      ;[status, release, abort, supervisor].forEach((file) => {
        try {
          if (existsSync(file)) unlinkSync(file)
        } catch (cause) {
          recordError(errors, `failed to remove Windows job control for ${child.pid}`, cause)
        }
      })
    },
    signal: async (_signal, deadline, mark) => {
      await signalWindowsJob(
        child,
        root,
        deadline,
        mark,
        errors,
        async (inspectionDeadline) => (await windowsIdentity(child.pid, inspectionDeadline))[0],
        () => writeFileSync(abort, "abort"),
      )
    },
    errors,
  })
}

function prepareCgroup() {
  if (process.platform !== "linux") return
  let cgroupPath: string | undefined
  try {
    const parent = linuxCgroup(process.pid)
    const containment = `${parent === "/" ? "" : parent}/opencode-test-${process.pid}-${crypto.randomUUID()}`
    cgroupPath = path.join(containmentRoot, containment)
    mkdirSync(cgroupPath)
    accessSync(path.join(cgroupPath, "cgroup.procs"), constants.W_OK)
    return { containment, cgroupPath }
  } catch {
    if (cgroupPath) {
      try {
        if (containedPids(cgroupPath).length === 0) rmdirSync(cgroupPath)
      } catch {}
    }
    return
  }
}

async function removeAbortedCgroup(cgroupPath: string, containment: string, deadline: number) {
  const errors: Error[] = []
  while (Date.now() < deadline) {
    try {
      const pids = containedPids(cgroupPath)
      if (pids.length === 0) {
        rmdirSync(cgroupPath)
        if (errors.length) throw new AggregateError(errors, `failed to clean aborted containment ${cgroupPath}`)
        return
      }
      pids.forEach((pid) => {
        const identity = linuxSnapshot(pid)
        if (!identity || linuxCgroup(pid) !== containment) {
          throw new Error(`cannot safely clean aborted containment member ${pid}`)
        }
        signalExact(
          { ...identity, containment: linuxCgroup(pid) },
          (candidate) => {
            const snapshot = linuxSnapshot(candidate)
            if (!snapshot) return
            return { ...snapshot, containment: linuxCgroup(candidate) }
          },
          "SIGKILL",
          deadline,
          () => {},
          errors,
        )
      })
    } catch (cause) {
      if (isMissingProcess(cause)) continue
      recordError(errors, `failed to remove aborted containment ${cgroupPath}`, cause)
      if (Date.now() >= deadline) throw new AggregateError(errors, `failed to remove aborted containment ${cgroupPath}`)
    }
    await new Promise<void>((resolve) => setTimeout(resolve, Math.min(5, Math.max(deadline - Date.now(), 0))))
  }
  throw new AggregateError(
    [...errors, new Error(`aborted containment ${cgroupPath} did not become empty`)],
    `failed to remove aborted containment ${cgroupPath}`,
  )
}

function windowsSupervisorScript() {
  const separator = "\t"
  const script = String.raw`param([Parameter(ValueFromRemainingArguments=$true)][string[]]$Command)
Add-Type @'
using System;
using System.Text;
using System.Collections.Generic;
using System.Runtime.InteropServices;
public static class OpenCodeTestJob {
  const uint CREATE_SUSPENDED=4, EXTENDED_STARTUPINFO_PRESENT=0x80000, STARTF_USESTDHANDLES=256, WAIT_OBJECT_0=0, WAIT_TIMEOUT=258, WAIT_FAILED=0xffffffff, CLEANUP_WAIT_MS=4000;
  static readonly IntPtr PROC_THREAD_ATTRIBUTE_JOB_LIST = new IntPtr(0x0002000d);
  [StructLayout(LayoutKind.Sequential)] public struct Basic { public long PerProcessUserTimeLimit, PerJobUserTimeLimit; public uint LimitFlags; public UIntPtr MinimumWorkingSetSize, MaximumWorkingSetSize; public uint ActiveProcessLimit; public UIntPtr Affinity; public uint PriorityClass, SchedulingClass; }
  [StructLayout(LayoutKind.Sequential)] public struct IoCounters { public ulong ReadOperationCount, WriteOperationCount, OtherOperationCount, ReadTransferCount, WriteTransferCount, OtherTransferCount; }
  [StructLayout(LayoutKind.Sequential)] public struct Extended { public Basic BasicLimitInformation; public IoCounters IoInfo; public UIntPtr ProcessMemoryLimit, JobMemoryLimit, PeakProcessMemoryUsed, PeakJobMemoryUsed; }
  [StructLayout(LayoutKind.Sequential, CharSet=CharSet.Unicode)] struct StartupInfo { public uint cb; public string lpReserved, lpDesktop, lpTitle; public uint dwX, dwY, dwXSize, dwYSize, dwXCountChars, dwYCountChars, dwFillAttribute, dwFlags; public short wShowWindow, cbReserved2; public IntPtr lpReserved2, hStdInput, hStdOutput, hStdError; }
  [StructLayout(LayoutKind.Sequential)] struct StartupInfoEx { public StartupInfo StartupInfo; public IntPtr lpAttributeList; }
  [StructLayout(LayoutKind.Sequential)] struct ProcessInformation { public IntPtr hProcess, hThread; public uint dwProcessId, dwThreadId; }
  [DllImport("kernel32.dll", SetLastError=true, CharSet=CharSet.Unicode)] static extern bool CreateProcess(string app, StringBuilder command, IntPtr pa, IntPtr ta, bool inherit, uint flags, IntPtr env, string cwd, ref StartupInfoEx startup, out ProcessInformation process);
  [DllImport("kernel32.dll", SetLastError=true)] static extern IntPtr CreateJobObject(IntPtr a,string b);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool SetInformationJobObject(IntPtr j,int c,IntPtr i,int l);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool InitializeProcThreadAttributeList(IntPtr list,int count,int flags,ref IntPtr size);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool UpdateProcThreadAttribute(IntPtr list,uint flags,IntPtr attribute,IntPtr value,IntPtr size,IntPtr previous,IntPtr returned);
  [DllImport("kernel32.dll")] static extern void DeleteProcThreadAttributeList(IntPtr list);
  [DllImport("kernel32.dll", SetLastError=true)] static extern uint ResumeThread(IntPtr t);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool TerminateProcess(IntPtr h,uint code);
  [DllImport("kernel32.dll", SetLastError=true)] static extern uint WaitForSingleObject(IntPtr h,uint ms);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool GetExitCodeProcess(IntPtr h,out uint code);
  [DllImport("kernel32.dll")] static extern uint GetTickCount();
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool GetProcessTimes(IntPtr h,out long c,out long e,out long k,out long u);
  [DllImport("kernel32.dll")] static extern IntPtr GetCurrentProcess();
  [DllImport("kernel32.dll")] static extern IntPtr GetStdHandle(int n);
  [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr h);
  static Exception Failure(string call) { return new System.ComponentModel.Win32Exception(Marshal.GetLastWin32Error(),call); }
  static void Check(bool ok,string call) { if(!ok) throw Failure(call); }
  // GetTickCount is available to the .NET Framework used by Windows PowerShell.
  // Unsigned subtraction is defined across its 49.7-day wrap and this bounded
  // interval is far below half that range.
  static void WaitForExit(IntPtr process,string call) { var began=GetTickCount(); while(true) { var elapsed=unchecked(GetTickCount()-began); if(elapsed>=CLEANUP_WAIT_MS) throw new TimeoutException(call+" timed out"); var remaining=CLEANUP_WAIT_MS-elapsed; var wait=WaitForSingleObject(process,remaining<(uint)50 ? remaining : (uint)50); if(wait==WAIT_OBJECT_0) return; if(wait==WAIT_TIMEOUT) continue; if(wait==WAIT_FAILED) throw Failure(call); throw new Exception(call+" returned "+wait); } }
  static void StopProcess(IntPtr process,List<Exception> errors) { if(process==IntPtr.Zero) return; if(!TerminateProcess(process,125)) errors.Add(Failure("TerminateProcess")); try { WaitForExit(process,"WaitForSingleObject after TerminateProcess"); } catch(Exception error) { errors.Add(error); } }
  static void Close(IntPtr handle,string call,List<Exception> errors) { if(handle!=IntPtr.Zero&&!CloseHandle(handle)) errors.Add(Failure(call)); }
  static string Quote(string s) { var quoted=new StringBuilder("\""); var slashes=0; foreach(var c in s) { if(c=='\\') { slashes++; continue; } if(c=='\"') { quoted.Append('\\',slashes*2+1); quoted.Append('\"'); slashes=0; continue; } quoted.Append('\\',slashes); slashes=0; quoted.Append(c); } quoted.Append('\\',slashes*2); quoted.Append('\"'); return quoted.ToString(); }
  public static IntPtr Start(string status) {
    if(Marshal.SizeOf(typeof(Basic)) != (IntPtr.Size==8 ? 64 : 40) || Marshal.SizeOf(typeof(IoCounters)) != 48 || Marshal.SizeOf(typeof(Extended)) != (IntPtr.Size==8 ? 144 : 104)) throw new Exception("JOBOBJECT_EXTENDED_LIMIT_INFORMATION layout mismatch");
    var job=CreateJobObject(IntPtr.Zero,null); if(job==IntPtr.Zero) throw new System.ComponentModel.Win32Exception(Marshal.GetLastWin32Error(),"CreateJobObject");
    try { var info=new Extended(); info.BasicLimitInformation.LimitFlags=0x2000; var pointer=Marshal.AllocHGlobal(Marshal.SizeOf(typeof(Extended))); try { Marshal.StructureToPtr(info,pointer,false); Check(SetInformationJobObject(job,9,pointer,Marshal.SizeOf(typeof(Extended))),"SetInformationJobObject"); } finally { Marshal.FreeHGlobal(pointer); } long created,ignored1,ignored2,ignored3; Check(GetProcessTimes(GetCurrentProcess(),out created,out ignored1,out ignored2,out ignored3),"GetProcessTimes"); var pending=status+"."+Guid.NewGuid().ToString("N")+".pending"; System.IO.File.WriteAllText(pending,System.Diagnostics.Process.GetCurrentProcess().Id+"${separator}"+created); System.IO.File.Move(pending,status); return job; } catch(Exception error) { var errors=new List<Exception>(); errors.Add(error); Close(job,"CloseHandle job after Start failure",errors); if(errors.Count>1) throw new AggregateException("Windows Job Object startup cleanup failed",errors); throw; }
  }
  public static int Run(IntPtr job,string[] command,string abort) {
    var attributesSize=IntPtr.Zero; InitializeProcThreadAttributeList(IntPtr.Zero,1,0,ref attributesSize);
    var attributes=Marshal.AllocHGlobal(attributesSize); var jobList=Marshal.AllocHGlobal(IntPtr.Size); var initialized=false;
    try {
      Check(InitializeProcThreadAttributeList(attributes,1,0,ref attributesSize),"InitializeProcThreadAttributeList"); initialized=true; Marshal.WriteIntPtr(jobList,job); Check(UpdateProcThreadAttribute(attributes,0,PROC_THREAD_ATTRIBUTE_JOB_LIST,jobList,new IntPtr(IntPtr.Size),IntPtr.Zero,IntPtr.Zero),"UpdateProcThreadAttribute");
      var startup=new StartupInfoEx(); startup.StartupInfo.cb=(uint)Marshal.SizeOf(typeof(StartupInfoEx)); startup.StartupInfo.dwFlags=STARTF_USESTDHANDLES; startup.StartupInfo.hStdInput=GetStdHandle(-10); startup.StartupInfo.hStdOutput=GetStdHandle(-11); startup.StartupInfo.hStdError=GetStdHandle(-12); startup.lpAttributeList=attributes;
      var process=new ProcessInformation(); var line=new StringBuilder(Quote(command[0])); for(var n=1;n<command.Length;n++) line.Append(" "+Quote(command[n])); Check(CreateProcess(null,line,IntPtr.Zero,IntPtr.Zero,true,CREATE_SUSPENDED|EXTENDED_STARTUPINFO_PRESENT,IntPtr.Zero,null,ref startup,out process),"CreateProcess");
      var exited=false; var errors=new List<Exception>(); var code=125;
      try { if(ResumeThread(process.hThread)==0xffffffff) throw Failure("ResumeThread"); while(WaitForSingleObject(process.hProcess,20)==WAIT_TIMEOUT) if(System.IO.File.Exists(abort)) { Check(TerminateProcess(process.hProcess,125),"TerminateProcess"); WaitForExit(process.hProcess,"WaitForSingleObject after abort"); exited=true; code=125; break; } if(!exited) { WaitForExit(process.hProcess,"WaitForSingleObject"); exited=true; uint result; Check(GetExitCodeProcess(process.hProcess,out result),"GetExitCodeProcess"); code=unchecked((int)result); } }
      catch(Exception error) { errors.Add(error); }
      if(!exited) StopProcess(process.hProcess,errors); Close(process.hThread,"CloseHandle thread",errors); Close(process.hProcess,"CloseHandle process",errors); if(errors.Count>0) throw new AggregateException("Windows Job Object process cleanup failed",errors); return code;
    } finally { if(initialized) DeleteProcThreadAttributeList(attributes); Marshal.FreeHGlobal(jobList); Marshal.FreeHGlobal(attributes); }
  }
  public static void Stop(IntPtr job) { if(job!=IntPtr.Zero) Check(CloseHandle(job),"CloseHandle job"); }
}
'@
$job=$null
$failure=$null
try { $delay=[int]$env:OPENCODE_TEST_STATUS_DELAY_MS; if ($delay -gt 0) { Start-Sleep -Milliseconds $delay }; $job=[OpenCodeTestJob]::Start($env:OPENCODE_TEST_STATUS); while (!(Test-Path -LiteralPath $env:OPENCODE_TEST_RELEASE) -and !(Test-Path -LiteralPath $env:OPENCODE_TEST_ABORT)) { Start-Sleep -Milliseconds 10 }; if (Test-Path -LiteralPath $env:OPENCODE_TEST_ABORT) { exit 125 }; exit [OpenCodeTestJob]::Run($job,$Command,$env:OPENCODE_TEST_ABORT) }
catch { $failure=$_.Exception }
finally { if ($null -ne $job) { try { [OpenCodeTestJob]::Stop($job) } catch { if ($null -ne $failure) { throw [System.AggregateException]::new("Windows Job Object supervisor cleanup failed",[System.Exception[]]@($failure,$_.Exception)) }; throw } }; if ($null -ne $failure) { throw $failure } }`
  return script
}

// This deliberately checks the generated C# source rather than merely a
// separately constructed sample: a literal `\\t` in String.raw would make
// PowerShell emit a backslash and `t`, which the TypeScript tab reader cannot
// parse.
export function windowsSupervisorProtocolForTest() {
  return windowsSupervisorScript()
}

function windowsSupervisorStatus(record: string) {
  const match = /^(\d+)\t(\d+)/.exec(record)
  if (!match || match[0] !== record) return
  const pid = Number(match[1])
  if (!Number.isSafeInteger(pid) || pid <= 0 || !/^[1-9]\d*$/.test(match[2])) return
  return { pid, created: match[2] }
}

export function windowsSupervisorStatusForTest(record: string) {
  return windowsSupervisorStatus(record)
}

function windowsSupervisorArguments(record: string) {
  const encoded: unknown = JSON.parse(record)
  if (!Array.isArray(encoded) || !encoded.every((value) => typeof value === "string")) {
    throw new Error("Windows supervisor argv probe returned invalid JSON")
  }
  return encoded.map((value) => Buffer.from(value, "base64").toString())
}

export function windowsSupervisorArgumentsForTest(record: string) {
  return windowsSupervisorArguments(record)
}

// `powershell.exe -Command` consumes the rest of its command line as source
// text. Use `-File` so the supervisor's remaining positional arguments bind to
// `$Command` instead of being parsed as another command.
function writeWindowsSupervisor(nonce: string) {
  const supervisor = path.join(os.tmpdir(), `opencode-supervisor-${nonce}.ps1`)
  writeFileSync(supervisor, windowsSupervisorScript())
  return supervisor
}

// This crosses the real powershell.exe `-File` boundary used by spawnTracked.
// A C# quote-only assertion cannot prove that PowerShell bound the script's
// ValueFromRemainingArguments parameter rather than treating argv as source.
export async function windowsSupervisorArgumentRoundTripForTest(
  argv: string[],
  targetTimeoutMs = 4_000,
  targetSource = "$encoded=[System.Collections.Generic.List[string]]::new(); foreach($argument in $args) { [void]$encoded.Add([Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($argument))) }; [Console]::Out.Write((ConvertTo-Json -InputObject ($encoded.ToArray()) -Compress))",
  finalized?: (result: { readonly statusRemoved: boolean; readonly reaped: boolean }) => void,
  statusDelayMs = 0,
) {
  const nonce = crypto.randomUUID()
  const release = path.join(os.tmpdir(), `opencode-release-${nonce}`)
  const abort = path.join(os.tmpdir(), `opencode-abort-${nonce}`)
  const status = path.join(os.tmpdir(), `opencode-status-${nonce}`)
  const target = path.join(os.tmpdir(), `opencode-supervisor-argv-${nonce}.ps1`)
  const supervisor = writeWindowsSupervisor(nonce)
  let child: ReturnType<typeof Bun.spawn> | undefined
  let completed = false
  let operationError: Error | undefined
  try {
    writeFileSync(target, targetSource)
    child = Bun.spawn(
      ["powershell.exe", "-NoProfile", "-File", supervisor, "powershell.exe", "-NoProfile", "-File", target, ...argv],
      {
        stdout: "pipe",
        stderr: "pipe",
        env: {
          ...process.env,
          OPENCODE_TEST_RELEASE: release,
          OPENCODE_TEST_ABORT: abort,
          OPENCODE_TEST_STATUS: status,
          OPENCODE_TEST_STATUS_DELAY_MS: `${statusDelayMs}`,
        },
      },
    )
    // PowerShell startup and Add-Type compilation are acquisition, not target
    // execution. Drain both pipes before waiting for its status so a startup
    // diagnostic cannot fill a pipe and strand the captured native handle.
    const stdoutResult = new Response(child.stdout).text()
    const stderrResult = new Response(child.stderr).text()
    void stdoutResult.catch(() => {})
    void stderrResult.catch(() => {})
    const acquisitionDeadline = Date.now() + 4_000
    while (!existsSync(status) && Date.now() < acquisitionDeadline)
      await new Promise<void>((resolve) => setTimeout(resolve, 5))
    if (!existsSync(status)) throw new Error("Windows supervisor did not publish launch status")
    if (!windowsSupervisorStatus(readFileSync(status, "utf8")))
      throw new Error("Windows supervisor published an invalid launch status")
    // This deadline is deliberately created after the atomic status record.
    // It bounds the released target (including a pipe that never closes), not
    // the comparatively slow PowerShell/C# acquisition handshake.
    const operationDeadline = Date.now() + targetTimeoutMs
    writeFileSync(release, "release")
    const result = Promise.all([child.exited, stdoutResult, stderrResult])
    void result.catch(() => {})
    const remaining = operationDeadline - Date.now()
    if (remaining <= 0) throw new Error("Windows supervisor argv probe exceeded its deadline after release")
    let timer: ReturnType<typeof setTimeout> | undefined
    const [exitCode, stdout, stderr] = await Promise.race([
      result,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error("Windows supervisor argv probe exceeded its deadline after release")),
          remaining,
        )
      }),
    ]).finally(() => {
      if (timer) clearTimeout(timer)
    })
    if (exitCode !== 0) throw new Error(`Windows supervisor argv probe exited with ${exitCode}: ${stderr}`)
    completed = true
    return windowsSupervisorArguments(stdout)
  } catch (cause) {
    operationError = cause instanceof Error ? cause : new Error("Windows supervisor argv probe failed", { cause })
    throw operationError
  } finally {
    const cleanupErrors: Error[] = []
    let reaped = completed
    if (!completed && child) {
      const cleanupDeadline = Date.now() + 4_000
      try {
        writeFileSync(abort, "abort")
      } catch (cause) {
        recordError(cleanupErrors, "failed to abort Windows supervisor argv probe", cause)
      }
      try {
        child.kill()
      } catch (cause) {
        if (!isMissingProcess(cause))
          recordError(cleanupErrors, "failed to terminate Windows supervisor argv probe", cause)
      }
      reaped = await exitedWithin(
        child,
        Math.max(cleanupDeadline - Date.now(), 0),
        "after Windows argv probe failure",
        cleanupErrors,
      )
      if (!reaped) {
        cleanupErrors.push(new Error("Windows supervisor argv probe did not exit during cleanup"))
      }
    }
    ;[status, release, abort, target, supervisor].forEach((file) => {
      try {
        if (existsSync(file)) unlinkSync(file)
      } catch (cause) {
        recordError(cleanupErrors, `failed to remove Windows argv probe control ${file}`, cause)
      }
    })
    finalized?.({ statusRemoved: !existsSync(status), reaped })
    if (cleanupErrors.length)
      throw new AggregateError(
        operationError ? [operationError, ...cleanupErrors] : cleanupErrors,
        "Windows supervisor argv probe cleanup failed",
      )
  }
}

// CreateProcess receives one command line, then uses the same backslash/quote
// grammar as CommandLineToArgvW. Keep this mirror beside the supervisor source
// so the pure test vectors describe the native quote routine exactly.
function windowsQuote(argument: string) {
  let result = '"'
  let slashes = 0
  for (const character of argument) {
    if (character === "\\") {
      slashes += 1
      continue
    }
    if (character === '"') {
      result += "\\".repeat(slashes * 2 + 1) + '"'
      slashes = 0
      continue
    }
    result += "\\".repeat(slashes) + character
    slashes = 0
  }
  return result + "\\".repeat(slashes * 2) + '"'
}

export function windowsCommandLineForTest(command: string[]) {
  return command.map(windowsQuote).join(" ")
}

export function parseWindowsCommandLineForTest(commandLine: string) {
  const values: string[] = []
  let index = 0
  while (index < commandLine.length) {
    while (commandLine[index] === " " || commandLine[index] === "\t") index += 1
    if (index === commandLine.length) break
    let value = ""
    let quoted = false
    while (index < commandLine.length && (quoted || (commandLine[index] !== " " && commandLine[index] !== "\t"))) {
      let slashes = 0
      while (commandLine[index] === "\\") {
        slashes += 1
        index += 1
      }
      if (commandLine[index] === '"') {
        value += "\\".repeat(Math.floor(slashes / 2))
        if (slashes % 2 === 1) value += '"'
        else quoted = !quoted
        index += 1
        continue
      }
      value += "\\".repeat(slashes)
      if (index < commandLine.length) {
        value += commandLine[index]
        index += 1
      }
    }
    values.push(value)
  }
  return values
}

function windowsNativeArgvObservations(child: ReturnType<typeof Bun.spawn>) {
  const stdout = new Response(child.stdout).text()
  void Promise.resolve(stdout).catch(() => {})
  const stderr = new Response(child.stderr).text()
  void Promise.resolve(stderr).catch(() => {})
  const observed = child.exited
  void Promise.resolve(observed).catch(() => {})
  const result = Promise.all([observed, stdout, stderr])
  void result.catch(() => {})
  return { observed, result }
}

async function cleanupWindowsNativeArgvProbe(
  child: ReturnType<typeof Bun.spawn>,
  observed?: Promise<number>,
  cleanupDeadline = Date.now() + 4_000,
  termWaitMs = 2_000,
) {
  const errors: Error[] = []
  try {
    child.kill("SIGTERM")
  } catch (cause) {
    if (!isMissingProcess(cause)) recordError(errors, "failed to terminate CommandLineToArgvW test helper", cause)
  }
  if (
    !(await exitedWithin(
      child,
      Math.min(termWaitMs, Math.max(cleanupDeadline - Date.now(), 0)),
      "after CommandLineToArgvW test helper SIGTERM",
      errors,
      observed,
    ))
  ) {
    try {
      child.kill("SIGKILL")
    } catch (cause) {
      if (!isMissingProcess(cause)) recordError(errors, "failed to escalate CommandLineToArgvW test helper", cause)
    }
    if (
      !(await exitedWithin(
        child,
        Math.max(cleanupDeadline - Date.now(), 0),
        "after CommandLineToArgvW test helper SIGKILL",
        errors,
        observed,
      ))
    )
      errors.push(new Error("CommandLineToArgvW test helper did not exit during cleanup"))
  }
  return errors
}

export async function windowsNativeArgvGetterCleanupForTest() {
  const signals: string[] = []
  let reaped = false
  let resolveExit: (value: number) => void = () => {}
  const exited = new Promise<number>((resolve) => {
    resolveExit = (value) => {
      reaped = true
      resolve(value)
    }
  })
  const child = {
    pid: 42,
    kill: (signal?: string) => {
      signals.push(signal ?? "SIGTERM")
      if (signal === "SIGKILL") resolveExit(0)
    },
    get stdout() {
      throw new Error("synthetic native argv stdout getter failed")
    },
    stderr: new ReadableStream<Uint8Array>(),
    get exited() {
      return exited
    },
  } as ReturnType<typeof Bun.spawn>
  try {
    windowsNativeArgvObservations(child)
    throw new Error("synthetic native argv observation unexpectedly succeeded")
  } catch (cause) {
    const cleanupErrors = await cleanupWindowsNativeArgvProbe(child, undefined, Date.now() + 100, 10)
    return {
      cause: new AggregateError(
        [cause instanceof Error ? cause : new Error("synthetic native argv observation failed", { cause }), ...cleanupErrors],
        "CommandLineToArgvW test helper cleanup failed",
      ),
      reaped,
      signals,
    }
  }
}

export async function windowsNativeCommandLineRoundTripForTest(command: string[]) {
  const variable = `OPENCODE_TEST_COMMAND_LINE_${crypto.randomUUID().replaceAll("-", "")}`
  const script = String.raw`$source=@'
using System;
using System.Runtime.InteropServices;
public static class OpenCodeArgv {
  [DllImport("shell32.dll", CharSet=CharSet.Unicode)] public static extern IntPtr CommandLineToArgvW(string line,out int count);
  [DllImport("kernel32.dll")] public static extern IntPtr LocalFree(IntPtr value);
}
'@; Add-Type -TypeDefinition $source; $count=0; $pointer=[OpenCodeArgv]::CommandLineToArgvW([Environment]::GetEnvironmentVariable("${variable}"),[ref]$count); try { $values=for($index=0;$index -lt $count;$index++) { [System.Runtime.InteropServices.Marshal]::PtrToStringUni([System.Runtime.InteropServices.Marshal]::ReadIntPtr($pointer,$index*[IntPtr]::Size)) }; [Console]::Out.Write(($values | ConvertTo-Json -Compress)) } finally { [void][OpenCodeArgv]::LocalFree($pointer) }`
  // A native argv probe is test infrastructure: it must not retain a Windows
  // handle indefinitely if PowerShell or either captured pipe stalls.
  const operationDeadline = Date.now() + 4_000
  let child: ReturnType<typeof Bun.spawn> | undefined
  let observed: Promise<number> | undefined
  let completed = false
  let operationError: Error | undefined
  try {
    child = Bun.spawn(["powershell.exe", "-NoProfile", "-Command", script], {
      stdout: "pipe",
      stderr: "pipe",
      env: { ...process.env, [variable]: windowsCommandLineForTest(command) },
    })
    // Every post-spawn native-handle access belongs inside this cleanup
    // boundary. Bun may throw synchronously from a pipe getter or while
    // attaching `exited`; finalization still owns the handle in those cases.
    const acquisition = windowsNativeArgvObservations(child)
    observed = acquisition.observed
    const remaining = operationDeadline - Date.now()
    if (remaining <= 0) throw new Error("CommandLineToArgvW test helper exceeded its deadline")
    let timer: ReturnType<typeof setTimeout> | undefined
    const [exitCode, output, diagnostic] = await Promise.race([
      acquisition.result,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("CommandLineToArgvW test helper exceeded its deadline")), remaining)
      }),
    ]).finally(() => {
      if (timer) clearTimeout(timer)
    })
    if (exitCode !== 0) throw new Error(`CommandLineToArgvW test helper exited with ${exitCode}: ${diagnostic}`)
    const values: unknown = JSON.parse(output)
    if (!Array.isArray(values) || !values.every((value) => typeof value === "string"))
      throw new Error("CommandLineToArgvW test helper returned invalid JSON")
    completed = true
    return values
  } catch (cause) {
    operationError = cause instanceof Error ? cause : new Error("CommandLineToArgvW test helper failed", { cause })
    throw operationError
  } finally {
    if (!completed && child) {
      const cleanupErrors = await cleanupWindowsNativeArgvProbe(child, observed)
      if (operationError || cleanupErrors.length)
        throw new AggregateError(
          operationError ? [operationError, ...cleanupErrors] : cleanupErrors,
          "CommandLineToArgvW test helper cleanup failed",
        )
    }
  }
}

async function spawnTracked(
  command: string[],
  options: Parameters<typeof Bun.spawn>[1],
) {
  if (process.platform === "win32") {
    const nonce = crypto.randomUUID()
    const release = path.join(os.tmpdir(), `opencode-release-${nonce}`)
    const abort = path.join(os.tmpdir(), `opencode-abort-${nonce}`)
    const status = path.join(os.tmpdir(), `opencode-status-${nonce}`)
    const supervisor = writeWindowsSupervisor(nonce)
    let child: ReturnType<typeof Bun.spawn>
    try {
      // `-File` gives the script's ValueFromRemainingArguments parameter the
      // original argv. With `-Command`, these values are concatenated into
      // PowerShell source and no longer arrive in `$Command` reliably.
      child = Bun.spawn(["powershell.exe", "-NoProfile", "-File", supervisor, ...command], {
        ...options,
        env: {
          ...options.env,
          OPENCODE_TEST_RELEASE: release,
          OPENCODE_TEST_ABORT: abort,
          OPENCODE_TEST_STATUS: status,
        },
      })
    } catch (cause) {
      try {
        unlinkSync(supervisor)
      } catch (cleanup) {
        throw new AggregateError(
          [
            new Error("failed to spawn Windows job supervisor", { cause }),
            new Error("failed to remove Windows supervisor script", { cause: cleanup }),
          ],
          "failed to establish Windows Job Object containment",
        )
      }
      throw new Error("failed to spawn Windows job supervisor", { cause })
    }
    const acquisitionDeadline = Date.now() + 4_000
    let root: ProcessIdentity | undefined
    let rawExited: Promise<number> | undefined
    let stdout: ReadableStream<Uint8Array> | undefined
    let stderr: ReadableStream<Uint8Array> | undefined
    const abortLaunch = async (cause: unknown) => {
      const errors: Error[] = [cause instanceof Error ? cause : new Error("Windows acquisition failed", { cause })]
      const cleanupDeadline = Date.now() + 4_000
      // These must stay separate. If either filesystem operation fails, the
      // other still gives the provisional supervisor a path to exit.
      try {
        writeFileSync(abort, "abort")
      } catch (abortCause) {
        recordError(errors, "failed to write Windows acquisition abort", abortCause)
      }
      try {
        writeFileSync(release, "release")
      } catch (releaseCause) {
        recordError(errors, "failed to write Windows acquisition release", releaseCause)
      }
      try {
        // Bun's handle was captured as part of spawn and remains exact even
        // while the PowerShell supervisor is still initializing. If a status
        // record exists, also require its immutable creation token to match.
        if (root) {
          try {
            const current = (await windowsIdentity(child.pid, cleanupDeadline))[0]
            if (current && !sameIdentity(root, current))
              recordError(
                errors,
                `Windows job supervisor ${child.pid} identity changed during acquisition cleanup`,
                undefined,
              )
          } catch (termination) {
            recordError(errors, "failed to terminate Windows acquisition supervisor", termination)
          }
        }
        try {
          // Do this even if identity/status probing failed. `child` carries
          // Bun's exact native handle, so it cannot resolve a recycled PID.
          child.kill()
        } catch (termination) {
          if (!isMissingProcess(termination))
            recordError(errors, "failed to terminate Windows acquisition supervisor", termination)
        }
        if (
          !(await exitedWithin(
            child,
            Math.max(cleanupDeadline - Date.now(), 0),
            "after Windows acquisition failure",
            errors,
            rawExited,
          ))
        ) {
          errors.push(new Error("Windows Job Object supervisor did not exit after acquisition failure"))
        }
      } finally {
        // The supervisor may still be observing these files until it is known
        // dead. Do not remove a control signal before the reap above.
        ;[status, release, abort, supervisor].forEach((file) => {
          try {
            if (existsSync(file)) unlinkSync(file)
          } catch (cleanup) {
            recordError(errors, `failed to remove Windows acquisition control ${file}`, cleanup)
          }
        })
      }
      throw new AggregateError(errors, "failed to establish Windows Job Object containment")
    }
    // The status is written by Start() using GetProcessTimes on the inherited
    // native process handle, before this process is ever allowed to launch the
    // application. Do not defer this identity acquisition to cleanup.
    try {
      // Access each child-owned resource before any status, identity, or
      // release work. If Bun throws synchronously here, abortLaunch still owns
      // the exact supervisor handle and will terminate, escalate, and reap it.
      rawExited = child.exited
      stdout = child.stdout
      stderr = child.stderr
      void Promise.resolve(rawExited).catch(() => {})
      while (!existsSync(status) && Date.now() < acquisitionDeadline)
        await new Promise<void>((resolve) => setTimeout(resolve, 5))
      const trustedStatus = existsSync(status) ? windowsSupervisorStatus(readFileSync(status, "utf8")) : undefined
      root = (await windowsIdentity(child.pid, acquisitionDeadline))[0]
      if (!root || trustedStatus?.pid !== child.pid || root.started !== `native:${trustedStatus.created}`) {
        throw new Error("failed to capture exact Windows job supervisor identity before launching opencode")
      }
      const tracker = trackWindowsProcess(child, root, release, abort, status, supervisor)
      const captured = await tracker.scan(acquisitionDeadline)
      if (tracker.errors.length || !captured.some((identity) => sameIdentity(root!, identity))) {
        throw new Error("failed to establish Windows Job Object containment")
      }
      writeFileSync(release, "release")
      return { child, tracker, rawExited, stdout, stderr }
    } catch (cause) {
      return await abortLaunch(cause)
    }
  }
  const cgroup = prepareCgroup()
  if (cgroup) {
    const status = path.join(os.tmpdir(), `opencode-cgroup-${crypto.randomUUID()}`)
    const release = path.join(os.tmpdir(), `opencode-release-${crypto.randomUUID()}`)
    const abort = path.join(os.tmpdir(), `opencode-abort-${crypto.randomUUID()}`)
    let child: ReturnType<typeof Bun.spawn>
    try {
      child = Bun.spawn(
        [
          "/bin/sh",
          "-c",
          'if printf "%s\\n" "$$" > "$1/cgroup.procs"; then printf ready > "$2"; while [ ! -e "$3" ] && [ ! -e "$4" ]; do sleep 0.01; done; [ -e "$4" ] && exit 125; rm "$3"; shift 4; exec "$@"; fi; printf failed > "$2"; exit 125',
          "--",
          cgroup.cgroupPath,
          status,
          release,
          abort,
          ...command,
        ],
        options,
      )
    } catch (cause) {
      const errors: Error[] = [cause instanceof Error ? cause : new Error("failed to spawn cgroup gate", { cause })]
      try {
        await removeAbortedCgroup(cgroup.cgroupPath, cgroup.containment, Date.now() + 4_000)
      } catch (cleanup) {
        collectCleanupErrors(errors, cleanup)
      }
      throw new AggregateError(errors, "failed to spawn cgroup gate")
    }
    // The shell is already a provisional owned child. Register cleanup before
    // reading any status/proc record so every acquisition failure reaps it.
    const tracker = trackContainedProcess(child, cgroup.containment, cgroup.cgroupPath)
    try {
      const until = Date.now() + 1_000
      while (!existsSync(status) && Date.now() < until) await new Promise<void>((resolve) => setTimeout(resolve, 5))
      const outcome = existsSync(status) ? readFileSync(status, "utf8") : ""
      if (existsSync(status)) unlinkSync(status)
      const captured = await tracker.scan(Date.now() + 4_000)
      const root = captured.find((identity) => identity.pid === child.pid)
      if (!root || tracker.errors.length)
        throw new Error("failed to capture exact cgroup gate identity before launching opencode")
      const verified = await tracker.scan(Date.now() + 4_000)
      if (
        outcome === "ready" &&
        tracker.errors.length === 0 &&
        verified.some((identity) => sameIdentity(root, identity))
      ) {
        writeFileSync(release, "release")
        return { child, tracker }
      }
      throw new Error("failed to establish cgroup containment before launching opencode")
    } catch (cause) {
      const errors: Error[] = [cause instanceof Error ? cause : new Error("cgroup acquisition failed", { cause })]
      const deadline = Date.now() + 4_000
      try {
        writeFileSync(abort, "abort")
      } catch (abortCause) {
        recordError(errors, "failed to write cgroup acquisition abort", abortCause)
      }
      try {
        writeFileSync(release, "release")
      } catch (releaseCause) {
        recordError(errors, "failed to write cgroup acquisition release", releaseCause)
      }
      try {
        await terminateProcessPromise(child, () => {}, tracker, deadline)
      } catch (cleanup) {
        collectCleanupErrors(errors, cleanup)
      }
      try {
        await removeAbortedCgroup(cgroup.cgroupPath, cgroup.containment, deadline)
      } catch (cleanup) {
        collectCleanupErrors(errors, cleanup)
      }
      ;[release, abort].forEach((file) => {
        try {
          if (existsSync(file)) unlinkSync(file)
        } catch (cleanup) {
          recordError(errors, `failed to remove cgroup acquisition control ${file}`, cleanup)
        }
      })
      throw new AggregateError(errors, "failed to establish cgroup containment before launching opencode")
    }
  }
  // The portable contract is a dedicated POSIX group plus a per-launch nonce.
  // The gate prevents exec of OpenCode until the group and tracker are proven.
  const nonce = crypto.randomUUID()
  const release = path.join(os.tmpdir(), `opencode-release-${nonce}`)
  const abortFile = path.join(os.tmpdir(), `opencode-abort-${nonce}`)
  let child: ReturnType<typeof Bun.spawn>
  try {
    child = Bun.spawn(
      [
        "/bin/sh",
        "-c",
        'while [ ! -e "$1" ] && [ ! -e "$2" ]; do sleep 0.01; done; [ -e "$2" ] && exit 125; rm "$1"; shift 2; exec "$@"',
        "--",
        release,
        abortFile,
        ...command,
      ],
      {
        ...options,
        detached: true,
        env: { ...options.env, OPENCODE_TEST_PROCESS_NONCE: nonce },
      },
    )
  } catch (cause) {
    throw new Error("failed to spawn portable gate", { cause })
  }
  const acquisitionDeadline = Date.now() + 4_000
  let tracker: ProcessTracker | undefined
  const abort = async (cause: unknown) => {
    const errors: Error[] = [cause instanceof Error ? cause : new Error("portable acquisition failed", { cause })]
    const cleanupDeadline = Date.now() + 4_000
    // Abort opens the gate only far enough for the shell to exit. It must be
    // done before waiting, and cleanup never inherits an expired acquisition
    // deadline.
    try {
      writeFileSync(abortFile, "abort")
    } catch (abortCause) {
      recordError(errors, "failed to write portable acquisition abort", abortCause)
    }
    try {
      if (tracker) {
        await terminateProcessPromise(child, () => {}, tracker, cleanupDeadline)
      } else {
        // The gate is the direct Bun child captured at spawn. This is the
        // only pre-tracker fallback; later failures always use the tracker.
        child.kill()
      }
    } catch (cleanup) {
      collectCleanupErrors(errors, cleanup)
    }
    if (
      !(await exitedWithin(
        child,
        Math.max(cleanupDeadline - Date.now(), 0),
        "after portable acquisition failure",
        errors,
      ))
    ) {
      errors.push(new Error("portable gate shell remained unresolved after acquisition failure"))
    }
    ;[release, abortFile].forEach((file) => {
      try {
        if (existsSync(file)) unlinkSync(file)
      } catch (cleanup) {
        recordError(errors, `failed to remove portable acquisition control ${file}`, cleanup)
      }
    })
    throw new AggregateError(errors, "failed to establish portable process containment before launching opencode")
  }
  const initial = await portableSnapshots(nonce, acquisitionDeadline, child.pid).catch(abort)
  if (initial.errors.length) {
    return await abort(new AggregateError(initial.errors, "failed to snapshot portable containment before launch"))
  }
  const root = initial.snapshots.find((snapshot) => snapshot.pid === child.pid)
  if (!root || !root.nonce || root.group !== child.pid) {
    return await abort(new Error("failed to capture portable gate identity before launching opencode"))
  }
  tracker = trackPortableProcess(child, nonce, root)
  const captured = await tracker.scan(acquisitionDeadline)
  if (tracker.errors.length || !captured.some((identity) => sameIdentity(root, identity))) {
    const errors: Error[] = []
    const cleanupDeadline = Date.now() + 4_000
    try {
      writeFileSync(abortFile, "abort")
    } catch (cleanup) {
      collectCleanupErrors(errors, cleanup)
    }
    try {
      await terminateProcessPromise(child, () => {}, tracker, cleanupDeadline)
    } catch (cleanup) {
      collectCleanupErrors(errors, cleanup)
    }
    ;[release, abortFile].forEach((file) => {
      try {
        if (existsSync(file)) unlinkSync(file)
      } catch (cleanup) {
        collectCleanupErrors(errors, cleanup)
      }
    })
    throw new AggregateError(
      [...tracker.errors, ...errors],
      "failed to establish portable process containment before launching opencode",
    )
  }
  try {
    writeFileSync(release, "release")
  } catch (cause) {
    const errors: Error[] = [new Error("failed to open portable launch gate", { cause })]
    const cleanupDeadline = Date.now() + 4_000
    try {
      writeFileSync(abortFile, "abort")
    } catch (cleanup) {
      collectCleanupErrors(errors, cleanup)
    }
    try {
      await terminateProcessPromise(child, () => {}, tracker, cleanupDeadline)
    } catch (cleanup) {
      collectCleanupErrors(errors, cleanup)
    }
    ;[release, abortFile].forEach((file) => {
      try {
        if (existsSync(file)) unlinkSync(file)
      } catch (cleanup) {
        collectCleanupErrors(errors, cleanup)
      }
    })
    throw new AggregateError(errors, "failed to release portable launch gate")
  }
  const transitionDeadline = acquisitionDeadline
  while (Date.now() < transitionDeadline) {
    const observed = (await tracker.scan(transitionDeadline)).find((identity) => identity.pid === child.pid)
    if (tracker.errors.length) break
    if (observed && observed.started === root.started && observed.executable !== root.executable)
      return { child, tracker }
    await new Promise<void>((resolve) => setTimeout(resolve, 5))
  }
  const errors: Error[] = [new Error("portable gate shell did not exec OpenCode with its recorded identity")]
  try {
    await terminateProcessPromise(child, () => {}, tracker, Date.now() + 4_000)
  } catch (cleanup) {
    collectCleanupErrors(errors, cleanup)
  }
  ;[release, abortFile].forEach((file) => {
    try {
      if (existsSync(file)) unlinkSync(file)
    } catch (cleanup) {
      collectCleanupErrors(errors, cleanup)
    }
  })
  throw new AggregateError(errors, "portable gate did not complete its exact exec transition")
}

async function observeOwnedExit(
  proc: Omit<TrackedProcess, "exited">,
  mark: ReturnType<typeof unitDiagnostic>,
): Promise<TrackedProcess> {
  const cleanup = async (cause: unknown): Promise<never> => {
    const errors: Error[] = [
      new Error(`failed to observe process ${proc.child.pid} exit during acquisition`, { cause }),
    ]
    try {
      await terminateProcessPromise(proc.child, mark, proc.tracker)
    } catch (cleanup) {
      collectCleanupErrors(errors, cleanup)
    }
    throw new AggregateError(errors, `failed to acquire owned process ${proc.child.pid}`)
  }
  try {
    // Read the getter before registering diagnostics: both access and the
    // diagnostic wrapper are acquisition steps, and either can fail.
    const exited = proc.rawExited ?? proc.child.exited
    const observed = diagnosticExit(exited, mark, proc.child.pid)
    const monitored = Promise.resolve(observed).catch(cleanup)
    // A long-lived handle may intentionally leave `.exited` unread. Mark the
    // rejection handled here while retaining the same rejected promise for a
    // caller that does await it.
    void monitored.catch(() => {})
    return { ...proc, exited: monitored }
  } catch (cause) {
    return await cleanup(cause)
  }
}

export async function ownedExitObservationFailureForTest(kind: "getter" | "then") {
  const errors: Error[] = []
  let stopped = false
  let terminated = false
  const child = {
    pid: 42,
    kill: () => {},
    get exited() {
      if (kind === "getter") throw new Error("synthetic exited getter failed")
      return {
        then: () => {
          throw new Error("synthetic exited then attachment failed")
        },
      } as Promise<number>
    },
  } as Pick<ReturnType<typeof Bun.spawn>, "pid" | "kill" | "exited">
  const tracker: ProcessTracker = {
    scan: async () => (terminated ? [] : [{ pid: 42, started: "started", executable: "synthetic", containment: "test" }]),
    signal: async () => {
      terminated = true
    },
    stop: async () => {
      stopped = true
    },
    errors,
  }
  try {
    const observed = await observeOwnedExit({ child: child as ReturnType<typeof Bun.spawn>, tracker }, () => {})
    await observed.exited
    throw new Error("synthetic exit observation unexpectedly succeeded")
  } catch (cause) {
    return { cause, stopped, terminated }
  }
}

export async function lateExitObservationFailureForTest() {
  const errors: Error[] = []
  let stopped = false
  let terminated = false
  const exited = new Promise<number>((_, reject) => {
    setTimeout(() => reject(new Error("synthetic late exit observation rejected")), 10)
  })
  void exited.catch(() => {})
  const child = {
    pid: 42,
    kill: () => {},
    exited,
  } as Pick<ReturnType<typeof Bun.spawn>, "pid" | "kill" | "exited">
  const tracker: ProcessTracker = {
    scan: async () => (terminated ? [] : [{ pid: 42, started: "started", executable: "synthetic", containment: "test" }]),
    signal: async () => {
      terminated = true
    },
    stop: async () => {
      stopped = true
    },
    errors,
  }
  try {
    await terminateProcessPromise(child, () => {}, tracker, Date.now() + 500)
    throw new Error("synthetic late exit observation unexpectedly succeeded")
  } catch (cause) {
    return { cause, stopped, terminated }
  }
}

async function terminateProcessPromise(
  child: Pick<ReturnType<typeof Bun.spawn>, "pid" | "kill" | "exited">,
  mark: ReturnType<typeof unitDiagnostic>,
  tracker: ProcessTracker,
  deadline = Date.now() + 4_000,
  observed?: Promise<number>,
) {
  if (tracker.run && tracker.unlocked) {
    return tracker.run(() => terminateProcessPromiseUnlocked(child, mark, tracker.unlocked!, deadline, observed))
  }
  return terminateProcessPromiseUnlocked(child, mark, tracker, deadline, observed)
}

async function terminateProcessPromiseUnlocked(
  child: Pick<ReturnType<typeof Bun.spawn>, "pid" | "kill" | "exited">,
  mark: ReturnType<typeof unitDiagnostic>,
  tracker: ProcessTracker,
  deadline: number,
  observed?: Promise<number>,
) {
  const errors: Error[] = []
  const owned = async () => {
    const priorErrors = tracker.errors.length
    const processes = await tracker.scan(deadline)
    if (tracker.errors.length !== priorErrors) errors.push(...tracker.errors.slice(priorErrors))
    return processes
  }
  const goneWithin = async (
    milliseconds: number,
    resignal?: "SIGTERM" | "SIGKILL",
    returnOnKnown = false,
  ) => {
    const until = Math.min(deadline, Date.now() + milliseconds)
    let emptyScans = 0
    while (true) {
      const processes = await owned()
      if (resignal && processes.length) await signal(resignal)
      if (returnOnKnown && tracker.hasKnown?.()) return false
      if (tracker.errors.length === 0 && tracker.scanComplete?.() !== false && processes.length === 0) {
        emptyScans += 1
        if (emptyScans >= quiescenceScans) return true
      } else {
        emptyScans = 0
      }
      const remaining = until - Date.now()
      if (remaining <= 0) return false
      await new Promise<void>((resolve) => setTimeout(resolve, Math.min(remaining, quiescenceDelayMs)))
    }
  }
  const signal = async (name: "SIGTERM" | "SIGKILL") => {
    const priorErrors = tracker.errors.length
    // Darwin's broad nonce discovery has a separate budget. The finalizer
    // must spend its TERM/KILL and direct-child reap reserve on identities it
    // already owns before starting a broad PID walk.
    if (tracker.broadDiscovery && tracker.signalKnown) await tracker.signalKnown(name, deadline, mark)
    else await tracker.signal(name, deadline, mark)
    errors.push(...tracker.errors.slice(priorErrors))
    mark("exit.wait", child.pid)
  }
  const reap = async (stage: string, limit = deadline) => {
    const remaining = Math.max(Math.min(limit, deadline) - Date.now(), 0)
    return exitedWithin(child, remaining, stage, errors, observed)
  }
  // A direct child exit is not proof that a shell, grandchild, or pipe owner
  // is gone. This applies to ACP's EOF path as much as forced termination.
  try {
    if (tracker.broadDiscovery) {
      // Darwin's nonce walk is broad, potentially slow, and its cursor can be
      // incomplete. Keep it entirely after the direct child's TERM/KILL/reap
      // path: neither arbitrary procargs helpers nor their cleanup can spend
      // the reserve needed to contain the exact child we already own.
      await signal("SIGTERM")
      await signal("SIGKILL")
      const reaped = await reap("after SIGKILL before Darwin discovery")
      if (reaped) {
        // Each scan consumes only its bounded Darwin slice and advances the
        // cursor. Do not use a separate 1.5s cap here: a late PID-list prefix
        // remains a possible nonce descendant until enumeration completes.
        const killed = await goneWithin(Math.max(deadline - Date.now(), 0), "SIGKILL")
        if (!killed) {
          if (tracker.scanComplete?.() === false)
            errors.push(
              new Error(
                `Darwin nonce discovery for ${child.pid} remained incomplete at the cleanup deadline; possible descendant leak`,
              ),
            )
          if (tracker.scanComplete?.() !== false)
            errors.push(new Error(`tracked processes for ${child.pid} did not exit after SIGKILL`))
        }
      }
      if (!reaped) errors.push(new Error(`direct child ${child.pid} did not reap before Darwin discovery could begin`))
    }
    if (!tracker.broadDiscovery) {
      const initiallyGone = await goneWithin(quiescenceDelayMs * quiescenceScans)
      if (!initiallyGone) {
        await signal("SIGTERM")
        const terminated = await goneWithin(1_500)
        if (!terminated) {
          await signal("SIGKILL")
          const killed = await goneWithin(1_500, "SIGKILL")
          if (!killed) errors.push(new Error(`tracked processes for ${child.pid} did not exit after SIGKILL`))
        }
      }
    }
    // Membership scans exclude zombies by design, but that is only evidence
    // that a target cannot be signalled. The direct Bun child is reaped only
    // when its own exit promise settles, so never finish containment cleanup
    // on tracker quiescence alone.
    if (!(await reap("during process tree cleanup")))
      errors.push(new Error(`direct child ${child.pid} did not reap before the cleanup deadline`))
  } finally {
    try {
      await tracker.stop()
    } catch (cause) {
      collectCleanupErrors(errors, cause)
    }
    tracker.errors.forEach((error) => {
      if (!errors.includes(error)) errors.push(error)
    })
  }
  if (errors.length) throw new AggregateError(errors, `failed to terminate process tree for ${child.pid}`)
}

function terminateProcess(
  child: Pick<ReturnType<typeof Bun.spawn>, "pid" | "kill" | "exited">,
  mark: ReturnType<typeof unitDiagnostic>,
  tracker: ProcessTracker,
  observed?: Promise<number>,
) {
  return Effect.tryPromise({
    try: () => terminateProcessPromise(child, mark, tracker, undefined, observed),
    catch: (cause) =>
      cause instanceof Error ? cause : new Error(`failed to terminate process ${child.pid}`, { cause }),
  })
}

type AcpCleanupChild = Pick<ReturnType<typeof Bun.spawn>, "pid" | "kill" | "exited"> & {
  readonly stdin: { end: () => unknown }
}

async function cleanupAcpProcess(child: AcpCleanupChild, tracker: ProcessTracker, mark: ReturnType<typeof unitDiagnostic>) {
  const errors: Error[] = []
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    await Promise.race([
      Promise.resolve().then(() => {
        mark("stdin.close.request", child.pid)
        return child.stdin.end()
      }),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`timed out closing stdin for process ${child.pid}`)), 2_000)
      }),
    ])
    mark("exit.wait", child.pid)
  } catch (cause) {
    collectCleanupErrors(errors, new Error(`failed to close stdin for process ${child.pid}`, { cause }))
  } finally {
    if (timer) clearTimeout(timer)
  }

  // The close operation and direct-child exit are advisory shutdown only. A
  // fresh cleanup reserve follows them so either failure still reaches the
  // tracker, escalation, and reaping path.
  await exitedWithin(child, 2_000, "after stdin close", errors)
  try {
    await terminateProcessPromise(child, mark, tracker, Date.now() + 4_000)
  } catch (cause) {
    collectCleanupErrors(errors, cause)
  }
  if (errors.length) throw new AggregateError(errors, `failed to clean up ACP process ${child.pid}`)
}

export async function acpCleanupFailuresForTest() {
  const errors: Error[] = []
  let stopped = false
  let terminated = false
  const exited = new Promise<number>((_, reject) => {
    setTimeout(() => reject(new Error("synthetic ACP exit observation rejected")), 10)
  })
  const child = {
    pid: 42,
    kill: () => {},
    get exited() {
      return exited
    },
    stdin: {
      end: () => {
        throw new Error("synthetic ACP stdin end failed")
      },
    },
  } as AcpCleanupChild
  const tracker: ProcessTracker = {
    scan: async () => (terminated ? [] : [{ pid: 42, started: "started", executable: "synthetic", containment: "test" }]),
    signal: async () => {
      terminated = true
    },
    stop: async () => {
      stopped = true
    },
    errors,
  }
  try {
    await cleanupAcpProcess(child, tracker, () => {})
    throw new Error("synthetic ACP cleanup unexpectedly succeeded")
  } catch (cause) {
    return { cause, stopped, terminated }
  }
}

function collectCleanupErrors(errors: Error[], cause: unknown) {
  // Keep aggregate nodes intact. Their message, members, and optional cause
  // identify the cleanup operation; flattening them leaves a RunResult with
  // unrelated leaf errors and no explanation of how they were collected.
  errors.push(cause instanceof Error ? cause : new Error("unknown process cleanup error", { cause }))
}

export function formatProcessErrorForTest(cause: unknown) {
  const lines: string[] = []
  const seen = new Set<object>()
  const visit = (current: unknown, prefix: string) => {
    if (typeof current === "object" && current !== null) {
      if (seen.has(current)) {
        lines.push(`${prefix}[circular error]`)
        return
      }
      seen.add(current)
    }
    if (!(current instanceof Error)) {
      lines.push(`${prefix}${String(current)}`)
      return
    }
    lines.push(`${prefix}${current.message}`)
    if (current instanceof AggregateError) {
      Array.from(current.errors).forEach((error, index) => visit(error, `${prefix}  [${index}] `))
    }
    if ("cause" in current && current.cause !== undefined) visit(current.cause, `${prefix}  caused by: `)
  }
  visit(cause, "")
  return lines.join("\n")
}

function isolatedEnv(home: string, configJson: string): Record<string, string> {
  return {
    OPENCODE_TEST_HOME: home,
    HOME: home,
    XDG_CONFIG_HOME: path.join(home, ".config"),
    XDG_DATA_HOME: path.join(home, ".local/share"),
    XDG_STATE_HOME: path.join(home, ".local/state"),
    XDG_CACHE_HOME: path.join(home, ".cache"),
    OPENCODE_CONFIG_CONTENT: configJson,
    OPENCODE_DISABLE_PROJECT_CONFIG: "1",
    OPENCODE_PURE: "1",
    OPENCODE_DISABLE_AUTOUPDATE: "1",
    OPENCODE_DISABLE_AUTOCOMPACT: "1",
    OPENCODE_DISABLE_MODELS_FETCH: "1",
    OPENCODE_AUTH_CONTENT: "{}",
  }
}

export type RunResult = {
  readonly exitCode: number
  readonly stdout: string
  readonly stderr: string
  readonly durationMs: number
}

export type RunHandle = {
  readonly interrupt: () => void
  readonly result: Effect.Effect<RunResult>
}

export type SpawnOpts = { readonly timeoutMs?: number; readonly env?: Record<string, string> }

// Typed equivalent of constructing argv for `opencode run`. New flags should
// land here so tests stay grep-able and refactor-safe.
export type RunOpts = SpawnOpts & {
  readonly model?: string
  readonly agent?: string
  readonly format?: "default" | "json"
  readonly command?: string
  readonly printLogs?: boolean
  readonly permission?: Record<string, "ask" | "allow" | "deny">
  readonly extraArgs?: string[]
}

// `opencode serve` is a long-lived process — it never exits on its own.
// `serve(opts)` therefore returns a handle inside the caller's Scope: the
// subprocess is killed when the scope closes (test end), and the URL the
// server actually bound to (port 0 means OS-assigned) is parsed off stdout.
export type ServeOpts = SpawnOpts & {
  readonly port?: number
  readonly hostname?: string
  readonly extraArgs?: string[]
  // How long to wait for the "listening on http://..." line before failing.
  // Default 15s — startup is dominated by bun's transpile + plugin init, not
  // the actual listen() call.
  readonly readyTimeoutMs?: number
}

export type ServeHandle = {
  // Full URL the server is bound to, e.g. "http://127.0.0.1:54321". Use this
  // as the base for HTTP requests in tests — never assume the port.
  readonly url: string
  readonly hostname: string
  readonly port: number
  // Sends SIGTERM. The scope finalizer also calls this, so tests rarely need
  // to invoke it directly — useful for tests that assert exit behavior.
  readonly kill: () => void
  // Resolves with the exit code once the process exits. Bun returns a number.
  readonly exited: Promise<number>
}

// `opencode acp` speaks newline-delimited JSON-RPC over stdin/stdout. It is
// long-lived and exits cleanly when stdin is closed. The handle exposes the
// duplex stream as send/receive rather than raw pipes so tests don't have to
// reimplement framing on every call site.
export type AcpOpts = SpawnOpts & {
  readonly cwd?: string
  readonly extraArgs?: string[]
}

export type AcpHandle = {
  // Opt-in waits: acp() itself returns before readiness so callers can close
  // stdin immediately. Source startup is bounded separately from RPC/EOF.
  readonly ready: Effect.Effect<void, Error>
  readonly exitAfterStartup: Effect.Effect<number, Error>
  // Writes a single JSON-RPC message to the child's stdin as one ndjson line.
  readonly send: (msg: object) => Effect.Effect<void>
  // Resolves with the next parsed JSON-RPC line from the child's stdout.
  // Lines are buffered in a queue so multiple receives in a row won't drop
  // anything. Pair with `Effect.timeout` if a test wants a deadline.
  readonly receive: Effect.Effect<unknown>
  // Closes stdin. ACP exits cleanly on stdin EOF; the scope finalizer also
  // calls this, so tests only need it when asserting exit behavior.
  readonly close: () => void
  readonly exited: Promise<number>
  readonly diagnostics: () => string
}

export type OpencodeCli = {
  // High-level: run a single prompt against the test model. Short-lived.
  readonly run: (message: string, opts?: RunOpts) => Effect.Effect<RunResult>
  readonly startRun: (message: string, opts?: RunOpts) => Effect.Effect<RunHandle, never, Scope.Scope>
  // Spawn `opencode serve` and wait until it's listening. Long-lived: the
  // returned handle is killed when the caller's Scope closes. Fails if the
  // listening line doesn't appear within `readyTimeoutMs`.
  readonly serve: (opts?: ServeOpts) => Effect.Effect<ServeHandle, Error, Scope.Scope>
  // Spawn `opencode acp` and return a duplex JSON-RPC handle. Long-lived:
  // the subprocess exits on stdin close, which the scope finalizer triggers.
  readonly acp: (opts?: AcpOpts) => Effect.Effect<AcpHandle, Error, Scope.Scope>
  // Escape hatch: any CLI invocation with full control over argv. Used to test
  // commands that don't yet have a typed builder.
  readonly spawn: (args: string[], opts?: SpawnOpts) => Effect.Effect<RunResult>
  // Convenience assertion. Dumps captured stderr/stdout on mismatch so CI
  // failures are debuggable without re-running locally.
  readonly expectExit: (result: RunResult, expected: number, label?: string) => void
  // Parse `--format json` stdout into one event object per non-empty line.
  // The CLI writes `JSON.stringify({ type, sessionID, ... }) + EOL` for each
  // event (see src/cli/cmd/run.ts `emit`). Throws on a malformed line so
  // tests fail loudly rather than silently skipping data.
  readonly parseJsonEvents: (stdout: string) => Array<Record<string, unknown>>
}

export type CliFixture = {
  readonly llm: TestLLMServer["Service"]
  readonly home: string
  readonly opencode: OpencodeCli
}

// Provisions a TestLLMServer + tmpdir + spawn helper and invokes fn. Cleans
// up the tmpdir on scope exit. TestLLMServer.layer is provided internally so
// the caller doesn't need to wire it up — the fixture's lifetime is tied to
// the surrounding Scope.
export function withCliFixture<A, E>(
  fn: (input: CliFixture) => Effect.Effect<A, E, Scope.Scope | HttpClient.HttpClient>,
): Effect.Effect<A, E | unknown, Scope.Scope> {
  return Effect.gen(function* () {
    const llm = yield* TestLLMServer
    const fs = yield* FSUtil.Service

    const home = yield* fs.makeTempDirectory({ directory: os.tmpdir(), prefix: "oc-cli-" })
    yield* Effect.addFinalizer(() =>
      fs
        .remove(home, { recursive: true })
        .pipe(Effect.retry(Schedule.spaced("50 millis").pipe(Schedule.both(Schedule.recurs(20)))), Effect.ignore),
    )

    const configJson = JSON.stringify(testProviderConfig(llm.url))
    const env = isolatedEnv(home, configJson)

    const spawn = (args: string[], opts?: SpawnOpts): Effect.Effect<RunResult> => {
      const start = Date.now()
      const timeoutMs = opts?.timeoutMs ?? 90_000
      // `spawn` is the short-lived public builder, so it owns the resource
      // scope itself. Expected spawn/exit/deadline failures retain the legacy
      // RunResult contract; a release failure remains a defect after scope
      // close instead of being silently converted into a passing result.
      return Effect.scoped(
        Effect.gen(function* () {
          const owner = yield* DiagnosticOwner
          const mark = unitDiagnostic(args[0] === "run" ? "run" : "other", undefined, owner)
          mark("spawn.request")
          const proc = yield* Effect.acquireRelease(
            Effect.tryPromise({
              try: async () => {
                const proc = await spawnTracked([process.execPath, "run", cliEntry, ...args], {
                  cwd: home,
                  detached: process.platform !== "win32",
                  env: { ...process.env, ...env, ...opts?.env },
                  stdin: "ignore",
                  stdout: "pipe",
                  stderr: "pipe",
                })
                mark("spawn", proc.child.pid)
                return await observeOwnedExit(proc, mark)
              },
              catch: (cause) => new Error("failed to spawn opencode process", { cause }),
            }),
            (owned) => terminateProcess(owned.child, mark, owned.tracker, owned.rawExited).pipe(Effect.orDie),
          )
          mark("stdout.drain.start", proc.child.pid)
          const stdout = new Response(proc.stdout ?? proc.child.stdout).text()
          diagnosticText(stdout, mark, "stdout", proc.child.pid)
          mark("stderr.drain.start", proc.child.pid)
          const stderr = new Response(proc.stderr ?? proc.child.stderr).text()
          diagnosticText(stderr, mark, "stderr", proc.child.pid)
          const result = yield* Effect.all([
            Effect.tryPromise({
              try: () => proc.exited,
              catch: (cause) => new Error(`failed to observe process ${proc.child.pid} exit`, { cause }),
            }),
            Effect.promise(() => Promise.all([stdout, stderr])),
          ]).pipe(
            Effect.timeoutOrElse({
              duration: Duration.millis(timeoutMs),
              orElse: () => Effect.fail(new Error(`opencode process ${proc.child.pid} exceeded ${timeoutMs}ms`)),
            }),
          )
          mark("spawn.settled")
          return {
            exitCode: result[0],
            stdout: normalizeLines(result[1][0]),
            stderr: normalizeLines(result[1][1]),
            durationMs: Date.now() - start,
          }
        }).pipe(
          Effect.catch((cause) =>
            Effect.succeed({
              exitCode: -1,
              stdout: "",
              stderr: `${formatProcessErrorForTest(cause)}\n`,
              durationMs: Date.now() - start,
            }),
          ),
        ),
      ).pipe(Effect.orDie)
    }

    const runArgs = (message: string, opts?: RunOpts) => {
      const argv: string[] = ["run"]
      if (opts?.printLogs) argv.push("--print-logs")
      argv.push("--model", opts?.model ?? testModelID)
      if (opts?.agent) argv.push("--agent", opts.agent)
      if (opts?.format) argv.push("--format", opts.format)
      if (opts?.command) argv.push("--command", opts.command)
      if (opts?.extraArgs) argv.push(...opts.extraArgs)
      argv.push(message)
      return argv
    }

    const runOpts = (opts?: RunOpts): SpawnOpts | undefined => {
      if (!opts?.permission) return opts
      return {
        ...opts,
        env: {
          ...opts.env,
          OPENCODE_CONFIG_CONTENT: JSON.stringify({
            ...testProviderConfig(llm.url),
            permission: opts.permission,
          }),
        },
      }
    }

    const run = (message: string, opts?: RunOpts): Effect.Effect<RunResult> => {
      return spawn(runArgs(message, opts), runOpts(opts))
    }

    const startRun = Effect.fn("opencode.startRun")(function* (message: string, opts?: RunOpts) {
      const owner = yield* DiagnosticOwner
      const mark = unitDiagnostic("run", undefined, owner)
      mark("spawn.request")
      const start = Date.now()
      const options = runOpts(opts)
      const proc = yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: async () => {
            const proc = await spawnTracked([process.execPath, "run", cliEntry, ...runArgs(message, opts)], {
              cwd: home,
              detached: process.platform !== "win32",
              env: { ...process.env, ...env, ...options?.env },
              stdin: "ignore",
              stdout: "pipe",
              stderr: "pipe",
            })
            mark("spawn", proc.child.pid)
            return await observeOwnedExit(proc, mark)
          },
          catch: (cause) => new Error("failed to spawn opencode process", { cause }),
        }).pipe(Effect.orDie),
        (owned) => terminateProcess(owned.child, mark, owned.tracker, owned.rawExited).pipe(Effect.orDie),
      )
      mark("stdout.drain.start", proc.child.pid)
      const stdout = new Response(proc.stdout ?? proc.child.stdout).text()
      diagnosticText(stdout, mark, "stdout", proc.child.pid)
      mark("stderr.drain.start", proc.child.pid)
      const stderr = new Response(proc.stderr ?? proc.child.stderr).text()
      diagnosticText(stderr, mark, "stderr", proc.child.pid)

      return {
        interrupt: () => {
          // Do not use Bun's cached ChildProcess handle here: a recycled PID
          // must never receive a signal. The tracker reads cgroup membership
          // and the immutable start token immediately before each signal.
          proc.tracker.signal("SIGINT", Date.now() + 1_000, mark)
        },
        result: Effect.promise(async () => ({
          exitCode: await proc.exited,
          stdout: normalizeLines(await stdout),
          stderr: normalizeLines(await stderr),
          durationMs: Date.now() - start,
        })),
      } satisfies RunHandle
    })

    const serve = Effect.fn("opencode.serve")(function* (opts?: ServeOpts) {
      const owner = yield* DiagnosticOwner
      const mark = unitDiagnostic("serve", undefined, owner)
      mark("spawn.request")
      const readyTimeoutMs = opts?.readyTimeoutMs ?? 15_000
      const readyDeadline = Date.now() + readyTimeoutMs
      const argv = ["serve"]
      // Default port 0 — let the OS pick a free port, parse the actual one
      // off stdout. Hard-coded ports flake under parallel tests.
      argv.push("--port", String(opts?.port ?? 0))
      if (opts?.hostname) argv.push("--hostname", opts.hostname)
      if (opts?.extraArgs) argv.push(...opts.extraArgs)

      // Acquire the subprocess; release sends SIGTERM and awaits exit on
      // scope close, escalating to SIGKILL on a bounded deadline.
      const proc = yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: async () => {
            const proc = await spawnTracked([process.execPath, "run", cliEntry, ...argv], {
              cwd: home,
              detached: process.platform !== "win32",
              env: { ...process.env, ...env, ...opts?.env },
              stdout: "pipe",
              stderr: "pipe",
            })
            mark("spawn", proc.child.pid)
            return await observeOwnedExit(proc, mark)
          },
          catch: (cause) => new Error("failed to spawn opencode process", { cause }),
        }),
        (owned) => terminateProcess(owned.child, mark, owned.tracker, owned.rawExited).pipe(Effect.orDie),
      )

      // Tail buffer so timeout failures can include stderr context. The fork
      // also keeps the OS pipe buffer from filling and wedging the child.
      const stderrChunks: string[] = []
      yield* forkStderrDrain(proc.stderr ?? proc.child.stderr, stderrChunks, mark, proc.child.pid)

      // Watch stdout line-by-line for the listening sentinel. Format
      // (see src/cli/cmd/serve.ts):
      //   "opencode server listening on http://<host>:<port>"
      const readyRe = /listening on (http:\/\/([^\s:]+):(\d+))/
      const readyDeferred = yield* Deferred.make<{ url: string; hostname: string; port: number }>()
      const stdoutChunks: string[] = []
      yield* Effect.forkScoped(
        fromBunStream("stdout", () => proc.stdout ?? proc.child.stdout).pipe(
          Stream.decodeText(),
          Stream.splitLines,
          Stream.runForEach((line) => {
            stdoutChunks.push(line + "\n")
            const m = line.match(readyRe)
            return m ? Deferred.succeed(readyDeferred, { url: m[1], hostname: m[2], port: Number(m[3]) }) : Effect.void
          }),
          (effect) => diagnosticDrain(effect, mark, "stdout", proc.child.pid),
          Effect.ignore({ log: true }),
        ),
      )

      const readiness = yield* Effect.raceFirst(
        Deferred.await(readyDeferred).pipe(Effect.map((match) => ({ _tag: "ready" as const, match }))),
        Effect.tryPromise({
          try: () => proc.exited,
          catch: (cause) => new Error(`failed to observe serve process ${proc.child.pid} exit`, { cause }),
        }).pipe(Effect.map((exitCode) => ({ _tag: "exited" as const, exitCode, signalCode: proc.child.signalCode }))),
      ).pipe(
        Effect.timeoutOrElse({
          duration: Duration.millis(Math.max(readyDeadline - Date.now(), 1)),
          orElse: () => Effect.succeed({ _tag: "timed-out" as const }),
        }),
      )
      if (readiness._tag === "ready") {
        return {
          url: readiness.match.url,
          hostname: readiness.match.hostname,
          port: readiness.match.port,
          kill: () => {
            proc.tracker.signal("SIGTERM", Date.now() + 1_000, mark)
          },
          exited: proc.exited,
        } satisfies ServeHandle
      }

      const identity =
        readiness._tag === "exited"
          ? `process ${proc.child.pid} exited with ${readiness.signalCode ?? `code ${readiness.exitCode}`}`
          : `process ${proc.child.pid} did not become ready within ${readyTimeoutMs}ms`
      return yield* Effect.fail(
        new Error(
          `opencode serve readiness failed: ${identity}\n` +
            `stdout (last 2000):\n${stdoutChunks.join("").slice(-2000)}\n` +
            `stderr (last 2000):\n${stderrChunks.join("").slice(-2000)}`,
        ),
      )
    })

    const acp = Effect.fn("opencode.acp")(function* (opts?: AcpOpts) {
      const owner = yield* DiagnosticOwner
      const mark = unitDiagnostic("acp", undefined, owner)
      mark("spawn.request")
      const started = yield* Clock.currentTimeMillis
      const argv = ["acp"]
      if (opts?.cwd) argv.push("--cwd", opts.cwd)
      if (opts?.extraArgs) argv.push(...opts.extraArgs)

      // Acquire the subprocess. Release ends stdin (clean shutdown — ACP exits
      // on stdin EOF) and escalates through bounded SIGTERM/SIGKILL waits if
      // it does not exit promptly.
      const proc = yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: async () => {
            const proc = await spawnTracked([process.execPath, "run", cliEntry, ...argv], {
              cwd: opts?.cwd ?? home,
              detached: process.platform !== "win32",
              env: { ...process.env, ...env, ...opts?.env, OPENCODE_ACP_PROFILE: "1" },
              stdin: "pipe",
              stdout: "pipe",
              stderr: "pipe",
            })
            mark("spawn", proc.child.pid)
            return await observeOwnedExit(proc, mark)
          },
          catch: (cause) => new Error("failed to spawn opencode process", { cause }),
        }),
        (p) =>
          Effect.tryPromise({
            try: () => cleanupAcpProcess(p.child, p.tracker, mark),
            catch: (cause) =>
              cause instanceof Error ? cause : new Error(`failed to clean up ACP process ${p.child.pid}`, { cause }),
          }).pipe(Effect.orDie),
      )

      const startup = yield* trackAcpStartup({
        started,
        exited: Effect.tryPromise({ try: () => proc.exited, catch: (cause) => new Error(String(cause)) }),
        exitCode: () => proc.child.exitCode,
      })
      const observeProfile = diagnosticAcp(mark, proc.child.pid)
      yield* Effect.forkScoped(
        fromBunStream("stderr", () => proc.stderr ?? proc.child.stderr).pipe(
          Stream.decodeText(),
          Stream.runForEach((chunk) => {
            observeProfile(chunk)
            return startup.observe(chunk)
          }),
          (effect) => diagnosticDrain(effect, mark, "stderr", proc.child.pid),
          Effect.ignore({ log: true }),
        ),
      )

      // Each ndjson line becomes one queue entry. JSON.parse failures are
      // surfaced as the raw string so a malformed protocol message doesn't
      // silently wedge the test in `receive`.
      const responses = yield* Queue.unbounded<unknown>()
      yield* Effect.forkScoped(
        fromBunStream("stdout", () => proc.stdout ?? proc.child.stdout).pipe(
          Stream.decodeText(),
          Stream.splitLines,
          Stream.runForEach((line) => {
            if (line.length === 0) return Effect.void
            let parsed: unknown
            try {
              parsed = JSON.parse(line)
            } catch {
              parsed = { _rawLine: line }
            }
            return Queue.offer(responses, parsed)
          }),
          (effect) => diagnosticDrain(effect, mark, "stdout", proc.child.pid),
          Effect.ignore({ log: true }),
        ),
      )

      return {
        // `proc.stdin.write` returns `number | Promise<number>`. The promise
        // form is the backpressure signal — if we don't await it, rapid
        // successive sends can interleave under pipe-buffer-full conditions
        // and corrupt the ndjson framing.
        send: (msg: object) =>
          Effect.promise(async () => {
            const ret = proc.child.stdin.write(JSON.stringify(msg) + "\n")
            if (typeof ret !== "number") await ret
          }),
        receive: Queue.take(responses),
        // proc.stdin.end() is idempotent in Bun; no try/catch needed.
        close: () => {
          mark("stdin.close.request", proc.child.pid)
          return proc.child.stdin.end()
        },
        exited: proc.exited,
        ready: startup.ready,
        exitAfterStartup: startup.exitAfterStartup,
        diagnostics: startup.diagnostics,
      } satisfies AcpHandle
    })

    const opencode: OpencodeCli = { run, startRun, serve, acp, spawn, expectExit, parseJsonEvents }

    const owner = yield* DiagnosticOwner
    return yield* diagnosticCallback(() => fn({ llm, home, opencode }), owner)
    // FetchHttpClient is provided so test bodies can `yield* HttpClient.HttpClient`
    // and hit endpoints on `opencode.serve()` without rolling their own fetch.
  }).pipe(
    Effect.provide(
      Layer.mergeAll(TestLLMServer.layer, FetchHttpClient.layer, AppNodeBuilder.build(LayerNode.group([FSUtil.node]))),
    ),
  )
}

function parseJsonEvents(stdout: string): Array<Record<string, unknown>> {
  return stdout
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as Record<string, unknown>)
}

function normalizeLines(value: string) {
  return value.replaceAll("\r\n", "\n")
}

// Convenience for the common assertion pattern. Dumps stderr/stdout when
// the exit code doesn't match — saves debugging time on CI failures.
function expectExit(result: RunResult, expected: number, label = "opencode") {
  if (result.exitCode === expected) return
  const tail = (s: string, n: number) => (s.length > n ? "..." + s.slice(-n) : s)
  // eslint-disable-next-line no-console
  console.error(`[${label}] expected exit ${expected}, got ${result.exitCode} after ${result.durationMs}ms`)
  // eslint-disable-next-line no-console
  console.error(`[${label}] stderr (last 2000):\n${tail(result.stderr, 2000)}`)
  // eslint-disable-next-line no-console
  console.error(`[${label}] stdout (last 500):\n${tail(result.stdout, 500)}`)
  throw new Error(`${label}: expected exit ${expected}, got ${result.exitCode}`)
}

// `cliIt.live(name, fixture => effect)` uses Bun's serial runner while
// `cliIt.concurrent` retains Bun's non-Windows concurrent runner.
//
// Subprocess tests must run against the real clock — a TestClock-paused
// environment can't drive a child process. If you need `.only` or `.skip`, fall
// back to `test` + `withCliFixture` directly.
// Body's R is `Scope.Scope | never` so tests can yield* scope-requiring
// resources (e.g. `opencode.serve`) without an extra `Effect.scoped` wrapper —
// `withCliFixture`'s outer scope is the natural lifetime.
const cliSemaphore = Semaphore.makeUnsafe(2)
const cliFixtureDeadlineMs = 90_000
const cliCleanupHeadroomMs = 15_000
// run-process has 13 fixtures. With two permits, the final callback can wait
// through six 94s admissions (90s fixture deadline plus four seconds of child
// termination) before its own admission. 675s covers all seven cohorts and a
// final cleanup reserve; Bun gets another 15s solely as a last-resort guard.
const cliCallbackDeadlineMs = 675_000
const cliBunTimeoutMs = cliCallbackDeadlineMs + cliCleanupHeadroomMs

function cliFixtureDeadline(opts?: number | TestOptions) {
  return typeof opts === "number" ? opts : (opts?.timeout ?? cliFixtureDeadlineMs)
}

function cliTestOptions(opts?: number | TestOptions) {
  return { ...(typeof opts === "object" ? opts : {}), timeout: cliBunTimeoutMs }
}

function withCliPermit<A, E, R>(effect: Effect.Effect<A, E, R>, opts?: number | TestOptions) {
  return cliSemaphore.withPermits(1)(
    effect.pipe(
      Effect.timeoutOrElse({
        duration: Duration.millis(cliFixtureDeadline(opts)),
        orElse: () => Effect.fail(new Error(`CLI fixture timed out after ${cliFixtureDeadline(opts)}ms`)),
      }),
    ),
  )
}

function runCliTest<A, E>(effect: Effect.Effect<A, E, never>) {
  // Encode the callback deadline before the runner evaluates the semaphore
  // acquisition. On timeout Effect interrupts the whole scoped program and
  // waits for its finalizers, so a cancelled waiter cannot later spawn.
  return Effect.runPromise(
    effect.pipe(
      Effect.timeoutOrElse({
        duration: Duration.millis(cliCallbackDeadlineMs),
        orElse: () => Effect.fail(new Error(`CLI test callback timed out after ${cliCallbackDeadlineMs}ms`)),
      }),
    ),
  )
}

export const cliIt = {
  live: <A, E>(
    name: string,
    body: (input: CliFixture) => Effect.Effect<A, E, Scope.Scope | HttpClient.HttpClient>,
    opts?: number | TestOptions,
  ) =>
    test(
      name,
      diagnosticRegistration(name, (owner) =>
        runCliTest(diagnosticContext(withCliPermit(Effect.scoped(diagnosticBody(withCliFixture(body))), opts), owner)),
      ),
      cliTestOptions(opts),
    ),
  concurrent: <A, E>(
    name: string,
    body: (input: CliFixture) => Effect.Effect<A, E, Scope.Scope | HttpClient.HttpClient>,
    opts?: number | TestOptions,
  ) =>
    (process.platform === "win32" ? test : test.concurrent)(
      name,
      diagnosticRegistration(name, (owner) =>
        runCliTest(diagnosticContext(withCliPermit(Effect.scoped(diagnosticBody(withCliFixture(body))), opts), owner)),
      ),
      cliTestOptions(opts),
    ),
}
