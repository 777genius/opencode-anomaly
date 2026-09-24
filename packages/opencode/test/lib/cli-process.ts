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
  closeSync,
  constants,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  readdirSync,
  renameSync,
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
  readonly windowsJob?: boolean
  // Only the native Job Object's member query can attest containment on
  // Windows. A vanished supervisor is merely a vanished observer.
  readonly jobDrained?: () => boolean
  readonly stop: (deadline?: number) => Promise<void>
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
  // A portable launch starts by tracking the gate shell. Once its direct PID
  // has been reconciled after exec, cleanup must own the program now using
  // that PID rather than retaining the shell executable identity.
  readonly handoff?: (identity: ProcessIdentity) => Promise<void>
  // A portable gate installs a member of its own group before exec. This
  // control is therefore an owned termination capability, not a saved PID.
  readonly abort?: () => Promise<void>
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
// The native supervisor waits this long for every Job Object member to exit
// before it can publish its drained record. Cleanup must reserve the same
// bounded window instead of killing that observer after the ordinary TERM
// grace period.
const windowsJobDrainReserveMs = 4_000
// Do not lend the native drain window to ordinary cleanup. Once the Job
// Object has had its full wait, this separate window still has to reap the
// captured supervisor and prove that containment is gone when it failed to
// acknowledge the drain.
const windowsJobFallbackReserveMs = 4_000
const windowsJobInspectionReserveMs = 500
const windowsJobContainmentReserveMs = 500
// The Job Object controller has to compile its C# interop on each fresh
// PowerShell host. This is admission work, never target runtime work, and has
// a bounded but deliberately independent budget on cold Windows runners.
const windowsSupervisorAdmissionTimeoutMs = 12_000
const linuxCgroupDiscoverySliceMs = 250
const linuxProcDiscoverySliceMs = 250
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

type PortableController = {
  readonly identity: ProcessSnapshot
  readonly nonce: string
  readonly target?: ProcessSnapshot
  readonly abort: string
  readonly acknowledgement: string
  readonly completion: string
  readonly controls: readonly string[]
  readonly filesystem: PortableFilesystem
  readonly onFallback?: () => void
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

function isPermissionDenied(cause: unknown): boolean {
  if (typeof cause !== "object" || cause === null) return false
  if ("code" in cause && (cause.code === "EACCES" || cause.code === "EPERM")) return true
  return "cause" in cause && isPermissionDenied(cause.cause)
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

type LinuxStat = {
  readonly parent: number
  readonly group: number
  readonly started: string
  readonly state: string
}

function linuxStat(
  pid: number,
  read = (pid: number) => readFileSync(`/proc/${pid}/stat`, "utf8"),
): LinuxStat | undefined {
  let stat: string
  try {
    stat = read(pid)
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
  return {
    parent: Number(fields[1]),
    group: Number(fields[2]),
    started: `${linuxBootIdentity}:${fields[19]}`,
    state: fields[0]!,
  }
}

function linuxSnapshot(pid: number): ProcessSnapshot | undefined {
  const stat = linuxStat(pid)
  if (!stat) return
  // /proc keeps a zombie's stat file after its executable link is gone. Both
  // states mean the process is no longer signalable; do not turn ordinary
  // reaping into an identity-corruption error that prevents other cleanup.
  if (stat.state === "Z") return
  let executable: string
  try {
    executable = readlinkSync(`/proc/${pid}/exe`)
  } catch (cause) {
    if (isMissingProcess(cause)) return
    throw new Error(`failed to read executable for process ${pid}`, { cause })
  }
  return {
    pid,
    parent: stat.parent,
    group: stat.group,
    started: stat.started,
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

function containedPids(cgroupPath: string, deadline?: number, now = Date.now) {
  if (deadline !== undefined && now() >= deadline)
    throw new Error(`cgroup discovery deadline elapsed for ${cgroupPath}`)
  return readFileSync(path.join(cgroupPath, "cgroup.procs"), "utf8")
    .split("\n")
    .flatMap((value) => {
      if (!value) return []
      if (deadline !== undefined && now() >= deadline)
        throw new Error(`cgroup discovery deadline elapsed for ${cgroupPath}`)
      const pid = Number(value)
      if (!Number.isSafeInteger(pid)) throw new Error(`containment ${cgroupPath} returned an invalid PID`)
      return [pid]
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
  now = Date.now,
) {
  if (now() >= deadline) {
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
  if (now() >= deadline) {
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
  test?: {
    readonly now?: () => number
    readonly members?: (cgroupPath: string) => number[]
    readonly snapshot?: (pid: number) => ProcessSnapshot | undefined
    readonly containmentForPID?: (pid: number) => string
  },
): ProcessTracker {
  const tracked = new Map<number, ProcessIdentity>()
  const errors: Error[] = []
  let emptyScans = 0
  let scanComplete = true
  const now = test?.now ?? Date.now
  const snapshot = test?.snapshot ?? linuxSnapshot
  const containmentForPID = test?.containmentForPID ?? linuxCgroup
  const inspect = (pid: number, deadline?: number) => {
    if (deadline !== undefined && now() >= deadline) return
    const current = snapshot(pid)
    if (!current) return
    if (deadline !== undefined && now() >= deadline) return
    const currentContainment = containmentForPID(pid)
    if (deadline !== undefined && now() >= deadline) return
    if (currentContainment !== containment) throw new Error(`process ${pid} escaped containment ${containment}`)
    return { ...current, containment }
  }
  const scan = async (deadline = now() + 4_000) => {
    // Cgroup membership can grow while a target forks. Bound every /proc and
    // cgroup walk to a short slice so TERM/KILL/reap retain time from the
    // caller's absolute cleanup deadline.
    const scanDeadline = Math.min(deadline, now() + linuxCgroupDiscoverySliceMs)
    if (now() >= scanDeadline) {
      scanComplete = false
      emptyScans = 0
      recordError(errors, `cgroup discovery deadline elapsed for process ${child.pid}`, undefined)
      return
    }
    try {
      let pids: number[]
      try {
        pids = test?.members?.(cgroupPath) ?? containedPids(cgroupPath, scanDeadline, now)
      } catch (cause) {
        // A cgroup can disappear after its last process exits (for example
        // when a parent cleanup races ours). That is already quiescent.
        if (isMissingProcess(cause)) {
          tracked.clear()
          scanComplete = true
          emptyScans += 1
          return
        }
        throw cause
      }
      if (now() >= scanDeadline) {
        scanComplete = false
        emptyScans = 0
        recordError(errors, `cgroup discovery deadline elapsed for process ${child.pid}`, undefined)
        return
      }
      let complete = true
      const current = pids.flatMap((pid) => {
        if (now() >= scanDeadline) {
          complete = false
          return []
        }
        let identity: ProcessIdentity | undefined
        try {
          identity = inspect(pid, scanDeadline)
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
      if (now() >= scanDeadline) complete = false
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
      // An incomplete enumeration cannot prove a previously known member
      // absent, nor can it certify a newly skipped member harmless.
      if (complete)
        Array.from(tracked.values())
          .filter((identity) => !members.has(identity.pid))
          .some((identity) => {
            if (now() >= scanDeadline) {
              complete = false
              return true
            }
            try {
              const live = snapshot(identity.pid)
              if (now() >= scanDeadline) complete = false
              if (live)
                recordError(errors, `recorded process ${identity.pid} left containment while still live`, undefined)
              if (!live) tracked.delete(identity.pid)
            } catch (cause) {
              if (!isMissingProcess(cause))
                recordError(errors, `failed to confirm containment member ${identity.pid} absence`, cause)
            }
            return !complete
          })
      scanComplete = complete
      emptyScans = complete && tracked.size === 0 ? emptyScans + 1 : 0
    } catch (cause) {
      scanComplete = false
      emptyScans = 0
      recordError(errors, `failed to inspect containment for process ${child.pid}`, cause)
    }
  }
  return serializeTracker({
    scan: async (deadline) => {
      await scan(deadline)
      return Array.from(tracked.values())
    },
    scanComplete: () => scanComplete,
    stop: async (deadline) => {
      await scan(deadline)
      if (errors.length || !scanComplete || tracked.size || emptyScans < quiescenceScans) {
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
      await scan(deadline)
      Array.from(tracked.values()).forEach((identity) => signalExact(identity, inspect, signal, deadline, mark, errors))
    },
    errors,
  })
}

export async function linuxCgroupDiscoveryDeadlineForTest() {
  let now = 0
  let memberInspections = 0
  let missingMemberInspections = 0
  let initial = true
  const tracker = trackContainedProcess(
    { pid: 42 },
    "test",
    "/synthetic",
    {
      now: () => now,
      members: () => (initial ? Array.from({ length: 64 }, (_, index) => index + 100) : []),
      snapshot: (pid) => {
        if (initial) memberInspections += 1
        if (!initial) {
          missingMemberInspections += 1
          now += 100
          return
        }
        return {
          pid,
          parent: 1,
          group: 42,
          started: `started:${pid}`,
          executable: "synthetic",
          containment: "",
          nonce: true,
        }
      },
      containmentForPID: () => "test",
    },
  )
  await tracker.scan(1_000)
  initial = false
  const deadline = 1_000
  await tracker.scan(deadline)
  return {
    memberInspections,
    missingMemberInspections,
    complete: tracker.scanComplete?.(),
    escalationRemaining: deadline - now,
  }
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
    readonly abort?: () => void | Promise<void>
    readonly controller?: PortableController
    readonly onSignalPath?: (path: "signal" | "signalKnown", signal: "SIGINT" | "SIGTERM" | "SIGKILL") => void
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
    stop: async (deadline = Date.now() + 4_000) => {
      if (!options?.controller) return
      await finalizePortableController(options.controller, deadline, errors)
    },
    signalKnown: async (signal, deadline, mark) => {
      options?.onSignalPath?.("signalKnown", signal)
      for (const identity of tracked.values()) {
        await signalPortableExact(identity, inspectKnown, signal, deadline, mark, errors, options?.send)
      }
    },
    signal: async (signal, deadline, mark) => {
      options?.onSignalPath?.("signal", signal)
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
    handoff: async (identity) => {
      if (
        identity.pid !== child.pid ||
        identity.started !== gateRoot.started ||
        identity.containment !== gateRoot.containment
      )
        throw new Error(`portable gate PID ${child.pid} changed identity during exec handoff`)
      awaitingGateExec = false
      tracked.set(identity.pid, identity)
    },
    abort: async () => {
      await options?.abort?.()
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
  kind: "slow-probes" | "partial-prefix" | "long-prefix" | "exited-root",
): Promise<DarwinFinalizerSyntheticResult>
export function darwinFinalizerDiscoveryForTest(kind: "initial-observation-failure"): Promise<DarwinFinalizerRealChildResult>
export async function darwinFinalizerDiscoveryForTest(
  kind: "slow-probes" | "partial-prefix" | "long-prefix" | "exited-root" | "initial-observation-failure",
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
  const exited =
    kind === "exited-root"
      ? Promise.resolve(0)
      : new Promise<number>((resolve) => {
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
        if (kind === "exited-root" && identity.pid === root.pid) {
          known = known.filter((candidate) => candidate.pid !== root.pid)
          return
        }
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

type PortableLaunchObservation =
  | { readonly _tag: "pending" }
  | { readonly _tag: "ready"; readonly target: ProcessSnapshot }
  | { readonly _tag: "exited" }
  | { readonly _tag: "conflict"; readonly message: string }
  | { readonly _tag: "deadline" }

type PortableCleanupHandoff =
  | { readonly _tag: "ready" }
  | { readonly _tag: "exited" }
  | { readonly _tag: "unresolved" }

// Reconciliation is diagnostic and must not consume the containment work that
// follows it. These slices leave deterministic room for every finalizer phase.
const portableCleanupObservationSliceMs = 250
const portableCleanupTermReserveMs = 500
const portableCleanupKillReserveMs = 500
const portableCleanupReapReserveMs = 1_000
const portableCleanupDescendantReserveMs = 1_000
const portableCleanupContainmentReserveMs =
  portableCleanupTermReserveMs +
  portableCleanupKillReserveMs +
  portableCleanupReapReserveMs +
  portableCleanupDescendantReserveMs
// The controller protocol runs after ordinary target cleanup. Keep a separate
// window for an identity-checked group KILL and proof that its target group
// drained; a durable marker alone cannot prove either operation happened.
const portableControllerFallbackReserveMs = 1_000
// Acquisition cleanup may begin before the tracker exists. Bound status-file
// retries independently so direct-gate TERM plus controller group KILL/reap
// still have an explicit window if that status path is permanently opaque.
const portableControllerStatusRecoverySliceMs = 250
const portableControllerAcquisitionReserveMs = portableCleanupTermReserveMs + portableControllerFallbackReserveMs

function portableControllerRecoveryDeadline(deadline: number, now = Date.now()) {
  return Math.min(deadline - portableControllerAcquisitionReserveMs, now + portableControllerStatusRecoverySliceMs)
}

function windowsJobCleanupPhases(deadline: number, now = Date.now()) {
  const drainDeadline = Math.min(deadline - windowsJobFallbackReserveMs, now + windowsJobDrainReserveMs)
  return {
    drainDeadline,
    inspectionDeadline: Math.min(deadline - windowsJobContainmentReserveMs, drainDeadline + windowsJobInspectionReserveMs),
    reapDeadline: deadline - windowsJobContainmentReserveMs,
    fallbackDeadline: deadline,
  }
}

export function windowsJobCleanupPhasesForTest(deadline: number, now: number) {
  return windowsJobCleanupPhases(deadline, now)
}

function portableCleanupObservationDeadline(deadline: number, now = Date.now()) {
  return Math.min(
    deadline - portableCleanupContainmentReserveMs,
    now + portableCleanupObservationSliceMs,
  )
}

export function portableCleanupReserveForTest(deadline: number, now: number) {
  return {
    observationDeadline: portableCleanupObservationDeadline(deadline, now),
    containmentReserve: portableCleanupContainmentReserveMs,
    termReserve: portableCleanupTermReserveMs,
    killReserve: portableCleanupKillReserveMs,
    reapReserve: portableCleanupReapReserveMs,
    descendantReserve: portableCleanupDescendantReserveMs,
  }
}

async function acquirePortableGate(
  pid: number,
  nonce: string,
  deadline: number,
  exited: () => "pending" | "exited" | "failed",
  observe: PortableObserve = portableSnapshot,
): Promise<PortableLaunchObservation> {
  while (Date.now() < deadline) {
    try {
      // A short direct observation keeps a transient /proc or procargs failure
      // from consuming the whole admission window. The nonce and native
      // creation token are still checked by portableSnapshot before ownership
      // crosses into the tracker.
      const current = await observePortable(
        observe,
        pid,
        nonce,
        Math.min(deadline, Date.now() + 250),
        true,
        "initial",
      )
      if (current) {
        if (current.group !== pid || !current.nonce)
          return { _tag: "conflict", message: `portable gate PID ${pid} changed identity before launch` }
        return { _tag: "ready", target: current }
      }
    } catch {
      // A nonce read or native observation can be transient while the gate is
      // starting. Only an observed identity mismatch is a launch conflict.
    }
    const state = exited()
    if (state === "exited") return { _tag: "exited" }
    if (state === "failed") return { _tag: "conflict", message: `failed to observe portable process ${pid} exit` }
    await Bun.sleep(5)
  }
  return { _tag: "deadline" }
}

async function handoffPortableCleanupTarget(
  tracker: ProcessTracker,
  gate: ProcessSnapshot,
  nonce: string,
  deadline: number,
  exited: () => "pending" | "exited" | "failed",
  errors: Error[],
  observe: PortableObserve = portableSnapshot,
): Promise<PortableCleanupHandoff> {
  if (!tracker.handoff) return { _tag: "unresolved" }
  let observationError: unknown
  const observationDeadline = portableCleanupObservationDeadline(deadline)
  while (Date.now() < observationDeadline) {
    try {
      const current = await observePortable(observe, gate.pid, nonce, observationDeadline, true, "cleanup")
      if (!current) {
        if (exited() === "exited") return { _tag: "exited" }
        await Bun.sleep(5)
        continue
      }
      if (
        current.started !== gate.started ||
        current.group !== gate.group ||
        current.group !== gate.pid ||
        !current.nonce
      ) {
        recordError(errors, `portable gate PID ${gate.pid} identity changed during acquisition cleanup`, undefined)
        return { _tag: "unresolved" }
      }
      // The shell's `exec` retains the PID and creation token while replacing
      // its executable. Adopt that exact, freshly nonce-authorized target
      // before TERM/KILL so tracker cleanup cannot retain the old shell image.
      if (current.executable !== gate.executable) await tracker.handoff(current)
      return { _tag: "ready" }
    } catch (cause) {
      observationError = cause
      if (exited() === "exited") return { _tag: "exited" }
      await Bun.sleep(5)
    }
  }
  if (observationError)
    recordError(errors, `failed to reconcile portable gate PID ${gate.pid} during acquisition cleanup`, observationError)
  return { _tag: "unresolved" }
}

// This is deliberately a direct-PID protocol. Broad nonce discovery is for
// cleanup only: using it to decide that the launch gate may open made a busy
// Darwin /proc walk both a readiness dependency and a PID-reuse window.
async function reconcilePortableLaunch(
  pid: number,
  nonce: string,
  gate: ProcessSnapshot,
  deadline: number,
  exited: () => "pending" | "exited" | "failed",
  observe: PortableObserve = portableSnapshot,
): Promise<PortableLaunchObservation> {
  if (Date.now() >= deadline) return { _tag: "deadline" }
  const current = await observePortable(observe, pid, nonce, deadline, true, "reconcile")
  if (!current) {
    const state = exited()
    if (state === "exited") return { _tag: "exited" }
    if (state === "failed") return { _tag: "conflict", message: `failed to observe portable process ${pid} exit` }
    return { _tag: "pending" }
  }
  if (
    current.started !== gate.started ||
    current.group !== gate.group ||
    current.group !== pid ||
    !current.nonce
  )
    return { _tag: "conflict", message: `portable gate PID ${pid} changed identity before launch` }
  if (current.executable === gate.executable) return { _tag: "pending" }
  return { _tag: "ready", target: current }
}

export async function portableLaunchReconciliationForTest(
  states: readonly (ProcessSnapshot | undefined)[],
  exit: "pending" | "exited" | "failed" = "pending",
  deadline = Date.now() + 1_000,
) {
  let next = 0
  const gate = {
    pid: 42,
    parent: 1,
    group: 42,
    started: "started",
    executable: "gate",
    containment: "nonce:test",
    nonce: true,
  } satisfies ProcessSnapshot
  return reconcilePortableLaunch(42, "test", gate, deadline, () => exit, async () => states[next++])
}

async function signalPortableGateGroup(
  gate: ProcessSnapshot,
  nonce: string,
  signal: "SIGTERM" | "SIGKILL",
  deadline: number,
  errors: Error[],
  observe: (pid: number, nonce: string, deadline: number, confirmedNonce?: boolean) => Promise<ProcessSnapshot | undefined> =
    portableSnapshot,
  send: (pid: number, signal: "SIGTERM" | "SIGKILL") => void = (pid, name) => process.kill(pid, name),
) {
  try {
    const current = await observe(gate.pid, nonce, deadline, true)
    if (!current) return
    if (
      current.started !== gate.started ||
      current.executable !== gate.executable ||
      current.group !== gate.group ||
      current.group !== gate.pid
    ) {
      recordError(errors, `portable gate PID ${gate.pid} identity changed before ${signal}`, undefined)
      return
    }
    send(-gate.group, signal)
  } catch (cause) {
    if (!isMissingProcess(cause)) recordError(errors, `failed to signal portable gate group ${gate.group} with ${signal}`, cause)
  }
}

async function signalPortableControllerGroup(
  controller: PortableController,
  signal: "SIGKILL",
  deadline: number,
  errors: Error[],
) {
  try {
    const current = await portableSnapshot(controller.identity.pid, controller.nonce, deadline, true)
    if (!current) return
    if (
      current.started !== controller.identity.started ||
      current.executable !== controller.identity.executable ||
      current.group !== controller.identity.group
    ) {
      recordError(errors, `portable controller PID ${controller.identity.pid} changed identity before ${signal}`, undefined)
      return
    }
    process.kill(-current.group, signal)
  } catch (cause) {
    if (!isMissingProcess(cause))
      recordError(errors, `failed to signal portable controller group ${controller.identity.group} with ${signal}`, cause)
  }
}

type PortableControllerCompletion = {
  readonly acknowledged: boolean
  readonly killIssued: boolean
  readonly failed: boolean
  readonly controllerExited: boolean
  readonly targetExited: boolean
  readonly groupDrained: boolean
  readonly killFinished: boolean
}

async function portableIdentityExited(identity: ProcessSnapshot, deadline: number, errors: Error[], name: string) {
  try {
    if (Date.now() >= deadline) return false
    const current =
      process.platform === "linux" ? linuxSnapshot(identity.pid) : await darwinNativeSnapshot(identity.pid)
    if (!current) return true
    if (current.started !== identity.started) return true
    if (current.group !== identity.group)
      recordError(errors, `portable ${name} PID ${identity.pid} changed group during finalization`, undefined)
    return false
  } catch (cause) {
    recordError(errors, `failed to verify portable ${name} ${identity.pid} exit during finalization`, cause)
    return false
  }
}

function linuxGroupDrained(
  group: number,
  deadline: number,
  list = () => readdirSync("/proc"),
  inspect = linuxStat,
  groupExists = (group: number) => {
    try {
      process.kill(-group, 0)
      return true
    } catch (cause) {
      if (isMissingProcess(cause)) return false
      if (typeof cause === "object" && cause !== null && "code" in cause && cause.code === "EPERM") return true
      throw cause
    }
  },
) {
  let inaccessible = false
  const memberOrDeadline = list()
    .filter((entry) => /^\d+$/.test(entry))
    .some((entry) => {
      if (Date.now() >= deadline) return true
      // Only stat is needed to identify group membership. Reading exe for
      // every unrelated PID can fail with EACCES on shared CI hosts.
      try {
        const stat = inspect(Number(entry))
        return stat?.group === group && stat.state !== "Z"
      } catch (cause) {
        if (isPermissionDenied(cause)) {
          inaccessible = true
          return false
        }
        throw cause
      }
    })
  if (memberOrDeadline || Date.now() >= deadline) return false
  // An unreadable stat could belong to this group. Only an absent process
  // group proves drainage; otherwise retain ownership uncertainty.
  return !inaccessible || !groupExists(group)
}

export function portableLinuxGroupDrainedForTest(
  group: number,
  records: Readonly<Record<number, string | Error>>,
  groupExists = false,
) {
  return linuxGroupDrained(
    group,
    Date.now() + 1_000,
    () => Object.keys(records),
    (pid) => linuxStat(pid, () => {
      const record = records[pid]!
      if (record instanceof Error) throw record
      return record
    }),
    () => groupExists,
  )
}

async function portableGroupDrained(group: number, deadline: number, errors: Error[]) {
  try {
    if (process.platform === "linux") return linuxGroupDrained(group, deadline)
    const output = await commandOutput(["ps", "-axo", "pid=,pgid="], deadline)
    return !output.split("\n").some((line) => {
      const [pid, observed] = line.trim().split(/\s+/)
      return /^\d+$/.test(pid ?? "") && Number(observed) === group
    })
  } catch (cause) {
    recordError(errors, `failed to verify portable group ${group} drain during finalization`, cause)
    return false
  }
}

async function finalizePortableController(controller: PortableController, deadline: number, errors: Error[]) {
  const priorErrors = errors.length
  const state = () => {
    try {
      const completion = controller.filesystem.exists(controller.completion)
        ? controller.filesystem.read(controller.completion)
        : ""
      return {
        acknowledged: controller.filesystem.exists(controller.acknowledgement),
        killIssued: completion === "kill-issued",
        failed: completion === "failed",
      }
    } catch (cause) {
      recordError(errors, `failed to read portable controller completion for ${controller.identity.pid}`, cause)
      return { acknowledged: false, killIssued: false, failed: false }
    }
  }
  const completion = async (limit: number): Promise<PortableControllerCompletion> => {
    const marker = state()
    const [controllerExited, targetExited, groupDrained] = await Promise.all([
      portableIdentityExited(controller.identity, limit, errors, "controller"),
      controller.target ? portableIdentityExited(controller.target, limit, errors, "target") : Promise.resolve(true),
      portableGroupDrained(controller.identity.group, limit, errors),
    ])
    return {
      ...marker,
      controllerExited,
      targetExited,
      groupDrained,
      // kill-issued is only an intent record. A KILL finishes only once the
      // protected controller is gone and neither its direct target nor group
      // has a surviving member.
      killFinished: marker.killIssued && controllerExited && targetExited && groupDrained,
    }
  }
  const wait = async (limit: number, acceptDrained = false) => {
    while (Date.now() < limit) {
      const current = await completion(Math.min(limit, Date.now() + 250))
      if (
        current.failed ||
        (current.acknowledged && current.killFinished) ||
        (acceptDrained && current.controllerExited && current.targetExited && current.groupDrained)
      )
        return current
      await Bun.sleep(5)
    }
    return completion(limit)
  }
  let aborted = true
  try {
    // This write is intentionally the tracker\'s only immediate controller
    // action. The controller retains the verified PGID and performs TERM/KILL
    // itself, so no stale PID or group escapes this ownership boundary.
    ;(controller.filesystem.write ?? writeFileSync)(controller.abort, "abort")
  } catch (cause) {
    aborted = false
    recordError(errors, `failed to write portable controller abort for ${controller.identity.pid}`, cause)
  }
  const grace = Math.min(deadline - portableControllerFallbackReserveMs, Date.now() + 750)
  const completed = aborted ? await wait(grace) : undefined
  if (!(completed?.acknowledged && completed.killFinished)) {
    // A sleeping or wedged controller cannot leave its group alive forever.
    // Revalidate the controller\'s immutable identity immediately before the
    // parent performs the same group KILL as the controller would have.
    controller.onFallback?.()
    await signalPortableControllerGroup(controller, "SIGKILL", deadline, errors)
  }
  // Once fallback has run, verified group disappearance is enough to stop
  // waiting even if a failed abort write made acknowledgement impossible.
  // Missing protocol records remain diagnostics below; controls stay retained.
  const final = await wait(deadline, true)
  if (!final.acknowledged)
    recordError(errors, `portable controller ${controller.identity.pid} did not acknowledge abort before cleanup deadline`, undefined)
  if (!final.killIssued)
    recordError(errors, `portable controller ${controller.identity.pid} did not issue its bounded group KILL before cleanup deadline`, undefined)
  if (final.failed)
    recordError(errors, `portable controller ${controller.identity.pid} failed its bounded group KILL`, undefined)
  if (!final.controllerExited)
    recordError(errors, `portable controller ${controller.identity.pid} did not exit after group KILL`, undefined)
  if (controller.target && !final.targetExited)
    recordError(errors, `portable target ${controller.target.pid} did not exit after group KILL`, undefined)
  if (!final.groupDrained)
    recordError(errors, `portable controller group ${controller.identity.group} did not drain after group KILL`, undefined)
  if (errors.length > priorErrors) return
  controller.controls.forEach((file) => {
    try {
      if (controller.filesystem.exists(file)) controller.filesystem.unlink(file)
    } catch (cause) {
      recordError(errors, `failed to remove portable controller control ${file}`, cause)
    }
  })
}

export async function portableGateEscalationForTest(changedIdentity = false) {
  const gate = {
    pid: 42,
    parent: 1,
    group: 42,
    started: "started",
    executable: "gate",
    containment: "nonce:test",
    nonce: true,
  } satisfies ProcessSnapshot
  const errors: Error[] = []
  const events: string[] = []
  const observe = async () => (changedIdentity ? { ...gate, started: "reused" } : gate)
  const send = (pid: number, signal: "SIGTERM" | "SIGKILL") => events.push(`${signal}:${pid}`)
  await signalPortableGateGroup(gate, "test", "SIGTERM", Date.now() + 1_000, errors, observe, send)
  await signalPortableGateGroup(gate, "test", "SIGKILL", Date.now() + 1_000, errors, observe, send)
  return { events, errors }
}

// Exercise the production portable tracker after the same direct-PID exec
// reconciliation used by spawnTracked. Without the handoff, signalKnown sees
// the gate executable and rejects the running program as an identity change.
export async function portableTrackerHandoffCleanupForTest() {
  const gate = {
    pid: 42,
    parent: 1,
    group: 42,
    started: "started",
    executable: "gate",
    containment: "nonce:test",
    nonce: true,
  } satisfies ProcessSnapshot
  const target = { ...gate, executable: "opencode" }
  const events: string[] = []
  const tracker = trackPortableProcess({ pid: 42 }, "test", gate, {
    broadDiscovery: true,
    inspectKnown: async () => target,
    send: (_pid, signal) => events.push(`${signal}:${target.executable}`),
  })
  if (!tracker.handoff || !tracker.signalKnown) throw new Error("portable tracker lacks known-target handoff cleanup")
  await tracker.handoff(target)
  await tracker.signalKnown("SIGTERM", Date.now() + 1_000, () => {})
  await tracker.signalKnown("SIGKILL", Date.now() + 1_000, () => {})
  return { events, errors: tracker.errors }
}

export async function portableFailedReconciliationCleanupHandoffForTest() {
  const gate = {
    pid: 42,
    parent: 1,
    group: 42,
    started: "started",
    executable: "gate",
    containment: "nonce:test",
    nonce: true,
  } satisfies ProcessSnapshot
  const target = { ...gate, executable: "opencode" }
  const events: string[] = []
  const cleanupErrors: Error[] = []
  const tracker = trackPortableProcess({ pid: 42 }, "test", gate, {
    broadDiscovery: true,
    inspectKnown: async () => target,
    send: (_pid, signal) => events.push(`${signal}:${target.executable}`),
  })
  if (!tracker.signalKnown) throw new Error("portable tracker lacks known-target cleanup")
  await reconcilePortableLaunch(42, "test", gate, Date.now() + 1_000, () => "pending", async () => {
    throw new Error("synthetic reconciliation failure")
  }).catch(() => undefined)
  await handoffPortableCleanupTarget(
    tracker,
    gate,
    "test",
    Date.now() + 4_000,
    () => "pending",
    cleanupErrors,
    async () => target,
  )
  await tracker.signalKnown("SIGTERM", Date.now() + 1_000, () => {})
  await tracker.signalKnown("SIGKILL", Date.now() + 1_000, () => {})
  return { events, errors: [...tracker.errors, ...cleanupErrors] }
}

async function portableSnapshots(
  nonce: string,
  deadline = Date.now() + 4_000,
  preferredPID?: number,
  cursor?: DarwinScanCursor,
): Promise<PortableSnapshots> {
  if (process.platform === "linux") {
    const errors: Error[] = []
    const discoveryDeadline = Math.min(deadline, Date.now() + linuxProcDiscoverySliceMs)
    if (Date.now() >= discoveryDeadline) return { snapshots: [], errors, complete: false }
    let complete = true
    const snapshots = readdirSync("/proc").flatMap((entry) => {
      if (Date.now() >= discoveryDeadline) {
        complete = false
        return []
      }
      if (!/^\d+$/.test(entry)) return []
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
    return { snapshots, errors, complete }
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

async function discoverPortableController(nonce: string, group: number, deadline: number) {
  let failure: unknown
  while (Date.now() < deadline) {
    const result = await portableSnapshots(nonce, deadline)
    const candidates = result.snapshots.filter((snapshot) => snapshot.nonce && snapshot.group === group)
    // The controller's private nonce is inherited by its ordinary shell
    // children (including `sleep`). The durable authority is the unique root
    // of that nonce-bearing family in the verified launch group, not an
    // arbitrary individual match. A second root remains ambiguous: it may be
    // an unrelated process that forged the nonce, so never select either.
    const authorities = portableControllerAuthorities(candidates)
    if (result.complete && result.errors.length === 0 && authorities.length === 1) return authorities[0]
    if (authorities.length > 1)
      failure = new Error(`portable controller discovery found multiple nonce-authorized authorities in group ${group}`)
    else if (candidates.length)
      failure = new Error(`portable controller discovery could not identify one nonce-authorized authority in group ${group}`)
    else failure = result.errors[0] ?? new Error(`portable controller was not yet visible in group ${group}`)
    if (Date.now() < deadline) await Bun.sleep(5)
  }
  throw new Error(`portable controller could not be independently discovered in group ${group}`, { cause: failure })
}

function portableControllerAuthorities(candidates: readonly ProcessSnapshot[]) {
  const candidatePIDs = new Set(candidates.map((candidate) => candidate.pid))
  return candidates.filter((candidate) => !candidatePIDs.has(candidate.parent))
}

export function portableControllerAuthorityForTest() {
  const controller = {
    pid: 42,
    parent: 1,
    group: 7,
    started: "controller",
    executable: "controller",
    containment: "nonce:test",
    nonce: true,
  } satisfies ProcessSnapshot
  const child = { ...controller, pid: 43, parent: controller.pid, started: "child", executable: "sleep" }
  const forgery = { ...controller, pid: 44, started: "forgery", executable: "forgery" }
  const descendantAuthorities = portableControllerAuthorities([controller, child])
  const forgeryAuthorities = portableControllerAuthorities([controller, child, forgery])
  return {
    descendantAuthority: descendantAuthorities.length === 1 ? descendantAuthorities[0]?.pid : undefined,
    forgeryAuthority: forgeryAuthorities.length === 1 ? forgeryAuthorities[0]?.pid : undefined,
  }
}

export async function linuxProcDiscoveryDeadlineForTest() {
  if (process.platform !== "linux") throw new Error("Linux /proc discovery regression requires Linux")
  return portableSnapshots("test", Date.now())
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
  // This is called from a bounded discovery slice. Every owned helper action,
  // including its failure cleanup, inherits that same slice.
  const probeDeadline = darwinProbeDeadline(deadline)
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
      probeDeadline,
      cause instanceof Error ? cause : new Error(`failed to set up macOS process ${pid} argument observation`, { cause }),
    )
  }
  // A single inaccessible unrelated PID must not consume the launch-wide
  // discovery budget. A nonce target that was already observed is handled by
  // darwinSnapshot's fail-closed path instead of being retried indefinitely.
  const remaining = probeDeadline - Date.now()
  if (remaining <= 0)
    return cleanupDarwinProcArgs(
      child,
      identity,
      stdout,
      stderr,
      probeDeadline,
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
      probeDeadline,
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
  deadline: number,
  operationError: Error,
): Promise<never> {
  const cleanupErrors: Error[] = []
  const reserveDeadline = deadline
  const signal = async (name: "SIGTERM" | "SIGKILL") => {
    try {
      // The Bun handle belongs to this helper even if native identity capture
      // failed. It is the only safe pre-identity escape hatch and keeps a
      // synchronous getter failure from leaking the probe.
      if (!identity || Date.now() >= reserveDeadline) {
        child.kill(Date.now() >= reserveDeadline ? "SIGKILL" : name)
        return
      }
      const current = await darwinNativeSnapshot(child.pid)
      if (!current) return
      if (!sameDarwinInstance(identity, current))
        throw new Error(`macOS argument probe ${child.pid} identity changed before ${name}`)
      if (Date.now() >= reserveDeadline) {
        child.kill("SIGKILL")
        return
      }
      process.kill(child.pid, name)
    } catch (cause) {
      if (!isMissingProcess(cause))
        recordError(cleanupErrors, `failed to send ${name} to macOS argument probe ${child.pid}`, cause)
    }
  }
  await signal("SIGTERM")
  // Leave the remainder of the inherited slice for a hard kill and stream
  // closure; a TERM-resistant sysctl helper must not consume that reserve.
  const termWait = Math.min(50, Math.max(reserveDeadline - Date.now(), 0))
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

const commandHelperCleanupReserveMs = 1_000

type CommandFilesystem = {
  readonly exists: (file: string) => boolean
  readonly read: (file: string) => string
  readonly write: (file: string, contents: string) => void
  readonly unlink: (file: string) => void
}

type CommandHelperTestOptions = {
  readonly filesystem?: CommandFilesystem
  readonly onControls?: (controls: readonly string[]) => void
}

async function commandResult(command: string[], deadline: number, test?: CommandHelperTestOptions) {
  if (Date.now() >= deadline) throw new Error(`cleanup deadline elapsed before ${command[0]}`)
  const filesystem =
    test?.filesystem ??
    ({ exists: existsSync, read: (file) => readFileSync(file, "utf8"), write: writeFileSync, unlink: unlinkSync } satisfies CommandFilesystem)
  const nonce = crypto.randomUUID()
  const status = path.join(os.tmpdir(), `opencode-command-status-${nonce}`)
  const pendingStatus = `${status}.pending`
  const release = path.join(os.tmpdir(), `opencode-command-release-${nonce}`)
  const abort = path.join(os.tmpdir(), `opencode-command-abort-${nonce}`)
  const abortAcknowledgement = `${abort}.ack`
  const controls = [status, pendingStatus, release, abort, abortAcknowledgement]
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
          'status="$1" pending="$2" release="$3" abort="$4" acknowledged="$5"; shift 5; (while [ ! -e "$release" ] && [ ! -e "$abort" ]; do sleep 0.01; done; if [ -e "$abort" ]; then printf "aborted\\n" > "$acknowledged"; kill -KILL -$$; fi) >/dev/null 2>&1 & watcher=$!; "$@" & command=$!; wait "$command"; code=$?; printf "%s\\n" "$code" > "$pending" && mv -f "$pending" "$status"; exec 1>&-; while [ ! -e "$release" ] && [ ! -e "$abort" ]; do sleep 0.01; done; if [ -e "$abort" ]; then printf "aborted\\n" > "$acknowledged"; kill -KILL -$$; fi; kill "$watcher" 2>/dev/null || :; exit "$code"',
          "--",
          status,
          pendingStatus,
          release,
          abort,
          abortAcknowledgement,
          ...command,
        ],
    { stdout: "pipe", stderr: "ignore", detached: process.platform !== "win32" },
  )
  test?.onControls?.(controls)
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
      while (!filesystem.exists(status)) {
        if (Date.now() >= deadline) throw new Error(`${command[0]} did not exit before the cleanup deadline`)
        await new Promise<void>((resolve) => setTimeout(resolve, 5))
      }
      const exitCode = commandStatus(filesystem.read(status))
      if (exitCode === undefined) throw new Error(`${command[0]} returned an invalid exit status`)
      // Keep the helper watchdog armed until every inherited stdout writer
      // closes. The drain is already concurrent, so this cannot deadlock on a
      // full pipe; it does let a deadline abort an orphaned descendant which
      // inherited that pipe after its command root exited.
      const output = await ownedStdout
      if (aborted) throw new Error(`${command[0]} helper operation was aborted during cleanup`)
      filesystem.write(release, "release")
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
    let cleanupAcknowledged = completed
    let cleanupReaped = completed
    if (!completed) {
      // Promise.race does not cancel `result()`. Prevent its delayed stdout
      // continuation from publishing a release after this finalizer begins.
      aborted = true
      // The operation deadline protects the caller's probe. Once it expires,
      // the owned helper still needs a small, independent interval to deliver
      // abort, observe its acknowledgement, and reap its process group.
      const cleanupDeadline = Date.now() + commandHelperCleanupReserveMs
      try {
        cleanupAcknowledged = await stopCommandChild(child, abort, abortAcknowledgement, cleanupDeadline, filesystem)
      } catch (cause) {
        collectCleanupErrors(cleanupErrors, cause)
      }
      cleanupReaped = await exitedWithin(
        child,
        Math.max(cleanupDeadline - Date.now(), 0),
        `after failed ${command[0]}`,
        cleanupErrors,
      )
      if (!cleanupReaped) {
        cleanupErrors.push(new Error(`${command[0]} helper did not exit before the cleanup deadline`))
      }
    }
    // A surviving helper may still need its abort control. Retain the whole
    // control set unless the independent cleanup reserve observed both its
    // acknowledgement and its captured-handle reap.
    if (!cleanupAcknowledged || !cleanupReaped)
      cleanupErrors.push(new Error(`${command[0]} helper controls were retained because cleanup was not acknowledged and reaped`))
    if (cleanupAcknowledged && cleanupReaped)
      controls.forEach((file) => {
        try {
          if (filesystem.exists(file)) filesystem.unlink(file)
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
export function commandResultForTest(command: string[], timeoutMs: number, test?: CommandHelperTestOptions) {
  return commandResult(command, Date.now() + timeoutMs, test)
}

export async function commandCleanupReserveForTest() {
  if (process.platform === "win32") throw new Error("POSIX helper cleanup regression requires a POSIX host")
  let controls: readonly string[] = []
  const started = Date.now()
  const cause = await commandResult(
    ["/bin/sh", "-c", "sleep 30"],
    Date.now() + 25,
    {
      filesystem: {
        exists: (file) => (file.endsWith(".ack") ? false : existsSync(file)),
        read: (file) => readFileSync(file, "utf8"),
        write: writeFileSync,
        unlink: unlinkSync,
      },
      onControls: (files) => {
        controls = files
      },
    },
  ).catch((cause) => cause)
  const retained = controls.filter((file) => file.includes("opencode-command-abort-")).every(existsSync)
  controls.forEach((file) => {
    if (existsSync(file)) unlinkSync(file)
  })
  return { cause, retained, durationMs: Date.now() - started }
}

async function stopCommandChild(
  child: Pick<ReturnType<typeof Bun.spawn>, "pid" | "kill">,
  abort: string,
  acknowledgement: string,
  deadline: number,
  filesystem: Pick<CommandFilesystem, "exists" | "write"> = { exists: existsSync, write: writeFileSync },
) {
  if (process.platform === "win32") {
    // Bun retains the native process handle for a helper it spawned, unlike a
    // PID-only taskkill lookup. It is therefore safe to close that helper.
    try {
      child.kill()
    } catch (cause) {
      if (!isMissingProcess(cause)) throw cause
    }
    return true
  }
  const errors: Error[] = []
  try {
    // The spawned group leader's watcher evaluates `$$` in its own original
    // session. A stale numeric group is never sent from this process.
    filesystem.write(abort, "abort")
  } catch (cause) {
    errors.push(new Error(`failed to abort helper process group ${child.pid}`, { cause }))
  }
  while (errors.length === 0 && !filesystem.exists(acknowledgement) && Date.now() < deadline) await Bun.sleep(5)
  if (errors.length === 0 && !filesystem.exists(acknowledgement))
    errors.push(new Error(`helper process group ${child.pid} did not acknowledge abort before the cleanup deadline`))
  if (errors.length) throw new AggregateError(errors, `failed to stop helper process tree ${child.pid}`)
  return true
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
    scanComplete: tracker.scanComplete,
    hasKnown: tracker.hasKnown,
    broadDiscovery: tracker.broadDiscovery,
    windowsJob: tracker.windowsJob,
    jobDrained: tracker.jobDrained,
    signal: (signal, deadline, mark) => run(() => tracker.signal(signal, deadline, mark)),
    signalKnown: tracker.signalKnown
      ? (signal, deadline, mark) => run(() => tracker.signalKnown!(signal, deadline, mark))
      : undefined,
    handoff: tracker.handoff ? (identity) => run(() => tracker.handoff!(identity)) : undefined,
    abort: tracker.abort ? () => run(() => tracker.abort!()) : undefined,
    stop: (deadline) => run(() => tracker.stop(deadline)),
    errors: tracker.errors,
    run: (operation) => run(operation),
    unlocked: tracker,
  }
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
  completed?: () => boolean,
  fallbackDeadline = deadline + windowsJobFallbackReserveMs,
  timing?: {
    readonly now: () => number
    readonly sleep: (milliseconds: number) => Promise<void>
  },
): Promise<boolean> {
  const now = timing?.now ?? Date.now
  const sleep = timing?.sleep ?? Bun.sleep
  // Writing the abort begins native Job Object draining. Do it before any
  // potentially slow identity probe: observation belongs to the fallback
  // phase and cannot steal the drain interval.
  let abortWritten = true
  try {
    abort()
  } catch (abortCause) {
    abortWritten = false
    recordError(errors, `failed to write Windows job abort for ${child.pid}`, abortCause)
  }
  if (abortWritten && completed) {
    while (now() < deadline) {
      try {
        if (completed()) return true
      } catch (cause) {
        recordError(errors, `failed to read Windows job completion for process ${child.pid}`, cause)
        break
      }
      await sleep(5)
    }
    recordError(
      errors,
      `Windows Job Object for process ${child.pid} did not acknowledge descendant drain before the reserved cleanup window elapsed`,
      undefined,
    )
  }
  try {
    // A Job Object owns every descendant from creation, including a root
    // which exits between scans. This probe deliberately runs only after the
    // dedicated native drain phase; Bun's captured handle remains exact even
    // if the observation itself fails.
    const current = await inspect(Math.min(fallbackDeadline, deadline + windowsJobInspectionReserveMs))
    if (current && !sameIdentity(root, current))
      recordError(errors, `Windows job supervisor ${child.pid} identity changed before close`, undefined)
    if (now() >= fallbackDeadline)
      recordError(errors, `cleanup fallback deadline elapsed before closing Windows job ${child.pid}`, undefined)
  } catch (cause) {
    // A failed CIM/status read is not authority to leave a known native
    // child alive. Bun's captured handle targets exactly this supervisor.
    recordError(errors, `failed to inspect Windows job for process ${child.pid}`, cause)
  }
  try {
    child.kill()
    mark("kill.request", child.pid)
  } catch (termination) {
    if (!isMissingProcess(termination))
      recordError(errors, `failed to terminate Windows job supervisor ${child.pid}`, termination)
  }
  return false
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
  completion: string,
  supervisor: string,
  job: string,
): ProcessTracker {
  const errors: Error[] = []
  let drained = false
  // The status record is atomically published by the native Job Object
  // controller after it has created the job and read its own process creation
  // token. Combining that immutable record with Bun's captured process handle
  // is the ownership proof. Starting another PowerShell/CIM process here made
  // containment admission depend on an unrelated controller's startup and
  // could consume the entire bounded acquisition window.
  const scan = async () => [root]
  return serializeTracker({
    windowsJob: true,
    jobDrained: () => drained,
    scan,
    stop: async () => {
      ;[status, release, abort, completion, supervisor].forEach((file) => {
        try {
          if (existsSync(file)) unlinkSync(file)
        } catch (cause) {
          recordError(errors, `failed to remove Windows job control for ${child.pid}`, cause)
        }
      })
    },
    signal: async (_signal, deadline, mark) => {
      drained = await signalWindowsJob(
        child,
        root,
        deadline,
        mark,
        errors,
        async () => root,
        () => writeFileSync(abort, "abort"),
        () => existsSync(completion) && windowsSupervisorCompletion(readFileSync(completion, "utf8"), job),
      )
    },
    errors,
  })
}

// Exercise the same abort, acknowledgement, and captured-handle path as the
// Windows tracker without requiring a Windows host. The observer publishes its
// drain record only after its asynchronous Job Object cleanup completes.
export async function windowsSupervisorDrainLifecycleForTest(
  acknowledgementDelayMs?: number,
  deadline = 8_000,
  supervisorDisappears = false,
) {
  const errors: Error[] = []
  const events: string[] = []
  const root = { pid: 42, started: "started", executable: "supervisor", containment: "job" }
  let state: "running" | "draining" | "exited" | "terminated" = "running"
  let acknowledged = false
  let now = 0
  await signalWindowsJob(
    {
      pid: root.pid,
      kill: () => {
        state = "terminated"
        events.push("terminated")
      },
    },
    root,
    windowsJobCleanupPhases(deadline, now).drainDeadline,
    () => {},
    errors,
    async () => {
      events.push("inspected")
      return supervisorDisappears ? undefined : root
    },
    () => {
      state = "draining"
      events.push("abort")
    },
    () => {
      if (acknowledgementDelayMs === undefined || now < acknowledgementDelayMs) return false
      acknowledged = true
      state = "exited"
      if (!events.includes("job-drained")) events.push("job-drained")
      return true
    },
    deadline,
    {
      now: () => now,
      sleep: async (milliseconds) => {
        now += milliseconds
      },
    },
  )
  return { state, acknowledged, events, errors }
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

async function removeAbortedCgroup(
  cgroupPath: string,
  containment: string,
  deadline: number,
  test?: {
    readonly now?: () => number
    readonly members?: (cgroupPath: string, deadline: number, now: () => number) => number[]
    readonly snapshot?: (pid: number) => ProcessSnapshot | undefined
    readonly containmentForPID?: (pid: number) => string
    readonly send?: (pid: number) => void
    readonly sleep?: (milliseconds: number) => Promise<void>
  },
) {
  const errors: Error[] = []
  const now = test?.now ?? Date.now
  const members = test?.members ?? containedPids
  const snapshot = test?.snapshot ?? linuxSnapshot
  const containmentForPID = test?.containmentForPID ?? linuxCgroup
  const send = test?.send ?? ((pid: number) => process.kill(pid, "SIGKILL"))
  const inspect = (pid: number): ProcessIdentity | undefined => {
    if (now() >= deadline) return
    const identity = snapshot(pid)
    if (!identity || now() >= deadline) return
    const currentContainment = containmentForPID(pid)
    if (now() >= deadline) return
    return { ...identity, containment: currentContainment }
  }
  while (now() < deadline) {
    try {
      // Aborted acquisition has no later escalation phase to reserve. Spend
      // the full deadline here, but do not start even one more /proc or
      // cgroup read after it has elapsed.
      if (now() >= deadline) break
      const pids = members(cgroupPath, deadline, now)
      if (now() >= deadline) break
      if (pids.length === 0) {
        rmdirSync(cgroupPath)
        if (errors.length) throw new AggregateError(errors, `failed to clean aborted containment ${cgroupPath}`)
        return
      }
      for (const pid of pids) {
        if (now() >= deadline) break
        const identity = snapshot(pid)
        if (!identity) continue
        if (now() >= deadline) break
        const currentContainment = containmentForPID(pid)
        if (now() >= deadline) break
        if (currentContainment !== containment) {
          throw new Error(`cannot safely clean aborted containment member ${pid}`)
        }
        signalExact(
          { ...identity, containment: currentContainment },
          inspect,
          "SIGKILL",
          deadline,
          () => {},
          errors,
          (candidate) => send(candidate),
          now,
        )
      }
    } catch (cause) {
      if (isMissingProcess(cause)) continue
      recordError(errors, `failed to remove aborted containment ${cgroupPath}`, cause)
      if (now() >= deadline) throw new AggregateError(errors, `failed to remove aborted containment ${cgroupPath}`)
    }
    const remaining = deadline - now()
    if (remaining <= 0) break
    if (test?.sleep) await test.sleep(Math.min(5, remaining))
    else await new Promise<void>((resolve) => setTimeout(resolve, Math.min(5, remaining)))
  }
  throw new AggregateError(
    [...errors, new Error(`aborted containment ${cgroupPath} did not become empty`)],
    `failed to remove aborted containment ${cgroupPath}`,
  )
}

export async function abortedCgroupMembershipDeadlineForTest() {
  const started = Date.now()
  let now = started
  let membershipReads = 0
  let snapshotReads = 0
  let containmentReads = 0
  let readsAfterDeadline = 0
  let signals = 0
  const deadline = started + 100
  const members = Array.from({ length: 10_000 }, (_, index) => index + 100)
  await removeAbortedCgroup("/synthetic", "test", deadline, {
    now: () => now,
    members: () => {
      if (now >= deadline) readsAfterDeadline += 1
      membershipReads += 1
      return members
    },
    snapshot: (pid) => {
      if (now >= deadline) readsAfterDeadline += 1
      snapshotReads += 1
      now += 12
      return {
        pid,
        parent: 1,
        group: 42,
        started: `started:${pid}`,
        executable: "synthetic",
        containment: "",
        nonce: true,
      }
    },
    containmentForPID: () => {
      if (now >= deadline) readsAfterDeadline += 1
      containmentReads += 1
      now += 10
      return "test"
    },
    send: () => {
      signals += 1
    },
    sleep: async () => {},
  }).catch(() => undefined)
  return { membershipReads, snapshotReads, containmentReads, readsAfterDeadline, signals, elapsed: now - started }
}

function windowsSupervisorScript() {
  const separator = "\t"
  const script = String.raw`param([Parameter(ValueFromRemainingArguments=$true)][string[]]$Command)
Add-Type @'
using System;
using System.Text;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
public static class OpenCodeTestJob {
  const uint CREATE_SUSPENDED=4, EXTENDED_STARTUPINFO_PRESENT=0x80000, STARTF_USESTDHANDLES=256, WAIT_OBJECT_0=0, WAIT_TIMEOUT=258, WAIT_FAILED=0xffffffff, CLEANUP_WAIT_MS=4000;
  static readonly IntPtr PROC_THREAD_ATTRIBUTE_JOB_LIST = new IntPtr(0x0002000d);
  [StructLayout(LayoutKind.Sequential)] public struct Basic { public long PerProcessUserTimeLimit, PerJobUserTimeLimit; public uint LimitFlags; public UIntPtr MinimumWorkingSetSize, MaximumWorkingSetSize; public uint ActiveProcessLimit; public UIntPtr Affinity; public uint PriorityClass, SchedulingClass; }
  [StructLayout(LayoutKind.Sequential)] public struct IoCounters { public ulong ReadOperationCount, WriteOperationCount, OtherOperationCount, ReadTransferCount, WriteTransferCount, OtherTransferCount; }
  [StructLayout(LayoutKind.Sequential)] public struct Extended { public Basic BasicLimitInformation; public IoCounters IoInfo; public UIntPtr ProcessMemoryLimit, JobMemoryLimit, PeakProcessMemoryUsed, PeakJobMemoryUsed; }
  [StructLayout(LayoutKind.Sequential)] public struct Accounting { public long TotalUserTime, TotalKernelTime, ThisPeriodTotalUserTime, ThisPeriodTotalKernelTime; public uint TotalPageFaultCount, TotalProcesses, ActiveProcesses, TotalTerminatedProcesses; }
  [StructLayout(LayoutKind.Sequential, CharSet=CharSet.Unicode)] struct StartupInfo { public uint cb; public string lpReserved, lpDesktop, lpTitle; public uint dwX, dwY, dwXSize, dwYSize, dwXCountChars, dwYCountChars, dwFillAttribute, dwFlags; public short wShowWindow, cbReserved2; public IntPtr lpReserved2, hStdInput, hStdOutput, hStdError; }
  [StructLayout(LayoutKind.Sequential)] struct StartupInfoEx { public StartupInfo StartupInfo; public IntPtr lpAttributeList; }
  [StructLayout(LayoutKind.Sequential)] struct ProcessInformation { public IntPtr hProcess, hThread; public uint dwProcessId, dwThreadId; }
  [DllImport("kernel32.dll", SetLastError=true, CharSet=CharSet.Unicode)] static extern bool CreateProcess(string app, StringBuilder command, IntPtr pa, IntPtr ta, bool inherit, uint flags, IntPtr env, string cwd, ref StartupInfoEx startup, out ProcessInformation process);
  [DllImport("kernel32.dll", SetLastError=true)] static extern IntPtr CreateJobObject(IntPtr a,string b);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool SetInformationJobObject(IntPtr j,int c,IntPtr i,int l);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool QueryInformationJobObject(IntPtr j,int c,out Accounting i,int l,IntPtr r);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool TerminateJobObject(IntPtr j,uint code);
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
  static void WaitForJobDrain(IntPtr job) { var began=GetTickCount(); while(true) { var elapsed=unchecked(GetTickCount()-began); if(elapsed>=CLEANUP_WAIT_MS) throw new TimeoutException("Job Object descendant drain timed out"); Accounting accounting; Check(QueryInformationJobObject(job,1,out accounting,Marshal.SizeOf(typeof(Accounting)),IntPtr.Zero),"QueryInformationJobObject"); if(accounting.ActiveProcesses==0) return; Thread.Sleep(10); } }
  static void StopProcess(IntPtr process,List<Exception> errors) { if(process==IntPtr.Zero) return; if(!TerminateProcess(process,125)) errors.Add(Failure("TerminateProcess")); try { WaitForExit(process,"WaitForSingleObject after TerminateProcess"); } catch(Exception error) { errors.Add(error); } }
  static void Close(IntPtr handle,string call,List<Exception> errors) { if(handle!=IntPtr.Zero&&!CloseHandle(handle)) errors.Add(Failure(call)); }
  static string Quote(string s) { var quoted=new StringBuilder("\""); var slashes=0; foreach(var c in s) { if(c=='\\') { slashes++; continue; } if(c=='\"') { quoted.Append('\\',slashes*2+1); quoted.Append('\"'); slashes=0; continue; } quoted.Append('\\',slashes); slashes=0; quoted.Append(c); } quoted.Append('\\',slashes*2); quoted.Append('\"'); return quoted.ToString(); }
  public static IntPtr Start(string status,string jobName) {
    if(Marshal.SizeOf(typeof(Basic)) != (IntPtr.Size==8 ? 64 : 40) || Marshal.SizeOf(typeof(IoCounters)) != 48 || Marshal.SizeOf(typeof(Extended)) != (IntPtr.Size==8 ? 144 : 104)) throw new Exception("JOBOBJECT_EXTENDED_LIMIT_INFORMATION layout mismatch");
    var job=CreateJobObject(IntPtr.Zero,"Local\\OpenCodeTestJob-"+jobName); if(job==IntPtr.Zero) throw new System.ComponentModel.Win32Exception(Marshal.GetLastWin32Error(),"CreateJobObject");
    try { if(Marshal.GetLastWin32Error()==183) throw new Exception("named Job Object already exists"); var info=new Extended(); info.BasicLimitInformation.LimitFlags=0x2000; var pointer=Marshal.AllocHGlobal(Marshal.SizeOf(typeof(Extended))); try { Marshal.StructureToPtr(info,pointer,false); Check(SetInformationJobObject(job,9,pointer,Marshal.SizeOf(typeof(Extended))),"SetInformationJobObject"); } finally { Marshal.FreeHGlobal(pointer); } long created,ignored1,ignored2,ignored3; Check(GetProcessTimes(GetCurrentProcess(),out created,out ignored1,out ignored2,out ignored3),"GetProcessTimes"); var pending=status+"."+Guid.NewGuid().ToString("N")+".pending"; System.IO.File.WriteAllText(pending,System.Diagnostics.Process.GetCurrentProcess().Id+"${separator}"+created+"${separator}"+jobName); System.IO.File.Move(pending,status); return job; } catch(Exception error) { var errors=new List<Exception>(); errors.Add(error); Close(job,"CloseHandle job after Start failure",errors); if(errors.Count>1) throw new AggregateException("Windows Job Object startup cleanup failed",errors); throw; }
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
  public static void Stop(IntPtr job,string completion,string jobName) { if(job==IntPtr.Zero) return; var errors=new List<Exception>(); if(!TerminateJobObject(job,125)) errors.Add(Failure("TerminateJobObject")); try { WaitForJobDrain(job); var pending=completion+"."+Guid.NewGuid().ToString("N")+".pending"; System.IO.File.WriteAllText(pending,jobName+"${separator}"+"0"); System.IO.File.Move(pending,completion); } catch(Exception error) { errors.Add(error); } if(!CloseHandle(job)) errors.Add(Failure("CloseHandle job")); if(errors.Count>0) throw new AggregateException("Windows Job Object descendant cleanup failed",errors); }
}
'@
$job=$null
$failure=$null
try { $delay=[int]$env:OPENCODE_TEST_STATUS_DELAY_MS; if ($delay -gt 0) { Start-Sleep -Milliseconds $delay }; $job=[OpenCodeTestJob]::Start($env:OPENCODE_TEST_STATUS,$env:OPENCODE_TEST_JOB_ID); while (!(Test-Path -LiteralPath $env:OPENCODE_TEST_RELEASE) -and !(Test-Path -LiteralPath $env:OPENCODE_TEST_ABORT)) { Start-Sleep -Milliseconds 10 }; if (Test-Path -LiteralPath $env:OPENCODE_TEST_ABORT) { exit 125 }; exit [OpenCodeTestJob]::Run($job,$Command,$env:OPENCODE_TEST_ABORT) }
catch { $failure=$_.Exception }
finally { if ($null -ne $job) { try { [OpenCodeTestJob]::Stop($job,$env:OPENCODE_TEST_COMPLETION,$env:OPENCODE_TEST_JOB_ID) } catch { if ($null -ne $failure) { throw [System.AggregateException]::new("Windows Job Object supervisor cleanup failed",[System.Exception[]]@($failure,$_.Exception)) }; throw } }; if ($null -ne $failure) { throw $failure } }`
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
  const match = /^(\d+)\t(\d+)\t([0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12})$/i.exec(record)
  if (!match || match[0] !== record) return
  const pid = Number(match[1])
  if (!Number.isSafeInteger(pid) || pid <= 0 || !/^[1-9]\d*$/.test(match[2])) return
  return { pid, created: match[2], job: match[3] }
}

export function windowsSupervisorStatusForTest(record: string) {
  return windowsSupervisorStatus(record)
}

function windowsSupervisorIdentity(record: string, pid: number, job: string) {
  const status = windowsSupervisorStatus(record)
  if (!status || status.pid !== pid || status.job !== job) return
  return {
    pid,
    // FILETIME is a 64-bit value. Keep its validated decimal spelling intact:
    // converting it to Number would collapse distinct identities.
    started: `native:${status.created}`,
    // The native Job Object, not a PID lookup, is the Windows containment
    // authority. Keep the authenticated job token as this identity's stable
    // executable component so later cleanup never starts a second PowerShell
    // observer merely to rediscover the controller.
    executable: `job:${status.job}`,
    containment: "job",
  } satisfies ProcessIdentity
}

export function windowsSupervisorIdentityForTest(record: string, pid: number, job: string) {
  return windowsSupervisorIdentity(record, pid, job)
}

function windowsSupervisorCompletion(record: string, job: string) {
  return record === `${job}\t0`
}

export function windowsSupervisorCompletionForTest(record: string, job: string) {
  return windowsSupervisorCompletion(record, job)
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
  const completion = path.join(os.tmpdir(), `opencode-complete-${nonce}`)
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
          OPENCODE_TEST_COMPLETION: completion,
          OPENCODE_TEST_JOB_ID: nonce,
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
    const acquisitionDeadline = Date.now() + windowsSupervisorAdmissionTimeoutMs
    while (!existsSync(status) && Date.now() < acquisitionDeadline)
      await new Promise<void>((resolve) => setTimeout(resolve, 5))
    if (!existsSync(status)) throw new Error("Windows supervisor did not publish launch status")
    if (windowsSupervisorStatus(readFileSync(status, "utf8"))?.job !== nonce)
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
    if (!existsSync(completion) || !windowsSupervisorCompletion(readFileSync(completion, "utf8"), nonce))
      throw new Error("Windows supervisor exited without acknowledging Job Object descendant drain")
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
    ;[status, release, abort, completion, target, supervisor].forEach((file) => {
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

type PortableFilesystem = {
  readonly exists: (file: string) => boolean
  readonly read: (file: string) => string
  readonly write?: (file: string, contents: string) => void
  readonly unlink: (file: string) => void
}

const portableGateControllerScript = `leader=$$; controller_nonce="$8"; (
  # A shell assignment in this fork is not visible in Linux /proc until
  # an exec. The controller owns the eventual group KILL, so publish
  # its PID only after exec has installed its distinct kernel env.
  exec env OPENCODE_TEST_PROCESS_NONCE="$controller_nonce" /bin/sh -c '
    leader=$1
    abort=$2
    pending=$3
    ready=$4
    admission=$5
    acknowledgement=$6
    completion=$7
    pause=$8
    controller_status=$9
    controller_descendant=\${10}
    publication_delay=\${11}
    startup_failure=\${12}
    controller_pending="$controller_status.pending"
    [ "$startup_failure" = "true" ] && exit 125
    if [ -n "$controller_descendant" ]; then
      # This is a separate shell rather than sleep. It ignores TERM after
      # the controller exits, so group KILL proves containment.
      /bin/sh -c "trap \\\"\\\" TERM; while :; do sleep 1; done" &
      printf "%s:%s" "$$" "$!" > "$controller_descendant"
    fi
    [ -n "$publication_delay" ] && sleep "$publication_delay"
    printf "controller:%s:%s" "$OPENCODE_TEST_PROCESS_NONCE" "$$" > "$controller_pending" && mv -f "$controller_pending" "$controller_status"
    trap "" TERM
    expected_admission="admitted:$OPENCODE_TEST_PROCESS_NONCE:$$"
    while [ ! -e "$admission" ] && [ ! -e "$abort" ]; do sleep 0.01; done
    [ -e "$abort" ] && exit 125
    [ "$(cat "$admission")" = "$expected_admission" ] || exit 125
    printf "ready:%s" "$$" > "$pending" && mv -f "$pending" "$ready"
    while [ ! -e "$abort" ]; do sleep 0.01; done
    printf "ack" > "$acknowledgement"
    kill -TERM -"$leader" 2>/dev/null || :
    sleep 0.5
    # This only records that KILL is about to be issued. The parent
    # must still verify controller exit and group drain before it can
    # treat the kill as finished.
    printf "kill-issued" > "$completion"
    while [ -n "$pause" ] && [ -e "$pause" ]; do sleep 0.01; done
    kill -KILL -"$leader" 2>/dev/null || { printf "failed" > "$completion"; exit 1; }
  ' controller "$leader" "$4" "$1" "$2" "$5" "$6" "$7" "$9" "\${10}" "\${11}" "\${12}" "\${13}"
) >/dev/null 2>&1 & controller=$!; while [ ! -e "$3" ] && [ ! -e "$4" ]; do kill -0 "$controller" 2>/dev/null || exit 125; sleep 0.01; done; [ -e "$4" ] && exit 125; rm "$3"; shift 13; exec "$@"`

type PortableObserve = (
  pid: number,
  nonce: string,
  deadline: number,
  confirmedNonce?: boolean,
  stage?: "initial" | "reconcile" | "cleanup",
  signal?: AbortSignal,
) => Promise<ProcessSnapshot | undefined>

async function observePortable(
  observe: PortableObserve,
  pid: number,
  nonce: string,
  deadline: number,
  confirmedNonce: boolean,
  stage: "initial" | "reconcile" | "cleanup",
) {
  const remaining = deadline - Date.now()
  if (remaining <= 0) throw new Error(`portable ${stage} observation deadline elapsed`)
  const controller = new AbortController()
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      observe(pid, nonce, deadline, confirmedNonce, stage, controller.signal),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          controller.abort()
          reject(new Error(`portable ${stage} observation exceeded its deadline`))
        }, remaining)
      }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}

type SpawnTrackedTestOptions = {
  readonly forcePortable?: boolean
  readonly portableFilesystem?: PortableFilesystem
  readonly portableObserve?: PortableObserve
  // The test-only pause sits after the controller publishes kill-issued and
  // before its group KILL, making that otherwise tiny crash window observable.
  readonly portableControllerPause?: string
  // A private-nonce controller naturally has short-lived `sleep` children.
  // This fixture keeps one alive so recovery has to distinguish the durable
  // controller authority from an ordinary inherited-nonce descendant.
  readonly portableControllerDescendant?: string
  readonly portableControllerPublicationDelayMs?: number
  readonly portableControllerStartupFailure?: boolean
  readonly portableAdmissionDirectorySync?: (descriptor: number) => void
  readonly portableExitObservationFailure?: boolean
  readonly onPortableControllerCapture?: (controller: ProcessSnapshot) => void
  readonly onPortableControllerRecovered?: (controller: ProcessSnapshot) => void
  readonly onPortableControllerSetup?: (controls: { readonly ready: string; readonly completion: string }) => void
  readonly onPortableControllerFallback?: () => void
  readonly portableKnownInspect?: (pid: number, deadline: number) => Promise<ProcessIdentity | undefined>
  readonly onPortableTrackerSignal?: (path: "signal" | "signalKnown", signal: "SIGINT" | "SIGTERM" | "SIGKILL") => void
  readonly onPortableSpawn?: (child: ReturnType<typeof Bun.spawn>) => void
  readonly onPortableGateSpawn?: (child: ReturnType<typeof Bun.spawn>) => void
}

async function spawnTracked(
  command: string[],
  options: Parameters<typeof Bun.spawn>[1],
  test?: SpawnTrackedTestOptions,
) {
  if (process.platform === "win32") {
    const nonce = crypto.randomUUID()
    const release = path.join(os.tmpdir(), `opencode-release-${nonce}`)
    const abort = path.join(os.tmpdir(), `opencode-abort-${nonce}`)
    const status = path.join(os.tmpdir(), `opencode-status-${nonce}`)
    const completion = path.join(os.tmpdir(), `opencode-complete-${nonce}`)
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
          OPENCODE_TEST_COMPLETION: completion,
          OPENCODE_TEST_JOB_ID: nonce,
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
    const acquisitionDeadline = Date.now() + windowsSupervisorAdmissionTimeoutMs
    let root: ProcessIdentity | undefined
    let rawExited: Promise<number> | undefined
    let stdout: ReadableStream<Uint8Array> | undefined
    let stderr: ReadableStream<Uint8Array> | undefined
    let supervisorExited = false
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
        ;[status, release, abort, completion, supervisor].forEach((file) => {
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
      rawExited.then(
        () => {
          supervisorExited = true
        },
        () => {
          supervisorExited = true
        },
      )
      while (!existsSync(status) && !supervisorExited && Date.now() < acquisitionDeadline)
        await new Promise<void>((resolve) => setTimeout(resolve, 5))
      if (supervisorExited)
        throw new Error("Windows Job Object supervisor exited before publishing its authenticated launch status")
      root = existsSync(status) ? windowsSupervisorIdentity(readFileSync(status, "utf8"), child.pid, nonce) : undefined
      if (!root) {
        throw new Error("failed to capture exact Windows job supervisor identity before launching opencode")
      }
      const tracker = trackWindowsProcess(child, root, release, abort, status, completion, supervisor, nonce)
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
  const cgroup = test?.forcePortable ? undefined : prepareCgroup()
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
  // The controller stays in the launch group after the direct child exits.
  // It must not look like an ordinary nonce descendant: Darwin discovery can
  // signal such members during TERM grace and prevent its unconditional group
  // KILL from reaching a TERM-ignoring descendant.
  const controllerNonce = crypto.randomUUID()
  const ready = path.join(os.tmpdir(), `opencode-ready-${nonce}`)
  const pendingReady = `${ready}.pending`
  const release = path.join(os.tmpdir(), `opencode-release-${nonce}`)
  const abortFile = path.join(os.tmpdir(), `opencode-abort-${nonce}`)
  // The controller cannot make the gate ready until this parent-issued,
  // nonce-bound acknowledgement arrives. It turns publication into a real
  // child-to-parent admission handshake instead of a lossy observation race.
  const admission = path.join(os.tmpdir(), `opencode-admission-${nonce}`)
  const pendingAdmission = `${admission}.pending`
  const acknowledgement = path.join(os.tmpdir(), `opencode-ack-${nonce}`)
  const completion = path.join(os.tmpdir(), `opencode-complete-${nonce}`)
  // This separate, atomically published controller record is deliberately
  // admitted before any gate/readiness observation. It gives acquisition
  // cleanup a nonce-authenticated group authority even if the gate vanishes
  // or a readiness read throws.
  const controllerStatus = path.join(os.tmpdir(), `opencode-controller-${nonce}`)
  const pendingControllerStatus = `${controllerStatus}.pending`
  const controllerPause = test?.portableControllerPause
  const controllerDescendant = test?.portableControllerDescendant
  const controllerPublicationDelay = test?.portableControllerPublicationDelayMs
  const filesystem =
    test?.portableFilesystem ??
    ({
      exists: existsSync,
      read: (file) => readFileSync(file, "utf8"),
      write: writeFileSync,
      unlink: unlinkSync,
    } satisfies PortableFilesystem)
  let child: ReturnType<typeof Bun.spawn>
  try {
    child = Bun.spawn(
      [
        "/bin/sh",
        "-c",
        // Publish readiness before waiting for release. Rename makes the
        // marker visible as one complete record, never an exists-but-empty
        // file. The parent verifies this particular group leader directly;
        // the target cannot exec yet.
        // Once abort is observed, the controller remains in the verified
        // group after the leader exits. It keeps that numeric group from
        // being recycled, then always kills it after a bounded TERM grace.
        portableGateControllerScript,
        "--",
        pendingReady,
        ready,
        release,
        abortFile,
        admission,
        acknowledgement,
        completion,
        controllerNonce,
        controllerPause ?? "",
        controllerStatus,
        controllerDescendant ?? "",
        controllerPublicationDelay ? `${controllerPublicationDelay / 1_000}` : "",
        test?.portableControllerStartupFailure ? "true" : "",
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
  test?.onPortableGateSpawn?.(child)
  const acquisitionDeadline = Date.now() + 4_000
  let rawExited: Promise<number> | undefined
  let tracker: ProcessTracker | undefined
  let gate: ProcessSnapshot | undefined
  let controller: ProcessSnapshot | undefined
  let exitState: "pending" | "exited" | "failed" = "pending"
  // Own the native handle before any filesystem or /proc operation. This is
  // the provisional ownership boundary for failures before the tracker exists.
  let exitObservationFailure: Error | undefined
  try {
    if (test?.portableExitObservationFailure) throw new Error("synthetic portable exit observation failed")
    rawExited = child.exited
    void rawExited.catch(() => {})
  } catch (cause) {
    exitObservationFailure = new Error("failed to observe portable gate exit", { cause })
  }
  const captureController = async (deadline: number, retryFailures = false) => {
    let controllerRecord = ""
    let failure: unknown
    while (Date.now() < deadline) {
      if (exitState === "exited") throw new Error("portable gate exited before controller handshake")
      if (exitState === "failed") throw new Error("failed to observe portable gate exit before controller handshake")
      try {
        controllerRecord = filesystem.read(controllerStatus)
        const prefix = `controller:${controllerNonce}:`
        if (!controllerRecord.startsWith(prefix)) {
          await Bun.sleep(5)
          continue
        }
        const controllerPID = Number(controllerRecord.slice(prefix.length))
        if (!Number.isSafeInteger(controllerPID) || controllerPID <= 0)
          throw new Error("portable gate did not publish a valid controller identity before launch")
        const captured = await observePortable(
          test?.portableObserve ?? portableSnapshot,
          controllerPID,
          controllerNonce,
          deadline,
          true,
          "initial",
        )
        // Publishing the record is ordered after exec, but /proc can still
        // briefly return no environment for that just-exec'd controller. A
        // missing exact observation is readiness, not authority: retry the
        // same authenticated PID until the admission deadline. A visible
        // wrong nonce or group remains fail-closed and is never retried.
        if (!captured) {
          failure = new Error("portable gate controller was not yet visible before launch")
          await Bun.sleep(5)
          continue
        }
        if (!captured.nonce || captured.group !== child.pid)
          throw new Error("portable gate controller was not nonce-authenticated before launch")
        // The controller uses an exists-then-read protocol. Publish a fully
        // durable record as one namespace transition so it can never accept a
        // partly written acknowledgement after a parent crash.
        const descriptor = openSync(
          pendingAdmission,
          constants.O_WRONLY | constants.O_CREAT | constants.O_TRUNC,
          0o600,
        )
        try {
          writeFileSync(descriptor, `admitted:${controllerNonce}:${controllerPID}`)
          fsyncSync(descriptor)
        } finally {
          closeSync(descriptor)
        }
        renameSync(pendingAdmission, admission)
        const directoryDescriptor = openSync(path.dirname(admission), constants.O_RDONLY)
        try {
          ;(test?.portableAdmissionDirectorySync ?? fsyncSync)(directoryDescriptor)
        } finally {
          closeSync(directoryDescriptor)
        }
        return captured
      } catch (cause) {
        if (!retryFailures && !isMissingProcess(cause)) throw cause
        failure = cause
        await Bun.sleep(5)
      }
    }
    throw new Error("portable gate controller could not be located and authenticated during cleanup", { cause: failure })
  }
  const abort = async (cause: unknown) => {
    const errors: Error[] = [cause instanceof Error ? cause : new Error("portable acquisition failed", { cause })]
    const cleanupDeadline = Date.now() + 4_000
    // The controller record publishes a nonce-authenticated member before
    // readiness. Its filesystem and exact-PID authentication are advisory
    // during cleanup: bound retries, then independently find the controller
    // by its private nonce in the launch process group. That leaves time for
    // direct-gate TERM plus controller group KILL and reap when the status
    // path remains persistently unavailable.
    if (!tracker && !controller) {
      let statusFailure: unknown
      try {
        controller = await captureController(portableControllerRecoveryDeadline(cleanupDeadline), true)
      } catch (controllerCause) {
        statusFailure = controllerCause
      }
      if (!controller) {
        try {
          controller = await discoverPortableController(
            controllerNonce,
            child.pid,
            portableControllerRecoveryDeadline(cleanupDeadline),
          )
        } catch (discoveryCause) {
          recordError(
            errors,
            "failed to independently recover portable controller during acquisition cleanup",
            statusFailure ? new AggregateError([statusFailure, discoveryCause]) : discoveryCause,
          )
        }
      }
      if (controller) test?.onPortableControllerRecovered?.(controller)
    }
    // Controller verification happens before tracker creation. Preserve that
    // identity across an acquisition failure: the direct gate can reap after
    // TERM while this TERM-ignoring group member still owns a descendant.
    const provisionalController =
      !tracker && controller
        ? {
            identity: controller,
            nonce: controllerNonce,
            target: gate,
            abort: abortFile,
            acknowledgement,
            completion,
            controls: [
              ready,
              pendingReady,
              release,
              abortFile,
              admission,
              pendingAdmission,
              acknowledgement,
              completion,
              controllerStatus,
              pendingControllerStatus,
              ...(controllerPause ? [controllerPause] : []),
            ],
            filesystem,
            onFallback: test?.onPortableControllerFallback,
          }
        : undefined
    // Abort opens the gate only far enough for the shell to exit. It must be
    // done before waiting, and cleanup never inherits an expired acquisition
    // deadline.
    try {
      ;(filesystem.write ?? writeFileSync)(abortFile, "abort")
    } catch (abortCause) {
      recordError(errors, "failed to write portable acquisition abort", abortCause)
    }
    try {
      if (tracker) {
        // Release may race a reconciliation exception or deadline. Re-observe
        // this exact nonce-bearing PID before signalling so cleanup owns the
        // exec'd program rather than the obsolete gate-shell executable.
        if (gate) {
          const handoff = await handoffPortableCleanupTarget(
            tracker,
            gate,
            nonce,
            cleanupDeadline,
            () => exitState,
            errors,
            test?.portableObserve,
          )
          if (handoff._tag === "unresolved")
            recordError(
              errors,
              `portable cleanup handoff for gate PID ${gate.pid} remained unresolved; activating controlled group termination`,
              undefined,
            )
        }
        await terminateProcessPromise(child, () => {}, tracker, cleanupDeadline, rawExited)
      }
      if (!tracker) {
        // Before tracking, abort keeps the target gated. A reconciled gate
        // permits identity-safe group cleanup of its shell helper; otherwise
        // the exact Bun handle is the only safe signal target.
        if (gate) await signalPortableGateGroup(gate, nonce, "SIGTERM", cleanupDeadline, errors)
        else child.kill("SIGTERM")
        await exitedWithin(child, 500, "after portable acquisition TERM", errors, rawExited)
        // A reaped direct gate does not prove the group is gone. In
        // particular, its controller ignores TERM and can keep descendants
        // alive after the shell's exit. Its verified identity authorizes the
        // finalizer's group KILL and disappearance checks even before a
        // ProcessTracker has been installed.
        if (provisionalController) await finalizePortableController(provisionalController, cleanupDeadline, errors)
        else if (gate) await signalPortableGateGroup(gate, nonce, "SIGKILL", cleanupDeadline, errors)
        else child.kill("SIGKILL")
        await exitedWithin(
          child,
          Math.max(cleanupDeadline - Date.now(), 0),
          "after portable acquisition SIGKILL",
          errors,
          rawExited,
        )
      }
    } catch (cleanup) {
      collectCleanupErrors(errors, cleanup)
    }
    const reaped = await exitedWithin(
      child,
      Math.max(cleanupDeadline - Date.now(), 0),
      "after portable acquisition failure",
      errors,
      rawExited,
    )
    if (!reaped) {
      errors.push(new Error("portable gate shell remained unresolved after acquisition failure"))
      // The gate may still be reading these controls. Retain them until the
      // process is no longer observable instead of clearing its abort signal.
      throw new AggregateError(errors, "failed to establish portable process containment before launching opencode")
    }
    // Before controller identity is verified, retaining its controls is safer
    // than removing an abort request a live controller may still be reading.
    // Once a tracker exists, its finalizer owns acknowledgement, completion,
    // and removal on both successful and failed launches.
    throw new AggregateError(errors, "failed to establish portable process containment before launching opencode")
  }
  // The shell is owned from spawn through the direct-PID handoff. Keep every
  // readiness/control-file operation inside this boundary: a synchronous
  // filesystem failure must still abort, escalate, and reap the gate/target.
  try {
    if (rawExited)
      rawExited.then(
        () => {
          exitState = "exited"
        },
        () => {
          exitState = "failed"
        },
      )
    // Capture the protected controller before reading or reconciling the
    // gate. From this point an abort-write failure cannot strand a member
    // after the direct gate has exited: its controller nonce authenticates
    // the exact group KILL and final drain verification.
    controller = await captureController(acquisitionDeadline)
    test?.onPortableControllerCapture?.(controller)
    test?.onPortableSpawn?.(child)
    if (exitObservationFailure) throw exitObservationFailure
    // This direct identity is independent of readiness-file contents. A
    // transient unreadable nonce or process observation is pending, not an
    // acquisition failure, until the gate exits or the admission deadline.
    const initial = await acquirePortableGate(child.pid, nonce, acquisitionDeadline, () => exitState, test?.portableObserve)
    if (initial._tag !== "ready") {
      if (initial._tag === "exited") throw new Error("portable gate exited before publishing readiness")
      if (initial._tag === "conflict") throw new Error(initial.message)
      if (initial._tag === "deadline") throw new Error("portable gate did not publish an identity before launch deadline")
      throw new Error("portable gate identity remained pending after acquisition")
    }
    gate = initial.target
    // Read the marker before checking its content. Empty or incomplete data is
    // retried until the deadline, so a concurrent publisher cannot expose an
    // exists-then-empty one-shot failure even on a filesystem without rename.
    let readiness = ""
    while (Date.now() < acquisitionDeadline) {
      try {
        readiness = filesystem.read(ready)
      } catch (cause) {
        if (!isMissingProcess(cause)) throw cause
      }
      if (readiness.startsWith("ready:")) break
      await Bun.sleep(5)
    }
    const readyControllerPID = Number(readiness.slice("ready:".length))
    if (!Number.isSafeInteger(readyControllerPID) || readyControllerPID !== controller.pid)
      throw new Error("portable gate did not publish a valid controller before launch")
    if (controller.group !== gate.group)
      throw new Error("portable gate controller was not verified in the launch group")
    test?.onPortableControllerSetup?.({ ready, completion })
    tracker = trackPortableProcess(child, nonce, gate, {
      abort: () => (filesystem.write ?? writeFileSync)(abortFile, "abort"),
      controller: {
        identity: controller,
        nonce: controllerNonce,
        target: gate,
        abort: abortFile,
        acknowledgement,
        completion,
        controls: [
          ready,
          pendingReady,
          release,
          abortFile,
          admission,
          pendingAdmission,
          acknowledgement,
          completion,
          controllerStatus,
          pendingControllerStatus,
          ...(controllerPause ? [controllerPause] : []),
        ],
        filesystem,
        onFallback: test?.onPortableControllerFallback,
      },
      inspectKnown: test?.portableKnownInspect,
      onSignalPath: test?.onPortableTrackerSignal,
    })
    // trackPortableProcess admits the reconciled direct gate immediately. Its
    // broad nonce walk remains a cleanup mechanism and cannot delay launch.
    writeFileSync(release, "release")
    while (true) {
      const observation = await reconcilePortableLaunch(
        child.pid,
        nonce,
        gate,
        acquisitionDeadline,
        () => exitState,
        test?.portableObserve,
      )
      if (observation._tag === "ready") {
        if (!tracker.handoff) throw new Error("portable tracker cannot adopt reconciled exec target")
        await tracker.handoff(observation.target)
        return { child, tracker, rawExited }
      }
      if (observation._tag === "exited") {
        return { child, tracker, rawExited }
      }
      if (observation._tag === "conflict") throw new Error(observation.message)
      if (observation._tag === "deadline")
        throw new Error("portable gate did not reconcile its direct PID before launch deadline")
      await Bun.sleep(5)
    }
  } catch (cause) {
    return await abort(cause)
  }
}

// These use the same gated spawn path as the CLI builders while forcing the
// portable branch. The injected filesystem is deliberately limited to test
// seams around operations which otherwise throw synchronously from node:fs.
export async function portableFilesystemFailureForTest(kind: "read" | "unlink") {
  let exited: Promise<number> | undefined
  let failed = false
  const acquired = await spawnTracked(
    [process.execPath, "-e", 'process.stdout.write("started\\n"); process.stderr.write("started\\n"); setInterval(() => {}, 1_000)'],
    { stdout: "pipe", stderr: "pipe" },
    {
      forcePortable: true,
      portableFilesystem: {
        exists: existsSync,
        read: (file) => {
          if (kind === "read" && file.includes("opencode-ready-")) throw new Error("synthetic readiness read failed")
          return readFileSync(file, "utf8")
        },
        unlink: (file) => {
          if (kind === "unlink" && !failed && file.includes("opencode-ready-")) {
            failed = true
            throw new Error("synthetic readiness unlink failed")
          }
          unlinkSync(file)
        },
      },
      onPortableSpawn: (child) => {
        exited = child.exited
        void exited.catch(() => {})
      },
    },
  ).catch((cause) => cause)
  // The readiness read happens during acquisition. Control-file unlink now
  // happens during finalization, after the gated target has been released.
  // Finalize any successfully acquired child so a test failure cannot leave
  // its interval process running or wait forever on its exit promise.
  const cause =
    acquired instanceof Error
      ? acquired
      : await terminateProcessPromise(acquired.child, () => {}, acquired.tracker, Date.now() + 4_000, acquired.rawExited).catch(
          (cause) => cause,
        )
  const reaped = await Promise.resolve(exited).then(
    () => true,
    () => true,
  )
  return { cause, reaped }
}

export async function portableShortLivedProcessForTest(exitCode: number) {
  const proc = await spawnTracked(
    [
      process.execPath,
      "-e",
      `process.stdout.write("short stdout\\n"); process.stderr.write("short stderr\\n"); process.exit(${exitCode})`,
    ],
    { stdout: "pipe", stderr: "pipe" },
    { forcePortable: true },
  )
  const stdout = new Response(proc.child.stdout).text()
  const stderr = new Response(proc.child.stderr).text()
  const observed = proc.rawExited ?? proc.child.exited
  const actual = await observed
  await terminateProcessPromise(proc.child, () => {}, proc.tracker, Date.now() + 4_000, observed)
  return { exitCode: actual, stdout: await stdout, stderr: await stderr }
}

export async function portableInitialIdentityRetryForTest(kind: "delayed" | "unreadable") {
  let attempts = 0
  let controllerCaptured = false
  const proc = await spawnTracked(
    [process.execPath, "-e", "setInterval(() => {}, 1_000)"],
    { stdout: "pipe", stderr: "pipe" },
    {
      forcePortable: true,
      onPortableControllerCapture: () => {
        controllerCaptured = true
      },
      portableObserve: async (pid, nonce, deadline, confirmedNonce, stage) => {
        if (stage === "initial" && controllerCaptured && attempts++ < 2) {
          if (kind === "unreadable") throw new Error("synthetic unreadable nonce")
          return
        }
        return portableSnapshot(pid, nonce, deadline, confirmedNonce)
      },
    },
  )
  await terminateProcessPromise(proc.child, () => {}, proc.tracker, Date.now() + 4_000, proc.rawExited)
  return attempts
}

// The controller publishes its atomically renamed status after exec, but an
// immediate Linux /proc environment read can still be briefly empty. Exercise
// that exact controller-only admission race: retries must retain the same
// status PID and eventually establish authenticated containment.
export async function portableControllerVisibilityRetryForTest() {
  let controllerObservations = 0
  const proc = await spawnTracked(
    [process.execPath, "-e", "setInterval(() => {}, 1_000)"],
    { stdout: "pipe", stderr: "pipe" },
    {
      forcePortable: true,
      portableObserve: async (pid, nonce, deadline, confirmedNonce, stage) => {
        if (stage === "initial" && controllerObservations++ < 2) return
        return portableSnapshot(pid, nonce, deadline, confirmedNonce)
      },
    },
  )
  await terminateProcessPromise(proc.child, () => {}, proc.tracker, Date.now() + 4_000, proc.rawExited)
  return controllerObservations
}

// This uses the production gate. The controller delays its signed publication,
// then waits for the parent's exact acknowledgement before it can publish
// readiness or permit the target to exec.
export async function portableDelayedControllerHandshakeForTest() {
  const marker = path.join(os.tmpdir(), `opencode-delayed-controller-target-${crypto.randomUUID()}`)
  const started = Date.now()
  let admittedAt = 0
  let targetExecutedBeforeAdmission = false
  try {
    const proc = await spawnTracked(
      [
        process.execPath,
        "-e",
        `require("node:fs").writeFileSync(${JSON.stringify(marker)}, "executed"); setInterval(() => {}, 1_000)`,
      ],
      { stdout: "pipe", stderr: "pipe" },
      {
        forcePortable: true,
        portableControllerPublicationDelayMs: 100,
        onPortableControllerCapture: () => {
          admittedAt = Date.now()
          targetExecutedBeforeAdmission = existsSync(marker)
        },
      },
    )
    const targetDeadline = Date.now() + 1_000
    while (!existsSync(marker) && Date.now() < targetDeadline) await Bun.sleep(5)
    const targetExecuted = existsSync(marker)
    await terminateProcessPromise(proc.child, () => {}, proc.tracker, Date.now() + 4_000, proc.rawExited)
    return {
      admissionDurationMs: admittedAt - started,
      targetExecuted,
      targetExecutedBeforeAdmission,
    }
  } finally {
    if (existsSync(marker)) unlinkSync(marker)
  }
}

export function portableControllerScriptSyntaxForTest() {
  return Bun.spawnSync(["/bin/sh", "-n", "-c", portableGateControllerScript]).exitCode
}

export async function portableAdmissionDirectorySyncFailureForTest() {
  const marker = path.join(os.tmpdir(), `opencode-admission-sync-target-${crypto.randomUUID()}`)
  let exited: Promise<number> | undefined
  try {
    const cause = await spawnTracked(
      [process.execPath, "-e", `require("node:fs").writeFileSync(${JSON.stringify(marker)}, "executed")`],
      { stdout: "pipe", stderr: "pipe" },
      {
        forcePortable: true,
        portableAdmissionDirectorySync: () => {
          throw new Error("synthetic admission directory sync failed")
        },
        onPortableGateSpawn: (child) => {
          exited = child.exited
          void exited.catch(() => {})
        },
      },
    ).catch((cause) => cause)
    const reaped = await Promise.resolve(exited).then(
      () => true,
      () => true,
    )
    return { failed: cause instanceof Error, reaped, targetExecuted: existsSync(marker) }
  } finally {
    if (existsSync(marker)) unlinkSync(marker)
  }
}

// A controller that exits before publication must make its gate exit too.
// That gives the parent a bounded, owned cleanup path without accepting a
// missing authority or guessing at a process group discovered after the fact.
export async function portableControllerStartupFailureForTest() {
  let exited: Promise<number> | undefined
  const started = Date.now()
  const cause = await spawnTracked(
    [process.execPath, "-e", "setInterval(() => {}, 1_000)"],
    { stdout: "pipe", stderr: "pipe" },
    {
      forcePortable: true,
      portableControllerStartupFailure: true,
      onPortableGateSpawn: (child) => {
        exited = child.exited
        void exited.catch(() => {})
      },
    },
  ).catch((cause) => cause)
  const reaped = await Promise.resolve(exited).then(
    () => true,
    () => true,
  )
  return { cause, reaped, durationMs: Date.now() - started }
}

// Linux exposes the environment supplied at exec(2), not a shell's later
// export. This forced-portable path proves the controller's protected nonce is
// actually visible to /proc before it is admitted as the group sentinel.
export async function portableLinuxControllerEnvironmentForTest() {
  if (process.platform !== "linux") throw new Error("Linux /proc environment regression requires Linux")
  let gatePID = 0
  let controllerHasDistinctKernelNonce = false
  const proc = await spawnTracked(
    [process.execPath, "-e", "setInterval(() => {}, 1_000)"],
    { stdout: "pipe", stderr: "pipe" },
    {
      forcePortable: true,
      onPortableSpawn: (child) => {
        gatePID = child.pid
      },
      portableObserve: async (pid, nonce, deadline, confirmedNonce, stage) => {
        if (stage === "initial" && pid !== gatePID) controllerHasDistinctKernelNonce = linuxHasNonce(pid, nonce)
        return portableSnapshot(pid, nonce, deadline, confirmedNonce)
      },
    },
  )
  await terminateProcessPromise(proc.child, () => {}, proc.tracker, Date.now() + 4_000, proc.rawExited)
  return controllerHasDistinctKernelNonce
}

export async function portableReadinessPartialPublicationForTest() {
  const start = async () => {
    let partialReads = 0
    const proc = await spawnTracked(
      [process.execPath, "-e", "setInterval(() => {}, 1_000)"],
      { stdout: "pipe", stderr: "pipe" },
      {
        forcePortable: true,
        portableFilesystem: {
          exists: existsSync,
          read: (file) => {
            const value = readFileSync(file, "utf8")
            if (file.includes("opencode-ready-") && partialReads++ < 2) return partialReads === 1 ? "" : "rea"
            return value
          },
          unlink: unlinkSync,
        },
      },
    )
    await terminateProcessPromise(proc.child, () => {}, proc.tracker, Date.now() + 4_000, proc.rawExited)
    return partialReads
  }
  return Promise.all([start(), start()])
}

export async function portableReconciliationFailureCleanupForTest() {
  let exited: Promise<number> | undefined
  const cause = await spawnTracked(
    [process.execPath, "-e", "setInterval(() => {}, 1_000)"],
    { stdout: "pipe", stderr: "pipe" },
    {
      forcePortable: true,
      portableObserve: async (pid, nonce, deadline, confirmedNonce, stage) => {
        if (stage === "reconcile") throw new Error("synthetic reconciliation failure")
        return portableSnapshot(pid, nonce, deadline, confirmedNonce)
      },
      onPortableSpawn: (child) => {
        exited = child.exited
        void exited.catch(() => {})
      },
    },
  ).catch((cause) => cause)
  const reaped = await Promise.resolve(exited).then(
    () => true,
    () => true,
  )
  return { cause, reaped }
}

// This exercises the actual gated spawn and finalizer path. After exec, the
// root exits on TERM while a descendant ignores it. The verified controller
// must still KILL the group, and a cleanup unlink failure must be reported
// after containment rather than preventing it.
export async function portableOpaqueExecCleanupForTest() {
  if (process.platform === "win32") throw new Error("portable process groups are unavailable on Windows")
  const marker = path.join(os.tmpdir(), `opencode-portable-descendant-${crypto.randomUUID()}`)
  const rootTermMarker = `${marker}.root-term`
  const descendant = 'process.on("SIGTERM", () => {}); setInterval(() => {}, 1_000)'
  const target = `const child = require("node:child_process").spawn(process.execPath, ["-e", ${JSON.stringify(descendant)}], { stdio: "ignore" }); require("node:fs").writeFileSync(${JSON.stringify(marker)}, String(child.pid)); process.on("SIGTERM", () => { require("node:fs").writeFileSync(${JSON.stringify(rootTermMarker)}, "TERM"); process.exit(0) }); setInterval(() => {}, 1_000)`
  const started = Date.now()
  const signalPaths: string[] = []
  const initialNonces = new Map<number, string>()
  const acquired = await spawnTracked([process.execPath, "-e", target], { stdout: "pipe", stderr: "pipe" }, {
    forcePortable: true,
    portableFilesystem: {
      exists: existsSync,
      read: (file) => readFileSync(file, "utf8"),
      unlink: (file) => {
        if (file.includes("opencode-ready-")) throw new Error("synthetic post-exec cleanup trigger")
        unlinkSync(file)
      },
    },
    portableObserve: async (pid, nonce, deadline, confirmedNonce, stage) => {
      if (stage === "initial") initialNonces.set(pid, nonce)
      if (stage === "reconcile") {
        while (!existsSync(marker)) {
          if (Date.now() >= deadline) throw new Error("descendant did not start before reconciliation deadline")
          await Bun.sleep(5)
        }
      }
      return portableSnapshot(pid, nonce, deadline, confirmedNonce)
    },
    // Darwin finalization uses signalKnown before its broad nonce walk, so
    // this reaches the native known-target branch. Linux uses tracker.signal;
    // keep that assertion on its real branch below.
    portableKnownInspect:
      process.platform === "darwin"
        ? async () => {
            throw new Error("synthetic persistent post-exec native observation failure")
          }
        : undefined,
    onPortableTrackerSignal: (path, signal) => signalPaths.push(`${path}:${signal}`),
  }).catch((cause) => cause)
  // The synthetic unlink failure occurs in finalization now that the gate
  // consumes its release marker before exec. Exercise that owned boundary.
  const cause =
    acquired instanceof Error
      ? acquired
      : await terminateProcessPromise(acquired.child, () => {}, acquired.tracker, Date.now() + 4_000, acquired.rawExited).catch(
          (cause) => cause,
        )
  const descendantPID = existsSync(marker) ? Number(readFileSync(marker, "utf8")) : 0
  const descendantGone =
    descendantPID > 0 &&
    (() => {
      try {
        process.kill(descendantPID, 0)
        return false
      } catch (error) {
        return isMissingProcess(error)
      }
    })()
  const rootExitedAfterTerm = existsSync(rootTermMarker)
  if (existsSync(marker)) unlinkSync(marker)
  if (existsSync(rootTermMarker)) unlinkSync(rootTermMarker)
  return {
    cause,
    descendantGone,
    rootExitedAfterTerm,
    durationMs: Date.now() - started,
    signalPaths,
    controllerHasDistinctNonce: new Set(initialNonces.values()).size === 2,
  }
}

// A controller is deliberately outside nonce discovery. This regression keeps
// a non-nonce child in the verified group, so ordinary scans become empty as
// soon as the direct root exits. Successful finalization must still wait for
// the controller's own bounded group KILL before it reports success.
export async function portableControllerCompletionForTest() {
  if (process.platform === "win32") throw new Error("portable process groups are unavailable on Windows")
  const marker = path.join(os.tmpdir(), `opencode-portable-controller-child-${crypto.randomUUID()}`)
  const child = `process.on("SIGTERM", () => {}); setInterval(() => {}, 1_000)`
  const target = `const child = require("node:child_process").spawn("/usr/bin/env", ["-u", "OPENCODE_TEST_PROCESS_NONCE", ${JSON.stringify(process.execPath)}, "-e", ${JSON.stringify(child)}], { stdio: "ignore" }); require("node:fs").writeFileSync(${JSON.stringify(marker)}, String(child.pid)); process.on("SIGTERM", () => process.exit(0)); setInterval(() => {}, 1_000)`
  const removed = new Set<string>()
  const proc = await spawnTracked([process.execPath, "-e", target], { stdout: "pipe", stderr: "pipe" }, {
    forcePortable: true,
    portableFilesystem: {
      exists: existsSync,
      read: (file) => readFileSync(file, "utf8"),
      unlink: (file) => {
        removed.add(path.basename(file).replace(/-[^-]+$/, ""))
        unlinkSync(file)
      },
    },
  })
  while (!existsSync(marker)) await Bun.sleep(5)
  const started = Date.now()
  await terminateProcessPromise(proc.child, () => {}, proc.tracker, Date.now() + 4_000, proc.rawExited)
  const pid = Number(readFileSync(marker, "utf8"))
  const descendantGone = (() => {
    try {
      process.kill(pid, 0)
      return false
    } catch (cause) {
      return isMissingProcess(cause)
    }
  })()
  if (existsSync(marker)) unlinkSync(marker)
  return { descendantGone, durationMs: Date.now() - started, removedControls: removed.size }
}

// Stop the controller at the exact point where the old protocol wrote its
// success marker. The marker is now only kill-issued, so finalization must use
// its own identity-checked group KILL rather than trusting that publication.
export async function portableCompletionBeforeKillFallbackForTest() {
  if (process.platform === "win32") throw new Error("portable process groups are unavailable on Windows")
  const marker = path.join(os.tmpdir(), `opencode-portable-paused-child-${crypto.randomUUID()}`)
  const pause = path.join(os.tmpdir(), `opencode-portable-pause-${crypto.randomUUID()}`)
  writeFileSync(pause, "pause")
  const child = 'process.on("SIGTERM", () => {}); setInterval(() => {}, 1_000)'
  const target = `const child = require("node:child_process").spawn("/usr/bin/env", ["-u", "OPENCODE_TEST_PROCESS_NONCE", ${JSON.stringify(process.execPath)}, "-e", ${JSON.stringify(child)}], { stdio: "ignore" }); require("node:fs").writeFileSync(${JSON.stringify(marker)}, String(child.pid)); process.on("SIGTERM", () => process.exit(0)); setInterval(() => {}, 1_000)`
  let controls: { readonly ready: string; readonly completion: string } | undefined
  let fallback = false
  const proc = await spawnTracked([process.execPath, "-e", target], { stdout: "pipe", stderr: "pipe" }, {
    forcePortable: true,
    portableControllerPause: pause,
    onPortableControllerSetup: (value) => {
      controls = value
    },
    onPortableControllerFallback: () => {
      fallback = true
    },
  })
  if (!controls) throw new Error("portable controller controls were not published")
  const controllerControls = controls
  while (!existsSync(marker)) await Bun.sleep(5)
  const controllerPID = Number(readFileSync(controllerControls.ready, "utf8").slice("ready:".length))
  const termination = terminateProcessPromise(proc.child, () => {}, proc.tracker, Date.now() + 4_000, proc.rawExited)
  try {
    const issuedDeadline = Date.now() + 2_000
    while (!existsSync(controllerControls.completion) && Date.now() < issuedDeadline) await Bun.sleep(5)
    if (!existsSync(controllerControls.completion)) throw new Error("portable controller did not publish kill-issued before its pause")
    const killIssued = readFileSync(controllerControls.completion, "utf8") === "kill-issued"
    await termination
    const descendantPID = Number(readFileSync(marker, "utf8"))
    const rootGone = !(await portablePidExists(proc.child.pid, Date.now() + 1_000))
    const descendantGone = !(await portablePidExists(descendantPID, Date.now() + 1_000))
    const controllerGone = !(await portablePidExists(controllerPID, Date.now() + 1_000))
    return { fallback, killIssued, rootGone, descendantGone, controllerGone }
  } finally {
    if (existsSync(pause)) unlinkSync(pause)
    await termination.catch(() => undefined)
    if (existsSync(marker)) unlinkSync(marker)
  }
}

// An abort control can fail after the controller has been verified. That
// failure remains diagnostic-only: exact controller-group fallback KILL and
// all disappearance checks still have to run before cleanup returns.
export async function portableAbortWriteFailureForTest() {
  if (process.platform === "win32") throw new Error("portable process groups are unavailable on Windows")
  const marker = path.join(os.tmpdir(), `opencode-portable-abort-write-${crypto.randomUUID()}`)
  const descendant = 'process.on("SIGTERM", () => {}); setInterval(() => {}, 1_000)'
  const target = `const child = require("node:child_process").spawn("/usr/bin/env", ["-u", "OPENCODE_TEST_PROCESS_NONCE", ${JSON.stringify(process.execPath)}, "-e", ${JSON.stringify(descendant)}], { stdio: "ignore" }); require("node:fs").writeFileSync(${JSON.stringify(marker)}, String(child.pid)); process.on("SIGTERM", () => process.exit(0)); setInterval(() => {}, 1_000)`
  let fallback = false
  const proc = await spawnTracked([process.execPath, "-e", target], { stdout: "pipe", stderr: "pipe" }, {
    forcePortable: true,
    portableFilesystem: {
      exists: existsSync,
      read: (file) => readFileSync(file, "utf8"),
      write: (file, contents) => {
        if (file.includes("opencode-abort-")) throw new Error("synthetic portable abort write failed")
        writeFileSync(file, contents)
      },
      unlink: unlinkSync,
    },
    onPortableControllerFallback: () => {
      fallback = true
    },
  })
  while (!existsSync(marker)) await Bun.sleep(5)
  const cause = await terminateProcessPromise(proc.child, () => {}, proc.tracker, Date.now() + 4_000, proc.rawExited).catch(
    (cause) => cause,
  )
  const descendantPID = Number(readFileSync(marker, "utf8"))
  const descendantGone = !(await portablePidExists(descendantPID, Date.now() + 1_000))
  if (existsSync(marker)) unlinkSync(marker)
  return { cause, fallback, descendantGone }
}

// Force failure after controller identity verification but before tracker
// creation. The gate exits on TERM; cleanup still has to use the verified
// controller identity to KILL the group and prove that controller vanished.
export async function portablePreTrackerControllerFailureForTest() {
  if (process.platform === "win32") throw new Error("portable process groups are unavailable on Windows")
  let controllerPID = 0
  let rootExited: Promise<number> | undefined
  let fallback = false
  const cause = await spawnTracked([process.execPath, "-e", "setInterval(() => {}, 1_000)"], { stdout: "pipe", stderr: "pipe" }, {
    forcePortable: true,
    portableFilesystem: {
      exists: existsSync,
      read: (file) => readFileSync(file, "utf8"),
      write: (file, contents) => {
        if (file.includes("opencode-abort-")) throw new Error("synthetic pre-tracker portable abort write failed")
        writeFileSync(file, contents)
      },
      unlink: unlinkSync,
    },
    onPortableSpawn: (child) => {
      rootExited = child.exited
      void rootExited.catch(() => {})
    },
    onPortableControllerSetup: ({ ready }) => {
      controllerPID = Number(readFileSync(ready, "utf8").slice("ready:".length))
      throw new Error("synthetic pre-tracker controller failure")
    },
    onPortableControllerFallback: () => {
      fallback = true
    },
  }).catch((cause) => cause)
  const rootReaped = await Promise.resolve(rootExited).then(
    () => true,
    () => true,
  )
  const controllerGone = controllerPID > 0 && !(await portablePidExists(controllerPID, Date.now() + 1_000))
  return { cause, fallback, rootReaped, controllerGone }
}

// The first fallible owner observation happens before the gate can be
// reconciled. It must be deferred until the nonce-authenticated controller is
// captured, so an abort-write failure still reaches group KILL and drain
// verification after the direct gate disappears.
export async function portablePreControllerCaptureFailureForTest() {
  if (process.platform === "win32") throw new Error("portable process groups are unavailable on Windows")
  let controllerPID = 0
  let rootExited: Promise<number> | undefined
  let fallback = false
  const cause = await spawnTracked([process.execPath, "-e", "setInterval(() => {}, 1_000)"], { stdout: "pipe", stderr: "pipe" }, {
    forcePortable: true,
    portableExitObservationFailure: true,
    portableFilesystem: {
      exists: existsSync,
      read: (file) => readFileSync(file, "utf8"),
      write: (file, contents) => {
        if (file.includes("opencode-abort-")) throw new Error("synthetic pre-controller portable abort write failed")
        writeFileSync(file, contents)
      },
      unlink: unlinkSync,
    },
    onPortableSpawn: (child) => {
      rootExited = child.exited
      void rootExited.catch(() => {})
    },
    onPortableControllerCapture: (controller) => {
      controllerPID = controller.pid
    },
    onPortableControllerFallback: () => {
      fallback = true
    },
  }).catch((cause) => cause)
  const rootReaped = await Promise.resolve(rootExited).then(
    () => true,
    () => true,
  )
  const controllerGone = controllerPID > 0 && !(await portablePidExists(controllerPID, Date.now() + 1_000))
  return { cause, fallback, rootReaped, controllerGone }
}

async function portableControllerRecoveryForTest(
  kind: "read" | "authenticate",
  failures: number,
  controllerDescendant?: string,
) {
  if (process.platform === "win32") throw new Error("portable process groups are unavailable on Windows")
  let attempts = 0
  let controllerPID = 0
  let fallback = false
  const started = Date.now()
  const cause = await spawnTracked([process.execPath, "-e", "setInterval(() => {}, 1_000)"], { stdout: "pipe", stderr: "pipe" }, {
    forcePortable: true,
    portableControllerDescendant: controllerDescendant,
    portableFilesystem: {
      exists: existsSync,
      read: (file) => {
        if (kind === "read" && file.includes("opencode-controller-") && attempts++ < failures)
          throw new Error("synthetic controllerStatus read failed")
        return readFileSync(file, "utf8")
      },
      write: (file, contents) => {
        if (file.includes("opencode-abort-")) throw new Error("synthetic controller recovery abort write failed")
        writeFileSync(file, contents)
      },
      unlink: unlinkSync,
    },
    portableObserve: async (pid, nonce, deadline, confirmedNonce, stage) => {
      if (kind === "authenticate" && stage === "initial" && attempts++ < failures)
        throw new Error("synthetic controllerStatus authentication failed")
      return portableSnapshot(pid, nonce, deadline, confirmedNonce)
    },
    onPortableControllerRecovered: (controller) => {
      controllerPID = controller.pid
    },
    onPortableControllerFallback: () => {
      fallback = true
    },
  }).catch((cause) => cause)
  return {
    cause,
    fallback,
    controllerPID,
    controllerGone: controllerPID > 0 && !(await portablePidExists(controllerPID, Date.now() + 1_000)),
    attempts,
    durationMs: Date.now() - started,
  }
}

export function portableControllerRecoveryFailureForTest(kind: "read" | "authenticate") {
  return portableControllerRecoveryForTest(kind, 1)
}

// A permanently unreadable status record must not consume the acquisition
// cleanup deadline. The controller's distinct nonce provides an independent,
// authenticated route to the launch group even while the abort write fails.
export function portablePersistentControllerRecoveryForTest(kind: "read" | "authenticate") {
  return portableControllerRecoveryForTest(kind, Number.POSITIVE_INFINITY)
}

// Keep a bounded status retry window: publication or authentication may become
// available after the first failing read without delaying the reserved final
// TERM/KILL/reap phase.
export function portableLateControllerRecoveryForTest(kind: "read" | "authenticate") {
  return portableControllerRecoveryForTest(kind, 3)
}

export async function portableControllerDescendantPersistentRecoveryForTest(kind: "read" | "authenticate") {
  const marker = path.join(os.tmpdir(), `opencode-controller-descendant-${crypto.randomUUID()}`)
  try {
    const result = await portableControllerRecoveryForTest(kind, Number.POSITIVE_INFINITY, marker)
    const [controller, descendant] = existsSync(marker) ? readFileSync(marker, "utf8").split(":").map(Number) : []
    return {
      ...result,
      recoveredController: result.controllerPID === controller,
      descendantGone:
        typeof descendant === "number" &&
        Number.isSafeInteger(descendant) &&
        !(await portablePidExists(descendant, Date.now() + 1_000)),
    }
  } finally {
    if (existsSync(marker)) unlinkSync(marker)
  }
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
  deadline = Date.now() + (tracker.windowsJob ? windowsJobDrainReserveMs + windowsJobFallbackReserveMs : 4_000),
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
  const windowsPhases = tracker.windowsJob ? windowsJobCleanupPhases(deadline) : undefined
  // Portable cleanup delegates its last containment step to a protected
  // controller. Do not let ordinary nonce discovery consume the controller's
  // fallback KILL and identity-verification window.
  const containmentDeadline = tracker.abort
    ? Math.max(Date.now(), deadline - portableControllerFallbackReserveMs)
    : (windowsPhases?.fallbackDeadline ?? deadline)
  const owned = async () => {
    const priorErrors = tracker.errors.length
    const processes = await tracker.scan(containmentDeadline)
    if (tracker.errors.length !== priorErrors) errors.push(...tracker.errors.slice(priorErrors))
    return processes
  }
  const goneWithin = async (
    milliseconds: number,
    resignal?: "SIGTERM" | "SIGKILL",
    returnOnKnown = false,
  ) => {
    const until = Math.min(containmentDeadline, Date.now() + milliseconds)
    let emptyScans = 0
    while (true) {
      const processes = await owned()
      if (resignal && processes.length) await signal(resignal)
      if (returnOnKnown && tracker.hasKnown?.()) return false
      // An opaque post-exec Darwin observation is still reported, but the
      // controlled gate has already terminated its own verified group. Do not
      // spend the descendant reserve treating that diagnostic as live
      // membership once complete scans prove the group is empty.
      if ((tracker.abort || tracker.errors.length === 0) && tracker.scanComplete?.() !== false && processes.length === 0) {
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
  const signal = async (name: "SIGTERM" | "SIGKILL", limit = containmentDeadline) => {
    const priorErrors = tracker.errors.length
    // Darwin's broad nonce discovery has a separate budget. The finalizer
    // must spend its TERM/KILL and direct-child reap reserve on identities it
    // already owns before starting a broad PID walk.
    if (tracker.broadDiscovery && tracker.signalKnown) await tracker.signalKnown(name, limit, mark)
    else await tracker.signal(name, limit, mark)
    errors.push(...tracker.errors.slice(priorErrors))
    mark("exit.wait", child.pid)
  }
  const reap = async (stage: string, limit = containmentDeadline) => {
    const remaining = Math.max(Math.min(limit, containmentDeadline) - Date.now(), 0)
    return exitedWithin(child, remaining, stage, errors, observed)
  }
  // A direct child exit is not proof that a shell, grandchild, or pipe owner
  // is gone. This applies to ACP's EOF path as much as forced termination.
  try {
    // A portable controller is a verified member of the launch group. Arm it
    // before any root-directed signal so it retains authority when TERM makes
    // the root disappear while a descendant ignores that TERM.
    try {
      await tracker.abort?.()
    } catch (cause) {
      collectCleanupErrors(errors, cause)
    }
    if (windowsPhases) {
      // The Job Object's own native drain must start before any membership
      // scan. It owns the entire first phase; the second is still available
      // to reap the captured supervisor and inspect failed containment.
      await signal("SIGTERM", windowsPhases.drainDeadline)
      const reaped = await reap("after Windows Job Object drain", windowsPhases.reapDeadline)
      // Do not infer Job Object membership from the supervisor PID. Its
      // disappearance can race a surviving member; only the authenticated
      // zero-member native acknowledgement can establish containment.
      if (!tracker.jobDrained?.())
        errors.push(new Error(`Windows Job Object containment for ${child.pid} did not acknowledge zero members`))
      if (!reaped)
        errors.push(new Error(`Windows job supervisor ${child.pid} did not reap after the reserved fallback window`))
    }
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
        const killed = await goneWithin(Math.max(containmentDeadline - Date.now(), 0), "SIGKILL")
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
    if (!tracker.broadDiscovery && !windowsPhases) {
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
      await tracker.stop(deadline)
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
    // A Windows Job Object needs its native drain acknowledgement interval
    // before the independent fallback/reap phase. Four seconds only covered
    // fallback, leaving its drain deadline at "now" for ACP cleanup.
    await terminateProcessPromise(
      child,
      mark,
      tracker,
      Date.now() + (tracker.windowsJob ? windowsJobDrainReserveMs + windowsJobFallbackReserveMs : 4_000),
    )
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

export async function acpWindowsDrainReserveForTest() {
  let signalDeadline = 0
  const started = Date.now()
  await cleanupAcpProcess(
    {
      pid: 42,
      kill: () => {},
      exited: Promise.resolve(0),
      stdin: { end: () => {} },
    } as AcpCleanupChild,
    {
      scan: async () => [],
      signal: async (_signal, deadline) => {
        signalDeadline = deadline
      },
      stop: async () => {},
      windowsJob: true,
      jobDrained: () => true,
      errors: [],
    },
    () => {},
  )
  return signalDeadline - started
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
