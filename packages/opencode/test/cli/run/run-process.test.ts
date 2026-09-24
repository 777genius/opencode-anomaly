// Subprocess integration tests for `opencode run` (non-interactive mode).
// These exercise the real CLI binary against a TestLLMServer running in the
// same process. See `test/lib/cli-process.ts` for the harness — each test uses
// `opencode.run(message, opts?)` to spawn `bun src/index.ts run ...` with
// `OPENCODE_CONFIG_CONTENT` providing the test provider config inline.
import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { existsSync, unlinkSync } from "node:fs"
import os from "node:os"
import { reply } from "../../lib/llm-server"
import {
  cliIt,
  commandStatusForTest,
  commandResultForTest,
  commandCleanupReserveForTest,
  acpCleanupFailuresForTest,
  acpWindowsDrainReserveForTest,
  darwinBroadPreOwnershipObservationForTest,
  darwinFinalizerDiscoveryForTest,
  darwinProbeDeadlineForTest,
  darwinProcArgsForTest,
  darwinProcArgsCommandForTest,
  formatProcessErrorForTest,
  lateExitObservationFailureForTest,
  ownedExitObservationFailureForTest,
  parseWindowsCommandLineForTest,
  darwinEnvironmentForTest,
  portableSignalRevalidationForTest,
  portableSignalDiscoveryBudgetForTest,
  portableCleanupReserveForTest,
  portableLaunchReconciliationForTest,
  portableGateEscalationForTest,
  portableFilesystemFailureForTest,
  portableInitialIdentityRetryForTest,
  portableControllerVisibilityRetryForTest,
  portableDelayedControllerHandshakeForTest,
  portableControllerScriptSyntaxForTest,
  portableAdmissionDirectorySyncFailureForTest,
  portableControllerStartupFailureForTest,
  portableLinuxControllerEnvironmentForTest,
  portableShortLivedProcessForTest,
  portableReadinessPartialPublicationForTest,
  portableReconciliationFailureCleanupForTest,
  portableOpaqueExecCleanupForTest,
  portableControllerCompletionForTest,
  portableCompletionBeforeKillFallbackForTest,
  portableAbortWriteFailureForTest,
  portablePreTrackerControllerFailureForTest,
  portablePreControllerCaptureFailureForTest,
  portableControllerRecoveryFailureForTest,
  portableControllerDescendantPersistentRecoveryForTest,
  portableControllerAuthorityForTest,
  portablePersistentControllerRecoveryForTest,
  portableLateControllerRecoveryForTest,
  portableFailedReconciliationCleanupHandoffForTest,
  portableTrackerHandoffCleanupForTest,
  portableLinuxGroupDrainedForTest,
  type ProcessIdentity,
  windowsIdentityObservationForTest,
  windowsJobCleanupPhasesForTest,
  windowsNativeArgvGetterCleanupForTest,
  windowsCommandLineForTest,
  windowsCapturedHandleCleanupForTest,
  windowsNativeCommandLineRoundTripForTest,
  windowsSupervisorArgumentsForTest,
  windowsSupervisorArgumentRoundTripForTest,
  windowsSupervisorDrainLifecycleForTest,
  windowsSupervisorCompletionForTest,
  windowsSupervisorProtocolForTest,
  windowsSupervisorIdentityForTest,
  windowsSupervisorStatusForTest,
  linuxCgroupDiscoveryDeadlineForTest,
  abortedCgroupMembershipDeadlineForTest,
  linuxProcDiscoveryDeadlineForTest,
} from "../../lib/cli-process"

const trackedIdentity: ProcessIdentity = {
  pid: 42,
  started: "started",
  executable: "opencode",
  containment: "test",
}

describe("opencode run (non-interactive subprocess)", () => {
  test("macOS process arguments preserve NUL-delimited environment boundaries", () => {
    const nonce = "0f2cf3c0-3c16-4d8b-8e01-5e0e8b93afbf"
    const encoder = new TextEncoder()
    const record = new Uint8Array([
      2,
      0,
      0,
      0,
      ...encoder.encode("/usr/local/bin/opencode\0\0opencode\0--serve\0OTHER=OPENCODE_TEST_PROCESS_NONCE="),
      ...encoder.encode(nonce),
      0,
      ...encoder.encode(`OPENCODE_TEST_PROCESS_NONCE=${nonce}\0`),
    ])
    expect(darwinEnvironmentForTest(record)).toEqual([
      `OTHER=OPENCODE_TEST_PROCESS_NONCE=${nonce}`,
      `OPENCODE_TEST_PROCESS_NONCE=${nonce}`,
    ])
    const argumentOnly = new Uint8Array([
      2,
      0,
      0,
      0,
      ...encoder.encode(`/usr/local/bin/opencode\0\0opencode\0OPENCODE_TEST_PROCESS_NONCE=${nonce}\0`),
    ])
    expect(darwinEnvironmentForTest(argumentOnly)).toEqual([])
    expect(darwinProcArgsCommandForTest(42)).toEqual(["sysctl", "-b", "kern.procargs2.42"])
  })

  test("macOS argument probes use an independent short deadline", () => {
    const started = Date.now()
    expect(darwinProbeDeadlineForTest(started + 10)).toBe(started + 10)
    expect(darwinProbeDeadlineForTest(started + 60_000)).toBeLessThanOrEqual(started + 250)
  })

  test("macOS inaccessible argument probes are bounded independently", async () => {
    if (process.platform !== "darwin") return
    const started = Date.now()
    const probes = Array.from({ length: 32 }, (_, index) =>
      darwinProcArgsForTest(999_999_999 - index, Date.now() + 4_000).catch(() => undefined),
    )
    await Promise.all(probes)
    expect(Date.now() - started).toBeLessThan(1_000)
  })

  test("macOS argument probe reaps an owned helper after acquisition expires", async () => {
    if (process.platform !== "darwin") return
    let helperPID: number | undefined
    const cause = await darwinProcArgsForTest(process.pid, Date.now(), (pid) => {
      helperPID = pid
    }).catch((cause) => cause)
    expect(cause).toBeInstanceOf(AggregateError)
    const findAcquisitionDeadline = (error: unknown): Error | undefined => {
      if (error instanceof AggregateError)
        return Array.from(error.errors)
          .map(findAcquisitionDeadline)
          .find((candidate): candidate is Error => candidate !== undefined)
      if (error instanceof Error && error.message === "macOS process acquisition deadline elapsed before reading environment") return error
    }
    expect(findAcquisitionDeadline(cause)).toBeInstanceOf(Error)
    expect(helperPID).toBeGreaterThan(0)
    const status = await Promise.resolve()
      .then(() => {
        process.kill(helperPID!, 0)
        return "present"
      })
      .catch((cause) => (typeof cause === "object" && cause !== null && "code" in cause ? cause.code : undefined))
    expect(status).toBe("ESRCH")
  })

  // Happy path: prompt completes, output reaches stdout, process exits 0.
  // If this fails, all the others likely will too — debug here first.
  cliIt.concurrent(
    "exits 0 and writes the response to stdout on a successful prompt",
    ({ llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.text("hello from the test llm")
        const result = yield* opencode.run("say hi")
        opencode.expectExit(result, 0)
        expect(result.stdout).toBe("hello from the test llm\n")
      }),
    90_000,
  )

  cliIt.concurrent(
    "prints each completed text part in order around a tool continuation",
    ({ llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.push(
          reply().text("  before tool  ").tool("bash", {
            command: "printf tool-output",
            description: "Print deterministic output",
          }),
        )
        yield* llm.text("  after tool  ")

        const result = yield* opencode.run("use a tool", {
          extraArgs: ["--dangerously-skip-permissions"],
        })

        opencode.expectExit(result, 0)
        expect(result.stdout).toBe("before tool\nafter tool\n")
      }),
    90_000,
  )

  cliIt.concurrent(
    "prints reasoning before text only with --thinking",
    ({ llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.reason("  considering  ", { text: "  answer  " })
        const thinking = yield* opencode.run("think", { extraArgs: ["--thinking"] })
        opencode.expectExit(thinking, 0)
        expect(thinking.stdout).toBe("Thinking: considering\nanswer\n")

        yield* llm.reason("hidden", { text: "visible" })
        const plain = yield* opencode.run("think again")
        opencode.expectExit(plain, 0)
        expect(plain.stdout).toBe("visible\n")
      }),
    90_000,
  )

  // Regression for #27371: an unknown model used to hang the process forever
  // waiting on a session.status === idle event that never arrived. The fix
  // makes the SDK call surface an error promptly so the process exits nonzero.
  // We assert nonzero exit AND wall-clock under the harness timeout — a hang
  // would expire the timeout and produce a different (signal-killed) failure.
  cliIt.concurrent(
    "exits nonzero promptly when the model is unknown (regression for #27371)",
    ({ opencode }) =>
      Effect.gen(function* () {
        const result = yield* opencode.run("say hi", {
          model: "test/nonexistent-model",
          timeoutMs: 90_000,
          printLogs: true,
        })
        opencode.expectExit(result, 1)
        expect(result.durationMs).toBeLessThan(30_000)
      }),
    90_000,
  )

  // The test provider's SSE error item is interpreted by the SDK as an unknown
  // finish, not a fatal provider/session error. Unknown finishes should continue
  // the prompt loop so a subsequent response can complete the run.
  cliIt.concurrent(
    "unknown stream finish preserves partial output and continues",
    ({ llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.push(
          reply().text("partial response").tool("bash", {
            command: "printf tool",
            description: "Print deterministic output",
          }),
        )
        yield* llm.fail("upstream provider exploded mid-stream")
        yield* llm.text("recovered")
        const result = yield* opencode.run("trigger midstream error", { timeoutMs: 90_000 })
        expect(result.exitCode).toBe(0)
        expect(result.stdout).toBe("partial response\nrecovered\n")
        expect(result.stderr).not.toContain("upstream provider exploded mid-stream")
      }),
    90_000,
  )

  // --format json puts one JSON object per line on stdout for each emitted
  // event. Consumers (CI scripts, tooling) parse this stream. Asserts the
  // shape so a future event-emit change has to update this expectation.
  cliIt.concurrent(
    "--format json emits parseable line-delimited JSON to stdout",
    ({ llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.text("structured output")
        const result = yield* opencode.run("say hi", { format: "json" })
        opencode.expectExit(result, 0)

        const events = opencode.parseJsonEvents(result.stdout)
        expect(events.length).toBeGreaterThan(0)
        for (const evt of events) {
          expect(typeof evt.type).toBe("string")
          expect(typeof evt.sessionID).toBe("string")
        }
        expect(events.map((event) => event.type)).toEqual(["step_start", "text", "step_finish"])
        expect(events.map(({ timestamp: _, sessionID: __, ...event }) => event)).toEqual([
          { type: "step_start", part: expect.objectContaining({ type: "step-start" }) },
          {
            type: "text",
            part: expect.objectContaining({ type: "text", text: "structured output" }),
          },
          { type: "step_finish", part: expect.objectContaining({ type: "step-finish" }) },
        ])
        expect(result.stdout.endsWith("\n")).toBe(true)
        expect(
          result.stdout
            .split("\n")
            .slice(0, -1)
            .every((line) => line.length > 0),
        ).toBe(true)
      }),
    90_000,
  )

  cliIt.concurrent(
    "--format json emits a pure error record for a rejected prompt request",
    ({ opencode }) =>
      Effect.gen(function* () {
        const result = yield* opencode.run("use an unknown model", {
          model: "test/nonexistent-model",
          format: "json",
        })

        expect(result.exitCode).not.toBe(0)
        const events = opencode.parseJsonEvents(result.stdout)
        expect(events.map((event) => event.type)).toEqual(["error"])
        expect(events[0]).toEqual({
          type: "error",
          timestamp: expect.any(Number),
          sessionID: expect.any(String),
          error: expect.any(Object),
        })
        expect(result.stdout.split("\n").filter(Boolean)).toHaveLength(1)
      }),
    90_000,
  )

  cliIt.concurrent(
    "--format json preserves reasoning, tool, and continuation ordering",
    ({ llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.push(
          reply().reason("reasoning").text("before").tool("bash", {
            command: "printf tool",
            description: "Print deterministic output",
          }),
        )
        yield* llm.text("after")

        const result = yield* opencode.run("exercise json records", {
          format: "json",
          extraArgs: ["--thinking", "--dangerously-skip-permissions"],
        })

        expect(result.exitCode).toBe(0)
        const events = opencode.parseJsonEvents(result.stdout)
        expect(events.map((event) => event.type)).toEqual([
          "step_start",
          "reasoning",
          "text",
          "tool_use",
          "step_finish",
          "step_start",
          "text",
          "step_finish",
        ])
        expect(events.find((event) => event.type === "reasoning")?.part).toEqual(
          expect.objectContaining({ type: "reasoning", text: "reasoning" }),
        )
        expect(events.find((event) => event.type === "tool_use")?.part).toEqual(
          expect.objectContaining({
            type: "tool",
            tool: "bash",
            state: expect.objectContaining({ status: "completed" }),
          }),
        )
        expect(
          result.stdout
            .split("\n")
            .slice(0, -1)
            .every((line) => line.startsWith("{")),
        ).toBe(true)
      }),
    90_000,
  )

  cliIt.concurrent(
    "--format json records an unknown stream finish and continuation",
    ({ llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.push(
          reply().text("partial json").tool("bash", {
            command: "printf tool",
            description: "Print deterministic output",
          }),
        )
        yield* llm.fail("provider failed")
        yield* llm.text("recovered")
        const result = yield* opencode.run("fail after output", {
          format: "json",
          timeoutMs: 90_000,
          // This fixture intentionally exercises stream continuation, not the
          // interactive permission protocol. Keep the tool call non-blocking
          // when hosted approval is enabled in the child process.
          extraArgs: ["--dangerously-skip-permissions"],
        })

        const events = opencode.parseJsonEvents(result.stdout)
        expect(result.exitCode).toBe(0)
        expect(events.map((event) => event.type)).toEqual([
          "step_start",
          "text",
          "tool_use",
          "step_finish",
          "step_start",
          "step_finish",
          "step_start",
          "text",
          "step_finish",
        ])
        expect(events[1]?.part).toEqual(expect.objectContaining({ type: "text", text: "partial json" }))
        expect(events[5]?.part).toEqual(expect.objectContaining({ type: "step-finish", reason: "unknown" }))
        expect(events[7]?.part).toEqual(expect.objectContaining({ type: "text", text: "recovered" }))
        expect(events.at(-1)?.part).toEqual(expect.objectContaining({ type: "step-finish", reason: "stop" }))
      }),
    90_000,
  )

  cliIt.concurrent(
    "rejects requested permissions by default and allows them with the dangerous flag",
    ({ home, llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.tool("bash", { command: "rm -f denied-file", description: "Remove a test file" })
        yield* llm.text("continued after rejection")
        const denied = yield* opencode.run("request permission", { permission: { bash: "ask" } })
        opencode.expectExit(denied, 0)
        expect(denied.stderr).toContain("permission requested: bash")
        expect(denied.stdout).toBe("")

        yield* llm.reset
        const runMatch = (prompt: string) => (hit: { body: Record<string, unknown> }) => {
          const body = JSON.stringify(hit.body)
          return body.includes(prompt) && !body.includes("Generate a title for this conversation")
        }
        yield* llm.toolMatch(runMatch("request allowed permission"), "bash", {
          command: "rm -f allowed-file",
          description: "Remove a test file",
        })
        yield* llm.textMatch(runMatch("request allowed permission"), "continued after approval")
        yield* llm.toolMatch(runMatch("request denied permission"), "bash", {
          command: "touch explicitly-denied",
          description: "Create a denied marker",
        })
        yield* llm.textMatch(runMatch("request denied permission"), "continued after explicit denial")

        const allowedRun = yield* opencode.startRun("request allowed permission", {
          permission: { bash: "ask" },
          extraArgs: ["--dangerously-skip-permissions"],
        })
        const explicitlyDeniedRun = yield* opencode.startRun("request denied permission", {
          permission: { bash: "deny" },
          extraArgs: ["--dangerously-skip-permissions"],
        })
        const [allowed, explicitlyDenied] = yield* Effect.all([allowedRun.result, explicitlyDeniedRun.result], {
          concurrency: "unbounded",
        }).pipe(Effect.timeout("60 seconds"))

        opencode.expectExit(allowed, 0)
        expect(allowed.stderr).not.toContain("permission requested: bash")
        expect(allowed.stdout).toContain("continued after approval")
        opencode.expectExit(explicitlyDenied, 0)
        expect(explicitlyDenied.stdout).toContain("continued after explicit denial")
        expect(yield* Effect.promise(() => Bun.file(`${home}/explicitly-denied`).exists())).toBe(false)
      }),
    90_000,
  )

  cliIt.live(
    "attach mode sends client-local file contents without a shared path",
    ({ home, llm, opencode }) =>
      Effect.gen(function* () {
        const source = `${home}/client-only.txt`
        const sentinel = "client-only attachment sentinel"
        yield* Effect.promise(() => Bun.write(source, sentinel))
        yield* llm.text("attachment received")
        const server = yield* opencode.serve()

        const result = yield* opencode.run("read the attachment", {
          extraArgs: ["--attach", server.url, `--file=${source}`, "--"],
        })

        opencode.expectExit(result, 0)
        const input = JSON.stringify(yield* llm.inputs)
        expect(input).toContain(sentinel)
        expect(input).not.toContain(`file://${source}`)
      }),
    90_000,
  )

  cliIt.concurrent(
    "attach mode rejects local directories before prompt admission",
    ({ home, opencode }) =>
      Effect.gen(function* () {
        const result = yield* opencode.run("read the directory", {
          extraArgs: ["--attach", "http://127.0.0.1:1", `--file=${home}`, "--"],
        })

        expect(result.exitCode).not.toBe(0)
        expect(result.stderr).toContain("Cannot attach local directory without a shared filesystem")
      }),
    90_000,
  )

  cliIt.live(
    "SIGINT interrupts an active non-interactive run without leaking the process",
    ({ llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.hang
        const run = yield* opencode.startRun("wait forever")
        yield* llm.wait(1)
        const waitForPrompt = (): Effect.Effect<void> =>
          Effect.gen(function* () {
            const inputs = yield* llm.inputs
            if (inputs.some((input) => JSON.stringify(input).includes("wait forever"))) return
            yield* Effect.sleep("50 millis")
            yield* waitForPrompt()
          })
        yield* waitForPrompt().pipe(Effect.timeout("5 seconds"))
        run.interrupt()
        const result = yield* run.result

        expect(result.exitCode).not.toBe(0)
        expect(result.durationMs).toBeLessThan(30_000)
      }),
    30_000,
  )
})

describe("CLI process cleanup containment", () => {
  test("Windows supervisor status handshake uses the reader's actual tab delimiter", () => {
    const script = windowsSupervisorProtocolForTest()
    const job = "3d2bb1c6-655a-4c55-ae29-a20439c11672"
    expect(script).toContain('Process.GetCurrentProcess().Id+"\t"+created+"\t"+jobName')
    expect(script).not.toContain('Process.GetCurrentProcess().Id+"\\t"+created')
    expect(script).toContain("File.Move(pending,status)")
    expect(windowsSupervisorStatusForTest(`123\t134102758400000000\t${job}`)).toEqual({
      pid: 123,
      created: "134102758400000000",
      job,
    })
    expect(windowsSupervisorCompletionForTest(`${job}\t0`, job)).toBe(true)
    expect(windowsSupervisorCompletionForTest(`${job}\t1`, job)).toBe(false)
    ;[
      "",
      "123",
      "123\t",
      "\t456",
      "0\t456",
      "123\t0",
      "123\t456\tbad-job",
      `123\t456\t${job}\n`,
      `123\t456\t${job} trailing`,
      "pid\t456",
    ].forEach((record) => {
      expect(windowsSupervisorStatusForTest(record)).toBeUndefined()
    })
  })

  test("Windows acquisition derives controller identity from its authenticated Job Object status", () => {
    const job = "3d2bb1c6-655a-4c55-ae29-a20439c11672"
    const record = `123\t134102758400000000\t${job}`
    expect(windowsSupervisorIdentityForTest(record, 123, job)).toEqual({
      pid: 123,
      started: "native:134102758400000000",
      executable: `job:${job}`,
      containment: "job",
    })
    expect(windowsSupervisorIdentityForTest(record, 124, job)).toBeUndefined()
    expect(windowsSupervisorIdentityForTest(record, 123, "8d2bb1c6-655a-4c55-ae29-a20439c11672")).toBeUndefined()
  })

  test("helper accepts only a complete atomically published numeric status", () => {
    expect(commandStatusForTest("0\n")).toBe(0)
    expect(commandStatusForTest("125\n")).toBe(125)
    ;["", "0", "\n", "12", "12\ntrailing", "12\n\n", "12\n\r", "-1\n", "1.5\n"].forEach((record) => {
      expect(commandStatusForTest(record)).toBeUndefined()
    })
  })

  test("Windows supervisor deadline remains compatible with powershell.exe's .NET Framework", () => {
    const script = windowsSupervisorProtocolForTest()
    expect(script).toContain('[DllImport("kernel32.dll")] static extern uint GetTickCount()')
    expect(script).toContain("unchecked(GetTickCount()-began)")
    expect(script).not.toContain("TickCount64")
    expect(script).toContain('TerminateJobObject(job,125)')
    expect(script).toContain('QueryInformationJobObject(job,1,out accounting')
    expect(script).toContain('jobName+"\t"+"0"')
  })

  test("Windows cleanup preserves the supervisor for a late Job Object drain acknowledgement", async () => {
    const result = await windowsSupervisorDrainLifecycleForTest(600)
    expect(result.state).toBe("exited")
    expect(result.acknowledged).toBe(true)
    expect(result.events).toEqual(["abort", "job-drained"])
    expect(result.errors).toEqual([])
  })

  test("Windows cleanup gives a fixed drain phase and a separate fallback phase", () => {
    expect(windowsJobCleanupPhasesForTest(8_000, 0)).toEqual({
      drainDeadline: 4_000,
      inspectionDeadline: 4_500,
      reapDeadline: 7_500,
      fallbackDeadline: 8_000,
    })
  })

  test("Windows ACP cleanup reserves a nonzero Job Object drain acknowledgement phase", async () => {
    const drainBudget = await acpWindowsDrainReserveForTest()
    expect(drainBudget).toBeGreaterThanOrEqual(3_500)
    expect(drainBudget).toBeLessThanOrEqual(4_100)
  })

  test("Windows cleanup fails closed when a fixed-deadline drain acknowledgement is late", async () => {
    const result = await windowsSupervisorDrainLifecycleForTest(4_001, 8_000)
    expect(result.state).toBe("terminated")
    expect(result.acknowledged).toBe(false)
    expect(result.events).toEqual(["abort", "inspected", "terminated"])
    expect(result.errors.map((error) => error.message)).toContain(
      "Windows Job Object for process 42 did not acknowledge descendant drain before the reserved cleanup window elapsed",
    )
  })

  test("Windows cleanup fails closed and terminates an unacknowledging Job Object supervisor", async () => {
    const result = await windowsSupervisorDrainLifecycleForTest(undefined, 8_000)
    expect(result.state).toBe("terminated")
    expect(result.acknowledged).toBe(false)
    expect(result.events).toEqual(["abort", "inspected", "terminated"])
    expect(result.errors.map((error) => error.message)).toContain(
      "Windows Job Object for process 42 did not acknowledge descendant drain before the reserved cleanup window elapsed",
    )
  })

  test("Windows supervisor disappearance cannot certify a surviving Job Object member", async () => {
    const result = await windowsSupervisorDrainLifecycleForTest(undefined, 8_000, true)
    expect(result.state).toBe("terminated")
    expect(result.acknowledged).toBe(false)
    expect(result.events).toEqual(["abort", "inspected", "terminated"])
    expect(result.errors.map((error) => error.message)).toContain(
      "Windows Job Object for process 42 did not acknowledge descendant drain before the reserved cleanup window elapsed",
    )
  })

  test("Windows identity observations accept one terminal line ending and reject embedded line breaks", () => {
    expect(windowsIdentityObservationForTest(123, "")).toEqual([])
    const fields = ["123", "456", "134102758400000000", "C:\\tool.exe"]
    const valid = fields.join("\t")
    const expected = [{ pid: 123, parent: 456, created: "134102758400000000", executable: "C:\\tool.exe" }]
    expect(windowsIdentityObservationForTest(123, `${valid}\n`)).toEqual(expected)
    expect(windowsIdentityObservationForTest(123, `${valid}\r\n`)).toEqual(expected)
    ;["\r", "\n", "\r\n"].forEach((lineEnding) => {
      fields.forEach((_, index) => {
        const malformed = fields
          .map((value, candidate) => (candidate === index ? `${value}${lineEnding}corrupt` : value))
          .join("\t")
        expect(() => windowsIdentityObservationForTest(123, malformed)).toThrow(
          "Windows process 123 returned a malformed identity observation",
        )
      })
    })
  })

  test("Windows executable mismatch still closes the captured native handle", async () => {
    const result = await windowsCapturedHandleCleanupForTest(trackedIdentity, {
      ...trackedIdentity,
      executable: "replacement.exe",
    })
    expect(result.killed).toBe(true)
    expect(result.errors.map((error) => error.message)).toContain(
      "Windows job supervisor 42 identity changed before close",
    )
  })

  test("Windows command line quoting round-trips empty, spaced, quoted, and trailing-slash arguments", () => {
    const argv = ["", "two words", 'say "hello"', "C:\\program files\\", 'escaped \\" command', "plain"]
    expect(parseWindowsCommandLineForTest(windowsCommandLineForTest(argv))).toEqual(argv)
    expect(windowsSupervisorProtocolForTest()).toContain("quoted.Append('\\\\',slashes*2+1)")
    expect(windowsSupervisorProtocolForTest()).toContain("quoted.Append('\\\\',slashes*2)")
  })

  test("Windows supervisor JSON argv framing preserves empty arguments with LF and CRLF output", () => {
    const argv = ["", "first", "", "two words", ""]
    const record = JSON.stringify(argv.map((argument) => Buffer.from(argument).toString("base64")))
    expect(windowsSupervisorArgumentsForTest(`${record}\n`)).toEqual(argv)
    expect(windowsSupervisorArgumentsForTest(`${record}\r\n`)).toEqual(argv)
  })

  test("Windows native CommandLineToArgvW round-trips quoted command text", async () => {
    if (process.platform !== "win32") return
    const argv = ["", "", "two words", 'say "hello"', "C:\\program files\\", 'escaped \\" command', "plain", ""]
    await expect(windowsNativeCommandLineRoundTripForTest(argv)).resolves.toEqual(argv)
  })

  test("Windows native argv getter failures terminate, escalate, reap, and aggregate", async () => {
    const result = await windowsNativeArgvGetterCleanupForTest()
    expect(result.signals).toEqual(["SIGTERM", "SIGKILL"])
    expect(result.reaped).toBe(true)
    expect(result.cause).toBeInstanceOf(AggregateError)
    expect(formatProcessErrorForTest(result.cause)).toContain("synthetic native argv stdout getter failed")
  })

  test("Windows supervisor binds real argv through its -File launch boundary", async () => {
    if (process.platform !== "win32") return
    const argv = ["", "", "two words", 'say "hello"', "C:\\program files\\", 'escaped \\" command', "plain", ""]
    let finalized: { statusRemoved: boolean; reaped: boolean } | undefined
    await expect(
      windowsSupervisorArgumentRoundTripForTest(argv, 4_000, undefined, (result) => {
        finalized = result
      }),
    ).resolves.toEqual(argv)
    expect(finalized).toEqual({ statusRemoved: true, reaped: true })
  })

  test("Windows supervisor gives its PowerShell/C# handshake a separate deadline", async () => {
    if (process.platform !== "win32") return
    await expect(
      windowsSupervisorArgumentRoundTripForTest(["slow handshake"], 1_000, undefined, undefined, 4_500),
    ).resolves.toEqual(["slow handshake"])
  })

  test("Windows supervisor starts the stalled target timeout after status and reaps its captured child", async () => {
    if (process.platform !== "win32") return
    let finalized: { statusRemoved: boolean; reaped: boolean } | undefined
    await expect(
      windowsSupervisorArgumentRoundTripForTest(
        [],
        100,
        "Start-Sleep -Seconds 30",
        (result) => {
          finalized = result
        },
        250,
      ),
    ).rejects.toThrow("Windows supervisor argv probe exceeded its deadline")
    expect(finalized).toEqual({ statusRemoved: true, reaped: true })
  })

  test("formats nested acquisition and cleanup errors into the RunResult diagnostic", () => {
    const formatted = formatProcessErrorForTest(
      new Error("failed to spawn opencode process", {
        cause: new AggregateError(
          [
            new Error("acquisition snapshot failed", { cause: new Error("nonce record unreadable") }),
            new AggregateError([new Error("SIGKILL failed")], "cleanup failed"),
          ],
          "failed to establish containment",
        ),
      }),
    )
    expect(formatted).toContain("failed to spawn opencode process")
    expect(formatted).toContain("failed to establish containment")
    expect(formatted).toContain("acquisition snapshot failed")
    expect(formatted).toContain("nonce record unreadable")
    expect(formatted).toContain("cleanup failed")
    expect(formatted).toContain("SIGKILL failed")
  })

  test("portable cleanup revalidates the exact target identity before signalling", async () => {
    const replacement = await portableSignalRevalidationForTest(trackedIdentity, {
      ...trackedIdentity,
      started: "reused",
    })
    expect(replacement.signalled).toBe(false)
    expect(replacement.errors.map((error) => error.message)).toContain("failed to send SIGTERM to process 42")

    const current = await portableSignalRevalidationForTest(trackedIdentity, trackedIdentity)
    expect(current.signalled).toBe(true)
  })

  test("portable launch readiness reconciles only the direct gated PID", async () => {
    const delayed = await portableLaunchReconciliationForTest([undefined])
    expect(delayed).toEqual({ _tag: "pending" })
    const pending = await portableLaunchReconciliationForTest([
      {
        pid: 42,
        parent: 1,
        group: 42,
        started: "started",
        executable: "gate",
        containment: "nonce:test",
        nonce: true,
      },
    ])
    expect(pending).toEqual({ _tag: "pending" })

    const ready = await portableLaunchReconciliationForTest([
      {
        pid: 42,
        parent: 1,
        group: 42,
        started: "started",
        executable: "opencode",
        containment: "nonce:test",
        nonce: true,
      },
    ])
    expect(ready).toMatchObject({ _tag: "ready", target: { executable: "opencode" } })
  })

  test("portable launch distinguishes short-lived exits from identity conflicts", async () => {
    await expect(portableLaunchReconciliationForTest([undefined], "exited")).resolves.toEqual({ _tag: "exited" })
    await expect(portableLaunchReconciliationForTest([], "pending", Date.now())).resolves.toEqual({ _tag: "deadline" })
    const conflict = await portableLaunchReconciliationForTest([
      {
        pid: 42,
        parent: 1,
        group: 99,
        started: "started",
        executable: "opencode",
        containment: "nonce:test",
        nonce: true,
      },
    ])
    expect(conflict).toEqual({ _tag: "conflict", message: "portable gate PID 42 changed identity before launch" })
  })

  test("portable pre-tracker escalation only signals the reconciled launch group", async () => {
    await expect(portableGateEscalationForTest()).resolves.toEqual({
      events: ["SIGTERM:-42", "SIGKILL:-42"],
      errors: [],
    })
    const changed = await portableGateEscalationForTest(true)
    expect(changed.events).toEqual([])
    expect(changed.errors.map((error) => error.message)).toEqual([
      "portable gate PID 42 identity changed before SIGTERM",
      "portable gate PID 42 identity changed before SIGKILL",
    ])
  })

  test("portable tracker hands the reconciled exec target to known-target cleanup", async () => {
    await expect(portableTrackerHandoffCleanupForTest()).resolves.toEqual({
      events: ["SIGTERM:opencode", "SIGKILL:opencode"],
      errors: [],
    })
  })

  test("portable readiness read and cleanup unlink failures reap the owned gate or target", async () => {
    if (process.platform === "win32") return
    for (const kind of ["read", "unlink"] as const) {
      const result = await portableFilesystemFailureForTest(kind)
      expect(result.reaped).toBe(true)
      expect(result.cause).toBeInstanceOf(AggregateError)
      expect(formatProcessErrorForTest(result.cause)).toContain(
        kind === "read" ? "synthetic readiness read failed" : "synthetic readiness unlink failed",
      )
    }
  })

  test("portable initial identity waits through delayed and unreadable nonce observations", async () => {
    if (process.platform === "win32") return
    await expect(portableInitialIdentityRetryForTest("delayed")).resolves.toBeGreaterThanOrEqual(3)
    await expect(portableInitialIdentityRetryForTest("unreadable")).resolves.toBeGreaterThanOrEqual(3)
  })

  test("portable controller admission retries an initially invisible authenticated status PID", async () => {
    if (process.platform === "win32") return
    await expect(portableControllerVisibilityRetryForTest()).resolves.toBeGreaterThanOrEqual(3)
  })

  test("portable controller admission waits for delayed signed publication before opening the gate", async () => {
    if (process.platform === "win32") return
    const result = await portableDelayedControllerHandshakeForTest()
    expect(result.targetExecutedBeforeAdmission).toBe(false)
    expect(result.targetExecuted).toBe(true)
    // This is recorded before cleanup's independent TERM grace begins.
    expect(result.admissionDurationMs).toBeGreaterThanOrEqual(75)
    expect(result.admissionDurationMs).toBeLessThan(2_000)
  })

  test("portable controller script parses as the exact production template", () => {
    if (process.platform === "win32") return
    expect(portableControllerScriptSyntaxForTest()).toBe(0)
  })

  test("portable admission fails closed when its containing directory cannot sync", async () => {
    if (process.platform === "win32") return
    await expect(portableAdmissionDirectorySyncFailureForTest()).resolves.toEqual({
      failed: true,
      reaped: true,
      targetExecuted: false,
    })
  })

  test("portable controller startup failure reaps the owned gate without accepting a missing controller", async () => {
    if (process.platform === "win32") return
    const result = await portableControllerStartupFailureForTest()
    expect(result.cause).toBeInstanceOf(AggregateError)
    expect(formatProcessErrorForTest(result.cause)).toContain("portable gate exited before controller handshake")
    expect(result.reaped).toBe(true)
    expect(result.durationMs).toBeLessThan(2_000)
  })

  test("forced-portable Linux controller installs its protected nonce through exec", async () => {
    if (process.platform !== "linux") return
    await expect(portableLinuxControllerEnvironmentForTest()).resolves.toBe(true)
  })

  test("portable reconciliation failure adopts the exec target before cleanup", async () => {
    if (process.platform === "win32") return
    await expect(portableFailedReconciliationCleanupHandoffForTest()).resolves.toEqual({
      events: ["SIGTERM:opencode", "SIGKILL:opencode"],
      errors: [],
    })
    const result = await portableReconciliationFailureCleanupForTest()
    expect(result.reaped).toBe(true)
    expect(result.cause).toBeInstanceOf(AggregateError)
    expect(formatProcessErrorForTest(result.cause)).toContain("synthetic reconciliation failure")
  })

  test("portable controller kills a TERM-ignoring descendant after the root exits", async () => {
    if (process.platform === "win32") return
    const result = await portableOpaqueExecCleanupForTest()
    expect(result.cause).toBeInstanceOf(AggregateError)
    expect(result.rootExitedAfterTerm).toBe(true)
    expect(result.descendantGone).toBe(true)
    expect(result.controllerHasDistinctNonce).toBe(true)
    expect(result.durationMs).toBeLessThan(2_500)
    expect(formatProcessErrorForTest(result.cause)).toContain("synthetic post-exec cleanup trigger")
    if (process.platform === "darwin") {
      expect(formatProcessErrorForTest(result.cause)).toContain("synthetic persistent post-exec native observation failure")
      expect(result.signalPaths).toContain("signalKnown:SIGKILL")
      return
    }
    // Linux may complete controller group KILL before the ordinary tracker
    // needs to signal a target directly.
  })

  test("successful portable cleanup waits for controller KILL and removes acknowledged controls", async () => {
    if (process.platform === "win32") return
    const result = await portableControllerCompletionForTest()
    expect(result.descendantGone).toBe(true)
    expect(result.durationMs).toBeGreaterThanOrEqual(400)
    expect(result.durationMs).toBeLessThan(2_500)
    // The gate consumes its release marker before exec, so finalization
    // removes the six remaining control-file kinds.
    expect(result.removedControls).toBe(6)
  })

  test("portable finalizer falls back when the controller pauses after kill-issued", async () => {
    if (process.platform === "win32") return
    await expect(portableCompletionBeforeKillFallbackForTest()).resolves.toEqual({
      fallback: true,
      killIssued: true,
      rootGone: true,
      descendantGone: true,
      controllerGone: true,
    })
  })

  test("portable abort-write failure still falls back and verifies descendant disappearance", async () => {
    if (process.platform === "win32") return
    const result = await portableAbortWriteFailureForTest()
    expect(result.cause).toBeInstanceOf(AggregateError)
    expect(formatProcessErrorForTest(result.cause)).toContain("synthetic portable abort write failed")
    expect(result.fallback).toBe(true)
    expect(result.descendantGone).toBe(true)
  })

  test("portable pre-tracker abort failure kills and verifies the TERM-ignoring controller", async () => {
    if (process.platform === "win32") return
    const result = await portablePreTrackerControllerFailureForTest()
    expect(result.cause).toBeInstanceOf(AggregateError)
    expect(formatProcessErrorForTest(result.cause)).toContain("synthetic pre-tracker portable abort write failed")
    expect(result.fallback).toBe(true)
    expect(result.rootReaped).toBe(true)
    expect(result.controllerGone).toBe(true)
  })

  test("portable failure before controller capture still finalizes the authenticated controller", async () => {
    if (process.platform === "win32") return
    const result = await portablePreControllerCaptureFailureForTest()
    expect(result.cause).toBeInstanceOf(AggregateError)
    expect(formatProcessErrorForTest(result.cause)).toContain("synthetic portable exit observation failed")
    expect(formatProcessErrorForTest(result.cause)).toContain("synthetic pre-controller portable abort write failed")
    expect(result.fallback).toBe(true)
    expect(result.rootReaped).toBe(true)
    expect(result.controllerGone).toBe(true)
  })

  test("portable controllerStatus read and authentication failures recover the controller before abort fallback", async () => {
    if (process.platform === "win32") return
    for (const kind of ["read", "authenticate"] as const) {
      const result = await portableControllerRecoveryFailureForTest(kind)
      expect(result.cause).toBeInstanceOf(AggregateError)
      expect(formatProcessErrorForTest(result.cause)).toContain(
        kind === "read" ? "synthetic controllerStatus read failed" : "synthetic controllerStatus authentication failed",
      )
      expect(formatProcessErrorForTest(result.cause)).toContain("synthetic controller recovery abort write failed")
      expect(result.fallback).toBe(true)
      expect(result.controllerGone).toBe(true)
    }
  })

  test("portable persistent controllerStatus failures use nonce discovery without spending finalization reserve", async () => {
    if (process.platform === "win32") return
    for (const kind of ["read", "authenticate"] as const) {
      const result = await portablePersistentControllerRecoveryForTest(kind)
      expect(result.cause).toBeInstanceOf(AggregateError)
      expect(formatProcessErrorForTest(result.cause)).toContain(
        kind === "read" ? "synthetic controllerStatus read failed" : "synthetic controllerStatus authentication failed",
      )
      expect(formatProcessErrorForTest(result.cause)).toContain("synthetic controller recovery abort write failed")
      expect(result.attempts).toBeGreaterThan(1)
      expect(result.fallback).toBe(true)
      expect(result.controllerGone).toBe(true)
      expect(result.durationMs).toBeLessThan(3_000)
    }
  })

  test("portable private-nonce controller recovery ignores ordinary controller descendants", async () => {
    if (process.platform === "win32") return
    for (const kind of ["read", "authenticate"] as const) {
      const result = await portableControllerDescendantPersistentRecoveryForTest(kind)
      expect(result.cause).toBeInstanceOf(AggregateError)
      expect(formatProcessErrorForTest(result.cause)).toContain(
        kind === "read" ? "synthetic controllerStatus read failed" : "synthetic controllerStatus authentication failed",
      )
      expect(formatProcessErrorForTest(result.cause)).toContain("synthetic controller recovery abort write failed")
      expect(result.recoveredController).toBe(true)
      expect(result.fallback).toBe(true)
      expect(result.controllerGone).toBe(true)
      expect(result.descendantGone).toBe(true)
      expect(result.durationMs).toBeLessThan(3_000)
    }
  })

  test("portable private-nonce authority rejects a second unrelated root", () => {
    expect(portableControllerAuthorityForTest()).toEqual({ descendantAuthority: 42, forgeryAuthority: undefined })
  })

  test("portable controllerStatus retries accept a late authenticated status before cleanup escalation", async () => {
    if (process.platform === "win32") return
    for (const kind of ["read", "authenticate"] as const) {
      const result = await portableLateControllerRecoveryForTest(kind)
      expect(result.cause).toBeInstanceOf(AggregateError)
      expect(result.attempts).toBe(4)
      expect(result.fallback).toBe(true)
      expect(result.controllerGone).toBe(true)
      expect(result.durationMs).toBeLessThan(3_000)
    }
  })

  test("portable readiness tolerates concurrent partial publication", async () => {
    if (process.platform === "win32") return
    await expect(portableReadinessPartialPublicationForTest()).resolves.toEqual([3, 3])
  })

  test("portable production spawn preserves short-lived stdout, stderr, and exit status", async () => {
    if (process.platform === "win32") return
    for (const exitCode of [0, 23]) {
      await expect(portableShortLivedProcessForTest(exitCode)).resolves.toEqual({
        exitCode,
        stdout: "short stdout\n",
        stderr: "short stderr\n",
      })
    }
  })

  test("portable signalling reserves TERM and KILL for known targets while discovery probes stall", async () => {
    await expect(portableSignalDiscoveryBudgetForTest()).resolves.toEqual([
      "SIGTERM",
      "discovery",
      "SIGKILL",
      "discovery",
    ])
  })

  test("portable cleanup bounds reconciliation before reserving containment phases", () => {
    expect(portableCleanupReserveForTest(10_000, 5_000)).toEqual({
      observationDeadline: 5_250,
      containmentReserve: 3_000,
      termReserve: 500,
      killReserve: 500,
      reapReserve: 1_000,
      descendantReserve: 1_000,
    })
  })

  test("production finalizer signals a known Darwin target before broad slow probes", async () => {
    const result = await darwinFinalizerDiscoveryForTest("slow-probes")
    const firstProbe = result.events.indexOf("probe:1")
    expect(result.events.indexOf("SIGTERM:42")).toBeLessThan(firstProbe)
    expect(result.events.indexOf("SIGKILL:42")).toBeLessThan(firstProbe)
    expect(result.events.indexOf("child.reaped")).toBeLessThan(firstProbe)
  })

  test("production finalizer keeps partial Darwin PID slices non-quiescent until a later nonce descendant is killed", async () => {
    const result = await darwinFinalizerDiscoveryForTest("partial-prefix")
    expect(result.events).toContain("slice:5")
    expect(result.events).toContain("SIGKILL:77")
    expect(result.events.indexOf("SIGKILL:77")).toBeGreaterThan(result.events.indexOf("slice:5"))
  })

  test("production finalizer moves from a confirmed Darwin root exit to descendants", async () => {
    const result = await darwinFinalizerDiscoveryForTest("exited-root")
    expect(result.events).not.toContain("SIGTERM:42")
    expect(result.events).not.toContain("SIGKILL:42")
    expect(result.events).toContain("slice:5")
    expect(result.events).toContain("SIGKILL:77")
    expect(result.events.indexOf("child.reaped")).toBeLessThan(result.events.indexOf("slice:1"))
  })

  test("production finalizer resumes Darwin discovery beyond the former KILL cap", async () => {
    const result = await darwinFinalizerDiscoveryForTest("long-prefix")
    expect(result.scans).toBeGreaterThan(6)
    expect(result.events).toContain("slice:8")
    expect(result.events).toContain("SIGKILL:77")
  })

  test("Darwin tracker retains and reaps an uncertain admitted child through cleanup", async () => {
    if (process.platform !== "darwin") return
    const result = await darwinFinalizerDiscoveryForTest("initial-observation-failure")
    expect(result.scans).toBeGreaterThan(2)
    expect(result.rootPresentAtAdmission).toBe(true)
    expect(result.processAdmitted).toBe(true)
    expect(result.processRetainedAfterUncertainty).toBe(true)
    expect(result.uncertaintyObserved).toBe(true)
    expect(result.exitCode).toBeNumber()
    expect(result.cause).toBeInstanceOf(AggregateError)
    expect(formatProcessErrorForTest(result.cause)).toContain(
      "failed to recheck macOS process",
    )
  })

  test("Linux group drain reads stat without inspecting unrelated executables", () => {
    const stat = (pid: number, group: number, state = "S") =>
      `${pid} (synthetic process) ${state} 1 ${group} ${"0 ".repeat(16)}123`
    const foreign = stat(41, 900)
    const member = stat(42, 800)
    expect(portableLinuxGroupDrainedForTest(800, { 41: foreign })).toBe(true)
    expect(portableLinuxGroupDrainedForTest(800, { 41: foreign, 42: member })).toBe(false)
    expect(portableLinuxGroupDrainedForTest(800, { 41: foreign, 42: stat(42, 800, "Z") })).toBe(true)
    const denied = Object.assign(new Error("foreign /proc stat is inaccessible"), { code: "EACCES" })
    expect(portableLinuxGroupDrainedForTest(800, { 41: denied }, false)).toBe(true)
    expect(portableLinuxGroupDrainedForTest(800, { 41: denied }, true)).toBe(false)
  })

  test("broad Darwin discovery skips an opaque PID before it has ownership evidence", async () => {
    await expect(darwinBroadPreOwnershipObservationForTest()).resolves.toBeUndefined()
  })

  test("Darwin nonce-confirmed observation uncertainty fails closed", async () => {
    await expect(darwinBroadPreOwnershipObservationForTest(true)).rejects.toThrow(
      "failed to recheck macOS process 42 after initial native observation",
    )
  })

  test("later opaque Darwin observations skip foreign PIDs and fail closed after ownership", async () => {
    await expect(darwinBroadPreOwnershipObservationForTest(false, "arguments-recheck")).resolves.toBeUndefined()
    for (const observation of ["arguments-recheck", "final"] as const) {
      await expect(darwinBroadPreOwnershipObservationForTest(true, observation)).rejects.toThrow(
        `opaque ${observation} observation`,
      )
    }
    // The final recheck runs after the procargs record has admitted this PID
    // by nonce, even though it began as a broad pre-ownership candidate.
    await expect(darwinBroadPreOwnershipObservationForTest(false, "final")).rejects.toThrow(
      "opaque final observation",
    )
  })

  test("owned exit observation getter and then failures still reap through the production finalizer", async () => {
    for (const kind of ["getter", "then"] as const) {
      const result = await ownedExitObservationFailureForTest(kind)
      expect(result.stopped).toBe(true)
      expect(result.terminated).toBe(true)
      expect(result.cause).toBeInstanceOf(AggregateError)
      expect(formatProcessErrorForTest(result.cause)).toContain(
        kind === "getter" ? "synthetic exited getter failed" : "synthetic exited then attachment failed",
      )
    }
  })

  test("termination retains a direct-child exit observation failure that arrives during cleanup", async () => {
    const result = await lateExitObservationFailureForTest()
    expect(result.stopped).toBe(true)
    expect(result.terminated).toBe(true)
    expect(result.cause).toBeInstanceOf(AggregateError)
    expect(formatProcessErrorForTest(result.cause)).toContain("synthetic late exit observation rejected")
  })

  test("ACP cleanup escalates after stdin close and initial exit observation failures", async () => {
    const result = await acpCleanupFailuresForTest()
    expect(result.stopped).toBe(true)
    expect(result.terminated).toBe(true)
    expect(result.cause).toBeInstanceOf(AggregateError)
    const formatted = formatProcessErrorForTest(result.cause)
    expect(formatted).toContain("synthetic ACP stdin end failed")
    expect(formatted).toContain("synthetic ACP exit observation rejected")
  })

  test("helper drains verbose probe stdout before status publication", async () => {
    if (process.platform === "win32") return
    const result = await commandResultForTest(["/bin/sh", "-c", "yes x | head -c 262144"], 2_000)
    expect(result.exitCode).toBe(0)
    expect(result.stdout.length).toBe(262_144)
  })

  test("helper timeout reaps a descendant after its shell root has exited", async () => {
    if (process.platform === "win32") return
    const started = Date.now()
    const pidFile = `${os.tmpdir()}/opencode-helper-descendant-${crypto.randomUUID()}`
    try {
      await expect(
        commandResultForTest(
          ["/bin/sh", "-c", 'sleep 30 & child=$!; printf "%s" "$child" > "$1"; exit 0', "--", pidFile],
          250,
        ),
      ).rejects.toThrow("did not exit before the cleanup deadline")
      const pid = Number((await Bun.file(pidFile).text()).trim())
      expect(Number.isSafeInteger(pid)).toBe(true)
      expect(pid).toBeGreaterThan(0)
      const deadline = Date.now() + 1_000
      const waitForDescendantExit = async (): Promise<void> => {
        // A container init can briefly retain a reaped child as a zombie.
        // It has no stdout writer and cannot survive the cleanup boundary.
        if (process.platform === "linux") {
          const stat = await Bun.file(`/proc/${pid}/stat`)
            .text()
            .catch((cause) => {
              if (typeof cause === "object" && cause !== null && "code" in cause && cause.code === "ENOENT") return
              throw cause
            })
          if (stat === undefined || stat.slice(stat.lastIndexOf(")") + 2).startsWith("Z ")) return
        } else {
          try {
            process.kill(pid, 0)
          } catch (cause) {
            if (typeof cause === "object" && cause !== null && "code" in cause && cause.code === "ESRCH") return
            throw cause
          }
        }
        if (Date.now() >= deadline) throw new Error(`helper descendant ${pid} remained after timeout cleanup`)
        await Bun.sleep(20)
        return waitForDescendantExit()
      }
      await waitForDescendantExit()
      expect(Date.now() - started).toBeLessThan(2_000)
    } finally {
      if (existsSync(pidFile)) unlinkSync(pidFile)
    }
  })

  test("helper finalization cannot overrun its absolute observation deadline", async () => {
    if (process.platform === "win32") return
    const started = Date.now()
    await expect(commandResultForTest(["/bin/sh", "-c", "sleep 30"], 100)).rejects.toThrow(
      "did not exit before the cleanup deadline",
    )
    // The operation deadline remains short, but owned abort/ack/reap has its
    // own bounded reserve after it so the helper cannot be orphaned.
    expect(Date.now() - started).toBeGreaterThanOrEqual(100)
    expect(Date.now() - started).toBeLessThan(1_500)
  })

  test("helper retains abort controls when acknowledgement cannot be observed during its cleanup reserve", async () => {
    if (process.platform === "win32") return
    const result = await commandCleanupReserveForTest()
    expect(result.cause).toBeInstanceOf(AggregateError)
    expect(formatProcessErrorForTest(result.cause)).toContain("did not acknowledge abort")
    expect(formatProcessErrorForTest(result.cause)).toContain("controls were retained")
    expect(result.retained).toBe(true)
    expect(result.durationMs).toBeGreaterThanOrEqual(900)
    expect(result.durationMs).toBeLessThan(1_750)
  })

  test("Linux cgroup discovery stops at its absolute slice and preserves escalation time", async () => {
    if (process.platform !== "linux") return
    await expect(linuxProcDiscoveryDeadlineForTest()).resolves.toMatchObject({ snapshots: [], complete: false })
    const result = await linuxCgroupDiscoveryDeadlineForTest()
    expect(result.memberInspections).toBe(64)
    expect(result.missingMemberInspections).toBe(3)
    expect(result.complete).toBe(false)
    expect(result.escalationRemaining).toBe(700)
  })

  test("Linux aborted cgroup cleanup stops large membership reads at its absolute deadline", async () => {
    if (process.platform !== "linux") return
    await expect(abortedCgroupMembershipDeadlineForTest()).resolves.toEqual({
      membershipReads: 1,
      snapshotReads: 5,
      containmentReads: 4,
      readsAfterDeadline: 0,
      signals: 2,
      elapsed: 100,
    })
  })

  cliIt.live(
    "cleans up a normal separately grouped tool descendant",
    ({ home, llm, opencode }) =>
      Effect.gen(function* () {
        // This is intentionally Linux-gated: `setsid` is not part of the
        // portable contract. It proves the production cleanup path captures
        // an ordinary inherited-environment descendant before it leaves the
        // tool shell's process group. A child that clears the tracking nonce
        // is deliberately unsupported by the portable fallback and must fail
        // cleanup rather than being claimed as safely owned.
        if (process.platform !== "linux") return
        const pidFile = `${home}/tool-descendant.pid`
        yield* llm.tool("bash", {
          command: `setsid sh -c 'trap "" TERM; while :; do sleep 1; done' </dev/null >/dev/null 2>&1 & child=$!; echo $child > '${pidFile}'; sleep 0.2`,
          description: "Launch a separately grouped cleanup probe",
        })
        yield* llm.text("cleanup scheduled")
        const result = yield* opencode.run("launch cleanup probe", {
          extraArgs: ["--dangerously-skip-permissions"],
        })
        opencode.expectExit(result, 0)
        const pid = Number((yield* Effect.promise(() => Bun.file(pidFile).text())).trim())
        expect(Number.isSafeInteger(pid)).toBe(true)
        expect(pid).toBeGreaterThan(0)
        const waitForExit = (): Effect.Effect<void> =>
          Effect.gen(function* () {
            // A container init can briefly retain a reaped child as a zombie.
            // It has no stdout writer and cannot survive the cleanup boundary.
            const stat = yield* Effect.promise(() =>
              Bun.file(`/proc/${pid}/stat`)
                .text()
                .catch((cause) => {
                  if (typeof cause === "object" && cause !== null && "code" in cause && cause.code === "ENOENT") return
                  throw cause
                }),
            )
            if (stat === undefined || stat.slice(stat.lastIndexOf(")") + 2).startsWith("Z ")) return
            yield* Effect.sleep("50 millis")
            yield* waitForExit()
          })
        yield* waitForExit().pipe(Effect.timeout("5 seconds"))
      }),
    45_000,
  )
})
