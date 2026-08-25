import { test, expect, describe } from "bun:test"
import { Effect } from "effect"
import path from "path"
import fs from "fs/promises"
import { Filesystem } from "../../src/util"
import { File } from "../../src/file"
import { Instance } from "../../src/project/instance"
import { provideInstance, tmpdir } from "../fixture/fixture"

const run = <A, E>(eff: Effect.Effect<A, E, File.Service>) =>
  Effect.runPromise(provideInstance(Instance.directory)(eff.pipe(Effect.provide(File.defaultLayer))))
const read = (file: string) => run(File.Service.use((svc) => svc.read(file)))
const list = (dir?: string) => run(File.Service.use((svc) => svc.list(dir)))

describe("Filesystem.contains", () => {
  test("allows paths within project", () => {
    expect(Filesystem.contains("/project", "/project/src")).toBe(true)
    expect(Filesystem.contains("/project", "/project/src/file.ts")).toBe(true)
    expect(Filesystem.contains("/project", "/project")).toBe(true)
  })

  test("blocks ../ traversal", () => {
    expect(Filesystem.contains("/project", "/project/../etc")).toBe(false)
    expect(Filesystem.contains("/project", "/project/src/../../etc")).toBe(false)
    expect(Filesystem.contains("/project", "/etc/passwd")).toBe(false)
  })

  test("blocks absolute paths outside project", () => {
    expect(Filesystem.contains("/project", "/etc/passwd")).toBe(false)
    expect(Filesystem.contains("/project", "/tmp/file")).toBe(false)
    expect(Filesystem.contains("/home/user/project", "/home/user/other")).toBe(false)
  })

  test("handles prefix collision edge cases", () => {
    expect(Filesystem.contains("/project", "/project-other/file")).toBe(false)
    expect(Filesystem.contains("/project", "/projectfile")).toBe(false)
  })

  test.skipIf(process.platform !== "win32")("blocks paths on a different Windows drive", () => {
    expect(Filesystem.contains("C:\\project", "D:\\project\\file.ts")).toBe(false)
  })
})

/*
 * Integration tests for read() and list() path traversal protection.
 *
 * These tests verify the HTTP API code path is protected. The HTTP endpoints
 * in server.ts (GET /file/content, GET /file) call read()/list()
 * directly - they do NOT go through ReadTool or the agent permission layer.
 *
 * This is a SEPARATE code path from ReadTool, which has its own checks.
 */
describe("File.read path traversal protection", () => {
  test("rejects ../ traversal attempting to read /etc/passwd", async () => {
    await using tmp = await tmpdir({
      init: async (dir) => {
        await Bun.write(path.join(dir, "allowed.txt"), "allowed content")
      },
    })

    await Instance.provide({
      directory: tmp.path,
      fn: async () => {
        await expect(read("../../../etc/passwd")).rejects.toThrow("Access denied: path escapes project directory")
      },
    })
  })

  test("rejects deeply nested traversal", async () => {
    await using tmp = await tmpdir()

    await Instance.provide({
      directory: tmp.path,
      fn: async () => {
        await expect(read("src/nested/../../../../../../../etc/passwd")).rejects.toThrow(
          "Access denied: path escapes project directory",
        )
      },
    })
  })

  test("allows valid paths within project", async () => {
    await using tmp = await tmpdir({
      init: async (dir) => {
        await Bun.write(path.join(dir, "valid.txt"), "valid content")
      },
    })

    await Instance.provide({
      directory: tmp.path,
      fn: async () => {
        const result = await read("valid.txt")
        expect(result.content).toBe("valid content")
      },
    })
  })
})

describe("File.list path traversal protection", () => {
  test("rejects ../ traversal attempting to list /etc", async () => {
    await using tmp = await tmpdir()

    await Instance.provide({
      directory: tmp.path,
      fn: async () => {
        await expect(list("../../../etc")).rejects.toThrow("Access denied: path escapes project directory")
      },
    })
  })

  test("allows valid subdirectory listing", async () => {
    await using tmp = await tmpdir({
      init: async (dir) => {
        await Bun.write(path.join(dir, "subdir", "file.txt"), "content")
      },
    })

    await Instance.provide({
      directory: tmp.path,
      fn: async () => {
        const result = await list("subdir")
        expect(Array.isArray(result)).toBe(true)
      },
    })
  })
})

describe("Instance.containsPath", () => {
  test("returns true for path inside directory", async () => {
    await using tmp = await tmpdir({ git: true })

    await Instance.provide({
      directory: tmp.path,
      fn: () => {
        expect(Instance.containsPath(path.join(tmp.path, "foo.txt"))).toBe(true)
        expect(Instance.containsPath(path.join(tmp.path, "src", "file.ts"))).toBe(true)
      },
    })
  })

  test("returns true for path inside worktree but outside directory (monorepo subdirectory scenario)", async () => {
    await using tmp = await tmpdir({ git: true })
    const subdir = path.join(tmp.path, "packages", "lib")
    await fs.mkdir(subdir, { recursive: true })

    await Instance.provide({
      directory: subdir,
      fn: () => {
        // .opencode at worktree root, but we're running from packages/lib
        expect(Instance.containsPath(path.join(tmp.path, ".opencode", "state"))).toBe(true)
        // sibling package should also be accessible
        expect(Instance.containsPath(path.join(tmp.path, "packages", "other", "file.ts"))).toBe(true)
        // worktree root itself
        expect(Instance.containsPath(tmp.path)).toBe(true)
      },
    })
  })

  test("returns false for path outside both directory and worktree", async () => {
    await using tmp = await tmpdir({ git: true })

    await Instance.provide({
      directory: tmp.path,
      fn: () => {
        expect(Instance.containsPath("/etc/passwd")).toBe(false)
        expect(Instance.containsPath("/tmp/other-project")).toBe(false)
      },
    })
  })

  test("returns false for path with .. escaping worktree", async () => {
    await using tmp = await tmpdir({ git: true })

    await Instance.provide({
      directory: tmp.path,
      fn: () => {
        expect(Instance.containsPath(path.join(tmp.path, "..", "escape.txt"))).toBe(false)
      },
    })
  })

  test("handles directory === worktree (running from repo root)", async () => {
    await using tmp = await tmpdir({ git: true })

    await Instance.provide({
      directory: tmp.path,
      fn: () => {
        expect(Instance.directory).toBe(Instance.worktree)
        expect(Instance.containsPath(path.join(tmp.path, "file.txt"))).toBe(true)
        expect(Instance.containsPath("/etc/passwd")).toBe(false)
      },
    })
  })

  test("filesystem root worktrees do not allow fallback containment", async () => {
    await using tmp = await tmpdir()

    await Instance.provide({
      directory: tmp.path,
      fn: () => {
        const ctx = { ...Instance.current, worktree: "/" }
        expect(Instance.containsPath(path.join(tmp.path, "file.txt"), ctx)).toBe(true)
        expect(Instance.containsPath("/etc/passwd", ctx)).toBe(false)
        expect(Instance.containsPath("/tmp/other", ctx)).toBe(false)

        const roots = [
          ["/", "/root-owned/file.txt"],
          ["C:\\", "C:\\outside\\file.txt"],
          ["D:/", "D:/outside/file.txt"],
          ["\\\\server\\share", "\\\\server\\share\\outside\\file.txt"],
          ["\\\\server\\share\\", "\\\\server\\share\\outside\\file.txt"],
          ["//server/share/", "//server/share/outside/file.txt"],
        ]
        for (const [worktree, filepath] of roots) {
          expect(Instance.containsPath(filepath, { ...ctx, worktree })).toBe(false)
        }
      },
    })
  })

  test.skipIf(process.platform !== "win32")("enforces Windows volume and share boundaries", async () => {
    await using tmp = await tmpdir()

    await Instance.provide({
      directory: tmp.path,
      fn: () => {
        const drive = {
          ...Instance.current,
          directory: "C:\\repo\\packages\\app",
          worktree: "C:\\repo",
        }
        expect(Instance.containsPath("C:\\repo\\packages\\app\\file.ts", drive)).toBe(true)
        expect(Instance.containsPath("C:\\repo\\packages\\other\\file.ts", drive)).toBe(true)
        expect(Instance.containsPath("D:\\repo\\packages\\app\\file.ts", drive)).toBe(false)
        expect(Instance.containsPath("\\\\server\\share\\repo\\file.ts", drive)).toBe(false)

        const unc = {
          ...Instance.current,
          directory: "\\\\server\\share\\repo\\packages\\app",
          worktree: "\\\\server\\share\\repo",
        }
        expect(Instance.containsPath("\\\\server\\share\\repo\\packages\\app\\file.ts", unc)).toBe(true)
        expect(Instance.containsPath("\\\\server\\share\\repo\\packages\\other\\file.ts", unc)).toBe(true)
        expect(Instance.containsPath("\\\\server\\other\\repo\\file.ts", unc)).toBe(false)
        expect(Instance.containsPath("\\\\other-server\\share\\repo\\file.ts", unc)).toBe(false)
        expect(Instance.containsPath("D:\\repo\\file.ts", unc)).toBe(false)
      },
    })
  })
})
