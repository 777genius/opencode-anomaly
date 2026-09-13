import { describe, expect, beforeAll, afterAll } from "bun:test"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Cause, Context, Deferred, Effect, Exit, Fiber, Layer, Logger, PlatformError } from "effect"
import { NodePath } from "@effect/platform-node"
import { EffectFlock } from "@opencode-ai/core/util/effect-flock"
import { SkillPublication } from "../../src/skill/publication"
import { tmpdirScoped } from "../fixture/fixture"
import { Discovery } from "../../src/skill/discovery"
import { Global } from "@opencode-ai/core/global"
import { Filesystem } from "@/util/filesystem"
import fs from "node:fs"
import { readFile, rm, stat, symlink } from "fs/promises"
import path from "path"
import { testEffect } from "../lib/effect"

let CLOUDFLARE_SKILLS_URL: string
let server: ReturnType<typeof Bun.serve>
let downloadCount = 0
let mutableVersion = "1"
let mutableContent = "# Old"
let mutableDownloadCount = 0
let mutableFiles = ["SKILL.md"]

const fixturePath = path.join(import.meta.dir, "../fixture/skills")
const generationDir = path.join(Global.Path.cache, "skill-generations")
const cacheDir = path.join(Global.Path.cache, "skills")
const it = testEffect(
  LayerNode.compile(LayerNode.group([Discovery.node, FSUtil.node, EffectFlock.node, CrossSpawnSpawner.node])),
)

// Recompiling keeps Discovery's underlying layer identity, so the test's memo map
// would reuse its captured dependencies. Build fresh in the test scope and select
// the service from that build's context, keeping its resources alive for each pull.
const actualDiscovery = (layer = LayerNode.compile(Discovery.node)) =>
  Layer.build(Layer.fresh(layer)).pipe(Effect.map((context) => Context.get(context, Discovery.Service)))

beforeAll(async () => {
  await rm(cacheDir, { recursive: true, force: true })
  await rm(generationDir, { recursive: true, force: true })

  server = Bun.serve({
    port: 0,
    async fetch(req) {
      const url = new URL(req.url)

      if (url.pathname === "/mutable/index.json") {
        return Response.json({ skills: [{ name: "mutable", version: mutableVersion, files: mutableFiles }] })
      }
      if (url.pathname === "/mutable/mutable/SKILL.md") {
        mutableDownloadCount++
        return new Response(mutableContent)
      }
      if (url.pathname === "/mutable/mutable/old.md") return new Response("old reference")

      // route /.well-known/skills/* to the fixture directory
      if (url.pathname.startsWith("/.well-known/skills/")) {
        const filePath = url.pathname.replace("/.well-known/skills/", "")
        const fullPath = path.join(fixturePath, filePath)

        if (await Filesystem.exists(fullPath)) {
          if (!fullPath.endsWith("index.json")) {
            downloadCount++
          }
          return new Response(Bun.file(fullPath))
        }
      }

      return new Response("Not Found", { status: 404 })
    },
  })

  CLOUDFLARE_SKILLS_URL = `http://localhost:${server.port}/.well-known/skills/`
})

afterAll(async () => {
  void server?.stop()
  await rm(cacheDir, { recursive: true, force: true })
  await rm(generationDir, { recursive: true, force: true })
})

describe("Discovery.pull", () => {
  // Only Discovery is exposed; Path and FSUtil remain private to its construction.
  testEffect(LayerNode.compile(Discovery.node)).live("pull uses captured services without caller Path or FSUtil", () =>
    Effect.gen(function* () {
      const discovery = yield* Discovery.Service
      const dirs = yield* discovery.pull(CLOUDFLARE_SKILLS_URL)
      expect(dirs.length).toBeGreaterThan(0)
      expect(
        yield* Effect.forEach(dirs, (dir) => Effect.promise(() => readFile(path.join(dir, "SKILL.md"), "utf8"))),
      ).toEqual(dirs.map(() => expect.any(String)))
    }),
  )

  it.live("downloads skills from cloudflare url", () =>
    Effect.gen(function* () {
      const fsys = yield* FSUtil.Service
      const discovery = yield* Discovery.Service
      const dirs = yield* discovery.pull(CLOUDFLARE_SKILLS_URL)
      expect(dirs.length).toBeGreaterThan(0)
      for (const dir of dirs) {
        expect(dir).toStartWith(cacheDir)
        const md = path.join(dir, "SKILL.md")
        expect(yield* fsys.existsSafe(md)).toBe(true)
      }
    }),
  )

  it.live("url without trailing slash works", () =>
    Effect.gen(function* () {
      const fsys = yield* FSUtil.Service
      const discovery = yield* Discovery.Service
      const dirs = yield* discovery.pull(CLOUDFLARE_SKILLS_URL.replace(/\/$/, ""))
      expect(dirs.length).toBeGreaterThan(0)
      for (const dir of dirs) {
        const md = path.join(dir, "SKILL.md")
        expect(yield* fsys.existsSafe(md)).toBe(true)
      }
    }),
  )

  it.live("returns empty array for invalid url", () =>
    Effect.gen(function* () {
      const discovery = yield* Discovery.Service
      const dirs = yield* discovery.pull(`http://localhost:${server.port}/invalid-url/`)
      expect(dirs).toEqual([])
    }),
  )

  it.live("returns empty array for non-json response", () =>
    Effect.gen(function* () {
      // any url not explicitly handled in server returns 404 text "Not Found"
      const discovery = yield* Discovery.Service
      const dirs = yield* discovery.pull(`http://localhost:${server.port}/some-other-path/`)
      expect(dirs).toEqual([])
    }),
  )

  it.live("downloads reference files alongside SKILL.md", () =>
    Effect.gen(function* () {
      const fsys = yield* FSUtil.Service
      const discovery = yield* Discovery.Service
      const dirs = yield* discovery.pull(CLOUDFLARE_SKILLS_URL)
      // find a skill dir that should have reference files (e.g. agents-sdk)
      const agentsSdk = dirs.find((d) => d.endsWith(path.sep + "agents-sdk"))
      expect(agentsSdk).toBeDefined()
      if (agentsSdk) {
        const refs = path.join(agentsSdk, "references")
        expect(yield* fsys.existsSafe(path.join(agentsSdk, "SKILL.md"))).toBe(true)
        // agents-sdk has reference files per the index
        const refDir = yield* Effect.promise(() =>
          Array.fromAsync(new Bun.Glob("**/*.md").scan({ cwd: refs, onlyFiles: true })),
        )
        expect(refDir.length).toBeGreaterThan(0)
      }
    }),
  )

  it.live("caches downloaded files on second pull", () =>
    Effect.gen(function* () {
      // clear dir and downloadCount
      yield* Effect.promise(() => rm(cacheDir, { recursive: true, force: true }))
      yield* Effect.promise(() => rm(generationDir, { recursive: true, force: true }))
      downloadCount = 0
      const discovery = yield* Discovery.Service

      // first pull to populate cache
      const first = yield* discovery.pull(CLOUDFLARE_SKILLS_URL)
      expect(first.length).toBeGreaterThan(0)
      const firstCount = downloadCount
      expect(firstCount).toBeGreaterThan(0)

      // second pull should return same results from cache
      const second = yield* discovery.pull(CLOUDFLARE_SKILLS_URL)
      expect(second.length).toBe(first.length)
      expect(second.sort()).toEqual(first.sort())

      // second pull should NOT increment download count
      expect(downloadCount).toBe(firstCount)
    }),
  )

  it.live("refreshes a remote skill when its version changes", () =>
    Effect.gen(function* () {
      yield* Effect.promise(() => rm(cacheDir, { recursive: true, force: true }))
      yield* Effect.promise(() => rm(generationDir, { recursive: true, force: true }))
      mutableVersion = "1"
      mutableContent = "# Old"
      mutableDownloadCount = 0
      mutableFiles = ["SKILL.md", "old.md"]
      const discovery = yield* Discovery.Service
      const url = `http://localhost:${server.port}/mutable/`

      const first = yield* discovery.pull(url)
      expect(yield* Effect.promise(() => Bun.file(path.join(first[0], "SKILL.md")).text())).toBe("# Old")

      mutableVersion = "2"
      mutableContent = "# Partial"
      mutableFiles = ["SKILL.md", "missing.md"]
      const second = yield* discovery.pull(url)
      expect(yield* Effect.promise(() => Bun.file(path.join(second[0], "SKILL.md")).text())).toBe("# Old")
      expect(yield* Effect.promise(() => Bun.file(path.join(second[0], "old.md")).text())).toBe("old reference")

      expect(second).toEqual(first)
      expect(yield* Effect.promise(() => Bun.file(path.join(first[0], ".opencode-version")).text())).toBe("1")

      mutableVersion = "3"
      mutableContent = "# New"
      mutableFiles = ["SKILL.md"]
      const third = yield* discovery.pull(url)
      // Inspect the newly returned generation with both readers on Windows.
      const observation = yield* Effect.promise(async () => {
        const file = Bun.file(path.join(third[0], "SKILL.md"))
        const reads = await Promise.allSettled([
          readFile(path.join(third[0], ".opencode-version"), "utf8"),
          readFile(path.join(third[0], "SKILL.md"), "utf8"),
          stat(path.join(third[0], "SKILL.md")),
          file.text(),
        ])
        return {
          marker: "mutable-skill-v3",
          version: reads[0].status === "fulfilled" ? reads[0].value : null,
          native: reads[1].status === "fulfilled" ? (reads[1].value === "# New" ? "new" : reads[1].value) : null,
          bun: reads[3].status === "fulfilled" ? (reads[3].value === "# New" ? "new" : reads[3].value) : null,
          errors: reads.map((result) => (result.status === "fulfilled" ? "none" : String(result.reason))),
          downloads: mutableDownloadCount,
          directories: third,
          reads: reads.map((result) =>
            result.status === "fulfilled"
              ? { status: result.status, value: result.value }
              : { status: result.status, error: String(result.reason) },
          ),
          bunMetadata: { size: file.size, lastModified: file.lastModified },
        }
      })
      if (process.env.OPENCODE_WINDOWS_UNIT_DIAGNOSTICS === "1") {
        fs.writeSync(2, "mutable-skill-v3 " + JSON.stringify(observation) + "\n")
      }
      expect(yield* Effect.promise(() => Bun.file(path.join(third[0], "SKILL.md")).text())).toBe("# New")
      expect(yield* Effect.promise(() => Bun.file(path.join(third[0], "old.md")).exists())).toBe(false)
      expect(mutableDownloadCount).toBe(3)

      expect(third[0]).not.toBe(first[0])
      expect(yield* Effect.promise(() => Bun.file(path.join(third[0], ".opencode-version")).text())).toBe("3")
      expect(yield* Effect.promise(() => readFile(path.join(third[0], "SKILL.md"), "utf8"))).toBe("# New")
      expect(yield* Effect.promise(() => Bun.file(path.join(first[0], ".opencode-version")).text())).toBe("1")
      expect(yield* Effect.promise(() => Bun.file(path.join(first[0], "SKILL.md")).text())).toBe("# Old")
      expect(yield* Effect.promise(() => Bun.file(path.join(first[0], "old.md")).text())).toBe("old reference")
      expect(yield* discovery.pull(url)).toEqual(third)
      expect(mutableDownloadCount).toBe(3)

      mutableVersion = "4"
      mutableFiles = ["SKILL.md", "missing.md"]
      expect(yield* discovery.pull(url)).toEqual(third)
      const fresh = yield* actualDiscovery()
      expect(fresh).not.toBe(discovery)
      expect(yield* fresh.pull(url)).toEqual(third)
      expect(yield* Effect.promise(() => Bun.file(path.join(third[0], "SKILL.md")).text())).toBe("# New")
    }),
  )
})

// Exercise the production publication boundary with scoped filesystem overrides.
const publicationFixture = Effect.gen(function* () {
  const directory = yield* tmpdirScoped()
  const root = path.join(directory, "skills", "fixture")
  const store = path.join(directory, "skill-generations", "fixture")
  const fsys = yield* FSUtil.Service
  const publication = yield* SkillPublication.make(root, store).pipe(Effect.provide(NodePath.layer))
  const write = (content: string) => (staging: string) => fsys.writeFileString(path.join(staging, "SKILL.md"), content)
  const first = yield* publication.refresh("old/opaque:version", write("old"))
  return { root, store, fsys, publication, write, first }
})

for (const boundary of [
  "download",
  "marker read",
  "cached stat",
  "staging stat",
  "staging IO",
  "version",
  "finalize",
  "selection",
] as const) {
  it.live(`publication preserves the original ${boundary} failure and old selection`, () =>
    Effect.gen(function* () {
      const fixture = yield* publicationFixture
      const cause = Object.assign(new Error(`${boundary} failed`), {
        code: boundary === "staging IO" ? "EIO" : "EACCES",
      })
      const error = new PlatformError.PlatformError(
        new PlatformError.SystemError({
          _tag: boundary === "staging IO" ? "Unknown" : "PermissionDenied",
          module: "FileSystem",
          method: boundary,
          cause,
        }),
      )
      const foreign = path.join(fixture.store, "tmp-another-owner")
      yield* fixture.fsys.makeDirectory(foreign)
      const publication = yield* SkillPublication.make(fixture.root, fixture.store).pipe(
        Effect.provide(NodePath.layer),
        Effect.provideService(FSUtil.Service, {
          ...fixture.fsys,
          readFileString: (file, encoding) =>
            boundary === "marker read" && file === path.join(fixture.first, ".opencode-version")
              ? Effect.fail(error)
              : fixture.fsys.readFileString(file, encoding),
          stat: (file) =>
            ((boundary === "staging stat" || boundary === "staging IO") &&
              path.basename(path.dirname(file)).startsWith("tmp-")) ||
            (boundary === "cached stat" && file === path.join(fixture.first, "SKILL.md"))
              ? Effect.fail(error)
              : fixture.fsys.stat(file),
          rename: (from, to) => {
            expect(from).not.toBe(fixture.first)
            if (
              (boundary === "finalize" && path.basename(to).startsWith("g-")) ||
              (boundary === "selection" && path.basename(to) === "current")
            )
              return Effect.fail(error)
            return fixture.fsys.rename(from, to)
          },
          writeFileString: (file, content, options) =>
            boundary === "version" && path.basename(file) === ".opencode-version"
              ? Effect.fail(error)
              : fixture.fsys.writeFileString(file, content, options),
          // A cleanup failure must never replace the publication failure.
          remove: () =>
            Effect.fail(
              new PlatformError.PlatformError(
                new PlatformError.SystemError({
                  _tag: "PermissionDenied",
                  module: "FileSystem",
                  method: "cleanup",
                }),
              ),
            ),
        }),
      )
      const before = yield* fixture.fsys.readDirectory(fixture.store)
      const writes: string[] = []
      const exit = yield* publication
        .refresh(
          boundary === "cached stat" ? "old/opaque:version" : "next",
          boundary === "download"
            ? () => Effect.fail(error)
            : (staging) => {
                writes.push(staging)
                return fixture.write("new")(staging)
              },
        )
        .pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBe(error)
      if (boundary === "staging IO") expect(error.cause).toBe(cause)
      expect(yield* fixture.publication.read()).toBe(fixture.first)
      expect(yield* fixture.fsys.readFileString(path.join(fixture.first, "SKILL.md"))).toBe("old")
      expect(yield* fixture.fsys.exists(foreign)).toBe(true)
      if (boundary === "marker read" || boundary === "cached stat") {
        expect(writes).toEqual([])
        expect((yield* fixture.fsys.readDirectory(fixture.store)).sort()).toEqual(before.sort())
      }
    }),
  )
}

it.live("publication cleans only owned unpublished work after selection failure", () =>
  Effect.gen(function* () {
    const fixture = yield* publicationFixture
    const foreign = path.join(fixture.store, "tmp-another-owner")
    yield* fixture.fsys.makeDirectory(foreign)
    const before = yield* fixture.fsys.readDirectory(fixture.store)
    const publication = yield* SkillPublication.make(fixture.root, fixture.store).pipe(
      Effect.provide(NodePath.layer),
      Effect.provideService(FSUtil.Service, {
        ...fixture.fsys,
        rename: (from, to) =>
          path.basename(to) === "current"
            ? Effect.fail(
                new PlatformError.PlatformError(
                  new PlatformError.SystemError({
                    _tag: "PermissionDenied",
                    module: "FileSystem",
                    method: "rename",
                  }),
                ),
              )
            : fixture.fsys.rename(from, to),
      }),
    )
    yield* publication.refresh("new", fixture.write("new")).pipe(Effect.exit)
    expect((yield* fixture.fsys.readDirectory(fixture.store)).sort()).toEqual(before.sort())
    expect(yield* fixture.publication.read()).toBe(fixture.first)
  }),
)

for (const boundary of ["before", "during"] as const) {
  it.live(`publication interruption ${boundary} selection commit retains a complete selection`, () =>
    Effect.gen(function* () {
      const fixture = yield* publicationFixture
      const ready = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const publication = yield* SkillPublication.make(fixture.root, fixture.store).pipe(
        Effect.provide(NodePath.layer),
        Effect.provideService(FSUtil.Service, {
          ...fixture.fsys,
          writeFileString: (file, content, options) =>
            boundary === "before" && path.basename(file).startsWith("current-")
              ? Deferred.succeed(ready, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.andThen(fixture.fsys.writeFileString(file, content, options)),
                )
              : fixture.fsys.writeFileString(file, content, options),
          rename: (from, to) =>
            boundary === "during" && path.basename(to) === "current"
              ? Deferred.succeed(ready, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.andThen(fixture.fsys.rename(from, to)),
                )
              : fixture.fsys.rename(from, to),
        }),
      )
      const fiber = yield* publication.refresh("new", fixture.write("new")).pipe(Effect.forkScoped)
      yield* Deferred.await(ready)
      // Request interruption synchronously before releasing the masked commit.
      yield* Fiber.interrupt(fiber).pipe(Effect.forkScoped({ startImmediately: true }))
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.await(fiber)
      const selected = yield* fixture.publication.read()
      expect(selected).toBeDefined()
      expect(yield* fixture.fsys.readFileString(path.join(selected!, "SKILL.md"))).toBe(
        boundary === "before" ? "old" : "new",
      )
      expect(yield* fixture.fsys.readFileString(path.join(fixture.first, "SKILL.md"))).toBe("old")
      expect(
        (yield* fixture.fsys.readDirectory(fixture.store)).some(
          (name) => name.startsWith("tmp-") || name.startsWith("current-"),
        ),
      ).toBe(false)
    }),
  )
}

for (const selection of ["../outside", "C:\\outside", "/outside", "g-" + "a".repeat(36), "x".repeat(4096)]) {
  it.live(`publication rejects invalid selection ${selection.slice(0, 20)}`, () =>
    Effect.gen(function* () {
      const fixture = yield* publicationFixture
      yield* fixture.fsys.writeFileString(path.join(fixture.store, "current"), selection)
      expect(Exit.isFailure(yield* fixture.publication.read().pipe(Effect.exit))).toBe(true)
      expect(yield* fixture.fsys.readFileString(path.join(fixture.first, "SKILL.md"))).toBe("old")
    }),
  )
}

it.live("publication preserves an existing legacy root without inventing a version", () =>
  Effect.gen(function* () {
    const fixture = yield* publicationFixture
    yield* fixture.fsys.remove(path.join(fixture.store, "current"))
    yield* fixture.fsys.writeWithDirs(path.join(fixture.root, "SKILL.md"), "legacy")
    const selected = yield* fixture.publication.refresh("new", fixture.write("new"))
    expect(selected).not.toBe(fixture.root)
    expect(yield* fixture.fsys.readFileString(path.join(fixture.root, "SKILL.md"))).toBe("legacy")
    expect(yield* fixture.fsys.exists(path.join(fixture.root, ".opencode-version"))).toBe(false)
  }),
)

it.live(
  "independent discovery services serialize index preparation and re-read the winner",
  () =>
    Effect.gen(function* () {
      const discovery = yield* actualDiscovery()
      const fsys = yield* FSUtil.Service
      const scope = yield* Effect.scope
      const contended = yield* Deferred.make<void>()
      const flock = yield* Layer.build(
        Layer.fresh(
          LayerNode.compile(EffectFlock.node, [
            [
              FSUtil.node,
              Layer.succeed(FSUtil.Service, {
                ...fsys,
                // Wait for a real contended mkdir, not just entry into acquire().
                makeDirectory: (directory, options) =>
                  fsys
                    .makeDirectory(directory, options)
                    .pipe(
                      Effect.tapError((error) =>
                        directory.endsWith(".lock") && error.reason._tag === "AlreadyExists"
                          ? Deferred.succeed(contended, undefined).pipe(Effect.asVoid)
                          : Effect.void,
                      ),
                    ),
              }),
            ],
          ]),
        ),
      ).pipe(Effect.map((context) => Context.get(context, EffectFlock.Service)))
      const locks: string[] = []
      const ready = Promise.withResolvers<void>()
      const release = Promise.withResolvers<void>()
      const name = `overlap-${crypto.randomUUID()}`
      let indexes = 0
      let downloads = 0
      const server = yield* Effect.acquireRelease(
        Effect.sync(() =>
          Bun.serve({
            port: 0,
            async fetch(req) {
              if (new URL(req.url).pathname.endsWith("index.json")) {
                const version = ++indexes === 1 ? "z-old" : "a-new"
                if (indexes === 1) {
                  ready.resolve()
                  await release.promise
                }
                return Response.json({ skills: [{ name, version, files: ["SKILL.md"] }] })
              }
              downloads++
              return new Response(downloads === 1 ? "old" : "new")
            },
          }),
        ),
        (server) =>
          Effect.sync(() => {
            release.resolve()
            server.stop(true)
          }),
      )
      const second = yield* actualDiscovery(
        LayerNode.compile(Discovery.node, [
          [
            EffectFlock.node,
            Layer.succeed(EffectFlock.Service, {
              ...flock,
              acquire: (key, dir) =>
                Effect.gen(function* () {
                  expect(key).toBe(`skill-discovery:${cacheDir}`)
                  expect(yield* Effect.scope).not.toBe(scope)
                  locks.push("attempt")
                  // Registered first so this observes closure after the real lock releases.
                  yield* Effect.addFinalizer(() =>
                    Effect.sync(() => {
                      locks.push("released")
                    }),
                  )
                  yield* flock.acquire(key, dir)
                  locks.push("acquired")
                }),
            }),
          ],
        ]),
      )
      expect(second).not.toBe(discovery)
      const url = `http://localhost:${server.port}/`
      const firstPull = yield* discovery.pull(url).pipe(Effect.forkScoped)
      yield* Effect.promise(() => ready.promise)
      const secondPull = yield* second.pull(url).pipe(Effect.forkScoped)
      yield* Deferred.await(contended)
      expect(locks).toEqual(["attempt"])
      expect(indexes).toBe(1)
      release.resolve()
      const first = yield* Fiber.join(firstPull)
      const winner = yield* Fiber.join(secondPull)
      expect(locks).toEqual(["attempt", "acquired", "released"])
      expect(first).toHaveLength(1)
      expect(winner).toHaveLength(1)
      expect(winner[0]).not.toBe(first[0])
      expect(yield* Effect.promise(() => readFile(path.join(first[0], ".opencode-version"), "utf8"))).toBe("z-old")
      expect(yield* Effect.promise(() => readFile(path.join(winner[0], ".opencode-version"), "utf8"))).toBe("a-new")
      expect(yield* Effect.promise(() => readFile(path.join(first[0], "SKILL.md"), "utf8"))).toBe("old")
      expect(yield* Effect.promise(() => readFile(path.join(winner[0], "SKILL.md"), "utf8"))).toBe("new")
      expect(yield* second.pull(url)).toEqual(winner)
      expect(locks).toEqual(["attempt", "acquired", "released", "attempt", "acquired", "released"])
      expect(downloads).toBe(2)
    }),
  30_000,
)

for (const target of ["current", "generation", "store"] as const) {
  it.live(`publication rejects a symlink ${target} without following it`, () =>
    Effect.gen(function* () {
      const fixture = yield* publicationFixture
      const file =
        target === "current"
          ? path.join(fixture.store, "current")
          : target === "generation"
            ? fixture.first
            : fixture.store
      const publication = yield* SkillPublication.make(fixture.root, fixture.store).pipe(
        Effect.provide(NodePath.layer),
        Effect.provideService(FSUtil.Service, {
          ...fixture.fsys,
          readDirectoryEntries: (directory) =>
            fixture.fsys
              .readDirectoryEntries(directory)
              .pipe(
                Effect.map((entries) =>
                  directory !== path.dirname(file)
                    ? entries
                    : entries.map((entry) =>
                        entry.name === path.basename(file) ? { ...entry, type: "symlink" as const } : entry,
                      ),
                ),
              ),
        }),
      )
      expect(Exit.isFailure(yield* publication.read().pipe(Effect.exit))).toBe(true)
      expect(yield* fixture.fsys.readFileString(path.join(fixture.first, "SKILL.md"))).toBe("old")
    }),
  )
}

it.live("successful publication never relocates or removes an earlier returned generation", () =>
  Effect.gen(function* () {
    const fixture = yield* publicationFixture
    const publication = yield* SkillPublication.make(fixture.root, fixture.store).pipe(
      Effect.provide(NodePath.layer),
      Effect.provideService(FSUtil.Service, {
        ...fixture.fsys,
        rename: (from, to) => {
          expect(from).not.toBe(fixture.first)
          expect(to).not.toBe(fixture.first)
          return fixture.fsys.rename(from, to)
        },
        remove: (file, options) => {
          expect(file).not.toBe(fixture.first)
          expect(file).not.toBe(path.join(fixture.store, "current"))
          return fixture.fsys.remove(file, options)
        },
      }),
    )
    const next = yield* publication.refresh("new", fixture.write("new"))
    expect(next).not.toBe(fixture.first)
    expect(yield* publication.refresh("new", () => Effect.die("cache hit downloaded"))).toBe(next)
    expect(yield* fixture.fsys.readFileString(path.join(fixture.first, "SKILL.md"))).toBe("old")
    expect(yield* fixture.fsys.readFileString(path.join(next, "SKILL.md"))).toBe("new")
  }),
)

it.live("publication does not clean a staging directory it failed to acquire", () =>
  Effect.gen(function* () {
    const fixture = yield* publicationFixture
    const foreign: string[] = []
    const publication = yield* SkillPublication.make(fixture.root, fixture.store).pipe(
      Effect.provide(NodePath.layer),
      Effect.provideService(FSUtil.Service, {
        ...fixture.fsys,
        makeDirectory: (directory, options) =>
          fixture.fsys.makeDirectory(directory, options).pipe(
            Effect.flatMap(() => {
              foreign.push(directory)
              return Effect.fail(
                new PlatformError.PlatformError(
                  new PlatformError.SystemError({
                    _tag: "AlreadyExists",
                    module: "FileSystem",
                    method: "makeDirectory",
                  }),
                ),
              )
            }),
          ),
      }),
    )
    expect(Exit.isFailure(yield* publication.refresh("new", fixture.write("new")).pipe(Effect.exit))).toBe(true)
    expect(foreign).toHaveLength(1)
    expect(yield* fixture.fsys.exists(foreign[0])).toBe(true)
    expect(yield* fixture.publication.read()).toBe(fixture.first)
  }),
)

for (const layout of [
  "references",
  "references/nested",
  "unlisted",
  "marked",
  "skill.md",
  ".OPENCODE-VERSION",
] as const) {
  it.live(`discovery rejects a legacy ${layout} link before downloads or returned-tree scans`, () =>
    Effect.gen(function* () {
      const fsys = yield* FSUtil.Service
      const discovery = yield* Discovery.Service
      const outside = yield* tmpdirScoped()
      const name = `legacy-link-${crypto.randomUUID()}`
      const root = path.join(cacheDir, name)
      yield* Effect.addFinalizer(() => fsys.remove(root, { recursive: true, force: true }).pipe(Effect.orDie))
      yield* fsys.writeWithDirs(path.join(root, "SKILL.md"), "legacy")
      yield* fsys.writeFileString(path.join(outside, "SKILL.md"), "outside skill")
      const relative = layout === "marked" ? "references" : layout
      // On Windows, differently cased SKILL.md names address the same entry.
      if (layout === "skill.md") yield* fsys.remove(path.join(root, "SKILL.md"))
      if (layout === "marked") yield* fsys.writeFileString(path.join(root, ".opencode-version"), "old")
      yield* fsys.ensureDir(path.dirname(path.join(root, relative)))
      yield* Effect.promise(() =>
        symlink(outside, path.join(root, relative), process.platform === "win32" ? "junction" : "dir"),
      )
      if (layout === "unlisted") {
        const matches = yield* fsys.glob("**/SKILL.md", { cwd: root, absolute: true, include: "file", symlink: true })
        expect(yield* Effect.forEach(matches, (file) => fsys.readFileString(file))).toContain("outside skill")
      }
      const requests: string[] = []
      const server = yield* Effect.acquireRelease(
        Effect.sync(() =>
          Bun.serve({
            port: 0,
            fetch(req) {
              const url = new URL(req.url)
              if (url.pathname.endsWith("index.json")) {
                return Response.json({
                  skills: [
                    { name, files: layout.startsWith("references") ? ["SKILL.md", `${layout}/new.md`] : ["SKILL.md"] },
                  ],
                })
              }
              requests.push(url.pathname)
              return new Response("remote reference")
            },
          }),
        ),
        (server) => Effect.sync(() => server.stop(true)),
      )
      const dirs = yield* discovery.pull(`http://localhost:${server.port}/`)
      expect(dirs).toEqual([])
      expect(requests).toEqual([])
      // Use the consumer's actual follow-links scan options on every returned root.
      expect(
        yield* Effect.forEach(dirs, (dir) =>
          fsys.glob("**/SKILL.md", {
            cwd: dir,
            absolute: true,
            include: "file",
            symlink: true,
          }),
        ),
      ).toEqual([])
      expect(yield* fsys.exists(path.join(outside, "new.md"))).toBe(false)
      expect(yield* fsys.readFileString(path.join(outside, "SKILL.md"))).toBe("outside skill")
      expect(
        (yield* fsys.readDirectoryEntries(path.dirname(path.join(root, relative)))).find(
          (entry) => entry.name === path.basename(relative),
        )?.type,
      ).toBe("symlink")
      if (layout !== "skill.md") expect(yield* fsys.readFileString(path.join(root, "SKILL.md"))).toBe("legacy")
      if (layout === "marked") expect(yield* fsys.readFileString(path.join(root, ".opencode-version"))).toBe("old")
    }),
  )
}

for (const name of ["skill.md", ".OPENCODE-VERSION"]) {
  it.live(`publication applies native Windows case semantics to the special directory ${name}`, () =>
    Effect.gen(function* () {
      const fixture = yield* publicationFixture
      yield* fixture.fsys.remove(path.join(fixture.store, "current"))
      yield* fixture.fsys.ensureDir(path.join(fixture.root, name))
      const exit = yield* fixture.publication.read().pipe(Effect.exit)
      // A differently cased directory aliases a special file only on Windows.
      expect(Exit.isFailure(exit)).toBe(process.platform === "win32")
      expect(yield* fixture.fsys.exists(path.join(fixture.root, name))).toBe(true)
      expect(yield* fixture.fsys.readFileString(path.join(fixture.first, "SKILL.md"))).toBe("old")
    }),
  )
}

for (const target of ["selected", "staging"] as const) {
  it.live(`publication rejects linked descendants of a ${target} generation`, () =>
    Effect.gen(function* () {
      const fixture = yield* publicationFixture
      const outside = yield* tmpdirScoped()
      yield* fixture.fsys.writeFileString(path.join(outside, "SKILL.md"), "outside")
      const link = (directory: string) =>
        Effect.promise(() =>
          symlink(outside, path.join(directory, "references"), process.platform === "win32" ? "junction" : "dir"),
        )
      if (target === "selected") yield* link(fixture.first)
      const exit = yield* (
        target === "selected"
          ? fixture.publication.read()
          : fixture.publication.refresh("new", (staging) =>
              fixture
                .write("new")(staging)
                .pipe(Effect.andThen(link(staging))),
            )
      ).pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
      expect(yield* fixture.fsys.readFileString(path.join(fixture.store, "current"))).toBe(path.basename(fixture.first))
      expect(yield* fixture.fsys.readFileString(path.join(fixture.first, "SKILL.md"))).toBe("old")
      expect(yield* fixture.fsys.readFileString(path.join(outside, "SKILL.md"))).toBe("outside")
      expect((yield* fixture.fsys.readDirectory(fixture.store)).some((entry) => entry.startsWith("tmp-"))).toBe(false)
    }),
  )
}

it.live("publication treats an actually missing staging skill as incomplete and keeps the selection", () =>
  Effect.gen(function* () {
    const fixture = yield* publicationFixture
    const before = yield* fixture.fsys.readDirectory(fixture.store)
    const exit = yield* fixture.publication.refresh("new", () => Effect.void).pipe(Effect.exit)
    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toMatchObject({ method: "incomplete skill generation" })
    expect((yield* fixture.fsys.readDirectory(fixture.store)).sort()).toEqual(before.sort())
    expect(yield* fixture.publication.read()).toBe(fixture.first)
  }),
)

for (const boundary of ["marker read", "staging stat", "download write"] as const) {
  it.live(`discovery logs the original ${boundary} error and falls back without republishing`, () =>
    Effect.gen(function* () {
      const fsys = yield* FSUtil.Service
      const name = `fallback-error-${crypto.randomUUID()}`
      const root = path.join(cacheDir, name)
      const store = path.join(generationDir, name)
      yield* Effect.addFinalizer(() => fsys.remove(root, { recursive: true, force: true }).pipe(Effect.orDie))
      yield* Effect.addFinalizer(() => fsys.remove(store, { recursive: true, force: true }).pipe(Effect.orDie))
      yield* fsys.writeWithDirs(path.join(root, "SKILL.md"), "legacy")
      yield* fsys.writeFileString(path.join(root, ".opencode-version"), "old")
      const error = new PlatformError.PlatformError(
        new PlatformError.SystemError({
          _tag: "PermissionDenied",
          module: "FileSystem",
          method: boundary,
        }),
      )
      const requests: string[] = []
      const messages: unknown[] = []
      const injected: string[] = []
      const ambient = yield* Discovery.Service
      const discovery = yield* actualDiscovery(
        LayerNode.compile(Discovery.node, [
          [
            FSUtil.node,
            Layer.succeed(FSUtil.Service, {
              ...fsys,
              readFileString: (file, encoding) =>
                boundary === "marker read" && file === path.join(root, ".opencode-version")
                  ? Effect.sync(() => injected.push(file)).pipe(Effect.andThen(Effect.fail(error)))
                  : fsys.readFileString(file, encoding),
              stat: (file) =>
                boundary === "staging stat" &&
                file.startsWith(store + path.sep) &&
                path.basename(path.dirname(file)).startsWith("tmp-")
                  ? Effect.sync(() => injected.push(file)).pipe(Effect.andThen(Effect.fail(error)))
                  : fsys.stat(file),
              writeWithDirs: (file, content, mode) =>
                boundary === "download write" && file.startsWith(store + path.sep)
                  ? Effect.sync(() => injected.push(file)).pipe(Effect.andThen(Effect.fail(error)))
                  : fsys.writeWithDirs(file, content, mode),
            }),
          ],
        ]),
      )
      expect(discovery).not.toBe(ambient)
      const server = yield* Effect.acquireRelease(
        Effect.sync(() =>
          Bun.serve({
            port: 0,
            fetch(req) {
              if (new URL(req.url).pathname.endsWith("index.json")) {
                return Response.json({ skills: [{ name, version: "new", files: ["SKILL.md"] }] })
              }
              requests.push(req.url)
              return new Response("new")
            },
          }),
        ),
        (server) => Effect.sync(() => server.stop(true)),
      )
      const dirs = yield* discovery.pull(`http://localhost:${server.port}/`).pipe(
        Effect.provide(
          Logger.layer([
            Logger.make((options) => {
              messages.push(...(Array.isArray(options.message) ? options.message : [options.message]))
            }),
          ]),
        ),
      )
      expect(injected).toHaveLength(1)
      if (boundary === "marker read") expect(injected).toEqual([path.join(root, ".opencode-version")])
      if (boundary !== "marker read") {
        expect(path.dirname(path.dirname(injected[0]))).toBe(store)
        expect(path.basename(path.dirname(injected[0]))).toStartWith("tmp-")
        expect(path.basename(injected[0])).toBe("SKILL.md")
      }
      expect(dirs).toEqual([root])
      expect(messages).toContain("failed to refresh skill")
      expect(
        messages.some(
          (message) => typeof message === "object" && message !== null && "error" in message && message.error === error,
        ),
      ).toBe(true)
      expect(requests).toHaveLength(boundary === "marker read" ? 0 : 1)
      expect(yield* fsys.exists(path.join(store, "current"))).toBe(false)
      if (yield* fsys.exists(store)) expect(yield* fsys.readDirectory(store)).toEqual([])
      expect(yield* fsys.readFileString(path.join(root, "SKILL.md"))).toBe("legacy")
      expect(yield* fsys.readFileString(path.join(root, ".opencode-version"))).toBe("old")
    }),
  )
}
