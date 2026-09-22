import { Effect, Path, Ref } from "effect"
import { FSUtil } from "@opencode-ai/core/fs-util"

// Only generated identifiers go in the selection record; remote versions are opaque.
const generationName = /^g-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/

export const make = Effect.fn("SkillPublication.make")(function* (root: string, store: string) {
  const fs = yield* FSUtil.Service
  const path = yield* Path.Path
  const selection = path.join(store, "current")
  const sameName = (left: string, right: string) =>
    process.platform === "win32" ? left.toLowerCase() === right.toLowerCase() : left === right

  const directory = Effect.fnUntraced(function* (target: string) {
    if (!(yield* fs.exists(path.dirname(target)))) return
    const entries = yield* fs.readDirectoryEntries(path.dirname(target))
    if (entries.some((entry) => sameName(entry.name, path.basename(target)) && entry.type !== "directory")) {
      return yield* Effect.fail(new FSUtil.FileSystemError({ method: "unsafe skill publication directory" }))
    }
  })

  // Check each managed component, including the generation itself, without following links.
  const validate = Effect.fnUntraced(function* () {
    yield* directory(path.dirname(root))
    yield* directory(root)
    yield* directory(path.dirname(store))
    yield* directory(store)
  })

  // Consumers recursively scan with symlink following enabled. Reject every linked
  // descendant, even one absent from the index, before writing or returning a tree.
  const tree: (target: string) => Effect.Effect<FSUtil.DirEntry[], FSUtil.Error> = Effect.fnUntraced(function* (
    target: string,
  ) {
    if (!(yield* fs.exists(target))) return []
    const entries = yield* fs.readDirectoryEntries(target)
    if (
      entries.some(
        (entry) =>
          (entry.type !== "file" && entry.type !== "directory") ||
          (["SKILL.md", ".opencode-version"].some((name) => sameName(entry.name, name)) && entry.type !== "file"),
      )
    ) {
      return yield* Effect.fail(new FSUtil.FileSystemError({ method: "unsafe skill tree entry" }))
    }
    yield* Effect.forEach(
      entries.filter((entry) => entry.type === "directory"),
      (entry) => tree(path.join(target, entry.name)),
      { discard: true },
    )
    return entries
  })

  const available = Effect.fn("SkillPublication.available")(function* (target: string) {
    yield* validate()
    yield* directory(target)
    yield* tree(target)
    const info = yield* fs
      .stat(path.join(target, "SKILL.md"))
      .pipe(Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed(undefined)))
    return info?.type === "File" ? target : null
  })

  const read = Effect.fn("SkillPublication.read")(function* () {
    yield* validate()
    yield* tree(root)
    if (!(yield* fs.exists(store))) return undefined
    const entry = (yield* fs.readDirectoryEntries(store)).find((entry) => sameName(entry.name, "current"))
    if (!entry) return undefined
    if (entry.type !== "file" || Number((yield* fs.stat(selection)).size) !== 38) {
      return yield* Effect.fail(new FSUtil.FileSystemError({ method: "invalid skill selection" }))
    }
    const name = yield* fs.readFileString(selection)
    if (!generationName.test(name)) {
      return yield* Effect.fail(new FSUtil.FileSystemError({ method: "invalid skill selection" }))
    }
    const selected = path.join(store, name)
    yield* directory(selected)
    const files = yield* tree(selected)
    if (
      !["SKILL.md", ".opencode-version"].every((name) =>
        files.some((entry) => sameName(entry.name, name) && entry.type === "file"),
      )
    ) {
      return yield* Effect.fail(new FSUtil.FileSystemError({ method: "incomplete skill selection" }))
    }
    return selected
  })

  // The caller holds the cache flock across index fetch, selection read and publication.
  const refresh = Effect.fn("SkillPublication.refresh")(function* <E, R>(
    version: string,
    write: (staging: string) => Effect.Effect<void, E, R>,
  ) {
    const selected = yield* read()
    const previous = selected ?? root
    const marker = yield* fs
      .readFileString(path.join(previous, ".opencode-version"))
      .pipe(Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed(undefined)))
    if (marker === version && (yield* available(previous))) return previous
    const owned = yield* Ref.make({ staging: false, generation: false, pending: false, committed: false })
    const token = crypto.randomUUID()
    const staging = path.join(store, `tmp-${token}`)
    const generation = path.join(store, `g-${token}`)
    const pending = path.join(store, `current-${token}`)
    yield* fs.ensureDir(store)
    yield* validate()
    // Published generations are retained indefinitely: callers may still be scanning them.
    // Cache growth is intentional until a separate reclamation protocol exists.
    return yield* Effect.gen(function* () {
      yield* Effect.uninterruptible(
        fs.makeDirectory(staging).pipe(Effect.tap(() => Ref.update(owned, (state) => ({ ...state, staging: true })))),
      )
      yield* write(staging)
      if (!(yield* available(staging))) {
        return yield* Effect.fail(new FSUtil.FileSystemError({ method: "incomplete skill generation" }))
      }
      yield* fs.writeFileString(path.join(staging, ".opencode-version"), version)
      if (yield* fs.exists(generation)) {
        return yield* Effect.fail(new FSUtil.FileSystemError({ method: "skill generation already exists" }))
      }
      yield* Effect.uninterruptible(
        fs
          .rename(staging, generation)
          .pipe(Effect.tap(() => Ref.update(owned, (state) => ({ ...state, staging: false, generation: true })))),
      )
      yield* Effect.uninterruptible(
        fs
          .writeFileString(pending, path.basename(generation), { flag: "wx" })
          .pipe(Effect.tap(() => Ref.update(owned, (state) => ({ ...state, pending: true })))),
      )
      // No unlink gap, and no relocation of a previously returned directory. Mask
      // the selection commit and its ownership flag so cleanup cannot remove a winner.
      yield* Effect.uninterruptible(
        fs
          .rename(pending, selection)
          .pipe(Effect.tap(() => Ref.update(owned, (state) => ({ ...state, committed: true })))),
      )
      return generation
    }).pipe(
      Effect.ensuring(
        Ref.get(owned).pipe(
          Effect.flatMap((state) =>
            Effect.all(
              [
                state.staging ? fs.remove(staging, { recursive: true, force: true }).pipe(Effect.ignore) : Effect.void,
                state.pending ? fs.remove(pending, { force: true }).pipe(Effect.ignore) : Effect.void,
                state.generation && !state.committed
                  ? fs.remove(generation, { recursive: true, force: true }).pipe(Effect.ignore)
                  : Effect.void,
              ],
              { discard: true },
            ),
          ),
        ),
      ),
    )
  })

  return { read, refresh, available }
})

export * as SkillPublication from "./publication"
