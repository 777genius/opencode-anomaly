import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { httpClient, path } from "@opencode-ai/core/effect/app-node-platform"
import { Effect, Layer, Path, Schema, Context } from "effect"
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http"
import { withTransientReadRetry } from "@/util/effect-http-client"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { EffectFlock } from "@opencode-ai/core/util/effect-flock"
import { SkillPublication } from "./publication"
import { Global } from "@opencode-ai/core/global"

const skillConcurrency = 4
const fileConcurrency = 8

class IndexSkill extends Schema.Class<IndexSkill>("IndexSkill")({
  name: Schema.String,
  files: Schema.Array(Schema.String),
  version: Schema.optional(Schema.String),
}) {}

class Index extends Schema.Class<Index>("Index")({
  skills: Schema.Array(IndexSkill),
}) {}

export interface Interface {
  readonly pull: (url: string) => Effect.Effect<string[]>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SkillDiscovery") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* FSUtil.Service
    const path = yield* Path.Path
    const http = HttpClient.filterStatusOk(withTransientReadRetry(yield* HttpClient.HttpClient))
    const flock = yield* EffectFlock.Service
    const cache = path.join(Global.Path.cache, "skills")

    const download = Effect.fn("Discovery.download")(function* (url: string, dest: string) {
      if (yield* fs.exists(dest)) return true

      const body = yield* HttpClientRequest.get(url).pipe(
        http.execute,
        Effect.flatMap((res) => res.arrayBuffer),
        Effect.catch((err) => Effect.logError("failed to download", { url: url, error: err }).pipe(Effect.as(null))),
      )
      if (body === null) return false
      yield* fs.writeWithDirs(dest, new Uint8Array(body))
      return true
    })

    const pull = Effect.fn("Discovery.pull")(function* (url: string) {
      const base = url.endsWith("/") ? url : `${url}/`
      const index = new URL("index.json", base).href
      const host = base.slice(0, -1)

      yield* Effect.logInfo("fetching index", { url: index })

      const data = yield* HttpClientRequest.get(index).pipe(
        HttpClientRequest.acceptJson,
        http.execute,
        Effect.flatMap(HttpClientResponse.schemaBodyJson(Index)),
        Effect.catch((err) =>
          Effect.logError("failed to fetch index", { url: index, error: err }).pipe(Effect.as(null)),
        ),
      )

      if (!data) return []

      const missing = data.skills.filter((skill) => !skill.files.includes("SKILL.md"))
      yield* Effect.forEach(
        missing,
        (skill) => Effect.logWarning("skill entry missing SKILL.md", { url: index, skill: skill.name }),
        { discard: true },
      )
      const list = data.skills.filter(
        (skill, index, skills) =>
          skills.findIndex((entry) => entry.name.toLowerCase() === skill.name.toLowerCase()) === index &&
          skill.files.includes("SKILL.md") &&
          safePart(skill.name) &&
          skill.files.every((file) => file.split("/").every(safePart) && file !== ".opencode-version"),
      )

      const dirs = yield* Effect.forEach(
        list,
        (skill) =>
          Effect.gen(function* () {
            const root = path.join(cache, skill.name)
            // Lazy construction must use this Discovery service's captured dependencies.
            const publication = yield* SkillPublication.make(
              root,
              path.join(Global.Path.cache, "skill-generations", skill.name),
            ).pipe(Effect.provideService(FSUtil.Service, fs), Effect.provideService(Path.Path, path))
            const selected = yield* publication
              .read()
              .pipe(
                Effect.catch((error) =>
                  Effect.logError("failed to read skill selection", { error }).pipe(Effect.as(null)),
                ),
              )
            if (selected === null) return null
            if (skill.version === undefined) {
              if (selected) return selected
              if (yield* fs.exists(path.join(root, ".opencode-version"))) return yield* publication.available(root)
              yield* Effect.forEach(
                skill.files,
                (file) => download(new URL(file, `${host}/${skill.name}/`).href, path.join(root, file)),
                { concurrency: fileConcurrency, discard: true },
              )
              return yield* publication.available(root)
            }
            return yield* publication
              .refresh(skill.version, (staging) =>
                Effect.forEach(
                  skill.files,
                  (file) => download(new URL(file, `${host}/${skill.name}/`).href, path.join(staging, file)),
                  { concurrency: fileConcurrency },
                ).pipe(
                  Effect.flatMap((downloaded) =>
                    downloaded.every(Boolean)
                      ? Effect.void
                      : Effect.fail(new FSUtil.FileSystemError({ method: "incomplete skill download" })),
                  ),
                ),
              )
              .pipe(
                Effect.catch((error) =>
                  Effect.gen(function* () {
                    yield* Effect.logError("failed to refresh skill", { skill: skill.name, error })
                    const previous = selected ?? root
                    return yield* publication.available(previous)
                  }),
                ),
              )
          }),
        { concurrency: skillConcurrency },
      )

      return dirs.filter((dir): dir is string => dir !== null)
    })

    // Acquire before fetching the index: a delayed index response cannot overwrite
    // a newer winner. The lock lives in state/locks, outside every scanned root.
    return Service.of({
      pull: (url) =>
        Effect.scoped(
          Effect.gen(function* () {
            yield* flock.acquire(`skill-discovery:${cache}`)
            return yield* pull(url)
          }),
        ).pipe(Effect.catch((error) => Effect.logError("failed to pull skill cache", { error }).pipe(Effect.as([])))),
    })
  }),
)

export const node = LayerNode.make({
  service: Service,
  layer: layer,
  deps: [FSUtil.node, path, httpClient, EffectFlock.node],
})

function safePart(value: string) {
  return (
    value.length > 0 &&
    value.length <= 255 &&
    value !== "." &&
    value !== ".." &&
    !/[\/<>:"\\|?*\x00-\x1f]/.test(value) &&
    !/[. ]$/.test(value) &&
    !/^(con|prn|aux|nul|com[0-9]|lpt[0-9])(?:\.|$)/i.test(value)
  )
}

export * as Discovery from "./discovery"
