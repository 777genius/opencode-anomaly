import "@opencode-ai/core/plugin/internal"
import { AISDK } from "@opencode-ai/core/aisdk"
import type { LanguageModelV3 } from "@ai-sdk/provider"
import { describe, expect } from "bun:test"
import { Effect } from "effect"
import { Catalog } from "@opencode-ai/core/catalog"
import { ModelV2 } from "@opencode-ai/core/model"
import { PluginV2 } from "@opencode-ai/core/plugin"
import { PluginHost } from "@opencode-ai/core/plugin/host"
import { GoogleVertexAnthropicPlugin, GoogleVertexPlugin } from "@opencode-ai/core/plugin/provider/google-vertex"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { testEffect } from "../lib/effect"
import { PluginTestLayer } from "./fixture"
import { vertexAnthropicPhase, vertexAnthropicPhases, vertexAnthropicPreimport } from "../lib/vertex-anthropic-phase"

// One controlled comparison process; this awaited import is outside test registration.
if (vertexAnthropicPreimport) {
  const phase = vertexAnthropicPhase("non-test:preimport")
  phase("import.entry")
  try {
    await import("@ai-sdk/google-vertex/anthropic")
    phase("import.ready")
  } finally {
    phase("import.settled")
  }
}

const addPlugin = Effect.fn(function* (
  phase: ReturnType<typeof vertexAnthropicPhase>,
  definition: typeof GoogleVertexAnthropicPlugin | typeof GoogleVertexPlugin,
) {
  phase("plugin.entry")
  const plugin = yield* PluginV2.Service
  const aisdk = yield* AISDK.Service
  const host = yield* PluginHost.make(plugin)
  yield* definition.effect(host)
  phase("plugin.ready")
})

function withEnv<A, E, R>(
  phase: ReturnType<typeof vertexAnthropicPhase>,
  vars: Record<string, string | undefined>,
  effect: () => Effect.Effect<A, E, R>,
) {
  return Effect.acquireUseRelease(
    Effect.sync(() => {
      const previous = Object.fromEntries(Object.keys(vars).map((key) => [key, process.env[key]]))
      Object.entries(vars).forEach(([key, value]) => {
        if (value === undefined) delete process.env[key]
        else process.env[key] = value
      })
      return previous
    }),
    effect,
    (previous) =>
      Effect.sync(() => {
        phase("environment.restore.entry")
        Object.entries(previous).forEach(([key, value]) => {
          if (value === undefined) delete process.env[key]
          else process.env[key] = value
        })
        phase("environment.restored")
      }),
  )
}

function selector(calls: string[]) {
  return (id: string) => {
    calls.push(`languageModel:${id}`)
    return { modelId: id, provider: "languageModel", specificationVersion: "v3" } as unknown as LanguageModelV3
  }
}

describe("GoogleVertexAnthropicPlugin", () => {
  {
    const phase = vertexAnthropicPhase("case-01")
    const it = testEffect(PluginTestLayer, vertexAnthropicPhases ? phase : undefined)
    it.effect("resolves legacy project and location env on provider update", () =>
      withEnv(
        phase,
        {
          GOOGLE_CLOUD_PROJECT: "cloud-project",
          GCP_PROJECT: "gcp-project",
          GCLOUD_PROJECT: "gcloud-project",
          GOOGLE_CLOUD_LOCATION: "cloud-location",
          VERTEX_LOCATION: "vertex-location",
          GOOGLE_VERTEX_LOCATION: "google-vertex-location",
        },
        () =>
          Effect.gen(function* () {
            const catalog = yield* Catalog.Service
            yield* catalog.transform((catalog) =>
              catalog.provider.update(ProviderV2.ID.make("google-vertex-anthropic"), (provider) => {
                provider.api = { type: "aisdk", package: "@ai-sdk/google-vertex/anthropic" }
              }),
            )
            yield* addPlugin(phase, GoogleVertexAnthropicPlugin)
            phase("assertion.entry")
            expect(
              (yield* catalog.provider.get(ProviderV2.ID.make("google-vertex-anthropic")))?.request.body.project,
            ).toBe("cloud-project")
            phase("assertion.ready")
            phase("assertion.entry")
            expect(
              (yield* catalog.provider.get(ProviderV2.ID.make("google-vertex-anthropic")))?.request.body.location,
            ).toBe("cloud-location")
            phase("assertion.ready")
          }),
      ),
    )
  }

  {
    const phase = vertexAnthropicPhase("case-02")
    const it = testEffect(PluginTestLayer, vertexAnthropicPhases ? phase : undefined)
    it.effect("keeps configured project and location over env fallback", () =>
      withEnv(phase, { GOOGLE_CLOUD_PROJECT: "env-project", GOOGLE_CLOUD_LOCATION: "env-location" }, () =>
        Effect.gen(function* () {
          const catalog = yield* Catalog.Service
          yield* catalog.transform((catalog) =>
            catalog.provider.update(ProviderV2.ID.make("google-vertex-anthropic"), (provider) => {
              provider.api = { type: "aisdk", package: "@ai-sdk/google-vertex/anthropic" }
              provider.request.body.project = "configured-project"
              provider.request.body.location = "configured-location"
            }),
          )
          yield* addPlugin(phase, GoogleVertexAnthropicPlugin)
          phase("assertion.entry")
          expect((yield* catalog.provider.get(ProviderV2.ID.make("google-vertex-anthropic")))?.request.body.project).toBe(
            "configured-project",
          )
          phase("assertion.ready")
          phase("assertion.entry")
          expect(
            (yield* catalog.provider.get(ProviderV2.ID.make("google-vertex-anthropic")))?.request.body.location,
          ).toBe("configured-location")
          phase("assertion.ready")
        }),
      ),
    )
  }

  {
    const phase = vertexAnthropicPhase("case-03")
    const it = testEffect(PluginTestLayer, vertexAnthropicPhases ? phase : undefined)
    it.effect("creates SDKs from legacy env fallback and default location", () =>
      withEnv(
        phase,
        {
          GOOGLE_CLOUD_PROJECT: undefined,
          GCP_PROJECT: "gcp-project",
          GCLOUD_PROJECT: "gcloud-project",
          GOOGLE_CLOUD_LOCATION: undefined,
          VERTEX_LOCATION: undefined,
          GOOGLE_VERTEX_LOCATION: "ignored-location",
        },
        () =>
          Effect.gen(function* () {
            const plugin = yield* PluginV2.Service
            const aisdk = yield* AISDK.Service
            yield* addPlugin(phase, GoogleVertexAnthropicPlugin)
            phase("runSDK.entry")
            const result = yield* aisdk.runSDK({
              model: ModelV2.Info.make({
                ...ModelV2.Info.empty(
                  ProviderV2.ID.make("google-vertex-anthropic"),
                  ModelV2.ID.make("claude-sonnet-4-5"),
                ),
                api: { id: ModelV2.ID.make("claude-sonnet-4-5"), type: "aisdk", package: "test-provider" },
              }),
              package: "@ai-sdk/google-vertex/anthropic",
              options: { name: "google-vertex-anthropic" },
            })
            phase("runSDK.ready")
            phase("assertion.entry")
            expect(result.sdk.languageModel("claude-sonnet-4-5").config.baseURL).toBe(
              "https://aiplatform.googleapis.com/v1/projects/gcp-project/locations/global/publishers/anthropic/models",
            )
            phase("assertion.ready")
          }),
      ),
    )
  }

  {
    const phase = vertexAnthropicPhase("case-04")
    const it = testEffect(PluginTestLayer, vertexAnthropicPhases ? phase : undefined)
    it.effect("uses GOOGLE_CLOUD_LOCATION before VERTEX_LOCATION when creating SDKs", () =>
      withEnv(
        phase,
        { GOOGLE_CLOUD_PROJECT: "project", GOOGLE_CLOUD_LOCATION: "cloud-location", VERTEX_LOCATION: "vertex-location" },
        () =>
          Effect.gen(function* () {
            const plugin = yield* PluginV2.Service
            const aisdk = yield* AISDK.Service
            yield* addPlugin(phase, GoogleVertexAnthropicPlugin)
            phase("runSDK.entry")
            const result = yield* aisdk.runSDK({
              model: ModelV2.Info.make({
                ...ModelV2.Info.empty(
                  ProviderV2.ID.make("google-vertex-anthropic"),
                  ModelV2.ID.make("claude-sonnet-4-5"),
                ),
                api: { id: ModelV2.ID.make("claude-sonnet-4-5"), type: "aisdk", package: "test-provider" },
              }),
              package: "@ai-sdk/google-vertex/anthropic",
              options: { name: "google-vertex-anthropic" },
            })
            phase("runSDK.ready")
            phase("assertion.entry")
            expect(result.sdk.languageModel("claude-sonnet-4-5").config.baseURL).toBe(
              "https://cloud-location-aiplatform.googleapis.com/v1/projects/project/locations/cloud-location/publishers/anthropic/models",
            )
            phase("assertion.ready")
          }),
      ),
    )
  }

  {
    const phase = vertexAnthropicPhase("case-05")
    const it = testEffect(PluginTestLayer, vertexAnthropicPhases ? phase : undefined)
    it.effect("creates SDKs for google-vertex Anthropic models with multi-region endpoints", () =>
      Effect.gen(function* () {
        const plugin = yield* PluginV2.Service
        const aisdk = yield* AISDK.Service
        yield* addPlugin(phase, GoogleVertexAnthropicPlugin)
        phase("runSDK.entry")
        const result = yield* aisdk.runSDK({
          model: ModelV2.Info.make({
            ...ModelV2.Info.empty(ProviderV2.ID.make("google-vertex"), ModelV2.ID.make("claude-sonnet-4-5")),
            api: { id: ModelV2.ID.make("claude-sonnet-4-5"), type: "aisdk", package: "test-provider" },
          }),
          package: "@ai-sdk/google-vertex/anthropic",
          options: { name: "google-vertex", project: "project", location: "eu" },
        })
        phase("runSDK.ready")
        phase("assertion.entry")
        expect(result.sdk.languageModel("claude-sonnet-4-5").config.baseURL).toBe(
          "https://aiplatform.eu.rep.googleapis.com/v1/projects/project/locations/eu/publishers/anthropic/models",
        )
        phase("assertion.ready")
      }),
    )
  }

  {
    const phase = vertexAnthropicPhase("case-06")
    const it = testEffect(PluginTestLayer, vertexAnthropicPhases ? phase : undefined)
    it.effect("keeps configured baseURL for google-vertex Anthropic models", () =>
      Effect.gen(function* () {
        const plugin = yield* PluginV2.Service
        const aisdk = yield* AISDK.Service
        yield* addPlugin(phase, GoogleVertexAnthropicPlugin)
        phase("runSDK.entry")
        const result = yield* aisdk.runSDK({
          model: ModelV2.Info.make({
            ...ModelV2.Info.empty(ProviderV2.ID.make("google-vertex"), ModelV2.ID.make("claude-sonnet-4-5")),
            api: { id: ModelV2.ID.make("claude-sonnet-4-5"), type: "aisdk", package: "test-provider" },
          }),
          package: "@ai-sdk/google-vertex/anthropic",
          options: { name: "google-vertex", project: "project", location: "eu", baseURL: "https://proxy.example/v1" },
        })
        phase("runSDK.ready")
        phase("assertion.entry")
        expect(result.sdk.languageModel("claude-sonnet-4-5").config.baseURL).toBe("https://proxy.example/v1")
        phase("assertion.ready")
      }),
    )
  }

  {
    const phase = vertexAnthropicPhase("case-07")
    const it = testEffect(PluginTestLayer, vertexAnthropicPhases ? phase : undefined)
    it.effect("selects google-vertex Anthropic language models through V2 plugins", () =>
      Effect.gen(function* () {
        const plugin = yield* PluginV2.Service
        const aisdk = yield* AISDK.Service
        yield* addPlugin(phase, GoogleVertexPlugin)
        yield* addPlugin(phase, GoogleVertexAnthropicPlugin)
        phase("runSDK.entry")
        const sdkResult = yield* aisdk.runSDK({
          model: ModelV2.Info.make({
            ...ModelV2.Info.empty(ProviderV2.ID.make("google-vertex"), ModelV2.ID.make(" claude-sonnet-4-5 ")),
            api: { id: ModelV2.ID.make(" claude-sonnet-4-5 "), type: "aisdk", package: "test-provider" },
          }),
          package: "@ai-sdk/google-vertex/anthropic",
          options: { name: "google-vertex", project: "project", location: "us" },
        })
        phase("runSDK.ready")
        const languageResult = yield* aisdk.runLanguage({
          model: ModelV2.Info.make({
            ...ModelV2.Info.empty(ProviderV2.ID.make("google-vertex"), ModelV2.ID.make(" claude-sonnet-4-5 ")),
            api: { id: ModelV2.ID.make(" claude-sonnet-4-5 "), type: "aisdk", package: "test-provider" },
          }),
          sdk: sdkResult.sdk,
          options: {},
        })
        const language = languageResult.language as unknown as { config: { baseURL: string }; modelId: string }
        phase("assertion.entry")
        expect(language.config.baseURL).toBe(
          "https://aiplatform.us.rep.googleapis.com/v1/projects/project/locations/us/publishers/anthropic/models",
        )
        phase("assertion.ready")
        phase("assertion.entry")
        expect(language.modelId).toBe("claude-sonnet-4-5")
        phase("assertion.ready")
      }),
    )
  }

  {
    const phase = vertexAnthropicPhase("case-08")
    const it = testEffect(PluginTestLayer, vertexAnthropicPhases ? phase : undefined)
    it.effect("trims model IDs before selecting language models", () =>
      Effect.gen(function* () {
        const plugin = yield* PluginV2.Service
        const aisdk = yield* AISDK.Service
        const calls: string[] = []
        yield* addPlugin(phase, GoogleVertexAnthropicPlugin)
        yield* aisdk.runLanguage({
          model: ModelV2.Info.make({
            ...ModelV2.Info.empty(ProviderV2.ID.make("google-vertex-anthropic"), ModelV2.ID.make(" claude-sonnet-4-5 ")),
            api: { id: ModelV2.ID.make(" claude-sonnet-4-5 "), type: "aisdk", package: "test-provider" },
          }),
          sdk: { languageModel: selector(calls) },
          options: {},
        })
        phase("assertion.entry")
        expect(calls).toEqual(["languageModel:claude-sonnet-4-5"])
        phase("assertion.ready")
      }),
    )
  }

  {
    const phase = vertexAnthropicPhase("case-09")
    const it = testEffect(PluginTestLayer, vertexAnthropicPhases ? phase : undefined)
    it.effect("ignores non Vertex Anthropic providers for language selection", () =>
      Effect.gen(function* () {
        const plugin = yield* PluginV2.Service
        const aisdk = yield* AISDK.Service
        const calls: string[] = []
        yield* addPlugin(phase, GoogleVertexAnthropicPlugin)
        const result = yield* aisdk.runLanguage({
          model: ModelV2.Info.make({
            ...ModelV2.Info.empty(ProviderV2.ID.make("google-vertex"), ModelV2.ID.make("claude-sonnet-4-5")),
            api: { id: ModelV2.ID.make("claude-sonnet-4-5"), type: "aisdk", package: "test-provider" },
          }),
          sdk: { languageModel: selector(calls) },
          options: {},
        })
        phase("assertion.entry")
        expect(calls).toEqual([])
        phase("assertion.ready")
        phase("assertion.entry")
        expect(result.language).toBeUndefined()
        phase("assertion.ready")
      }),
    )
  }
})
