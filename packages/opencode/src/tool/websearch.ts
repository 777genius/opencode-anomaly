import { Effect, Schema } from "effect"
import { HttpClient } from "effect/unstable/http"
import * as Tool from "./tool"
import * as McpExa from "./mcp-exa"
import DESCRIPTION from "./websearch.txt"

export const Parameters = Schema.Struct({
  query: Schema.String.annotate({ description: "Websearch query" }),
  numResults: Schema.Finite.pipe(Schema.withDecodingDefaultTypeKey(Effect.succeed(8))).annotate({
    description: "Number of search results to return (default: 8)",
  }),
  livecrawl: Schema.Literals(["fallback", "preferred"]).pipe(
    Schema.withDecodingDefaultTypeKey(Effect.succeed("fallback" as const)),
  ).annotate({
    description:
      "Live crawl mode - 'fallback': use live crawling as backup if cached content unavailable, 'preferred': prioritize live crawling (default: 'fallback')",
  }),
  type: Schema.Literals(["auto", "fast", "deep"]).pipe(
    Schema.withDecodingDefaultTypeKey(Effect.succeed("auto" as const)),
  ).annotate({
    description: "Search type - 'auto': balanced search (default), 'fast': quick results, 'deep': comprehensive search",
  }),
  contextMaxCharacters: Schema.optional(Schema.Finite).annotate({
    description: "Maximum characters for context string optimized for LLMs (default: 10000)",
  }),
})

export const WebSearchTool = Tool.define(
  "websearch",
  Effect.gen(function* () {
    const http = yield* HttpClient.HttpClient

    return {
      get description() {
        return DESCRIPTION.replace("{{year}}", new Date().getFullYear().toString())
      },
      parameters: Parameters,
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          yield* ctx.ask({
            permission: "websearch",
            patterns: [params.query],
            always: ["*"],
            metadata: {
              query: params.query,
              numResults: params.numResults,
              livecrawl: params.livecrawl,
              type: params.type,
              contextMaxCharacters: params.contextMaxCharacters,
            },
          })

          const result = yield* McpExa.call(
            http,
            "web_search_exa",
            McpExa.SearchArgs,
            {
              query: params.query,
              type: params.type,
              numResults: params.numResults,
              livecrawl: params.livecrawl,
              contextMaxCharacters: params.contextMaxCharacters,
            },
            "25 seconds",
          )

          return {
            output: result ?? "No search results found. Please try a different query.",
            title: `Web search: ${params.query}`,
            metadata: {},
          }
        }).pipe(Effect.orDie),
    }
  }),
)
