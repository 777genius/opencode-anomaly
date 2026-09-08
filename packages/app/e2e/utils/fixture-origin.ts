import { mockServerRoutePattern } from "./mock-server"

const app = new URL(process.env.PLAYWRIGHT_BASE_URL ?? `http://127.0.0.1:${process.env.PLAYWRIGHT_PORT ?? "3000"}`)
const canonical =
  process.env.PLAYWRIGHT_BUILT_APP === "1"
    ? app.origin
    : new URL(
        `http://${process.env.PLAYWRIGHT_SERVER_HOST ?? "127.0.0.1"}:${process.env.PLAYWRIGHT_SERVER_PORT ?? "4096"}`,
      ).origin

export const fixtureOrigins = {
  canonical,
  // Three candidates guarantee a remote distinct from both API and app origins.
  remote: [4097, 4098, 4099]
    .map((port) => `http://127.0.0.1:${port}`)
    .find((origin) => origin !== canonical && origin !== app.origin)!,
}

export function fixtureRoutePattern(...servers: string[]) {
  return new RegExp(
    servers
      .flatMap((server) => [
        // Use the shared-origin branch even for B to keep interception API-only.
        mockServerRoutePattern(server, server).source,
        // These settings endpoints are absent from the shared mock's route list.
        `^${server.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/(?:api/(?:provider|model(?:/default)?)|pty/shells)(?:[?#].*)?$`,
      ])
      .join("|"),
  )
}
