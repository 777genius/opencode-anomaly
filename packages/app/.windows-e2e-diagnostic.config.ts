import { defineConfig } from "@playwright/test"
import base from "./playwright.config"

export default defineConfig({
  ...base,
  testMatch: [
    "regression/session-timeline-collapse-state.spec.ts",
    "regression/session-timeline-context-state.spec.ts",
    "regression/session-timeline-projection.spec.ts",
  ],
  grep: /keeps a sticky edit header|preserves a collapsed context group|renders every admitted tool family/,
  workers: 5,
  repeatEach: 3,
  retries: 0,
  use: {
    ...base.use,
    trace: "on",
    video: process.env.DIAGNOSTIC_VIDEO === "off" ? "off" : "retain-on-failure",
  },
})
