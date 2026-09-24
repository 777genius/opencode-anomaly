import { expect, test } from "@playwright/test"
import { assistantMessage, setupTimeline, toolPart, userMessage } from "../performance/timeline-stability/fixture"
import { APP_READY_TIMEOUT, expectAppVisible } from "../utils/waits"

test("renders completed write content", async ({ page }) => {
  const id = "prt_file_projection_write"
  await setupTimeline(page, {
    messages: [
      userMessage(),
      assistantMessage([
        toolPart(id, "write", "completed", { filePath: "src/write.ts", content: "export const written = true\n" }),
      ]),
    ],
    settings: { editToolPartsExpanded: true },
  })

  await expect(page.locator(`[data-timeline-part-id="${id}"] [data-component="write-content"]`)).toBeVisible()
})

test("renders a completed single-file patch", async ({ page }) => {
  test.setTimeout(90_000)
  const id = "prt_file_projection_single_patch"
  await setupTimeline(page, {
    messages: [
      userMessage(),
      assistantMessage([
        toolPart(
          id,
          "apply_patch",
          "completed",
          { files: ["src/a.ts"] },
          {
            metadata: {
              files: [
                {
                  filePath: "src/a.ts",
                  relativePath: "src/a.ts",
                  type: "update",
                  additions: 1,
                  deletions: 1,
                  before: "export const value = 1\n",
                  after: "export const value = 2\n",
                },
              ],
            },
          },
        ),
      ]),
    ],
    settings: { editToolPartsExpanded: true },
  })

  const diff = page.locator(`[data-timeline-part-id="${id}"] [data-component="apply-patch-file-diff"]`)
  await expect(diff.locator("diffs-container [data-line]")).toHaveCount(2, { timeout: APP_READY_TIMEOUT })
  await expectAppVisible(diff)
  const row = page.locator("[data-timeline-key]", { has: diff })
  await expect
    .poll(
      () =>
        row.evaluate((element) => {
          const content = element.querySelector<HTMLElement>('[data-component="apply-patch-file-diff"]')
          if (!content) return false
          return content.getBoundingClientRect().bottom <= element.getBoundingClientRect().bottom + 0.5
        }),
      { timeout: APP_READY_TIMEOUT },
    )
    .toBe(true)
})
