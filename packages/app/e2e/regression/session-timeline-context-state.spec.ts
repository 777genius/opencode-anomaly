import { expect, test, type Locator } from "@playwright/test"
import {
  assistantMessage,
  completedAssistantInfo,
  messageUpdated,
  partUpdated,
  setupTimeline,
  status,
  toolPart,
  userMessage,
} from "../performance/timeline-stability/fixture"

test("preserves a collapsed context group through count and status updates", async ({ page }) => {
  const ids = ["prt_closed_01_read", "prt_closed_02_glob"]
  const inputs = {
    read: { filePath: "src/a.ts", offset: 0, limit: 120 },
    glob: { path: ".", pattern: "**/*.ts" },
  }
  const assistant = assistantMessage(
    [toolPart(ids[0]!, "read", "running", inputs.read), toolPart(ids[1]!, "glob", "running", inputs.glob)],
    { completed: false },
  )
  const timeline = await setupTimeline(page, {
    messages: [userMessage(), assistant],
  })
  const group = page.locator(`[data-timeline-part-ids="${ids.join(",")}"]`)
  const trigger = group.locator('[data-slot="collapsible-trigger"]')
  await expect(trigger).toBeVisible()
  await expect(trigger).toHaveAttribute("aria-expanded", "false")
  await expect(group.locator('[data-component="tool-status-title"]')).toHaveAttribute("aria-label", "Exploring")
  await expectContextCounts(group, 1, 1)

  const addedID = "prt_closed_03_read"
  const addedInput = { filePath: "src/b.ts", offset: 0, limit: 120 }
  await timeline.send(partUpdated(toolPart(addedID, "read", "running", addedInput)))
  const updated = page.locator(`[data-timeline-part-ids="${[...ids, addedID].join(",")}"]`)
  const updatedTrigger = updated.locator('[data-slot="collapsible-trigger"]')
  await expect(group).toHaveCount(0)
  await expect(updated).toHaveCount(1)
  await expectContextCounts(updated, 2, 1)
  await expect(updated.locator('[data-component="tool-status-title"]')).toHaveAttribute("aria-label", "Exploring")
  await expect(updatedTrigger).toBeVisible()
  await expect(updatedTrigger).toHaveAttribute("aria-expanded", "false")

  await timeline.send(partUpdated(toolPart(ids[0]!, "read", "completed", inputs.read)))
  await timeline.send(partUpdated(toolPart(ids[1]!, "glob", "completed", inputs.glob)))
  await timeline.send(partUpdated(toolPart(addedID, "read", "completed", addedInput)))
  await timeline.send(messageUpdated({ ...completedAssistantInfo(assistant.info), finish: "stop" }))
  await timeline.send(status("idle"))
  // Explored requires all three tools to finish and the active turn to leave busy.
  await expect(updated.locator('[data-component="tool-status-title"]')).toHaveAttribute("aria-label", "Explored")
  await expect(page.locator('[data-timeline-row="Thinking"]')).toHaveCount(0)
  await expect(updated).toHaveCount(1)
  await expectContextCounts(updated, 2, 1)
  await expect(updatedTrigger).toBeVisible()
  await expect(updatedTrigger).toHaveAttribute("aria-expanded", "false")
})

async function expectContextCounts(group: Locator, reads: number, searches: number) {
  const counts = group.locator('[data-slot="tool-count-summary-item"][data-active="true"]')
  await expect(counts.locator('[data-slot="tool-count-label-word"]')).toHaveText([
    reads === 1 ? "read" : "reads",
    searches === 1 ? "search" : "searches",
  ])
  await expect(counts.filter({ hasText: "read" }).locator('[data-component="animated-number"]')).toHaveAttribute(
    "aria-label",
    String(reads),
  )
  await expect(counts.filter({ hasText: "search" }).locator('[data-component="animated-number"]')).toHaveAttribute(
    "aria-label",
    String(searches),
  )
}
