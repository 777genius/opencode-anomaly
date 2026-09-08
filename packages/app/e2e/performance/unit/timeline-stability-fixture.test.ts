import { expect, test } from "bun:test"
import {
  assistantMessage,
  textPart,
  timelineReadinessMessageID,
  toolPart,
  userMessage,
} from "../timeline-stability/fixture"

test("keys fixture readiness to the latest user turn when the assistant has no visible part", () => {
  const earlier = userMessage(undefined, { id: "msg_readiness_earlier" })
  const current = userMessage(undefined, { id: "msg_readiness_current" })
  const hidden = assistantMessage([toolPart("prt_hidden_question", "question", "running", { questions: [] })], {
    id: "msg_readiness_assistant",
    parentID: current.info.id,
  })

  const previous = assistantMessage([textPart("prt_old", "old")], { parentID: earlier.info.id })

  expect(timelineReadinessMessageID([earlier, previous, current, hidden])).toBe(current.info.id)
})

test("rejects a fixture without a user turn", () => {
  expect(() => timelineReadinessMessageID([assistantMessage()])).toThrow("Timeline fixture requires a user message")
})
