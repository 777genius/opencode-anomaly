import { expect, spyOn, test } from "bun:test"
import { bundledLanguages } from "shiki"
import { FileDiff, preloadHighlighter, registerCustomLanguage } from "../../session-ui/test-browser/pierre"
import {
  createHoverCommentUtility,
  restoreCommentHover,
  type HoverCommentLine,
} from "@opencode-ai/session-ui/pierre/comment-hover"

// Run with packages/app/happydom.ts preloaded. Only hit testing is supplied by
// the test: Happy DOM has no layout. Listener spies observe real registration.
// Rendering, highlighting and interaction
// state all belong to the actual Pierre renderer.
test.each(["pointer", "focus", "viewport-exit"])(
  "handles %s through asynchronous highlighting",
  async (interaction) => {
    // A cold non-worker renderer has no synchronous rows until its theme loads.
    // Load only the theme; hold a uniquely named real grammar at the public loader
    // boundary so an already-warm shared highlighter cannot bypass the plain render.
    await preloadHighlighter({ themes: ["github-dark"], langs: [] })
    const release = Promise.withResolvers<void>()
    const lang = `hover-regression-${crypto.randomUUID()}`
    registerCustomLanguage(lang, async () => {
      await release.promise
      const grammar = await bundledLanguages.typescript()
      return { default: grammar.default.map((language) => ({ ...language, name: lang, aliases: [] })) }
    }, [lang])
    const host = document.createElement("diffs-container")
    document.body.append(host)
    const highlighted = Promise.withResolvers<void>()
    const selected: HoverCommentLine[] = []
    const added = spyOn(document, "addEventListener")
    const removed = spyOn(document, "removeEventListener")
    const renderer = new FileDiff({
      theme: "github-dark",
      diffStyle: "unified",
      lineDiffType: "none",
      disableFileHeader: true,
      disableErrorHandling: true,
      lineHoverHighlight: "both",
      enableGutterUtility: true,
      renderGutterUtility: (getHoveredLine) =>
        createHoverCommentUtility({
          label: "Comment",
          getHoveredLine,
          onSelect: (line) => selected.push(line),
        }),
      onPostRender: (node, instance, phase) => {
        restoreCommentHover(node, instance, phase)
        if (phase === "update" && (node.shadowRoot?.querySelectorAll('[data-line="1"] span').length ?? 0) > 1) {
          highlighted.resolve()
        }
      },
    })
    renderer.render({
      fileContainer: host,
      oldFile: { name: `review.${lang}`, contents: "export const first = 1\nexport const value = 'before'\n" },
      newFile: { name: `review.${lang}`, contents: "export const first = 1\nexport const value = 'after'\n" },
    })
    const root = host.shadowRoot!
    const row = () => {
      const element =
        root.querySelector<HTMLElement>('[data-additions] [data-column-number="1"]') ??
        root.querySelector<HTMLElement>('[data-column-number="1"]')
      expect(element).toBeInstanceOf(HTMLElement)
      if (!element) throw new Error("Expected Pierre's rendered line-1 gutter")
      return element
    }
    const documentHit = spyOn(document, "elementFromPoint").mockReturnValue(host)
    Object.defineProperty(root, "elementFromPoint", { configurable: true, value: row })
    const shadowHit = spyOn(root, "elementFromPoint").mockImplementation(row)
    try {
      const initial = row()
      expect(root.querySelectorAll("[data-line]")).toHaveLength(3)
      expect(root.querySelector('[data-line="1"]')?.textContent).toBe("export const first = 1")
      // Shiki's plain path emits one uncolored token, unlike the worker's text AST.
      expect(root.querySelectorAll('[data-line="1"] span')).toHaveLength(1)
      expect(root.querySelector('[data-line="1"] span')?.getAttribute("style")).toBeNull()
      initial.dispatchEvent(
        new PointerEvent("pointermove", {
          bubbles: true,
          composed: true,
          pointerType: "mouse",
          clientX: 20,
          clientY: 20,
        }),
      )
      expect(initial.getAttribute("data-hovered")).toBe("")
      expect(initial.querySelector("[data-gutter-utility-slot]")).not.toBeNull()
      expect(host.querySelectorAll("button")).toHaveLength(1)
      const button = host.querySelector("button")!
      if (interaction === "focus") {
        button.focus()
        expect(document.activeElement).toBe(button)
      }
      // An ordinary transition within the document must retain the mouse point.
      initial.dispatchEvent(
        new PointerEvent("pointerout", {
          bubbles: true,
          composed: true,
          pointerType: "mouse",
          relatedTarget: host,
        }),
      )
      if (interaction !== "pointer") {
        // Browser exit bubbles pointerout with no related target, then sends
        // non-bubbling pointerleave to the departed ancestors, including Pierre's pre.
        initial.dispatchEvent(
          new PointerEvent("pointerout", {
            bubbles: true,
            composed: true,
            pointerType: "mouse",
            relatedTarget: null,
          }),
        )
        root.querySelector("pre")!.dispatchEvent(new PointerEvent("pointerleave", { pointerType: "mouse" }))
        expect(root.querySelector("[data-hovered]")).toBeNull()
        expect(root.querySelector("[data-gutter-utility-slot]")).toBeNull()
        // Keep hit testing at the last inside position to expose stale-point replay.
      }

      const hitsBeforeHighlight = documentHit.mock.calls.length
      release.resolve()
      await highlighted.promise
      if (interaction !== "pointer") expect(documentHit.mock.calls).toHaveLength(hitsBeforeHighlight)
      expect(root.querySelectorAll('[data-line="1"] span').length).toBeGreaterThan(1)
      expect(root.querySelector('[data-line="1"] span[style]')).toBeInstanceOf(HTMLElement)
      expect(row()).not.toBe(initial)
      if (interaction === "viewport-exit") {
        expect(root.querySelector("[data-hovered]")).toBeNull()
        expect(root.querySelector("[data-gutter-utility-slot]")).toBeNull()
        expect(renderer.getHoveredLine()).toBeUndefined()
        row().dispatchEvent(
          new PointerEvent("pointermove", {
            bubbles: true,
            composed: true,
            pointerType: "mouse",
            clientX: 20,
            clientY: 20,
          }),
        )
      }
      expect(row().getAttribute("data-hovered")).toBe("")
      expect(root.querySelector('[data-line="1"]')?.getAttribute("data-hovered")).toBe("")
      expect(row().querySelectorAll("[data-gutter-utility-slot]")).toHaveLength(1)
      expect(renderer.getHoveredLine()).toEqual({ lineNumber: 1, side: "additions" })

      expect(host.querySelectorAll("button")).toHaveLength(1)
      expect(host.querySelector("button")).toBe(button)
      if (interaction === "focus") expect(document.activeElement).toBe(button)
      button.focus()
      documentHit.mockReturnValue(document.body)
      renderer.setOptions({ ...renderer.options, diffStyle: "split" })
      renderer.rerender()
      expect(document.activeElement).toBe(button)
      expect(row().querySelectorAll("[data-gutter-utility-slot]")).toHaveLength(1)
      button.click()
      expect(selected).toEqual([{ lineNumber: 1, side: "additions" }])

      button.blur()
      expect(root.querySelector("[data-hovered]")).toBeNull()
      expect(root.querySelector("[data-gutter-utility-slot]")).toBeNull()
      // Re-enter after the focused utility's viewport exit before testing scroll.
      document.body.dispatchEvent(
        new PointerEvent("pointermove", {
          bubbles: true,
          pointerType: "mouse",
          clientX: 20,
          clientY: 20,
        }),
      )
      documentHit.mockReturnValue(host)
      shadowHit.mockImplementation(() => root.querySelector('[data-column-number][data-line-type="change-deletion"]'))
      document.dispatchEvent(new Event("scroll"))
      expect(renderer.getHoveredLine()).toEqual({ lineNumber: 2, side: "deletions" })
      button.click()
      expect(selected[1]).toEqual({ lineNumber: 2, side: "deletions" })

      documentHit.mockReturnValue(document.body)
      document.dispatchEvent(new Event("scroll"))
      expect(root.querySelector("[data-hovered]")).toBeNull()
      expect(root.querySelector("[data-gutter-utility-slot]")).toBeNull()

      // Cleanup must detach document listeners even while the host is retained.
      const exits = added.mock.calls.filter(([type]) => type === "pointerout")
      expect(exits).toHaveLength(1)
      renderer.cleanUp()
      expect(removed.mock.calls.some(([type, listener]) => type === "pointerout" && listener === exits[0]![1])).toBe(
        true,
      )
      document.body.append(host)
      const hits = documentHit.mock.calls.length
      document.dispatchEvent(new Event("scroll"))
      expect(documentHit.mock.calls).toHaveLength(hits)
      // The owner clears the previous renderer's shell before reusing the host.
      root.replaceChildren()
      const replacement = new FileDiff(renderer.options)
      try {
        replacement.render({
          fileContainer: host,
          oldFile: { name: "next.txt", contents: "old\n" },
          newFile: { name: "next.txt", contents: "new\n" },
        })
        documentHit.mockReturnValue(host)
        shadowHit.mockImplementation(row)
        document.dispatchEvent(new Event("scroll"))
        expect(root.querySelector("[data-hovered]")).toBeNull()
        expect(root.querySelector("[data-gutter-utility-slot]")).toBeNull()
        row().dispatchEvent(
          new PointerEvent("pointermove", {
            bubbles: true,
            composed: true,
            pointerType: "mouse",
            clientX: 20,
            clientY: 20,
          }),
        )
        expect(replacement.getHoveredLine()).toEqual({ lineNumber: 1, side: "additions" })
        documentHit.mockReturnValue(document.body)
        document.body.dispatchEvent(
          new PointerEvent("pointermove", {
            bubbles: true,
            composed: true,
            pointerType: "mouse",
            clientX: 200,
            clientY: 200,
          }),
        )
        replacement.rerender()
        expect(root.querySelector("[data-hovered]")).toBeNull()
        expect(root.querySelector("[data-gutter-utility-slot]")).toBeNull()
      } finally {
        replacement.cleanUp()
      }
    } finally {
      release.resolve()
      renderer.cleanUp()
      host.remove()
      documentHit.mockRestore()
      shadowHit.mockRestore()
      added.mockRestore()
      removed.mockRestore()
    }
  },
)
