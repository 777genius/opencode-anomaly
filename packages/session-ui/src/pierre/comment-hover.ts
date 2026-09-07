import { findDiffSide } from "./diff-selection"

const redraws = new WeakMap<HTMLElement, { instance: object; refresh: () => void; dispose: () => void }>()
const utilities = new WeakMap<HTMLElement, () => void>()
const replays = new WeakSet<Event>()

// Pierre's post-render callback runs after its interaction manager has attached
// to the new rows. Re-hit-test there so the manager owns both hover and the slot.
export function restoreCommentHover(
  node: HTMLElement,
  instance: { getHoveredLine: () => HoverCommentLine | undefined },
  phase: "mount" | "update" | "unmount",
) {
  if (phase === "unmount") {
    if (redraws.get(node)?.instance !== instance) return
    redraws.get(node)?.dispose()
    redraws.delete(node)
    return
  }
  const existing = redraws.get(node)
  if (existing?.instance === instance) {
    existing.refresh()
    return
  }
  existing?.dispose()

  const doc = node.ownerDocument
  let point: { x: number; y: number } | undefined
  let line: HoverCommentLine | undefined
  const remember = () => {
    line = instance.getHoveredLine() ?? line
    node.querySelectorAll<HTMLElement>('[slot="gutter-utility-slot"] button').forEach((button) => {
      utilities.get(button)?.()
    })
  }
  const move = (event: PointerEvent) => {
    if (replays.has(event) || event.pointerType !== "mouse") return
    point = { x: event.clientX, y: event.clientY }
    if (event.composedPath().includes(node)) remember()
    else refresh()
  }
  const exit = (event: PointerEvent) => {
    // pointerout bubbles from the departing element; a null relatedTarget
    // marks document exit, unlike movement between elements in the document.
    if (event.pointerType !== "mouse" || event.relatedTarget !== null) return
    point = undefined
  }
  const refresh = () => {
    const root = node.shadowRoot
    if (!root || !node.isConnected) return
    const focused = doc.activeElement
    const keyboard =
      focused instanceof HTMLElement &&
      node.contains(focused) &&
      focused.closest('[slot="gutter-utility-slot"]') !== null
    // After viewport exit, blur must still clear a keyboard-restored hover.
    if (!point && !keyboard && !instance.getHoveredLine()) return
    const hit = point ? doc.elementFromPoint(point.x, point.y) : null
    const target =
      keyboard && line
        ? Array.from(root.querySelectorAll<HTMLElement>(`[data-line="${line.lineNumber}"]`)).find(
            (row) => !line?.side || findDiffSide(row) === line.side,
          )
        : point && hit && (hit === node || node.contains(hit))
          ? root.elementFromPoint(point.x, point.y)
          : undefined
    // Dispatch through the public DOM interaction boundary, rather than touching
    // Pierre's private cached elements or manufacturing a second gutter slot.
    const event = new PointerEvent("pointermove", {
      bubbles: true,
      composed: true,
      pointerType: "mouse",
      clientX: point?.x ?? 0,
      clientY: point?.y ?? 0,
    })
    replays.add(event)
    const receiver = target ?? root.querySelector("pre")
    receiver?.dispatchEvent(event)
    remember()
  }
  doc.addEventListener("pointermove", move, { passive: true })
  doc.addEventListener("pointerout", exit, { passive: true })
  doc.addEventListener("scroll", refresh, { passive: true, capture: true })
  node.addEventListener("focusin", remember)
  node.addEventListener("focusout", refresh)
  redraws.set(node, {
    instance,
    refresh,
    dispose: () => {
      doc.removeEventListener("pointermove", move)
      doc.removeEventListener("pointerout", exit)
      doc.removeEventListener("scroll", refresh, true)
      node.removeEventListener("focusin", remember)
      node.removeEventListener("focusout", refresh)
    },
  })
}

export type HoverCommentLine = {
  lineNumber: number
  side?: "additions" | "deletions"
}

export function createHoverCommentUtility(props: {
  label: string
  getHoveredLine: () => HoverCommentLine | undefined
  onSelect: (line: HoverCommentLine) => void
}) {
  if (typeof document === "undefined") return

  const button = document.createElement("button")
  button.type = "button"
  button.ariaLabel = props.label
  button.textContent = "+"
  button.style.width = "20px"
  button.style.height = "20px"
  button.style.display = "flex"
  button.style.alignItems = "center"
  button.style.justifyContent = "center"
  button.style.border = "none"
  button.style.borderRadius = "var(--radius-md)"
  button.style.background = "var(--icon-interactive-base)"
  button.style.color = "var(--white)"
  button.style.boxShadow = "var(--shadow-xs)"
  button.style.fontSize = "14px"
  button.style.lineHeight = "1"
  button.style.cursor = "pointer"
  button.style.position = "relative"
  button.style.left = "30px"
  button.style.top = "calc((var(--diffs-line-height, 24px) - 20px) / 2)"

  let line: HoverCommentLine | undefined

  const sync = () => {
    const next = props.getHoveredLine()
    if (!next) return
    line = next
  }

  // Pierre may call the factory again while retaining the original button.
  // Keep document listeners on the renderer lifecycle, never on discarded utilities.
  utilities.set(button, sync)

  const open = () => {
    const next = props.getHoveredLine() ?? line
    if (!next) return
    props.onSelect(next)
  }

  button.addEventListener("focus", sync)
  button.addEventListener("mouseenter", sync)
  button.addEventListener("mousemove", sync)
  button.addEventListener("pointerdown", (event) => {
    event.preventDefault()
    event.stopPropagation()
    sync()
  })
  button.addEventListener("mousedown", (event) => {
    event.preventDefault()
    event.stopPropagation()
    sync()
  })
  button.addEventListener("click", (event) => {
    event.preventDefault()
    event.stopPropagation()
    open()
  })

  return button
}
