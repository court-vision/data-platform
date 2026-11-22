import { useEffect, useLayoutEffect, useRef, useState, type PointerEvent } from "react"
import { createPortal } from "react-dom"

import { useDeskPortal } from "@/desk/DeskFrame"
import { formatMs, formatShare, partRows, segments, type Group } from "@/lib/latency"
import s from "./sheet.module.css"
import l from "./latency.module.css"

/** "ESPN 212 ms · Postgres 18 ms · Our code 40 ms": a bar's numbers in words, for screen readers. */
export function describeParts(parts: Record<string, number>): string {
  const segs = segments(parts)
  return segs.length === 0 ? "no time recorded" : segs.map((p) => `${p.group.label} ${formatMs(p.ms)}`).join(" · ")
}

/** Where the pointer is on a bar: the segment under it, and where the tooltip hangs from. */
interface Pointed {
  active: string
  x: number
  top: number
  bottom: number
  /** A tap pins the tip until the next tap elsewhere; a mouse's tip leaves with the mouse. */
  touch: boolean
}

/**
 * Where one request's time went (or a route's mean request), as segments in
 * the groups' colours. With a `scale`, its length is its total against that,
 * so the bars in one column compare: a longer bar is a slower request.
 * Without one it fills the cell, and only the shares show.
 *
 * Pointing at it (or tapping it on a phone) opens a tooltip that names each
 * segment's group with its time and share, the one under the pointer lit.
 */
export function AnatomyBar({ parts, scale, label }: { parts: Record<string, number>; scale?: number; label?: string }) {
  const segs = segments(parts)
  const total = segs.reduce((sum, part) => sum + part.ms, 0)
  const width = scale == null ? 100 : scale > 0 ? Math.max(0.5, Math.min(100, (total / scale) * 100)) : 0
  const text = `${label ? `${label}: ` : ""}${formatMs(total)} · ${describeParts(parts)}`
  const [pointed, setPointed] = useState<Pointed | null>(null)
  const hitRef = useRef<HTMLDivElement>(null)
  const barRef = useRef<HTMLDivElement>(null)
  const open = pointed != null

  function point(event: PointerEvent<HTMLDivElement>) {
    const bar = barRef.current
    if (!bar || segs.length === 0) return
    // The segment under the pointer, or the one left of the 2px gap it is in.
    let index = 0
    Array.from(bar.children).forEach((child, i) => {
      if (child.getBoundingClientRect().left <= event.clientX) index = i
    })
    const rect = bar.getBoundingClientRect()
    setPointed({ active: segs[index].group.key, x: event.clientX, top: rect.top, bottom: rect.bottom, touch: event.pointerType === "touch" })
  }

  // The tip is placed against the viewport, so a scroll moves the bar out from
  // under it: it closes then, and on a tap anywhere off this bar.
  useEffect(() => {
    if (!open) return
    const close = () => setPointed(null)
    const tapOff = (event: globalThis.PointerEvent) => {
      if (!hitRef.current?.contains(event.target as Node)) close()
    }
    window.addEventListener("scroll", close, { capture: true, passive: true })
    window.addEventListener("resize", close)
    document.addEventListener("pointerdown", tapOff)
    return () => {
      window.removeEventListener("scroll", close, { capture: true })
      window.removeEventListener("resize", close)
      document.removeEventListener("pointerdown", tapOff)
    }
  }, [open])

  return (
    <div
      ref={hitRef}
      className={l.hit}
      style={{ width: `${width}%` }}
      onPointerEnter={point}
      onPointerMove={point}
      onPointerLeave={(event) => {
        if (event.pointerType !== "touch") setPointed(null)
      }}
    >
      <div ref={barRef} className={`${l.viz} ${l.anatomy}`} role="img" aria-label={text} data-hovering={open ? "true" : undefined}>
        {segs.map((part) => (
          <span
            key={part.group.key}
            className={l.segment}
            data-group={part.group.key}
            data-active={pointed?.active === part.group.key ? "true" : undefined}
            style={{ width: `calc((100% - ${2 * (segs.length - 1)}px) * ${total > 0 ? part.ms / total : 0})` }}
          />
        ))}
      </div>
      {pointed && <AnatomyTip parts={parts} total={total} label={label} pointed={pointed} />}
    </div>
  )
}

const EDGE = 8 // the tip keeps this far from the viewport's sides
const OFFSET = 8 // and hangs this far above the bar (below it, with no room above)

/**
 * The bar's tooltip, placed by the pointer: above the bar, or below it with no
 * room above, and never past the viewport's edges. Portalled into the frame
 * so the theme's tokens apply; aria-hidden, since the bar's own label already
 * says all of it.
 */
function AnatomyTip({ parts, total, label, pointed }: { parts: Record<string, number>; total: number; label?: string; pointed: Pointed }) {
  const container = useDeskPortal()
  const ref = useRef<HTMLDivElement>(null)

  // Placed after layout, before paint: it needs its own size to stay on screen.
  useLayoutEffect(() => {
    const tip = ref.current
    if (!tip) return
    const { offsetWidth: w, offsetHeight: h } = tip
    const left = Math.max(EDGE, Math.min(pointed.x - w / 2, window.innerWidth - EDGE - w))
    // Above the bar, else below it; on a screen too short for either it slides
    // up over the bar (it takes no pointer events) rather than off the bottom.
    const above = pointed.top - OFFSET - h
    const top = above >= EDGE ? above : Math.max(EDGE, Math.min(pointed.bottom + OFFSET, window.innerHeight - EDGE - h))
    tip.style.left = `${left}px`
    tip.style.top = `${top}px`
  })

  if (!container) return null
  return createPortal(
    <div ref={ref} className={`${l.viz} ${l.tip}`} aria-hidden>
      <AnatomyTipBody parts={parts} total={total} label={label} active={pointed.active} />
    </div>,
    container,
  )
}

/**
 * What the tooltip says: the total, then a line per segment in the bar's
 * order (swatch, group, time, share), the buckets a folded group holds under
 * it, and what the segment pointed at is.
 */
export function AnatomyTipBody({ parts, total, label, active }: { parts: Record<string, number>; total: number; label?: string; active: string }) {
  const rows = partRows(parts)
  const pointedAt = rows.find((row) => row.group.key === active)
  return (
    <>
      <div className={l.tipHead}>
        <span>{label ?? "Total"}</span>
        <span className={l.tipNum}>{formatMs(total)}</span>
      </div>
      <div className={l.tipRows}>
        {rows.map((row) => (
          <div key={row.group.key} className={l.tipGroup} data-active={row.group.key === active ? "true" : undefined}>
            <div className={l.tipRow}>
              <span className={l.swatch} data-group={row.group.key} />
              <span className={l.tipLabel}>{row.group.label}</span>
              <span className={l.tipNum}>{formatMs(row.ms)}</span>
              <span className={l.tipNum}>{formatShare(row.share)}</span>
            </div>
            {row.buckets.map((bucket) => (
              <div key={bucket.bucket} className={`${l.tipRow} ${l.tipBucket}`}>
                <span />
                <span className={l.tipLabel}>{bucket.label}</span>
                <span className={l.tipNum}>{formatMs(bucket.ms)}</span>
                <span className={l.tipNum}>{formatShare(bucket.share)}</span>
              </div>
            ))}
          </div>
        ))}
      </div>
      {pointedAt && (
        <p className={l.tipNote}>
          <span className={l.tipNoteLabel}>{pointedAt.group.label}:</span> {pointedAt.group.note}
        </p>
      )}
    </>
  )
}

/** Names each colour a bar uses. */
export function AnatomyKey({ groups }: { groups: readonly Group[] }) {
  if (groups.length === 0) return null
  return (
    <ul className={`${s.legend} ${l.viz}`} aria-label="Key">
      {groups.map((group) => (
        <li key={group.key} title={group.note}>
          <span className={l.swatch} data-group={group.key} aria-hidden />
          {group.label}
        </li>
      ))}
    </ul>
  )
}
