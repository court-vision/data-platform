import { useEffect, useState } from "react"

import dk from "./desk.module.css"

/**
 * Fall leaves, a copy of the frontend's FallLeaves.tsx (styled by the copied
 * desk.module.css), for the themes that ask for them (`leaves` in themes.ts):
 * a few resting on the bar's bottom edge, and a short fall over the page when
 * the theme opens. Each leaf is drawn in currentColor from the theme's
 * --leaf-1 to --leaf-5, so one set of shapes serves light and dark.
 */

type Kind = "maple" | "oak" | "plain"

// 24 x 24, centred, stem down.
const SHAPE: Record<Kind, { body: string; vein: string }> = {
  maple: {
    body: "M12 1.5 13.6 5.6 15.9 4.4 15.3 8.8 18.4 6.9 17.6 9.6 21.5 9.9 19.2 12.4 20.4 13.6 16 14.8 16.8 17.2 12.7 16.3 12.6 22.5 11.4 22.5 11.3 16.3 7.2 17.2 8 14.8 3.6 13.6 4.8 12.4 2.5 9.9 6.4 9.6 5.6 6.9 8.7 8.8 8.1 4.4 10.4 5.6Z",
    vein: "M12 21.5V9M12 13 7.5 10.2M12 13 16.5 10.2M12 10.5V5.5",
  },
  oak: {
    body: "M12 2c1.6.4 1.3 2.2 2.6 2.6 1.5.4 2.4 1.6 1.2 2.7 1.7.2 2.6 1.9 1.1 3 1.6.4 2 2.1.3 2.8.9 1.2-.2 2.5-1.8 2.3.1 1.4-1 2.2-2.4 1.8L12 20l-1-2.8c-1.4.4-2.5-.4-2.4-1.8-1.6.2-2.7-1.1-1.8-2.3-1.7-.7-1.3-2.4.3-2.8-1.5-1.1-.6-2.8 1.1-3-1.2-1.1-.3-2.3 1.2-2.7C10.7 4.2 10.4 2.4 12 2Z",
    vein: "M12 23V5",
  },
  plain: {
    body: "M12 2C17.5 6.5 17 15 12 20.5 7 15 6.5 6.5 12 2Z",
    vein: "M12 4.5V23",
  },
}

function Leaf({ kind, tone, size, turn, flip = false }: { kind: Kind; tone: number; size: number; turn: number; flip?: boolean }) {
  const s = SHAPE[kind]
  return (
    <svg
      className={dk.leaf}
      viewBox="0 0 24 24"
      width={size}
      height={size}
      style={{ color: `var(--leaf-${tone})`, transform: `rotate(${turn}deg)${flip ? " scaleX(-1)" : ""}` }}
      aria-hidden
    >
      <path d={s.body} fill="currentColor" />
      <path d={s.vein} className={dk.leafVein} />
    </svg>
  )
}

/** Where a leaf sits in its cluster: px from the cluster's anchor, and from the edge's top. */
type Placed = { kind: Kind; tone: number; size: number; turn: number; flip?: boolean; x: number; y: number }

/** Three small piles along the edge: past the tabs, past the middle, and at the far end. */
const CLUSTERS: Array<{ at: string; leaves: Placed[] }> = [
  {
    at: "31%",
    leaves: [
      { kind: "oak", tone: 1, size: 18, turn: 14, flip: true, x: -16, y: 9 },
      { kind: "maple", tone: 2, size: 24, turn: -22, x: -2, y: 1 },
      { kind: "plain", tone: 3, size: 15, turn: 64, x: 18, y: 11 },
    ],
  },
  {
    at: "68%",
    leaves: [
      { kind: "oak", tone: 3, size: 21, turn: -30, x: 0, y: 4 },
      { kind: "maple", tone: 1, size: 22, turn: 20, flip: true, x: 18, y: 0 },
    ],
  },
  {
    at: "100%",
    leaves: [
      { kind: "plain", tone: 2, size: 15, turn: -60, x: -56, y: 12 },
      { kind: "maple", tone: 4, size: 23, turn: -12, x: -40, y: 2 },
      { kind: "maple", tone: 3, size: 18, turn: 35, flip: true, x: -20, y: 9 },
    ],
  },
]

/** Leaves resting on the bar's bottom edge. Decoration only: nothing under them stops taking clicks. */
export function CrustLeaves() {
  return (
    <span className={dk.crustLeaves} aria-hidden>
      {CLUSTERS.map((c) => (
        <span key={c.at} className={dk.leafPile} style={{ left: c.at }}>
          {c.leaves.map((l, i) => (
            <span key={i} className={dk.leafSpot} style={{ left: l.x, top: l.y }}>
              <Leaf {...l} />
            </span>
          ))}
        </span>
      ))}
    </span>
  )
}

const FALL_COUNT = 12
const KINDS: Kind[] = ["maple", "oak", "plain", "maple"]

/** A steady scatter, the same on every render: 0 to 1 from a leaf's index and a salt. */
function scatter(i: number, salt: number): number {
  const v = Math.sin(i * 12.9898 + salt * 78.233) * 43758.5453
  return v - Math.floor(v)
}

const FALL = Array.from({ length: FALL_COUNT }, (_, i) => ({
  kind: KINDS[i % KINDS.length],
  tone: (i * 3) % 5 + 1,
  size: 18 + Math.round(scatter(i, 1) * 12),
  turn: Math.round(scatter(i, 2) * 80 - 40),
  flip: i % 2 === 1,
  left: (i / FALL_COUNT) * 94 + scatter(i, 3) * 5,
  fall: 4.6 + scatter(i, 4) * 1.6,
  wait: scatter(i, 5) * 1.4,
  sway: 1.8 + scatter(i, 6) * 1.2,
}))

/** How long the longest leaf takes, with a margin: then the fall leaves the page. */
const FALL_MS = Math.ceil(Math.max(...FALL.map((f) => f.fall + f.wait)) * 1000) + 300

/**
 * About a dozen leaves drifting down over the desk once, when the theme opens
 * (a page load in it, or switching to it: the frame keys this by theme). It
 * takes no clicks, and a reduced-motion preference skips it.
 */
export function LeafFall() {
  const [falling, setFalling] = useState(true)
  useEffect(() => {
    const t = window.setTimeout(() => setFalling(false), FALL_MS)
    return () => window.clearTimeout(t)
  }, [])
  if (!falling) return null
  return (
    <div className={dk.leafFall} aria-hidden>
      {FALL.map((f, i) => (
        <span
          key={i}
          className={dk.leafDrop}
          style={
            {
              left: `${f.left}%`,
              "--leaf-fall": `${f.fall}s`,
              "--leaf-wait": `${f.wait}s`,
              "--leaf-sway": `${f.sway}s`,
            } as React.CSSProperties
          }
        >
          <span className={dk.leafSway}>
            <Leaf kind={f.kind} tone={f.tone} size={f.size} turn={f.turn} flip={f.flip} />
          </span>
        </span>
      ))}
    </div>
  )
}
