import { useState, type KeyboardEvent, type ReactNode } from "react"

import { describeParts } from "@/components/Anatomy"
import { formatMs, formatSlice, msTick, niceMs, segments, type LatencyBin } from "@/lib/latency"
import { roveIndex } from "@/lib/runs"
import s from "./sheet.module.css"
import c from "./chart.module.css"
import l from "./latency.module.css"

const PLOT_WIDTH = 756
const PLOT_HEIGHT = 120
const PLOT_TOP = 8 // room for the top tick's label
const HEIGHT = PLOT_TOP + PLOT_HEIGHT + 4
const GAP = 2

function slotWidth(count: number): number {
  return PLOT_WIDTH / Math.max(count, 1)
}

function y(value: number, top: number): number {
  return PLOT_TOP + PLOT_HEIGHT - (Math.min(value, top) / top) * PLOT_HEIGHT
}

function span(series: LatencyBin[], binSeconds: number): string {
  if (series.length === 0) return ""
  return `${formatSlice(series[0].at, binSeconds)} → ${formatSlice(series[series.length - 1].at, binSeconds)}`
}

function describeSlice(bin: LatencyBin, binSeconds: number): string {
  if (bin.requests === 0) return `${formatSlice(bin.at, binSeconds)} · no requests`
  const errors = bin.errors > 0 ? ` · ${bin.errors} 5xx` : ""
  return `${formatSlice(bin.at, binSeconds)} · ${bin.requests} ${bin.requests === 1 ? "request" : "requests"}${errors}`
}

/**
 * The frame both charts share: the HTML axis beside the plot, gridlines, one
 * hit slot per slice that the mouse and the arrow keys move between, and a
 * caption that says what is under the cursor (or the span, with nothing there).
 */
function SliceChart({
  series,
  binSeconds,
  top,
  label,
  describe,
  keyItems,
  children,
}: {
  series: LatencyBin[]
  binSeconds: number
  top: number
  label: string
  describe: (bin: LatencyBin) => string
  keyItems: ReactNode
  children: (active: number | null) => ReactNode
}) {
  const [active, setActive] = useState<number | null>(null)
  const slot = slotWidth(series.length)
  const gridlines = [0, top / 2, top]

  function onKeyDown(event: KeyboardEvent<SVGSVGElement>) {
    if (event.altKey || event.ctrlKey || event.metaKey) return
    const next = roveIndex(event.key, active ?? series.length - 1, series.length)
    if (next == null) return
    event.preventDefault()
    setActive(next)
  }

  return (
    <figure className={c.figure}>
      <div className={c.plotRow}>
        <div className={c.axis} aria-hidden>
          {gridlines.map((value) => (
            <span key={value} className={c.axisLabel} style={{ top: `${(y(value, top) / HEIGHT) * 100}%` }}>
              {msTick(value)}
            </span>
          ))}
        </div>
        <svg
          viewBox={`0 0 ${PLOT_WIDTH} ${HEIGHT}`}
          className={`${l.plot} ${l.viz}`}
          role="img"
          tabIndex={0}
          aria-label={`${label}. Arrow keys move between slices.`}
          data-hovering={active != null ? "true" : undefined}
          onKeyDown={onKeyDown}
          onBlur={() => setActive(null)}
          onMouseLeave={() => setActive(null)}
        >
          {gridlines.map((value) => (
            <line key={value} x1={0} x2={PLOT_WIDTH} y1={y(value, top)} y2={y(value, top)} className={c.gridline} strokeWidth={1} vectorEffect="non-scaling-stroke" />
          ))}
          {children(active)}
          {series.map((bin, index) => (
            <rect
              key={bin.at}
              x={index * slot}
              y={0}
              width={slot}
              height={HEIGHT}
              fill="transparent"
              onMouseEnter={() => setActive(index)}
            />
          ))}
        </svg>
      </div>
      <figcaption className={c.caption} aria-live="polite">
        <span>{active != null && series[active] ? describe(series[active]) : span(series, binSeconds)}</span>
        {keyItems}
      </figcaption>
    </figure>
  )
}

/** p50 and p95 per slice, as two lines that break where a slice had no requests. */
export function PercentileChart({ series, binSeconds }: { series: LatencyBin[]; binSeconds: number }) {
  const top = niceMs(Math.max(0, ...series.map((bin) => bin.p95_ms ?? 0)))
  const slot = slotWidth(series.length)
  const x = (index: number) => index * slot + slot / 2

  function path(value: (bin: LatencyBin) => number | null | undefined): string {
    let d = ""
    let drawing = false
    series.forEach((bin, index) => {
      const v = value(bin)
      if (v == null) {
        drawing = false
        return
      }
      d += `${drawing ? "L" : "M"}${x(index).toFixed(1)},${y(v, top).toFixed(1)} `
      drawing = true
    })
    return d.trim()
  }

  return (
    <SliceChart
      series={series}
      binSeconds={binSeconds}
      top={top}
      label={`p50 and p95 response time per slice, ${span(series, binSeconds)}`}
      describe={(bin) =>
        bin.requests === 0 ? describeSlice(bin, binSeconds) : `${describeSlice(bin, binSeconds)} · p50 ${formatMs(bin.p50_ms)} · p95 ${formatMs(bin.p95_ms)}`
      }
      keyItems={
        <span className={s.legend} aria-label="Key">
          <span>
            <span className={l.keyLine} style={{ ["--key" as string]: "var(--text-3)" }} aria-hidden />
            p50
          </span>
          <span>
            <span className={l.keyLine} style={{ ["--key" as string]: "var(--accent)" }} aria-hidden />
            p95
          </span>
        </span>
      }
    >
      {(active) => (
        <>
          {active != null && <line x1={x(active)} x2={x(active)} y1={PLOT_TOP} y2={PLOT_TOP + PLOT_HEIGHT} className={l.cursor} vectorEffect="non-scaling-stroke" />}
          <path d={path((bin) => bin.p50_ms)} className={`${l.line} ${l.p50}`} vectorEffect="non-scaling-stroke" />
          <path d={path((bin) => bin.p95_ms)} className={`${l.line} ${l.p95}`} vectorEffect="non-scaling-stroke" />
          {/* A slice between two empty ones has no line to sit on: its points are dots. */}
          {series.map((bin, index) =>
            bin.p95_ms != null && series[index - 1]?.p95_ms == null && series[index + 1]?.p95_ms == null ? (
              <g key={bin.at}>
                <circle cx={x(index)} cy={y(bin.p50_ms ?? 0, top)} r={3} className={l.dot50} />
                <circle cx={x(index)} cy={y(bin.p95_ms, top)} r={3} className={l.dot95} />
              </g>
            ) : null,
          )}
          {active != null && series[active]?.p95_ms != null && (
            <>
              <circle cx={x(active)} cy={y(series[active].p50_ms ?? 0, top)} r={4} className={l.dot50} />
              <circle cx={x(active)} cy={y(series[active].p95_ms ?? 0, top)} r={4} className={l.dot95} />
            </>
          )}
        </>
      )}
    </SliceChart>
  )
}

/** The mean request of each slice, split into where its time went: a stacked column per slice. */
export function AnatomyColumns({ series, binSeconds }: { series: LatencyBin[]; binSeconds: number }) {
  const totals = series.map((bin) => Object.values(bin.parts).reduce((sum, ms) => sum + ms, 0))
  const top = niceMs(Math.max(0, ...totals))
  const slot = slotWidth(series.length)
  const width = Math.max(1, Math.min(24, slot - GAP))

  return (
    <SliceChart
      series={series}
      binSeconds={binSeconds}
      top={top}
      label={`Mean request per slice, split into where its time went, ${span(series, binSeconds)}`}
      describe={(bin) =>
        bin.requests === 0
          ? describeSlice(bin, binSeconds)
          : `${describeSlice(bin, binSeconds)} · mean ${formatMs(Object.values(bin.parts).reduce((a, b) => a + b, 0))} · ${describeParts(bin.parts)}`
      }
      keyItems={null}
    >
      {(active) =>
        series.map((bin, index) => {
          const left = index * slot + (slot - width) / 2
          let base = PLOT_TOP + PLOT_HEIGHT
          const segs = segments(bin.parts)
          return (
            <g key={bin.at} className={l.slice} data-active={active === index ? "true" : undefined}>
              {segs.map((segment, i) => {
                const height = (Math.min(segment.ms, top) / top) * PLOT_HEIGHT
                // A 2px ground gap between stacked segments, taken from the segment above.
                const gap = i > 0 && height > GAP ? GAP : 0
                const rect = (
                  <rect
                    key={segment.group.key}
                    x={left}
                    y={base - height + gap}
                    width={width}
                    height={Math.max(0, height - gap)}
                    rx={i === segs.length - 1 ? Math.min(2, width / 2) : 0}
                    className={l.stack}
                    data-group={segment.group.key}
                  />
                )
                base -= height
                return rect
              })}
            </g>
          )
        })
      }
    </SliceChart>
  )
}
