import { useCallback, useState, type KeyboardEvent } from "react"

import { featureLabel, featuresUsed, formatUsd, niceTop, type Day, type FeatureKey } from "@/lib/aiSpend"
import { roveIndex } from "@/lib/runs"
import s from "./sheet.module.css"
import c from "./chart.module.css"
import a from "./aiSpend.module.css"

const PLOT_WIDTH = 756
const PLOT_HEIGHT = 120
const PLOT_TOP = 8 // room for the top tick's label
const HEIGHT = PLOT_TOP + PLOT_HEIGHT + 4
const GAP = 2 // the surface between stacked segments
const RADIUS = 2

/** "Nov 20" from "2026-11-20", read as the UTC day it is. */
export function dayLabel(day: string): string {
  return new Date(`${day}T00:00:00Z`).toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" })
}

export function describeDay(day: Day): string {
  if (day.total === 0) return `${dayLabel(day.day)} · nothing spent`
  const parts = (Object.entries(day.parts) as [FeatureKey, number][])
    .filter(([, cost]) => cost > 0)
    .sort((x, y) => y[1] - x[1])
    .map(([feature, cost]) => `${featureLabel(feature)} ${formatUsd(cost)}`)
  const open = day.open > 0 ? ` (${formatUsd(day.open)} still open)` : ""
  return `${dayLabel(day.day)} · ${formatUsd(day.total)}${open} · ${parts.join(" · ")}`
}

/** A column's top segment: square at the bottom, rounded at the data end. */
function topPath(x: number, y: number, width: number, height: number): string {
  const r = Math.min(RADIUS, width / 2, height)
  return `M${x} ${y + height}V${y + r}Q${x} ${y} ${x + r} ${y}H${x + width - r}Q${x + width} ${y} ${x + width} ${y + r}V${y + height}Z`
}

/**
 * Spend per UTC day, oldest on the left, each column split by feature in the
 * palette's order. Colour is the feature, so the key names each one, and the
 * caption and the tables under the chart carry the same numbers in text.
 */
export function SpendChart({ days }: { days: Day[] }) {
  const [hovered, setHovered] = useState<string | null>(null)
  const [cursor, setCursor] = useState<string | null>(null)

  // As in DurationChart: Chrome sends no `focusin` for SVG elements, so focus
  // is caught on the way down with the native events.
  const attach = useCallback((svg: SVGSVGElement | null) => {
    if (!svg) return
    const onFocus = (event: FocusEvent) => {
      const day = (event.target as Element | null)?.closest<SVGGElement>("[data-day]")?.dataset.day ?? null
      setHovered(day)
      if (day) setCursor(day)
    }
    const onBlur = () => setHovered(null)
    svg.addEventListener("focus", onFocus, true)
    svg.addEventListener("blur", onBlur, true)
    return () => {
      svg.removeEventListener("focus", onFocus, true)
      svg.removeEventListener("blur", onBlur, true)
    }
  }, [])

  if (days.length === 0 || days.every((day) => day.total === 0)) {
    return <p className={s.empty}>Nothing spent in this window.</p>
  }

  const top = niceTop(Math.max(...days.map((day) => day.total)))
  const slot = PLOT_WIDTH / days.length
  const width = Math.max(2, Math.min(28, slot * 0.7))
  const used = featuresUsed(days)
  const hoveredDay = days.find((day) => day.day === hovered)
  const at = days.findIndex((day) => day.day === cursor)
  const tabStop = at === -1 ? days.length - 1 : at
  const total = days.reduce((sum, day) => sum + day.total, 0)

  function onKeyDown(event: KeyboardEvent<SVGGElement>) {
    if (event.altKey || event.ctrlKey || event.metaKey) return
    const next = roveIndex(event.key, tabStop, days.length)
    if (next == null) return
    event.preventDefault()
    ;(event.currentTarget.children[next] as SVGGElement | undefined)?.focus()
  }

  const gridlines = [0, top / 2, top]
  const y = (value: number) => PLOT_TOP + PLOT_HEIGHT - (value / top) * PLOT_HEIGHT

  return (
    <figure className={`${c.figure} ${a.viz}`}>
      <div className={c.plotRow}>
        <div className={c.axis} aria-hidden>
          {gridlines.map((value) => (
            <span key={value} className={c.axisLabel} style={{ top: `${(y(value) / HEIGHT) * 100}%` }}>
              {formatUsd(value).replace(/\.00$/, "")}
            </span>
          ))}
        </div>
        <svg
          ref={attach}
          viewBox={`0 0 ${PLOT_WIDTH} ${HEIGHT}`}
          role="group"
          tabIndex={-1}
          aria-label={`AI spend per day over the last ${days.length} days, oldest first. Arrow keys move between days.`}
          className={`${c.plot} ${a.plot}`}
          data-hovering={hovered ? "true" : undefined}
          onMouseLeave={() => setHovered(null)}
        >
          {gridlines.map((value) => (
            <line key={value} x1={0} x2={PLOT_WIDTH} y1={y(value)} y2={y(value)} className={c.gridline} strokeWidth={1} vectorEffect="non-scaling-stroke" />
          ))}
          <g onKeyDown={onKeyDown}>
            {days.map((day, index) => {
              const x = index * slot + (slot - width) / 2
              const stack = used.filter((feature) => (day.parts[feature.key] ?? 0) > 0)
              let base = PLOT_TOP + PLOT_HEIGHT
              return (
                <g
                  key={day.day}
                  data-day={day.day}
                  data-active={hovered === day.day ? "true" : undefined}
                  role="img"
                  tabIndex={index === tabStop ? 0 : -1}
                  aria-label={describeDay(day)}
                  className={a.column}
                  onMouseEnter={() => setHovered(day.day)}
                >
                  <rect className={a.hit} x={index * slot} y={PLOT_TOP} width={slot} height={PLOT_HEIGHT} fill="transparent" />
                  {stack.map((feature, i) => {
                    const full = ((day.parts[feature.key] ?? 0) / top) * PLOT_HEIGHT
                    // The gap comes out of the segment above it, so the column keeps its height
                    const gap = i > 0 ? GAP : 0
                    const height = Math.max(full - gap, 0.75)
                    const segmentTop = base - gap - height
                    base = segmentTop
                    const last = i === stack.length - 1
                    return last ? (
                      <path key={feature.key} data-feature={feature.key} className={a.segment} d={topPath(x, segmentTop, width, height)} />
                    ) : (
                      <rect key={feature.key} data-feature={feature.key} className={a.segment} x={x} y={segmentTop} width={width} height={height} />
                    )
                  })}
                </g>
              )
            })}
          </g>
        </svg>
      </div>

      <figcaption className={c.caption}>
        <span>
          {hoveredDay
            ? describeDay(hoveredDay)
            : `${dayLabel(days[0].day)} → ${dayLabel(days[days.length - 1].day)} · ${formatUsd(total)} in all`}
        </span>
        <ul className={s.legend} aria-label="Key">
          {used.map((feature) => (
            <li key={feature.key} title={feature.note}>
              <span className={a.swatch} data-feature={feature.key} aria-hidden />
              {feature.label}
            </li>
          ))}
        </ul>
      </figcaption>
    </figure>
  )
}
