import { useCallback, useState, type KeyboardEvent } from "react"

import { chartGeometry, columnPath, roveIndex, tickLabel, ticks, type Bar, type PipelineRun } from "@/lib/runs"
import { formatCentralLong, formatDuration } from "@/lib/time"
import s from "./sheet.module.css"
import c from "./chart.module.css"

const PLOT_WIDTH = 756
const PLOT_HEIGHT = 120
const PLOT_TOP = 8 // room for the top tick's label
const HEIGHT = PLOT_TOP + PLOT_HEIGHT + 4

/** A run with no end yet, or none it will ever have: drawn dashed and hollow. */
const UNFINISHED = new Set(["running", "stuck"])

/**
 * Seconds per run, oldest on the left. Status is the colour, so a key names
 * each colour and the hover line and the table carry the same facts in text.
 */
export function DurationChart({ runs, now }: { runs: PipelineRun[]; now: number }) {
  const [hovered, setHovered] = useState<string | null>(null)
  // The chart is one tab stop, not one per run: Tab lands on this bar and the
  // arrow keys move it. The newest run until then.
  const [cursor, setCursor] = useState<string | null>(null)
  const geometry = chartGeometry(runs, now, PLOT_WIDTH, PLOT_HEIGHT)
  const gridlines = ticks(geometry.top)

  // Keyboard focus. React's onFocus listens for `focusin`, which Chrome does
  // not dispatch when an SVG element takes focus, so the native `focus` and
  // `blur` events are caught on the way down instead. From a ref callback, not
  // a mount effect: a pipeline with no runs yet renders no svg, and the
  // listeners have to arrive with the element when its first run does.
  const attach = useCallback((svg: SVGSVGElement | null) => {
    if (!svg) return
    const onFocus = (event: FocusEvent) => {
      const runId = (event.target as Element | null)?.closest<SVGGElement>("[data-run]")?.dataset.run ?? null
      setHovered(runId)
      if (runId) setCursor(runId)
    }
    const onBlur = () => setHovered(null)
    svg.addEventListener("focus", onFocus, true)
    svg.addEventListener("blur", onBlur, true)
    return () => {
      svg.removeEventListener("focus", onFocus, true)
      svg.removeEventListener("blur", onBlur, true)
    }
  }, [])

  if (runs.length === 0) {
    return <p className={s.empty}>No runs recorded.</p>
  }

  const { bars } = geometry
  const oldest = bars[0].run
  const newest = bars[bars.length - 1].run
  const hoveredBar = bars.find((bar) => bar.run.id === hovered)
  const at = bars.findIndex((bar) => bar.run.id === cursor)
  const tabStop = at === -1 ? bars.length - 1 : at

  function onKeyDown(event: KeyboardEvent<SVGGElement>) {
    if (event.altKey || event.ctrlKey || event.metaKey) return
    const next = roveIndex(event.key, tabStop, bars.length)
    if (next == null) return
    event.preventDefault() // the arrows, Home and End would scroll the page
    ;(event.currentTarget.children[next] as SVGGElement | undefined)?.focus()
  }

  return (
    <figure className={c.figure}>
      <div className={c.plotRow}>
        {/* The tick labels are HTML beside the plot, not text inside it: the
            svg scales with the page, and on a phone 10px of its viewBox is
            under 4px of screen. */}
        <div className={c.axis} aria-hidden>
          {gridlines.map((value) => (
            <span key={value} className={c.axisLabel} style={{ top: `${(gridlineY(value, geometry.top) / HEIGHT) * 100}%` }}>
              {tickLabel(value)}
            </span>
          ))}
        </div>
        <svg
          ref={attach}
          viewBox={`0 0 ${PLOT_WIDTH} ${HEIGHT}`}
          role="group"
          tabIndex={-1} // Chrome makes an outer <svg> a tab stop; the bar under the cursor is the stop
          aria-label={`Duration of the last ${runs.length} runs, oldest first. Arrow keys move between runs.`}
          className={c.plot}
          data-hovering={hovered ? "true" : undefined}
          onMouseLeave={() => setHovered(null)}
        >
          {gridlines.map((value) => {
            const y = gridlineY(value, geometry.top)
            return <line key={value} x1={0} x2={PLOT_WIDTH} y1={y} y2={y} className={c.gridline} strokeWidth={1} vectorEffect="non-scaling-stroke" />
          })}
          <g transform={`translate(0 ${PLOT_TOP})`} onKeyDown={onKeyDown}>
            {bars.map((bar, index) => {
              const unfinished = UNFINISHED.has(bar.run.status)
              return (
                <g
                  key={bar.run.id}
                  data-run={bar.run.id}
                  data-status={bar.run.status}
                  data-active={hovered === bar.run.id ? "true" : undefined}
                  role="img"
                  tabIndex={index === tabStop ? 0 : -1}
                  aria-label={describe(bar)}
                  className={c.bar}
                  onMouseEnter={() => setHovered(bar.run.id)}
                >
                  {/* The hit target is the whole slot, wider than the bar; focusable so the
                      keyboard can walk the runs and read each one in the caption. */}
                  <rect x={bar.x - 1} y={0} width={bar.width + 2} height={PLOT_HEIGHT} fill="transparent" />
                  <path
                    d={columnPath(bar.x, bar.y, bar.width, bar.height, PLOT_HEIGHT)}
                    className={c.column}
                    strokeDasharray={unfinished ? "3 2" : undefined}
                    strokeWidth={unfinished ? 1.5 : 0}
                    fillOpacity={unfinished ? 0.35 : 1}
                  />
                  {/* A finished run past the axis: a break in the bar says it goes on. */}
                  {bar.over && !unfinished && <rect x={bar.x - 1} y={5} width={bar.width + 2} height={2.5} className={c.break} />}
                </g>
              )
            })}
          </g>
        </svg>
      </div>

      <figcaption className={c.caption}>
        <span>{hoveredBar ? describe(hoveredBar) : `${formatCentralLong(oldest.started_at)} → ${formatCentralLong(newest.started_at)}`}</span>
        <span className={s.legend} aria-label="Key">
          <Key status="success" label="success" />
          <Key status="failed" label="failed" />
          <Key status="running" label="running (so far)" />
          {bars.some((bar) => bar.run.status === "stuck") && <Key status="stuck" label="stuck" />}
        </span>
      </figcaption>
    </figure>
  )
}

function gridlineY(value: number, top: number): number {
  return PLOT_TOP + PLOT_HEIGHT - (value / top) * PLOT_HEIGHT
}

function describe(bar: Bar): string {
  const { run } = bar
  const started = formatCentralLong(run.started_at)
  const error = run.error_message ? ` · ${run.error_message.slice(0, 60)}` : ""
  // No duration to give: the bar is a marker, not a measure.
  if (run.status === "stuck") return `${started} · stuck · never finished${error}`
  const records = run.status === "success" ? ` · ${run.records_processed.toLocaleString()} records` : ""
  const scale = bar.over ? " (off the scale)" : ""
  return `${started} · ${run.status} · ${formatDuration(bar.seconds)}${scale}${records}${error}`
}

function Key({ status, label }: { status: string; label: string }) {
  return (
    <span>
      <span className={c.keySwatch} data-status={status} aria-hidden />
      {label}
    </span>
  )
}
