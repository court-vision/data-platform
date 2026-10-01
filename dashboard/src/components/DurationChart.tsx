import { useCallback, useState, type KeyboardEvent } from "react"

import { chartGeometry, columnPath, roveIndex, tickLabel, ticks, type Bar, type PipelineRun } from "@/lib/runs"
import { formatCentralLong, formatDuration } from "@/lib/time"
import { cn } from "@/lib/utils"

const PLOT_WIDTH = 756
const PLOT_HEIGHT = 120
const PLOT_TOP = 8 // room for the top tick's label
const HEIGHT = PLOT_TOP + PLOT_HEIGHT + 4

const TONE: Record<string, string> = {
  success: "fill-status-win",
  failed: "fill-status-loss",
  running: "fill-signal-live stroke-signal-live",
  stuck: "fill-status-loss stroke-status-loss",
}

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
    return <p className="py-6 text-center text-sm text-muted-foreground">No runs recorded.</p>
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
    <figure className="flex flex-col gap-2">
      <div className="flex">
        {/* The tick labels are HTML beside the plot, not text inside it: the
            svg scales with the card, and on a phone 10px of its viewBox is
            under 4px of screen. */}
        <div className="relative w-11 shrink-0 font-mono text-[10px] leading-none text-muted-foreground" aria-hidden>
          {gridlines.map((value) => (
            <span
              key={value}
              className="absolute right-1.5 -translate-y-1/2 whitespace-nowrap"
              style={{ top: `${(gridlineY(value, geometry.top) / HEIGHT) * 100}%` }}
            >
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
          className="h-auto min-w-0 flex-1"
          onMouseLeave={() => setHovered(null)}
        >
          {gridlines.map((value) => {
            const y = gridlineY(value, geometry.top)
            return (
              <line
                key={value}
                x1={0}
                x2={PLOT_WIDTH}
                y1={y}
                y2={y}
                className="stroke-border"
                strokeWidth={1}
                vectorEffect="non-scaling-stroke"
              />
            )
          })}
          <g transform={`translate(0 ${PLOT_TOP})`} onKeyDown={onKeyDown}>
            {bars.map((bar, index) => {
              const active = hovered === bar.run.id
              const unfinished = UNFINISHED.has(bar.run.status)
              return (
                <g
                  key={bar.run.id}
                  data-run={bar.run.id}
                  role="img"
                  tabIndex={index === tabStop ? 0 : -1}
                  aria-label={describe(bar)}
                  className="outline-none"
                  onMouseEnter={() => setHovered(bar.run.id)}
                >
                  {/* The hit target is the whole slot, wider than the bar; focusable so the
                      keyboard can walk the runs and read each one in the caption. */}
                  <rect x={bar.x - 1} y={0} width={bar.width + 2} height={PLOT_HEIGHT} fill="transparent" />
                  <path
                    d={columnPath(bar.x, bar.y, bar.width, bar.height, PLOT_HEIGHT)}
                    className={cn(TONE[bar.run.status] ?? "fill-muted-foreground", active && "opacity-100", hovered && !active && "opacity-50")}
                    strokeDasharray={unfinished ? "3 2" : undefined}
                    strokeWidth={unfinished ? 1.5 : 0}
                    fillOpacity={unfinished ? 0.35 : 1}
                  />
                  {/* A finished run past the axis: a break in the bar says it goes on. */}
                  {bar.over && !unfinished && (
                    <rect x={bar.x - 1} y={5} width={bar.width + 2} height={2.5} className="fill-card" />
                  )}
                </g>
              )
            })}
          </g>
        </svg>
      </div>

      <figcaption className="flex flex-wrap items-center justify-between gap-x-4 gap-y-1 font-mono text-xs text-muted-foreground">
        <span>{hoveredBar ? describe(hoveredBar) : `${formatCentralLong(oldest.started_at)} → ${formatCentralLong(newest.started_at)}`}</span>
        <span className="flex items-center gap-3" aria-label="Key">
          <Key className="bg-status-win" label="success" />
          <Key className="bg-status-loss" label="failed" />
          <Key className="border border-dashed border-signal-live bg-signal-live/35" label="running (so far)" />
          {bars.some((bar) => bar.run.status === "stuck") && (
            <Key className="border border-dashed border-status-loss bg-status-loss/35" label="stuck" />
          )}
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

function Key({ className, label }: { className: string; label: string }) {
  return (
    <span className="flex items-center gap-1.5">
      <span className={cn("inline-block size-2.5 rounded-sm", className)} aria-hidden />
      {label}
    </span>
  )
}
