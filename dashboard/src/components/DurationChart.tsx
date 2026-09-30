import { useEffect, useId, useRef, useState } from "react"

import { chartGeometry, columnPath, tickLabel, ticks, type Bar, type PipelineRun } from "@/lib/runs"
import { formatCentralLong, formatDuration } from "@/lib/time"
import { cn } from "@/lib/utils"

const WIDTH = 800
const PLOT_HEIGHT = 120
const AXIS_WIDTH = 44
const PLOT_TOP = 8 // room for the top tick's label
const HEIGHT = PLOT_TOP + PLOT_HEIGHT + 4

const TONE: Record<string, string> = {
  success: "fill-status-win",
  failed: "fill-status-loss",
  running: "fill-signal-live",
}

/**
 * Seconds per run, oldest on the left. Status is the colour, so a key names
 * each colour and the hover line and the table carry the same facts in text.
 */
export function DurationChart({ runs, now }: { runs: PipelineRun[]; now: number }) {
  const [hovered, setHovered] = useState<Bar | null>(null)
  const id = useId()
  const geometry = chartGeometry(runs, now, WIDTH - AXIS_WIDTH, PLOT_HEIGHT)
  const gridlines = ticks(geometry.top)

  // Keyboard focus. React's onFocus listens for `focusin`, which Chrome does
  // not dispatch when an SVG element takes focus, so the native `focus` and
  // `blur` events are caught on the way down instead.
  const svgRef = useRef<SVGSVGElement>(null)
  const barsRef = useRef(geometry.bars)
  barsRef.current = geometry.bars
  useEffect(() => {
    const svg = svgRef.current
    if (!svg) return
    const onFocus = (event: FocusEvent) => {
      const runId = (event.target as Element | null)?.closest<SVGGElement>("[data-run]")?.dataset.run
      setHovered(barsRef.current.find((bar) => bar.run.id === runId) ?? null)
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

  const oldest = geometry.bars[0].run
  const newest = geometry.bars[geometry.bars.length - 1].run

  return (
    <figure className="flex flex-col gap-2">
      <svg
        ref={svgRef}
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        role="img"
        tabIndex={-1} // Chrome makes an outer <svg> a tab stop; the bars are the stops
        aria-labelledby={`${id}-title`}
        className="h-auto w-full"
        onMouseLeave={() => setHovered(null)}
      >
        <title id={`${id}-title`}>Duration of the last {runs.length} runs, oldest first</title>
        {gridlines.map((value) => {
          const y = PLOT_TOP + PLOT_HEIGHT - (value / geometry.top) * PLOT_HEIGHT
          return (
            <g key={value}>
              <line x1={AXIS_WIDTH} x2={WIDTH} y1={y} y2={y} className="stroke-border" strokeWidth={1} />
              <text x={AXIS_WIDTH - 6} y={y} dy={3} textAnchor="end" className="fill-muted-foreground font-mono text-[10px]">
                {tickLabel(value)}
              </text>
            </g>
          )
        })}
        <g transform={`translate(${AXIS_WIDTH} ${PLOT_TOP})`}>
          {geometry.bars.map((bar) => {
            const active = hovered?.run.id === bar.run.id
            const running = bar.run.status === "running"
            return (
              <g
                key={bar.run.id}
                data-run={bar.run.id}
                tabIndex={0}
                aria-label={describe(bar)}
                className="outline-none"
                onMouseEnter={() => setHovered(bar)}
              >
                {/* The hit target is the whole slot, wider than the bar; focusable so the
                    keyboard can walk the runs and read each one in the caption. */}
                <rect x={bar.x - 1} y={0} width={bar.width + 2} height={PLOT_HEIGHT} fill="transparent" />
                <path
                  d={columnPath(bar.x, bar.y, bar.width, Math.max(bar.height, 2), PLOT_HEIGHT)}
                  className={cn(TONE[bar.run.status] ?? "fill-muted-foreground", active && "opacity-100", hovered && !active && "opacity-50")}
                  strokeDasharray={running ? "3 2" : undefined}
                  strokeWidth={running ? 1.5 : 0}
                  fillOpacity={running ? 0.35 : 1}
                  stroke={running ? "hsl(var(--signal-live))" : undefined}
                />
              </g>
            )
          })}
        </g>
      </svg>

      <figcaption className="flex flex-wrap items-center justify-between gap-x-4 gap-y-1 font-mono text-xs text-muted-foreground">
        <span>{hovered ? describe(hovered) : `${formatCentralLong(oldest.started_at)} → ${formatCentralLong(newest.started_at)}`}</span>
        <span className="flex items-center gap-3" aria-label="Key">
          <Key className="bg-status-win" label="success" />
          <Key className="bg-status-loss" label="failed" />
          <Key className="border border-dashed border-signal-live bg-signal-live/35" label="running (so far)" />
        </span>
      </figcaption>
    </figure>
  )
}

function describe(bar: Bar): string {
  const { run } = bar
  const records = run.status === "success" ? ` · ${run.records_processed.toLocaleString()} records` : ""
  const error = run.error_message ? ` · ${run.error_message.slice(0, 60)}` : ""
  return `${formatCentralLong(run.started_at)} · ${run.status} · ${formatDuration(bar.seconds)}${records}${error}`
}

function Key({ className, label }: { className: string; label: string }) {
  return (
    <span className="flex items-center gap-1.5">
      <span className={cn("inline-block size-2.5 rounded-sm", className)} aria-hidden />
      {label}
    </span>
  )
}
