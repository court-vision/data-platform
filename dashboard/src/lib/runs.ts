import type { Schemas } from "@/lib/api"
import { parseUtc } from "@/lib/time"

export type PipelineRun = Schemas["PipelineRunEntry"]
export type RunsSummary = Schemas["RunsSummary"]
export type PipelineInfo = Schemas["PipelineInfo"]

export const LIMITS = [50, 100, 200] as const
export type Limit = (typeof LIMITS)[number]

export function parseLimit(raw: string | null): Limit {
  const n = Number(raw)
  return (LIMITS as readonly number[]).includes(n) ? (n as Limit) : 50
}

/** A round ceiling for an axis: 1, 2 or 5 times a power of ten, at or above `max`. */
export function niceMax(max: number): number {
  if (!(max > 0)) return 1
  const power = 10 ** Math.floor(Math.log10(max))
  for (const step of [1, 2, 5, 10]) {
    if (step * power >= max) return step * power
  }
  return 10 * power
}

/** Three gridline values: 0, half, top. */
export function ticks(top: number): [number, number, number] {
  return [0, top / 2, top]
}

export interface Bar {
  run: PipelineRun
  /** Seconds the bar stands for: the duration, or for a running run the time elapsed so far. */
  seconds: number
  x: number
  width: number
  y: number
  height: number
}

export interface ChartGeometry {
  bars: Bar[]        // oldest first, left to right
  top: number        // axis ceiling in seconds
  plotHeight: number
  width: number
}

export const BAR_MAX_WIDTH = 24
export const BAR_GAP = 2

/**
 * Lay the runs out as columns, oldest on the left. Every run gets a slot so a
 * gap in time is not drawn as a gap in the chart: the x axis is run order.
 */
export function chartGeometry(runs: PipelineRun[], now: number, width: number, plotHeight: number): ChartGeometry {
  const ordered = [...runs].reverse()
  const seconds = ordered.map((run) => runSeconds(run, now))
  const top = niceMax(Math.max(0, ...seconds))
  const slot = ordered.length > 0 ? width / ordered.length : width
  const barWidth = Math.max(1, Math.min(BAR_MAX_WIDTH, slot - BAR_GAP))
  const bars = ordered.map((run, i) => {
    const height = top > 0 ? (seconds[i] / top) * plotHeight : 0
    return {
      run,
      seconds: seconds[i],
      x: i * slot + (slot - barWidth) / 2,
      width: barWidth,
      y: plotHeight - height,
      height,
    }
  })
  return { bars, top, plotHeight, width }
}

export function runSeconds(run: PipelineRun, now: number): number {
  if (run.duration_seconds != null) return run.duration_seconds
  if (run.status === "running") {
    const started = parseUtc(run.started_at)?.getTime()
    return started == null ? 0 : Math.max(0, (now - started) / 1000)
  }
  return 0
}

/** Axis tick text: whole seconds stay whole ("25s"), minutes past 60. */
export function tickLabel(seconds: number): string {
  if (seconds === 0) return "0s"
  if (seconds < 1) return `${Math.round(seconds * 1000)}ms`
  if (seconds < 60) return Number.isInteger(seconds) ? `${seconds}s` : `${seconds.toFixed(1)}s`
  const minutes = Math.floor(seconds / 60)
  const rest = Math.round(seconds % 60)
  return rest === 0 ? `${minutes}m` : `${minutes}m ${rest}s`
}

/** Percent as the tiles show it, or a dash when nothing has finished. */
export function formatRate(rate: number | null | undefined): string {
  return rate == null ? "—" : `${Math.round(rate * 100)}%`
}

/** SVG path for a column with a 4px rounded top and a square base. */
export function columnPath(x: number, y: number, width: number, height: number, baseline: number, radius = 4): string {
  const r = Math.min(radius, width / 2, height)
  const right = x + width
  if (height <= 0) return ""
  return [
    `M${x},${baseline}`,
    `V${y + r}`,
    `Q${x},${y} ${x + r},${y}`,
    `H${right - r}`,
    `Q${right},${y} ${right},${y + r}`,
    `V${baseline}`,
    "Z",
  ].join(" ")
}
