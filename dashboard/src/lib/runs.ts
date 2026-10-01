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

const MINUTE = 60
const HOUR = 3600
// Ceilings past a minute, in the unit the tick is read in. Each is even in
// that unit or halves to a round one, so the middle tick reads "5m" or "3h":
// a ceiling picked in seconds gave "8m 20s" and "16m 40s", wider than the axis.
const MINUTE_STEPS = [1, 2, 4, 6, 10, 20, 30, 60]
const HOUR_STEPS = [2, 4, 6, 12, 24]

/** 1, 2 or 5 times a power of ten, at or above `max`. */
function decimalCeiling(max: number): number {
  const power = 10 ** Math.floor(Math.log10(max))
  for (const step of [1, 2, 5, 10]) {
    if (step * power >= max) return step * power
  }
  return 10 * power
}

/**
 * A round ceiling for an axis, at or above `max`, whose half is round too in
 * the unit it is read in: seconds up to 50, then minutes, then hours.
 */
export function niceMax(max: number): number {
  if (!(max > 0)) return 1
  const decimal = decimalCeiling(max)
  if (decimal < MINUTE) return decimal
  const minutes = MINUTE_STEPS.find((step) => step * MINUTE >= max)
  if (minutes != null) return minutes * MINUTE
  const hours = HOUR_STEPS.find((step) => step * HOUR >= max)
  return (hours ?? 2 * decimalCeiling(max / (2 * HOUR))) * HOUR
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
  /**
   * Drawn to the top of the plot rather than to scale: a stuck run, which has
   * no length to draw, or a failed one that ran past the axis.
   */
  over: boolean
}

export interface ChartGeometry {
  bars: Bar[]        // oldest first, left to right
  top: number        // axis ceiling in seconds
  plotHeight: number
  width: number
}

export const BAR_MAX_WIDTH = 24
export const BAR_GAP = 2
/** The shortest a bar is drawn, so a run that failed in 50ms still shows its colour. */
export const BAR_MIN_HEIGHT = 2

/**
 * Lay the runs out as columns, oldest on the left. Every run gets a slot so a
 * gap in time is not drawn as a gap in the chart: the x axis is run order.
 */
export function chartGeometry(runs: PipelineRun[], now: number, width: number, plotHeight: number): ChartGeometry {
  const ordered = [...runs].reverse()
  const seconds = ordered.map((run) => runSeconds(run, now))
  const top = niceMax(longest(ordered, seconds))
  const slot = ordered.length > 0 ? width / ordered.length : width
  const barWidth = Math.max(1, Math.min(BAR_MAX_WIDTH, slot - BAR_GAP))
  const bars = ordered.map((run, i) => {
    const over = run.status === "stuck" || seconds[i] > top
    const height = over ? plotHeight : Math.max(BAR_MIN_HEIGHT, (seconds[i] / top) * plotHeight)
    return {
      run,
      seconds: seconds[i],
      x: i * slot + (slot - barWidth) / 2,
      width: barWidth,
      y: plotHeight - height,
      height,
      over,
    }
  })
  return { bars, top, plotHeight, width }
}

/**
 * The longest run the axis has to reach: of the runs that worked and the ones
 * still going. A failed run does not set it. One the service swept at startup
 * "lasted" until the next boot, hours after it hung, and would flatten every
 * other bar for as long as it stayed in the window; it is drawn `over` instead.
 * With no run of the first kind, the failed ones are all there is to scale to.
 */
function longest(runs: PipelineRun[], seconds: number[]): number {
  const of = (...statuses: string[]) =>
    Math.max(0, ...seconds.filter((_, i) => statuses.includes(runs[i].status)))
  return of("success", "running") || of("failed")
}

/**
 * A stuck run (the API's word for a row left `running` past the cutoff) has no
 * length: it is not `now - started_at` and growing, it stopped at a time
 * nobody recorded.
 */
export function runSeconds(run: PipelineRun, now: number): number {
  if (run.duration_seconds != null) return run.duration_seconds
  if (run.status === "running") {
    const started = parseUtc(run.started_at)?.getTime()
    return started == null ? 0 : Math.max(0, (now - started) / 1000)
  }
  return 0
}

/**
 * Where a key moves the chart's one tab stop, from bar `index` of `count`;
 * null for a key that is not the chart's.
 */
export function roveIndex(key: string, index: number, count: number): number | null {
  switch (key) {
    case "ArrowLeft":
      return Math.max(0, index - 1)
    case "ArrowRight":
      return Math.min(count - 1, index + 1)
    case "Home":
      return 0
    case "End":
      return count - 1
    default:
      return null
  }
}

/** Axis tick text: whole seconds stay whole ("25s"), minutes past 60, hours past 60 of those. */
export function tickLabel(seconds: number): string {
  if (seconds === 0) return "0s"
  if (seconds < 1) return `${Math.round(seconds * 1000)}ms`
  if (seconds < MINUTE) return Number.isInteger(seconds) ? `${seconds}s` : `${seconds.toFixed(1)}s`
  if (seconds < HOUR) {
    const minutes = Math.floor(seconds / MINUTE)
    const rest = Math.round(seconds % MINUTE)
    return rest === 0 ? `${minutes}m` : `${minutes}m ${rest}s`
  }
  const hours = Math.floor(seconds / HOUR)
  const rest = Math.round((seconds % HOUR) / MINUTE)
  return rest === 0 ? `${hours}h` : `${hours}h ${rest}m`
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
