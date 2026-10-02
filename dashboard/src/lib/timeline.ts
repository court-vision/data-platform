import type { Schemas } from "@/lib/api"
import { parseUtc } from "@/lib/time"

export type CronRun = Schemas["CronJobRunEntry"]

const HOUR_MS = 60 * 60 * 1000

export const WINDOW_MS = 6 * HOUR_MS
export const TICK_MS = 30 * 60 * 1000

/** How far back the timeline looks. Six hours rides on the status payload; the rest are fetched. */
export interface TimelineRange {
  key: string
  hours: number
  /** "last …", for the card's description. */
  words: string
  tickMs: number
  /** Ticks name the day as well as the hour. */
  dayTicks: boolean
}

export const RANGES: readonly TimelineRange[] = [
  { key: "6h", hours: 6, words: "6 hours", tickMs: TICK_MS, dayTicks: false },
  { key: "24h", hours: 24, words: "24 hours", tickMs: 2 * HOUR_MS, dayTicks: false },
  // A day-and-hour label is wider than a clock: fewer ticks, so they do not collide.
  { key: "3d", hours: 72, words: "3 days", tickMs: 12 * HOUR_MS, dayTicks: true },
  { key: "7d", hours: 168, words: "7 days", tickMs: 24 * HOUR_MS, dayTicks: true },
]

export const DEFAULT_RANGE = RANGES[0]

export function parseRange(raw: string | null): TimelineRange {
  return RANGES.find((range) => range.key === raw) ?? DEFAULT_RANGE
}

export function rangeMs(range: TimelineRange): number {
  return range.hours * HOUR_MS
}

/** cron-runner's jobs in display order. The batch three always get a lane;
 * the rest appear when they have fired inside the window. */
export const LANES = ["pre-game", "live-stats", "post-game", "schedule-sync", "playoffs", "preseason-market"] as const
export const ALWAYS_SHOWN: ReadonlySet<string> = new Set(["pre-game", "live-stats", "post-game"])

export type RunTone = "success" | "failure" | "retried"

/** A retried success is worth a different colour: something was wrong for a while. */
export function runTone(run: Pick<CronRun, "result" | "attempts">): RunTone {
  if (run.result === "failure") return "failure"
  return run.attempts > 1 ? "retried" : "success"
}

/** Percent along the window (0 = window start, 100 = now), or null when outside it. */
export function markerPosition(triggeredAt: string, now: number, windowMs = WINDOW_MS): number | null {
  const at = parseUtc(triggeredAt)?.getTime()
  if (at == null) return null
  const start = now - windowMs
  if (at < start || at > now) return null
  return ((at - start) / windowMs) * 100
}

export interface Lane {
  job: string
  runs: CronRun[]
}

export function buildLanes(runs: CronRun[], now: number, windowMs = WINDOW_MS): Lane[] {
  const byJob = new Map<string, CronRun[]>()
  for (const run of runs) {
    if (markerPosition(run.triggered_at, now, windowMs) === null) continue
    const list = byJob.get(run.job_name) ?? []
    list.push(run)
    byJob.set(run.job_name, list)
  }

  const lanes: Lane[] = []
  for (const job of LANES) {
    const laneRuns = byJob.get(job) ?? []
    if (laneRuns.length > 0 || ALWAYS_SHOWN.has(job)) lanes.push({ job, runs: laneRuns })
    byJob.delete(job)
  }
  // A job this UI does not know still gets a lane, after the known ones.
  for (const [job, laneRuns] of byJob) lanes.push({ job, runs: laneRuns })
  return lanes
}

/** Tick timestamps from window start to now, every `tickMs`. */
export function axisTicks(now: number, windowMs = WINDOW_MS, tickMs = TICK_MS): number[] {
  const ticks: number[] = []
  for (let at = now - windowMs; at <= now; at += tickMs) ticks.push(at)
  return ticks
}

/** How many columns a lane is cut into: runs landing in one column are drawn as one mark. */
export const SLOTS = 96

const TONE_RANK: Record<RunTone, number> = { success: 0, retried: 1, failure: 2 }

/** The tone a group of runs is drawn in: its worst. */
export function worstTone(runs: Pick<CronRun, "result" | "attempts">[]): RunTone {
  let worst: RunTone = "success"
  for (const run of runs) {
    const tone = runTone(run)
    if (TONE_RANK[tone] > TONE_RANK[worst]) worst = tone
  }
  return worst
}

export interface Cluster {
  slot: number
  /** Percent along the window: a lone run's own moment, a group's column centre. */
  position: number
  /** Oldest first. */
  runs: CronRun[]
  tone: RunTone
}

/**
 * One lane's runs as marks. A job polled every minute puts hundreds of runs in
 * a lane; drawn one dot each they are a smear in which a failure is one pixel
 * among many. Runs sharing a column become one mark in the worst tone among
 * them, so a failure still shows at any range.
 */
export function clusterRuns(runs: CronRun[], now: number, windowMs = WINDOW_MS, slots = SLOTS): Cluster[] {
  const bySlot = new Map<number, { run: CronRun; position: number }[]>()
  for (const run of runs) {
    const position = markerPosition(run.triggered_at, now, windowMs)
    if (position === null) continue
    const slot = Math.min(slots - 1, Math.floor((position / 100) * slots))
    const list = bySlot.get(slot) ?? []
    list.push({ run, position })
    bySlot.set(slot, list)
  }
  return [...bySlot.entries()]
    .sort(([a], [b]) => a - b)
    .map(([slot, members]) => {
      members.sort((a, b) => a.position - b.position)
      const clustered = members.map((member) => member.run)
      return {
        slot,
        position: members.length === 1 ? members[0].position : ((slot + 0.5) / slots) * 100,
        runs: clustered,
        tone: worstTone(clustered),
      }
    })
}
