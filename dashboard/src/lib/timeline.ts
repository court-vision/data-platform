import type { Schemas } from "@/lib/api"
import { parseUtc } from "@/lib/time"

export type CronRun = Schemas["CronJobRunEntry"]
/** One job's runs in one column, counted on the server. */
export type RunBucket = Schemas["SchedulerBucket"]

/**
 * Past a day the reply counts the window's runs per job and column instead of
 * carrying them all: a week of 30-second polls is fourteen thousand rows. The
 * runs that do come are the ones a mark can open.
 */
export interface Counted {
  buckets: RunBucket[]
  /** A column's width. */
  bucketMs: number
}

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
  /** The runs in hand: every run in the window, or with `counted` only the ones a mark can open. */
  runs: CronRun[]
  /** The job's counted columns that reach into the window; null when every run is in hand. */
  counted: Counted | null
  /** How many runs the window holds. */
  count: number
}

export function buildLanes(runs: CronRun[], now: number, windowMs = WINDOW_MS, counted: Counted | null = null): Lane[] {
  const byJob = new Map<string, Lane>()
  const empty = (job: string): Lane => ({ job, runs: [], counted: counted && { ...counted, buckets: [] }, count: 0 })
  const laneFor = (job: string) => {
    const lane = byJob.get(job) ?? empty(job)
    byJob.set(job, lane)
    return lane
  }
  if (counted) {
    for (const bucket of counted.buckets) {
      if (columnSpan(bucket, now, windowMs, counted.bucketMs) === null) continue
      const lane = laneFor(bucket.job_name)
      lane.counted?.buckets.push(bucket)
      lane.count += bucket.runs
    }
    // A run belongs to its column, and the column is what is in the window or out of it.
    for (const run of runs) byJob.get(run.job_name)?.runs.push(run)
  } else {
    for (const run of runs) {
      if (markerPosition(run.triggered_at, now, windowMs) === null) continue
      const lane = laneFor(run.job_name)
      lane.runs.push(run)
      lane.count += 1
    }
  }

  const lanes: Lane[] = []
  for (const job of LANES) {
    const lane = byJob.get(job)
    if (lane || ALWAYS_SHOWN.has(job)) lanes.push(lane ?? empty(job))
    byJob.delete(job)
  }
  // A job this UI does not know still gets a lane, after the known ones.
  for (const lane of byJob.values()) lanes.push(lane)
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
  /** Percent of the window a group's mark spans: its column, or the part of it inside the window. */
  width: number
  /**
   * Oldest first. Every run of the group, or of a counted column the ones
   * that came with it: its newest, and its newest few that failed or were
   * retried.
   */
  runs: CronRun[]
  tone: RunTone
  /** How many runs the group holds, and how many of them failed or were retried. */
  count: number
  failed: number
  retried: number
  /** When the first and the last of them fired. */
  from: string
  to: string
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
        width: 100 / slots,
        runs: clustered,
        tone: worstTone(clustered),
        count: clustered.length,
        failed: clustered.filter((run) => runTone(run) === "failure").length,
        retried: clustered.filter((run) => runTone(run) === "retried").length,
        from: clustered[0].triggered_at,
        to: clustered[clustered.length - 1].triggered_at,
      }
    })
}

/**
 * The part of a counted column that is inside the window, in percent along it:
 * its centre and its width. Null when the column is outside the window.
 *
 * Columns are cut from the epoch and the window from now, so the first and the
 * last column reach past the lane. Each is drawn as the part inside it: moved
 * in whole instead, the newest column would sit on top of the one before it
 * and hide that one's failures.
 */
function columnSpan(bucket: RunBucket, now: number, windowMs: number, bucketMs: number): { position: number; width: number } | null {
  const from = parseUtc(bucket.start)?.getTime()
  if (from == null) return null
  const start = now - windowMs
  if (from + bucketMs <= start || from > now) return null
  const left = Math.max(from, start)
  const right = Math.min(from + bucketMs, now)
  return { position: ((left + right) / 2 - start) / windowMs * 100, width: ((right - left) / windowMs) * 100 }
}

/**
 * One lane's marks from its counted columns: one mark a column, in the worst
 * tone the counts hold, with the runs that came for it. A run alone in its
 * column keeps its own moment, as it does when every run is in hand.
 */
export function clusterBuckets(counted: Counted, runs: CronRun[], now: number, windowMs = WINDOW_MS): Cluster[] {
  const { buckets, bucketMs } = counted
  // Columns are cut from the epoch, on the server and here.
  const inHand = new Map<number, { run: CronRun; at: number }[]>()
  for (const run of runs) {
    const at = parseUtc(run.triggered_at)?.getTime()
    if (at == null) continue
    const column = Math.floor(at / bucketMs)
    const list = inHand.get(column) ?? []
    list.push({ run, at })
    inHand.set(column, list)
  }

  const clusters: Cluster[] = []
  for (const bucket of buckets) {
    const from = parseUtc(bucket.start)?.getTime()
    const span = columnSpan(bucket, now, windowMs, bucketMs)
    if (from == null || span === null) continue
    const slot = Math.round(from / bucketMs)
    const mine = (inHand.get(slot) ?? []).sort((a, b) => a.at - b.at).map((member) => member.run)
    const alone = bucket.runs === 1 && mine.length === 1
    const position = alone ? markerPosition(mine[0].triggered_at, now, windowMs) : span.position
    if (position === null) continue
    clusters.push({
      slot,
      position,
      width: span.width,
      runs: mine,
      tone: bucket.failed > 0 ? "failure" : bucket.retried > 0 ? "retried" : "success",
      count: bucket.runs,
      failed: bucket.failed,
      retried: bucket.retried,
      from: bucket.first_triggered_at,
      to: bucket.last_triggered_at,
    })
  }
  return clusters.sort((a, b) => a.slot - b.slot)
}
