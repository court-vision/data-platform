import type { Schemas } from "@/lib/api"
import { parseUtc } from "@/lib/time"

export type CronRun = Schemas["CronJobRunEntry"]

export const WINDOW_MS = 6 * 60 * 60 * 1000
export const TICK_MS = 30 * 60 * 1000

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
