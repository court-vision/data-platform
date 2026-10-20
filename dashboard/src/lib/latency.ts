import type { Schemas } from "@/lib/api"

export type LatencyOverview = Schemas["LatencyOverview"]
export type RouteLatency = Schemas["RouteLatency"]
export type RouteLatencyDetail = Schemas["RouteLatencyDetail"]
export type LatencyBin = Schemas["LatencyBin"]
export type SlowRequest = Schemas["SlowRequest"]
export type LatencyStats = Schemas["LatencyStats"]

export type LatencyWindow = "1h" | "24h" | "7d" | "30d"

export const WINDOWS: readonly { key: LatencyWindow; label: string }[] = [
  { key: "1h", label: "1h" },
  { key: "24h", label: "24h" },
  { key: "7d", label: "7d" },
  { key: "30d", label: "30d" },
]

export function parseWindow(raw: string | null): LatencyWindow {
  return WINDOWS.some((option) => option.key === raw) ? (raw as LatencyWindow) : "24h"
}

export type Sort = "time" | "p95" | "requests"

export const SORTS: readonly { key: Sort; label: string }[] = [
  { key: "time", label: "Total time" },
  { key: "p95", label: "p95" },
  { key: "requests", label: "Requests" },
]

export function parseSort(raw: string | null): Sort {
  return SORTS.some((option) => option.key === raw) ? (raw as Sort) : "time"
}

/** The time a route cost the service over the window. */
export function totalTime(route: RouteLatency): number {
  return route.requests * (route.mean_ms ?? 0)
}

export function sortRoutes(routes: RouteLatency[], sort: Sort): RouteLatency[] {
  const value = (route: RouteLatency) =>
    sort === "p95" ? route.p95_ms ?? 0 : sort === "requests" ? route.requests : totalTime(route)
  return [...routes].sort((a, b) => value(b) - value(a) || a.route.localeCompare(b.route))
}

/**
 * The colour groups a request's time is drawn in. The backend's buckets
 * (backend/core/request_timing.py) are finer; the groups keep the chart to
 * eight hues and fold the small ones (Clerk, Redis, any other HTTP) into
 * Other, which is grey. The order is the palette's slot order, validated so
 * that neighbours in a stack stay apart for colour-blind readers: keep it.
 */
export interface Group {
  key: string
  label: string
  buckets: readonly string[]
  /** What the group's time is, for the key and the table. */
  note: string
}

export const GROUPS: readonly Group[] = [
  { key: "espn", label: "ESPN", buckets: ["espn"], note: "ESPN's fantasy API" },
  { key: "postgres", label: "Postgres", buckets: ["postgres"], note: "queries, from checkout to the last row read" },
  { key: "app", label: "Our code", buckets: ["app"], note: "the backend's own Python, between calls" },
  { key: "wait", label: "Queued", buckets: ["wait"], note: "waiting for a free DB, CPU or provider slot" },
  { key: "nba", label: "NBA", buckets: ["nba"], note: "the NBA's stats API and CDN" },
  { key: "yahoo", label: "Yahoo", buckets: ["yahoo"], note: "Yahoo's fantasy API" },
  { key: "services", label: "Our services", buckets: ["features", "sqlmate", "fantasy_writer"], note: "features, sqlmate, fantasy-writer" },
  { key: "claude", label: "Claude", buckets: ["claude"], note: "the Claude API" },
  { key: "other", label: "Other", buckets: ["clerk", "redis", "other_http"], note: "Clerk, Redis, any other HTTP" },
]

const GROUP_OF = new Map(GROUPS.flatMap((group) => group.buckets.map((bucket) => [bucket, group.key] as const)))

/** A bucket's colour group; a bucket the page does not know yet is Other. */
export function groupOf(bucket: string): string {
  return GROUP_OF.get(bucket) ?? "other"
}

export interface Segment {
  group: Group
  ms: number
}

/** Parts (ms by bucket) as the groups' segments, in group order, empty ones left out. */
export function segments(parts: Record<string, number>): Segment[] {
  const totals = new Map<string, number>()
  for (const [bucket, ms] of Object.entries(parts)) {
    if (ms > 0) totals.set(groupOf(bucket), (totals.get(groupOf(bucket)) ?? 0) + ms)
  }
  return GROUPS.filter((group) => totals.has(group.key)).map((group) => ({ group, ms: totals.get(group.key) ?? 0 }))
}

/** The groups any of these parts use, in group order: what a key has to name. */
export function groupsUsed(partsList: Record<string, number>[]): Group[] {
  const used = new Set(partsList.flatMap((parts) => segments(parts).map((segment) => segment.group.key)))
  return GROUPS.filter((group) => used.has(group.key))
}

/** Time outside our own code and queues: what the providers and services cost. */
export function externalShare(parts: Record<string, number>): number | null {
  const total = Object.values(parts).reduce((sum, ms) => sum + ms, 0)
  if (total <= 0) return null
  const own = (parts.app ?? 0) + (parts.wait ?? 0)
  return (total - own) / total
}

/** 4 ms, 86 ms, 1.24 s, 12.4 s. */
export function formatMs(ms: number | null | undefined): string {
  if (ms == null) return "—"
  if (ms < 10) return `${ms.toFixed(1)} ms`
  if (ms < 1000) return `${Math.round(ms)} ms`
  if (ms < 10_000) return `${(ms / 1000).toFixed(2)} s`
  return `${(ms / 1000).toFixed(1)} s`
}

export function formatShare(share: number | null | undefined): string {
  if (share == null) return "—"
  if (share > 0 && share < 0.01) return "<1%"
  return `${Math.round(share * 100)}%`
}

/** "1.2 calls", "3 calls": a mean per request. */
export function formatCalls(calls: number | null | undefined): string {
  if (!calls) return "—"
  const rounded = calls >= 10 ? Math.round(calls) : Math.round(calls * 10) / 10
  return `${rounded} ${rounded === 1 ? "call" : "calls"}`
}

/** The route page's address for one route, keeping the window. */
export function routeHref(route: Pick<RouteLatency, "method" | "route">, window: LatencyWindow): string {
  const params = new URLSearchParams({ method: route.method, route: route.route })
  if (window !== "24h") params.set("window", window)
  return `/latency/route?${params.toString()}`
}

/** One row per bucket for a route's mean request: the table under the anatomy, biggest first. */
export function bucketRows(
  parts: Record<string, number>,
  calls: Record<string, number>,
): { bucket: string; group: Group; ms: number; share: number; calls: number }[] {
  const total = Object.values(parts).reduce((sum, ms) => sum + ms, 0)
  const groups = new Map(GROUPS.map((group) => [group.key, group]))
  return Object.entries(parts)
    .filter(([, ms]) => ms > 0)
    .map(([bucket, ms]) => ({
      bucket,
      group: groups.get(groupOf(bucket)) as Group,
      ms,
      share: total > 0 ? ms / total : 0,
      calls: calls[bucket] ?? 0,
    }))
    .sort((a, b) => b.ms - a.ms)
}

/** Names the backend's buckets as the table shows them. */
export const BUCKET_LABELS: Record<string, string> = {
  espn: "ESPN",
  yahoo: "Yahoo",
  nba: "NBA",
  claude: "Claude",
  clerk: "Clerk",
  other_http: "Other HTTP",
  features: "features",
  sqlmate: "sqlmate",
  fantasy_writer: "fantasy-writer",
  postgres: "Postgres",
  redis: "Redis",
  wait: "Queued",
  app: "Our code",
}

export function bucketLabel(bucket: string): string {
  return BUCKET_LABELS[bucket] ?? bucket
}

/** A round top for a millisecond axis whose half is round too: 1, 2, 2.5 or 5 times a power of ten. */
export function niceMs(max: number): number {
  if (!(max > 0)) return 10
  const power = 10 ** Math.floor(Math.log10(max))
  const step = [1, 2, 2.5, 5, 10].find((m) => m * power >= max) ?? 10
  return step * power
}

/** Axis text for a millisecond tick: "0", "250ms", "1.5s". */
export function msTick(ms: number): string {
  if (ms === 0) return "0"
  if (ms < 1000) return `${Math.round(ms)}ms`
  const seconds = ms / 1000
  return Number.isInteger(seconds) ? `${seconds}s` : `${seconds.toFixed(1)}s`
}

const sliceClock = new Intl.DateTimeFormat("en-US", { timeZone: "America/Chicago", hour: "numeric", minute: "2-digit" })
const sliceDayClock = new Intl.DateTimeFormat("en-US", { timeZone: "America/Chicago", weekday: "short", hour: "numeric" })
const sliceDay = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", month: "short", day: "numeric" })

/**
 * When a slice of the series starts, at its width's grain: a clock time in
 * Central for the hour's five-minute slices, a weekday and hour for hourly and
 * six-hourly ones (a day's ends share a clock time), and the date for daily
 * ones, which are UTC days.
 */
export function formatSlice(iso: string, binSeconds: number): string {
  const ms = Date.parse(iso)
  if (binSeconds >= 86_400) return `${sliceDay.format(ms)} UTC`
  if (binSeconds >= 3600) return sliceDayClock.format(ms)
  return sliceClock.format(ms)
}
