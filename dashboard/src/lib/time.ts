/**
 * The API serializes naive-UTC datetimes (`2026-09-21T07:04:11`, no offset).
 * `new Date()` reads that as local time, so every timestamp goes through here.
 */
export function parseUtc(iso: string | null | undefined): Date | null {
  if (!iso) return null
  const hasZone = /[Zz]$|[+-]\d{2}:?\d{2}$/.test(iso)
  const date = new Date(hasZone ? iso : `${iso}Z`)
  return Number.isNaN(date.getTime()) ? null : date
}

export function relativeTime(iso: string | null | undefined, now = Date.now()): string {
  const date = parseUtc(iso)
  if (!date) return "—"
  const seconds = Math.floor((now - date.getTime()) / 1000)
  if (seconds < 5) return "just now"
  if (seconds < 60) return `${seconds}s ago`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`
  return `${Math.floor(seconds / 86400)}d ago`
}

export function formatDuration(seconds: number | null | undefined): string {
  if (seconds == null) return "—"
  if (seconds < 1) return `${Math.round(seconds * 1000)}ms`
  if (seconds < 60) return `${seconds.toFixed(1)}s`
  const minutes = Math.floor(seconds / 60)
  return `${minutes}m ${Math.round(seconds % 60)}s`
}

const centralTime = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/Chicago",
  month: "short",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
})

/**
 * Absolute time in Central, the zone the pipelines' own gates are written in.
 * Assembled from parts: engines join date and time differently ("," vs " at ").
 */
export function formatCentral(iso: string | null | undefined): string {
  const date = parseUtc(iso)
  if (!date) return "—"
  const parts = Object.fromEntries(
    centralTime.formatToParts(date).map((part) => [part.type, part.value]),
  )
  return `${parts.month} ${parts.day}, ${parts.hour}:${parts.minute} ${parts.dayPeriod} CT`
}

const centralClock = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/Chicago",
  hour: "numeric",
  minute: "2-digit",
})

/** "2:30 PM" in Central, for axis ticks. */
export function formatClockCentral(ms: number): string {
  return centralClock.format(new Date(ms))
}

const centralLong = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/Chicago",
  month: "short",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
  second: "2-digit",
})

/** As formatCentral, with seconds: the exact moment a cron job fired. */
export function formatCentralLong(iso: string | null | undefined): string {
  const date = parseUtc(iso)
  if (!date) return "—"
  const parts = Object.fromEntries(centralLong.formatToParts(date).map((part) => [part.type, part.value]))
  return `${parts.month} ${parts.day}, ${parts.hour}:${parts.minute}:${parts.second} ${parts.dayPeriod} CT`
}

/** "3d 4h", "2h 10m", "5m", "40s": how long a process has been up. */
export function formatUptime(seconds: number | null | undefined): string {
  if (seconds == null) return "—"
  const days = Math.floor(seconds / 86400)
  const hours = Math.floor((seconds % 86400) / 3600)
  const minutes = Math.floor((seconds % 3600) / 60)
  if (days > 0) return `${days}d ${hours}h`
  if (hours > 0) return `${hours}h ${minutes}m`
  if (minutes > 0) return `${minutes}m`
  return `${Math.floor(seconds)}s`
}

const dayFormat = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", month: "short", day: "numeric" })

/**
 * "Mar 4" for a plain `YYYY-MM-DD`. A calendar date has no zone, so it is
 * formatted in UTC: read as local time it would shift a day west of Greenwich.
 */
export function formatDay(day: string | null | undefined): string {
  if (!day) return "—"
  const date = new Date(`${day}T00:00:00Z`)
  return Number.isNaN(date.getTime()) ? "—" : dayFormat.format(date)
}

/** Whole days from one plain date to another (negative when `to` is earlier). */
export function daysBetween(from: string, to: string): number {
  return Math.round((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / 86_400_000)
}
