import { relativeTime } from "@/lib/time"

/** "updated 12s ago · next in 18s": the poll, made visible. */
export function RefreshNote({
  updatedAt,
  fetching,
  now,
  intervalMs,
}: {
  updatedAt: number
  fetching: boolean
  now: number
  intervalMs: number
}) {
  if (!updatedAt) return null
  const next = Math.max(0, Math.ceil((updatedAt + intervalMs - now) / 1000))
  return (
    <p className="font-mono text-xs text-muted-foreground" aria-live="off">
      updated {relativeTime(new Date(updatedAt).toISOString(), now)}
      <span className="mx-1.5 text-border">·</span>
      {fetching ? "refreshing…" : `next in ${next}s`}
    </p>
  )
}
