import { Chip, type Tone } from "@/components/blocks"
import type { FreshnessState } from "@/lib/freshness"
import type { PipelineState } from "@/lib/pipelines"

const STATES: Record<PipelineState, { label: string; tone: Tone }> = {
  running: { label: "Running", tone: "live" },
  failed: { label: "Failed", tone: "down" },
  success: { label: "OK", tone: "up" },
  never: { label: "Never run", tone: "quiet" },
  stuck: { label: "Stuck", tone: "down" },
}

export function StateBadge({ state }: { state: PipelineState }) {
  const { label, tone } = STATES[state]
  return <Chip tone={tone}>{label}</Chip>
}

/** The API's status words (jobs, quality runs, checks, cron results), toned. */
const TONES: Record<string, Tone> = {
  running: "live",
  pending: "warn",
  success: "up",
  completed: "up",
  passed: "up",
  failed: "down",
  failure: "down",
  error: "down",
  degraded: "down",
}

export function StatusBadge({ status, label }: { status: string | null | undefined; label?: string }) {
  const key = (status ?? "").toLowerCase()
  return <Chip tone={TONES[key] ?? "quiet"}>{label ?? (status || "unknown")}</Chip>
}

const FRESHNESS: Record<FreshnessState, { label: string; tone: Tone }> = {
  fresh: { label: "Fresh", tone: "up" },
  stale: { label: "Stale", tone: "down" },
  empty: { label: "Empty", tone: "warn" },
  idle: { label: "Idle", tone: "quiet" },
  unjudged: { label: "Not judged", tone: "quiet" },
  error: { label: "Error", tone: "down" },
}

export function FreshnessBadge({ state }: { state: FreshnessState }) {
  const { label, tone } = FRESHNESS[state]
  return <Chip tone={tone}>{label}</Chip>
}
