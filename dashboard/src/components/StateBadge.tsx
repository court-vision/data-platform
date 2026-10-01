import type { ComponentProps } from "react"

import { Badge } from "@/components/ui/badge"
import type { FreshnessState } from "@/lib/freshness"
import type { PipelineState } from "@/lib/pipelines"

type Variant = NonNullable<ComponentProps<typeof Badge>["variant"]>

const STATES: Record<PipelineState, { label: string; variant: Variant }> = {
  running: { label: "Running", variant: "live" },
  failed: { label: "Failed", variant: "loss" },
  success: { label: "OK", variant: "win" },
  never: { label: "Never run", variant: "neutral" },
  stuck: { label: "Stuck", variant: "loss" },
}

export function StateBadge({ state }: { state: PipelineState }) {
  const { label, variant } = STATES[state]
  return <Badge variant={variant}>{label}</Badge>
}

/** The API's status words (jobs, quality runs, checks, cron results), toned. */
const TONES: Record<string, Variant> = {
  running: "live",
  pending: "projected",
  success: "win",
  completed: "win",
  passed: "win",
  failed: "loss",
  failure: "loss",
  error: "loss",
  degraded: "loss",
}

export function StatusBadge({ status, label }: { status: string | null | undefined; label?: string }) {
  const key = (status ?? "").toLowerCase()
  return <Badge variant={TONES[key] ?? "neutral"}>{label ?? (status || "unknown")}</Badge>
}

const FRESHNESS: Record<FreshnessState, { label: string; variant: Variant }> = {
  fresh: { label: "Fresh", variant: "win" },
  stale: { label: "Stale", variant: "loss" },
  empty: { label: "Empty", variant: "projected" },
  idle: { label: "Idle", variant: "neutral" },
  unjudged: { label: "Not judged", variant: "neutral" },
  error: { label: "Error", variant: "loss" },
}

export function FreshnessBadge({ state }: { state: FreshnessState }) {
  const { label, variant } = FRESHNESS[state]
  return <Badge variant={variant}>{label}</Badge>
}
