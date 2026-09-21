import { Badge } from "@/components/ui/badge"
import type { PipelineState } from "@/lib/pipelines"

const STATES: Record<PipelineState, { label: string; variant: "live" | "loss" | "win" | "neutral" }> = {
  running: { label: "Running", variant: "live" },
  failed: { label: "Failed", variant: "loss" },
  success: { label: "OK", variant: "win" },
  never: { label: "Never run", variant: "neutral" },
}

export function StateBadge({ state }: { state: PipelineState }) {
  const { label, variant } = STATES[state]
  return <Badge variant={variant}>{label}</Badge>
}
