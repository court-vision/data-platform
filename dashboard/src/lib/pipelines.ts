import type { PipelineHealth } from "@/hooks/useDashboardStatus"

export type PipelineState = "running" | "stuck" | "failed" | "success" | "never"

/**
 * One word for a pipeline's health; `running` wins over whatever ran last.
 *
 * `stuck` is a run still marked running after the backend stopped counting it
 * as live (PipelineRun.is_running gives up after two hours): it hung, and
 * nothing rewrites the row until the service restarts.
 */
export function pipelineState(pipeline: PipelineHealth): PipelineState {
  if (pipeline.is_running) return "running"
  if (pipeline.last_status === "running") return "stuck"
  if (pipeline.last_status === "failed") return "failed"
  if (pipeline.last_status === "success") return "success"
  return pipeline.last_run_at ? "success" : "never"
}

export type CategoryKey = "live" | "pre_game" | "post_game" | "scheduled"

export interface Category {
  key: CategoryKey
  label: string
  description: string
}

/** Display order: what is time-critical tonight first. Mirrors PipelineCategory. */
export const CATEGORIES: readonly Category[] = [
  { key: "live", label: "Live", description: "Every ~60s during active game windows" },
  { key: "pre_game", label: "Pre-game", description: "Timed against first tip-off" },
  { key: "post_game", label: "Post-game", description: "After the night's games and ESPN's flip" },
  { key: "scheduled", label: "Scheduled", description: "Fixed schedule, own cron job" },
]

export interface CategoryGroup extends Category {
  pipelines: PipelineHealth[]
}

/** Group by category in display order; a category the UI does not know goes last. */
export function groupByCategory(pipelines: PipelineHealth[]): CategoryGroup[] {
  const known = new Set<string>(CATEGORIES.map((category) => category.key))
  const groups: CategoryGroup[] = CATEGORIES.map((category) => ({
    ...category,
    pipelines: pipelines.filter((pipeline) => pipeline.category === category.key),
  }))
  const unknown = pipelines.filter((pipeline) => !known.has(pipeline.category))
  if (unknown.length > 0) {
    groups.push({
      key: "scheduled",
      label: "Other",
      description: "Category this dashboard does not know yet",
      pipelines: unknown,
    })
  }
  return groups.filter((group) => group.pipelines.length > 0)
}

export interface HealthSummary {
  total: number
  healthy: number
  failing: number
  running: number
  neverRun: number
}

export function summarize(pipelines: PipelineHealth[]): HealthSummary {
  const summary: HealthSummary = { total: pipelines.length, healthy: 0, failing: 0, running: 0, neverRun: 0 }
  for (const pipeline of pipelines) {
    const state = pipelineState(pipeline)
    if (state === "running") summary.running += 1
    else if (state === "failed" || state === "stuck") summary.failing += 1
    else if (state === "success") summary.healthy += 1
    else summary.neverRun += 1
  }
  return summary
}
