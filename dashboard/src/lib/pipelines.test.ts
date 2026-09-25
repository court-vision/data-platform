import { describe, expect, test } from "bun:test"

import type { PipelineHealth } from "@/hooks/useDashboardStatus"
import { groupByCategory, pipelineState, summarize } from "@/lib/pipelines"

function pipeline(overrides: Partial<PipelineHealth> = {}): PipelineHealth {
  return {
    name: "player_game_stats",
    display_name: "Player Game Stats",
    category: "post_game",
    trigger_endpoint: "/v1/internal/pipelines/daily-player-stats",
    last_run_at: "2026-09-21T07:04:11",
    last_status: "success",
    last_duration_seconds: 12.5,
    last_records_processed: 240,
    last_success_at: "2026-09-21T07:04:23",
    is_running: false,
    error_streak: 0,
    accepts_date: true,
    ...overrides,
  }
}

describe("pipelineState", () => {
  test("running wins over the previous run's failure", () => {
    expect(pipelineState(pipeline({ is_running: true, last_status: "failed" }))).toBe("running")
  })

  test("failed and success follow last_status", () => {
    expect(pipelineState(pipeline({ last_status: "failed" }))).toBe("failed")
    expect(pipelineState(pipeline())).toBe("success")
  })

  test("a run still marked running after the backend gave up on it is stuck, not OK", () => {
    expect(pipelineState(pipeline({ is_running: false, last_status: "running" }))).toBe("stuck")
  })

  test("no run at all is `never`, not a failure", () => {
    expect(pipelineState(pipeline({ last_status: null, last_run_at: null }))).toBe("never")
  })
})

describe("groupByCategory", () => {
  test("orders live, pre-game, post-game, scheduled and drops empty groups", () => {
    const groups = groupByCategory([
      pipeline({ name: "a", category: "scheduled" }),
      pipeline({ name: "b", category: "post_game" }),
      pipeline({ name: "c", category: "live" }),
    ])
    expect(groups.map((group) => group.label)).toEqual(["Live", "Post-game", "Scheduled"])
  })

  test("a category the UI does not know is still shown", () => {
    const groups = groupByCategory([pipeline({ name: "x", category: "weekly" })])
    expect(groups.map((group) => group.label)).toEqual(["Other"])
    expect(groups[0].pipelines).toHaveLength(1)
  })
})

test("summarize counts each pipeline once", () => {
  const summary = summarize([
    pipeline(),
    pipeline({ last_status: "failed" }),
    pipeline({ is_running: true }),
    pipeline({ last_status: null, last_run_at: null }),
    pipeline({ last_status: "running" }),
  ])
  expect(summary).toEqual({ total: 5, healthy: 1, failing: 2, running: 1, neverRun: 1 })
})
