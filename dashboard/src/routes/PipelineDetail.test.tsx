import { describe, expect, test } from "bun:test"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { renderToStaticMarkup } from "react-dom/server"
import { MemoryRouter, Route, Routes } from "react-router"

import { pipelineRunsQuery } from "@/hooks/usePipelineRuns"
import { qualityOverviewQuery } from "@/hooks/useQuality"
import { ApiError, type Schemas } from "@/lib/api"
import type { QualityCheckRow, QualityOverview } from "@/lib/quality"
import type { PipelineInfo, PipelineRun } from "@/lib/runs"
import { PipelineDetail } from "@/routes/PipelineDetail"

// No DOM and no network: the page is rendered to markup from a query cache
// filled by hand, as the server would render it, and read as text.

const NAME = "lineup_alerts"

function run(overrides: Partial<PipelineRun> = {}): PipelineRun {
  return {
    id: "r",
    started_at: "2026-03-05T12:00:00",
    completed_at: "2026-03-05T12:00:12",
    status: "success",
    duration_seconds: 12,
    records_processed: 240,
    error_message: null,
    ...overrides,
  }
}

function payload(runs: PipelineRun[], pipeline: Partial<PipelineInfo> = {}, summary: Partial<Schemas["RunsSummary"]> = {}): Schemas["PipelineRunsData"] {
  return {
    pipeline: {
      name: NAME,
      display_name: "Lineup Alerts",
      description: "Tells managers who is out before lock.",
      category: "pre_game",
      target_table: "usr.notifications",
      trigger_endpoint: "/v1/internal/pipelines/lineup-alerts",
      accepts_date: false,
      force_on_run: false,
      cron_job: "pre-game",
      depends_on: [],
      allow_concurrent: false,
      espn_gated: false,
      earliest_run_time_cst: null,
      pre_game_window_minutes: 90,
      is_running: false,
      ...pipeline,
    },
    runs,
    summary: {
      total: runs.length,
      succeeded: 1,
      failed: 0,
      running: 0,
      stuck: 0,
      success_rate: 1,
      median_duration_seconds: 12,
      max_duration_seconds: 12,
      last_success_at: "2026-03-05T12:00:12",
      oldest_started_at: "2026-03-05T12:00:00",
      ...summary,
    },
    limit: 50,
    fetched_at: "2026-03-05T12:00:30Z",
  }
}

function render(client: QueryClient, search = ""): string {
  return renderToStaticMarkup(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[`/pipelines/${NAME}${search}`]}>
        <Routes>
          <Route path="pipelines/:name" element={<PipelineDetail />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

function loaded(data: Schemas["PipelineRunsData"], limit = 50): QueryClient {
  const client = new QueryClient()
  client.setQueryData(pipelineRunsQuery(NAME, limit).queryKey, data)
  return client
}

function runsTable(html: string): string {
  return html.slice(html.indexOf("<table"), html.indexOf("</table>"))
}

describe("PipelineDetail", () => {
  test("a stuck run wears the Overview's Stuck badge, with no duration still to come", () => {
    const stuck = run({ id: "hung", status: "stuck", started_at: "2026-03-02T12:00:00", completed_at: null, duration_seconds: null })
    const html = render(loaded(payload([run({ id: "ok" }), stuck], {}, { stuck: 1 })))
    const row = runsTable(html).split("<tr").find((cells) => cells.includes("Mar 2,"))!
    expect(row).toContain(">Stuck</div>")
    expect(row).toContain("text-status-loss") // the loss badge, not the green beacon of a live run
    expect(row).not.toContain("animate-beacon")
    expect(row).not.toContain("…")
    expect(html).toContain("0 failed · 1 stuck")
  })

  test("a live run keeps the running badge and the ellipsis", () => {
    const live = run({ id: "live", status: "running", completed_at: null, duration_seconds: null })
    const row = runsTable(render(loaded(payload([live])))).split("<tr").at(-1)!
    expect(row).toContain("animate-beacon")
    expect(row).toContain("…")
  })

  test("the pre-game window is the one the API sends, and none is made up", () => {
    expect(render(loaded(payload([run()])))).toContain("90 min before first tip-off")
    // The page used to fill a missing window with a literal 150.
    expect(render(loaded(payload([run()], { pre_game_window_minutes: null })))).not.toContain("min before first tip-off")
  })

  test("no timeout is listed among the facts: nothing enforces one", () => {
    expect(render(loaded(payload([run()])))).not.toContain("Timeout")
  })

  test("a limit whose fetch failed still offers the way back to another", () => {
    const client = loaded(payload([run()]))
    const failed = client.getQueryCache().build(client, { queryKey: pipelineRunsQuery(NAME, 100).queryKey })
    failed.setState({ status: "error", error: new ApiError(502, "Bad Gateway"), fetchStatus: "idle" })

    const html = render(client, "?limit=100")

    expect(html).toContain("Could not refresh: Bad Gateway")
    expect(html).toContain('aria-label="How many runs"')
    expect(html).toMatch(/aria-pressed="true"[^>]*>100</)
  })

  describe("the checks on this pipeline's data", () => {
    function check(name: string, results: Array<string | null>, overrides: Partial<QualityCheckRow> = {}): QualityCheckRow {
      return {
        name,
        severity: "warning",
        group: "consistency",
        table: "usr.notifications",
        against: ["nba.games"],
        pipelines: [NAME],
        failure_message: "they disagree",
        sql: "SELECT COUNT(*) FROM usr.notifications",
        results,
        ...overrides,
      }
    }

    function withQuality(checks: QualityCheckRow[], runIds = ["new", "old"]): QueryClient {
      const client = loaded(payload([run()]))
      const runs = runIds.map((run_id) => ({
        run_id, status: "failed", started_at: "2026-03-05T08:00:00", completed_at: null, duration_seconds: 1,
        total_checks: 2, passed_checks: 1, failed_checks: 1, triggered_by: "schedule", error_message: null,
      }))
      const overview: QualityOverview = { runs, checks, limit: 20, fetched_at: "2026-03-05T12:00:30Z" }
      client.setQueryData(qualityOverviewQuery(20).queryKey, overview)
      return client
    }

    test("lists the checks that judge this pipeline, each linking to the newest run that has it", () => {
      const html = render(withQuality([
        check("alerts_match_schedule", ["failed", "failed"]),
        check("alerts_ran", [null, "passed"], { group: "timing", against: [], table: "nba.pipeline_runs" }),
        check("someone_elses", ["failed"], { pipelines: ["team_stats"] }),
      ]))
      expect(html).toContain("Checks on this data")
      expect(html).toContain("1 of 2 failing")
      const failing = html.split("<li").find((item) => item.includes('data-check="alerts_match_schedule"'))!
      expect(failing).toContain("usr.notifications against nba.games")
      // Two runs are the whole history here, fewer than were asked for: the count is exact.
      expect(failing).toContain(">2 runs<")
      expect(failing).toContain('href="/quality/runs/new"')
      // Left out of the newest run: its result is the older run's.
      const timing = html.split("<li").find((item) => item.includes('data-check="alerts_ran"'))!
      expect(timing).toContain('href="/quality/runs/old"')
      expect(timing).toContain("ran in the last 24 hours")
      expect(html).not.toContain("someone_elses")
    })

    test("a streak that fills the window it is counted over is at least that long, and says so", () => {
      const window = Array.from({ length: 20 }, (_, i) => `r${i}`)
      const html = render(withQuality([
        check("alerts_match_schedule", window.map(() => "failed")),
        check("alerts_ran", ["failed", "failed", ...window.slice(2).map(() => "passed")]),
      ], window))
      const row = (name: string) => html.split("<li").find((item) => item.includes(`data-check="${name}"`))!
      // Older runs are not on the page: "20 runs" would read as the streak's length.
      expect(row("alerts_match_schedule")).toContain(">20+ runs<")
      // A pass ends a streak where it is, however full the window.
      expect(row("alerts_ran")).toContain(">2 runs<")
    })

    test("is absent while quality has not answered, and for a pipeline no check looks at", () => {
      expect(render(loaded(payload([run()])))).not.toContain("Checks on this data")
      expect(render(withQuality([check("someone_elses", ["passed"], { pipelines: ["team_stats"] })]))).not.toContain("Checks on this data")
    })
  })
})
