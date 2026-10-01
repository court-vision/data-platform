import { describe, expect, test } from "bun:test"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { renderToStaticMarkup } from "react-dom/server"
import { MemoryRouter, Route, Routes } from "react-router"

import { pipelineRunsQuery } from "@/hooks/usePipelineRuns"
import { ApiError, type Schemas } from "@/lib/api"
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
})
