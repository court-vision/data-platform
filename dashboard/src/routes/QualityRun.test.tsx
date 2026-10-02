import { describe, expect, test } from "bun:test"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { renderToStaticMarkup } from "react-dom/server"
import { MemoryRouter, Route, Routes } from "react-router"

import { qualityRunQuery } from "@/hooks/useQuality"
import { ApiError } from "@/lib/api"
import type { QualityCheckInfo, QualityOutcome, QualityRunDetail } from "@/lib/quality"
import { QualityRun } from "@/routes/QualityRun"

const RUN_ID = "00000000-0000-0000-0000-000000000007"

const DEFINITION: QualityCheckInfo = {
  name: "ranges_valid",
  severity: "critical",
  group: "structural",
  table: "nba.player_game_stats",
  against: [],
  pipelines: ["player_game_stats"],
  failure_message: "player_game_stats contains out-of-range values",
  sql: "SELECT COUNT(*)\nFROM nba.player_game_stats\nWHERE pts < 0",
}

function outcome(overrides: Partial<QualityOutcome> = {}): QualityOutcome {
  return {
    check_name: "ranges_valid",
    status: "passed",
    severity: "critical",
    failures: 0,
    message: null,
    details: null,
    duration_ms: 12,
    definition: DEFINITION,
    ...overrides,
  }
}

function detail(checks: QualityOutcome[], overrides: Partial<QualityRunDetail> = {}): QualityRunDetail {
  return {
    run: {
      run_id: RUN_ID,
      status: "failed",
      started_at: "2026-03-05T08:00:00",
      completed_at: "2026-03-05T08:00:48",
      duration_seconds: 48,
      total_checks: checks.length,
      passed_checks: checks.filter((c) => c.status === "passed").length,
      failed_checks: checks.filter((c) => c.status !== "passed").length,
      triggered_by: "schedule",
      error_message: null,
    },
    checks,
    older_run_id: "older",
    newer_run_id: null,
    fetched_at: "2026-03-05T12:00:30Z",
    ...overrides,
  }
}

function render(client: QueryClient): string {
  return renderToStaticMarkup(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[`/quality/runs/${RUN_ID}`]}>
        <Routes>
          <Route path="quality/runs/:runId" element={<QualityRun />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

function loaded(data: QualityRunDetail): QueryClient {
  const client = new QueryClient()
  client.setQueryData(qualityRunQuery(RUN_ID).queryKey, data)
  return client
}

/** One check's row, as markup. */
function row(html: string, name: string): string {
  const found = html.split("<li").find((item) => item.includes(`data-check="${name}"`))
  if (!found) throw new Error(`no row for ${name}`)
  return found
}

const FAILED = outcome({ status: "failed", failures: 37, message: "player_game_stats contains out-of-range values", details: { failures: 37 } })
const BROKEN = outcome({
  check_name: "stale_running",
  status: "error",
  severity: "warning",
  failures: 1,
  message: "check execution error: relation does not exist",
  details: { error: 'relation "nba.pipeline_runs" does not exist' },
})
const PASSED = outcome({ check_name: "no_orphans" })
const RETIRED = outcome({ check_name: "a_check_since_removed", definition: null })

describe("QualityRun", () => {
  test("a failing check starts open with its message, count and query; a pass starts closed", () => {
    const html = render(loaded(detail([FAILED, PASSED])))
    const failed = row(html, "ranges_valid")
    expect(failed).toContain("<details open")
    expect(failed).toContain(">37<")
    expect(failed).toContain("player_game_stats contains out-of-range values")
    expect(failed).toContain("WHERE pts &lt; 0")
    expect(failed).toContain('href="/pipelines/player_game_stats"')
    const passed = row(html, "no_orphans")
    expect(passed).not.toContain("<details open")
    expect(passed).toContain(">—<") // no failure count on a pass
  })

  test("details beyond the count are shown; the count alone is not repeated", () => {
    const html = render(loaded(detail([FAILED, BROKEN])))
    expect(row(html, "stale_running")).toContain("relation &quot;nba.pipeline_runs&quot; does not exist")
    expect(row(html, "ranges_valid")).not.toContain("&quot;failures&quot;")
  })

  test("a failed consistency check shows the rows it kept, and what it compares", () => {
    const lagging = outcome({
      check_name: "season_keeps_pace",
      status: "failed",
      severity: "warning",
      failures: 24,
      message: "season games played and the game log have moved apart this week",
      details: {
        failures: 24,
        sample: [
          { player: "Devin Booker", season_row: "2026-04-08", season_gp: 63, games_logged: 64, gap: -1 },
          { player: "New Guy", season_row: null, season_gp: 0, games_logged: 1, gap: -1 },
        ],
      },
      definition: {
        ...DEFINITION,
        name: "season_keeps_pace",
        severity: "warning",
        group: "consistency",
        table: "nba.player_season_stats",
        against: ["nba.player_game_stats"],
        pipelines: ["player_season_stats", "player_game_stats"],
      },
    })
    const html = row(render(loaded(detail([lagging]))), "season_keeps_pace")
    expect(html).toContain("The first 2 of 24 offending rows")
    expect(html).toMatch(/<th[^>]*>player<\/th><th[^>]*>season_row<\/th>/)
    expect(html).toMatch(/<td[^>]*>Devin Booker<\/td><td[^>]*>2026-04-08<\/td><td[^>]*>63<\/td><td[^>]*>64<\/td><td[^>]*>-1<\/td>/)
    expect(html).toMatch(/<td[^>]*>New Guy<\/td><td[^>]*>—<\/td>/) // a null cell is a visible blank
    // The sample is a table, not a JSON blob beside it.
    expect(html).not.toContain("&quot;player&quot;")
    expect(html).toContain("against nba.player_game_stats")
    expect(html).toContain('href="/pipelines/player_season_stats"')
    expect(html).toContain('href="/pipelines/player_game_stats"')
  })

  test("a sample that is the whole failure says so", () => {
    const one = outcome({ status: "failed", failures: 1, details: { failures: 1, sample: [{ team: "GSW", score: 44 }] } })
    expect(row(render(loaded(detail([one]))), "ranges_valid")).toContain("The offending row")
    const two = outcome({ status: "failed", failures: 2, details: { failures: 2, sample: [{ team: "GSW" }, { team: "LAL" }] } })
    expect(row(render(loaded(detail([two]))), "ranges_valid")).toContain("All 2 offending rows")
  })

  test("a check removed from the code since keeps its result and says so", () => {
    const html = render(loaded(detail([RETIRED])))
    expect(row(html, "a_check_since_removed")).toContain("no longer defined in the code")
  })

  test("the tiles split this run's failures by severity", () => {
    const html = render(loaded(detail([FAILED, BROKEN, PASSED])))
    expect(html).toMatch(/Checks<\/dt><dd[^>]*>3</)
    expect(html).toMatch(/Critical failing<\/dt><dd[^>]*text-status-loss[^>]*>1</)
    expect(html).toMatch(/Warnings failing<\/dt><dd[^>]*text-status-projected[^>]*>1</)
  })

  test("the header says when, how long, and who triggered it; and steps to the neighbours that exist", () => {
    const html = render(loaded(detail([PASSED])))
    expect(html).toContain("Mar 5, 2:00:00 AM CT")
    expect(html).toContain("took 48.0s")
    expect(html).toContain("triggered by schedule")
    expect(html).toContain('href="/quality/runs/older"')
    expect(html).toContain('aria-label="Newer run: none"')
  })

  test("a run that failed as a whole says why", () => {
    const base = detail([PASSED])
    const html = render(loaded({ ...base, run: { ...base.run, error_message: "connection lost" } }))
    expect(html).toContain("The run itself failed: connection lost")
  })

  test("an unknown run is a 404 page with the way back", () => {
    const client = new QueryClient()
    const query = client.getQueryCache().build(client, { queryKey: qualityRunQuery(RUN_ID).queryKey })
    query.setState({ status: "error", error: new ApiError(404, "Quality run not found"), fetchStatus: "idle" })
    const html = render(client)
    expect(html).toContain("No quality run with that id")
    expect(html).toContain('href="/quality"')
  })
})
