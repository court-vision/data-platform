import { describe, expect, test } from "bun:test"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { renderToStaticMarkup } from "react-dom/server"
import { MemoryRouter, Route, Routes } from "react-router"

import { qualityOverviewQuery } from "@/hooks/useQuality"
import type { QualityCheckRow, QualityOverview, QualityRun } from "@/lib/quality"
import { Quality } from "@/routes/Quality"

// No DOM and no network: the page is rendered to markup from a query cache
// filled by hand, and read as text.

function run(id: string, overrides: Partial<QualityRun> = {}): QualityRun {
  return {
    run_id: id,
    status: "success",
    started_at: "2026-03-05T08:00:00",
    completed_at: "2026-03-05T08:00:48",
    duration_seconds: 48,
    total_checks: 2,
    passed_checks: 2,
    failed_checks: 0,
    triggered_by: "schedule",
    error_message: null,
    ...overrides,
  }
}

function check(name: string, results: Array<string | null>, overrides: Partial<QualityCheckRow> = {}): QualityCheckRow {
  return {
    name,
    severity: "critical",
    group: "structural",
    table: "nba.player_game_stats",
    pipelines: ["player_game_stats"],
    failure_message: "player_game_stats contains out-of-range values",
    sql: "SELECT COUNT(*)\nFROM nba.player_game_stats\nWHERE pts < 0",
    results,
    ...overrides,
  }
}

// Newest first, as the API sends them.
const RUNS = [
  run("new", { status: "failed", started_at: "2026-03-05T08:00:00", passed_checks: 0, failed_checks: 2, triggered_by: "dashboard" }),
  run("mid", { status: "failed", started_at: "2026-03-04T08:00:00", passed_checks: 1, failed_checks: 1 }),
  run("old", { started_at: "2026-03-03T08:00:00" }),
]

const DATA: QualityOverview = {
  runs: RUNS,
  checks: [
    check("ranges_valid", ["failed", "failed", "passed"]),
    check("ownership_ran_within_24h", ["error", "passed", null], {
      severity: "warning",
      group: "timing",
      table: "nba.pipeline_runs",
      pipelines: ["player_ownership"],
      failure_message: "Player Ownership has not run successfully in the last 24 hours",
    }),
  ],
  limit: 20,
  fetched_at: "2026-03-05T12:00:30Z",
}

function render(data: QualityOverview | null, search = ""): string {
  const client = new QueryClient()
  const limit = search.includes("limit=50") ? 50 : 20
  if (data) client.setQueryData(qualityOverviewQuery(limit).queryKey, data)
  return renderToStaticMarkup(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[`/quality${search}`]}>
        <Routes>
          <Route path="quality" element={<Quality />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

/** One check's row in the matrix, as markup. */
function matrixRow(html: string, name: string): string {
  const row = html.split("<li").find((cells) => cells.includes(`title="${name}"`))
  if (!row) throw new Error(`no matrix row for ${name}`)
  return row
}

describe("Quality", () => {
  test("the matrix reads oldest to newest, one cell per run, each a link to that run", () => {
    const row = matrixRow(render(DATA), "ranges_valid")
    // React Router writes href after the element's own attributes.
    const cells = [...row.matchAll(/data-result="(\w+)"[^>]*href="\/quality\/runs\/(\w+)"/g)].map((m) => [m[2], m[1]])
    expect(cells).toEqual([
      ["old", "passed"],
      ["mid", "failed"],
      ["new", "failed"],
    ])
  })

  test("a cell says what it is in words, and is not a tab stop", () => {
    const row = matrixRow(render(DATA), "ownership_ran_within_24h")
    expect(row).toContain("could not run")
    expect(row).toContain("not in this run")
    expect(row).toContain('data-result="none"')
    expect(row).not.toContain('tabindex="0"')
    expect(row.match(/tabindex="-1"/g)).toHaveLength(3)
  })

  test("a failing check says for how many runs, toned by severity", () => {
    const html = render(DATA)
    expect(matrixRow(html, "ranges_valid")).toContain("critical · 2 runs")
    const warning = matrixRow(html, "ownership_ran_within_24h")
    expect(warning).toContain("warning · 1 run")
    expect(warning).toContain("text-status-projected")
  })

  test("the tiles read the newest run, split by severity", () => {
    const html = render(DATA)
    expect(html).toContain(">0/2<") // passing
    expect(html).toMatch(/Critical failing<\/dt><dd[^>]*text-status-loss[^>]*>1</)
    expect(html).toMatch(/Warnings failing<\/dt><dd[^>]*text-status-projected[^>]*>1</)
    expect(html).toContain("failed · dashboard")
  })

  test("every check's definition is on the page: table, pipeline link, the SQL", () => {
    const html = render(DATA)
    expect(html).toContain("What each check asserts")
    expect(html).toContain('href="/pipelines/player_ownership"')
    expect(html).toContain("WHERE pts &lt; 0")
    expect(html).toContain("Fails when </span>player_game_stats contains out-of-range values")
    // Grouped: structural before timing.
    expect(html.indexOf("Structural check definitions")).toBeLessThan(html.indexOf("Timing check definitions"))
  })

  test("the runs table links each run and marks its failures", () => {
    const html = render(DATA)
    const table = html.slice(html.indexOf("<table"), html.indexOf("</table>"))
    expect(table.match(/href="\/quality\/runs\/(new|mid|old)"/g)).toHaveLength(3)
    expect(table).toContain("Mar 5, 2:00:00 AM CT")
  })

  test("no runs yet: the catalogue is still there, the matrix and the runs table are not", () => {
    const html = render({ ...DATA, runs: [], checks: DATA.checks.map((c) => ({ ...c, results: [] })) })
    expect(html).toContain("No quality runs yet")
    expect(html).not.toContain("<table")
    expect(html).toContain("What each check asserts")
    expect(html).toContain(">never<")
  })

  test("the limit in the URL is the pressed button", () => {
    expect(render({ ...DATA, limit: 50 }, "?limit=50")).toMatch(/aria-pressed="true"[^>]*>50</)
    expect(render(DATA)).toMatch(/aria-pressed="true"[^>]*>20</)
  })

  test("before the first answer it is a skeleton, not an empty page", () => {
    const html = render(null)
    expect(html).toContain('aria-label="Loading quality"')
    expect(html).not.toContain("Checks by run")
  })
})
