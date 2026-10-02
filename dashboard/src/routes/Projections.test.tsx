import { describe, expect, test } from "bun:test"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { renderToStaticMarkup } from "react-dom/server"
import { MemoryRouter } from "react-router"

import { adjustmentHistoryQuery, projectionsQuery } from "@/hooks/useProjections"
import { ApiError } from "@/lib/api"
import type { AdjustmentEntry, ProjectionLine, ProjectionRow, ProjectionsData } from "@/lib/projections"
import { Projections } from "@/routes/Projections"

// No DOM and no network: the page is rendered to markup from a query cache
// filled by hand, as the server would render it, and read as text.

function line(overrides: Partial<ProjectionLine> = {}): ProjectionLine {
  return {
    games: 70, min: 32, pts: 20, reb: 5, ast: 4, stl: 1, blk: 0.5, tov: 2,
    fgm: 7.5, fga: 16, fg3m: 2, fg3a: 5.5, ftm: 3, fta: 3.75,
    ...overrides,
  }
}

const LIVE: AdjustmentEntry = {
  id: 61, kind: "injury_return", minutes: 30, games: 64, return_date: null, usage: null, rates: null,
  note: "Back from a lost season; minutes managed early.", source_url: "https://example.test/kessler",
  author: "jp", created_at: "2026-10-01T12:00:00", state: "live",
}

function row(overrides: Partial<ProjectionRow> = {}): ProjectionRow {
  return {
    player_id: 1, name: "Nikola Jokić", team: "DEN", position: "C", age: 31.7, seasons: [2023, 2024, 2025], espn_weight: 0.5,
    statistical: line({ pts: 27.1 }), espn: line({ pts: 29.3 }), blended: line({ pts: 28.2 }), final: line({ pts: 28.2 }),
    ranks: { points: 1, categories: 1 }, espn_ranks: { points: 1, categories: 2 }, adjustment: null,
    ...overrides,
  }
}

const KESSLER = row({
  player_id: 2, name: "Walker Kessler", team: "LAL", position: "C", age: 25.2,
  blended: line({ games: 58, min: 27.4, blk: 2.1 }), final: line({ games: 64, min: 30, blk: 2.3 }),
  ranks: { points: 57, categories: 43 }, espn_ranks: { points: 95, categories: 38 }, adjustment: LIVE,
})
const ROOKIE = row({
  player_id: 3, name: "AJ Dybantsa", team: "WAS", position: "F", age: 19.9, seasons: [], espn_weight: 1,
  statistical: null, ranks: { points: 60, categories: 110 }, espn_ranks: { points: 52, categories: null },
})

function payload(overrides: Partial<ProjectionsData> = {}): ProjectionsData {
  return {
    season: "2026-27", coefficients_version: "cv-2026-27-2026-10-01", espn_weight: 0.5, espn_as_of: "2026-09-30",
    published_as_of: "2026-10-01", unpublished: 0, ranks_available: true, ranks_reason: null,
    league: { league_size: 12, rounds: 13, playoff_weight: 2, playoff_weeks: [20, 21, 22, 23] },
    kinds: ["year2", "trade", "role", "injury_return", "injury_current", "age", "other"],
    players: [row(), KESSLER, ROOKIE], fetched_at: "2026-10-01T15:00:00Z",
    ...overrides,
  }
}

function loaded(data: ProjectionsData): QueryClient {
  const client = new QueryClient()
  client.setQueryData(projectionsQuery().queryKey, data)
  return client
}

function render(client: QueryClient, search = ""): string {
  return renderToStaticMarkup(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[`/projections${search}`]}>
        <Projections />
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

/** The player rows of the main table, in order: the text of each row's name button. */
function names(html: string): string[] {
  return [...html.matchAll(/<span class="font-medium">([^<]+)<\/span>/g)].map((match) => match[1])
}

describe("Projections", () => {
  test("lists every player in Court Vision's order, with ESPN's rank and the gap beside it", () => {
    const html = render(loaded(payload()))
    expect(names(html)).toEqual(["Nikola Jokić", "Walker Kessler", "AJ Dybantsa"])
    expect(html).toContain("published Oct 1 · the board reads what this page shows")
    expect(html).toContain("ranks: standard 12-team, 13-round league · playoff weeks 20–23 count ×2")
    expect(html).toContain("model cv-2026-27-2026-10-01")
    // Kessler: ours 57, ESPN's 95 — thirty-eight places apart, toned as a disagreement.
    const kessler = html.split("<tr").find((cells) => cells.includes("Walker Kessler"))!
    expect(kessler).toContain(">57</td>")
    expect(kessler).toContain(">95</td>")
    expect(kessler).toMatch(/text-status-projected[^>]*>\+38</)
    // His adjustment reads as what it sets, and the row shows the final line.
    expect(kessler).toContain("Injury return")
    expect(kessler).toContain("30 min · 64 games")
    expect(kessler).toContain(">2.3</td>")
  })

  test("the format switch reads the other board, and the order follows it", () => {
    const html = render(loaded(payload()), "?format=categories")
    expect(names(html)).toEqual(["Nikola Jokić", "Walker Kessler", "AJ Dybantsa"])
    const rookie = html.split("<tr").find((cells) => cells.includes("AJ Dybantsa"))!
    expect(rookie).toContain(">110</td>")
    expect(rookie).toContain("No history")
    expect(html).toMatch(/aria-pressed="true"[^>]*>9-cat</)
  })

  test("filters, search and sort come from the URL", () => {
    expect(names(render(loaded(payload()), "?filter=adjusted"))).toEqual(["Walker Kessler"])
    expect(names(render(loaded(payload()), "?filter=rookies"))).toEqual(["AJ Dybantsa"])
    expect(names(render(loaded(payload()), "?q=jokic"))).toEqual(["Nikola Jokić"])
    expect(names(render(loaded(payload()), "?sort=gap"))).toEqual(["Walker Kessler", "AJ Dybantsa", "Nikola Jokić"])
    expect(render(loaded(payload()), "?q=nobody")).toContain("Nobody matches.")
    // An unknown value is the default, not an empty page.
    expect(names(render(loaded(payload()), "?filter=vibes&sort=x&format=y"))).toHaveLength(3)
  })

  test("an open row shows the four lines, the editor filled from the live adjustment, and its versions", () => {
    const client = loaded(payload())
    client.setQueryData(adjustmentHistoryQuery(2).queryKey, {
      player_id: 2, season: "2026-27",
      versions: [LIVE, { ...LIVE, id: 40, minutes: 28, note: "First read.", state: "superseded" }],
    })

    const html = render(client, "?open=2")

    expect(html).toContain('aria-expanded="true"')
    for (const label of ["Statistical", "ESPN", "Blend", "Final"]) expect(html).toContain(`>${label}</th>`)
    expect(html).toContain("ESPN&#x27;s share is 50%")
    // The form starts from the live adjustment.
    expect(html).toContain("Edit the adjustment")
    expect(html).toMatch(/<option value="injury_return" selected="">Injury return<\/option>/)
    expect(html).toMatch(/inputMode="decimal"[^>]*value="30"/)
    expect(html).toMatch(/inputMode="numeric"[^>]*value="64"/)
    expect(html).toContain("Back from a lost season; minutes managed early.")
    // Nothing has been previewed, so it cannot be saved yet, and the page says why.
    expect(html).toContain("Preview these numbers first.")
    expect(html).toMatch(/<button[^>]*type="submit"[^>]*disabled=""/)
    // Both versions, newest first, the old one marked.
    expect(html.indexOf("minutes managed early")).toBeLessThan(html.indexOf("First read."))
    expect(html).toContain(">superseded</div>")
    expect(html).toContain("28 min · 64 games")
    expect(html).toContain('href="https://example.test/kessler"')
  })

  test("a player with no adjustment opens on a blank form, and offers nothing to retire", () => {
    const html = render(loaded(payload()), "?open=1")
    expect(html).toContain("Add an adjustment")
    expect(html).toContain("Set minutes, games, a return date, usage or a stat multiplier.")
    expect(html).not.toContain(">Retire</button>")
  })

  test("a snapshot the board has not caught up with is said, in the tile and in the line", () => {
    const html = render(loaded(payload({ unpublished: 3 })))
    expect(html).toContain("published Oct 1 · 3 players differ from the published snapshot")
    const tile = html.split("<dt").find((cells) => cells.includes("Unpublished"))!
    expect(tile).toContain("text-status-projected")
    expect(tile).toContain(">3</dd>")
  })

  test("without the backend the lines are still shown, and the ranks say why they are missing", () => {
    const unranked = payload({
      ranks_available: false, ranks_reason: "timeout", league: null,
      players: [row({ ranks: { points: null, categories: null } })],
    })
    const html = render(loaded(unranked))
    expect(html).toContain("ranks unavailable (timeout): the lines below are still current")
    expect(html).toContain("Nikola Jokić")
    expect(html).toContain(">28.2</td>")
  })

  test("a failed load says so and shows no table; a failed refresh keeps the last good one", () => {
    const empty = new QueryClient()
    const failed = empty.getQueryCache().build(empty, { queryKey: projectionsQuery().queryKey })
    failed.setState({ status: "error", error: new ApiError(502, "Bad Gateway"), fetchStatus: "idle" })
    const html = render(empty)
    expect(html).toContain("Could not load the projections: Bad Gateway")
    expect(html).not.toContain("<table")

    const stale = loaded(payload())
    stale.getQueryCache().find({ queryKey: projectionsQuery().queryKey })!
      .setState({ error: new ApiError(502, "Bad Gateway"), status: "error" })
    const kept = render(stale)
    expect(kept).toContain("Showing the last good data.")
    expect(names(kept)).toHaveLength(3)
  })
})
