import { describe, expect, test } from "bun:test"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { renderToStaticMarkup } from "react-dom/server"
import { MemoryRouter } from "react-router"

import { adjustmentHistoryQuery, playerNewsQuery, projectionsQuery } from "@/hooks/useProjections"
import { ApiError } from "@/lib/api"
import type { AdjustmentEntry, ProjectionLine, ProjectionRow, ProjectionsData } from "@/lib/projections"
import { Projections } from "@/routes/Projections"
import { pressed } from "@/test-helpers"

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
  id: 61, scope: "preseason", kind: "injury_return", minutes: 30, games: 64, return_date: null, usage: null, rates: null,
  note: "Back from a lost season; minutes managed early.", source_url: "https://example.test/kessler",
  author: "jp", created_at: "2026-10-01T12:00:00", state: "live",
  return_earliest: null, return_latest: null, until_date: null, depends_on_player_id: null, depends_on_name: null,
  evidence: [], confidence: null, review_date: null, standing: null,
}

const REPORT = {
  type: "news", id: 501, source: "espn_injuries", kind: "injury", headline: "Jokić (knee) to be re-evaluated in two weeks",
  url: "https://example.test/jokic-knee", published_at: "2026-11-15T22:05:00Z",
}

/** Jokić's season note: out until about Dec 8, on the word of one report. */
const NOTE: AdjustmentEntry = {
  ...LIVE, id: 90, scope: "season", kind: "injury_current", minutes: null, games: null, return_date: "2026-12-08",
  return_earliest: "2026-12-01", return_latest: "2026-12-20", note: "Re-evaluated in two weeks is not back in two weeks.",
  source_url: null, created_at: "2026-11-16T14:00:00", evidence: [REPORT], confidence: "likely", review_date: "2026-11-30",
  standing: { state: "in_force", games: true, role: false, reasons: [] },
}

function row(overrides: Partial<ProjectionRow> = {}): ProjectionRow {
  return {
    player_id: 1, name: "Nikola Jokić", team: "DEN", position: "C", age: 31.7, seasons: [2023, 2024, 2025], espn_weight: 0.5,
    statistical: line({ pts: 27.1 }), espn: line({ pts: 29.3 }), blended: line({ pts: 28.2 }), final: line({ pts: 28.2 }),
    ranks: { points: 1, categories: 1 }, espn_ranks: { points: 1, categories: 2 }, adjustment: null,
    live: null, note: null,
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
    live_as_of: null, confidences: ["confirmed", "likely", "possible"], review_days: 14,
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
  return [...html.matchAll(/<span class="name">([^<]+)<\/span>/g)].map((match) => match[1])
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
    expect(kessler).toMatch(/wideGap[^>]*>\+38</)
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
    expect(html).toMatch(pressed("9-cat"))
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
    expect(html).toContain(">superseded</span>")
    expect(html).toContain("28 min · 64 games")
    expect(html).toContain('href="https://example.test/kessler"')
  })

  test("a player with no adjustment opens on a blank form, and offers nothing to retire", () => {
    const html = render(loaded(payload()), "?open=1")
    expect(html).toContain("Add an adjustment")
    expect(html).toContain("Set minutes, games, a return date, usage or a stat multiplier.")
    expect(html).not.toContain(">Retire</button>")
    // Before the season's first live projection there is no live line, and the note form is a click away
    expect(html).not.toContain("Live, from here")
    expect(html).toContain("no live projection yet: season notes wait for the first one")
    expect(html).toMatch(pressed("Preseason adjustment"))
  })

  test("a snapshot the board has not caught up with is said, in the tile and in the line", () => {
    const html = render(loaded(payload({ unpublished: 3 })))
    expect(html).toContain("published Oct 1 · 3 players differ from the published snapshot")
    const tile = html.split("<dt").find((cells) => cells.includes("Unpublished"))!
    expect(tile).toContain('data-tone="warn"')
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

  describe("in season", () => {
    const JOKIC = row({ live: line({ games: 31.4, min: 34.2, pts: 27.9 }), note: NOTE })
    const MURRAY = row({ player_id: 4, name: "Jamal Murray", position: "G", ranks: { points: 30, categories: 33 }, live: line({ games: 52.6 }) })
    const inSeason = (overrides: Partial<ProjectionsData> = {}) =>
      payload({ live_as_of: "2026-11-16", players: [JOKIC, KESSLER, ROOKIE, MURRAY], ...overrides })

    test("a row says what his note states and whether it is still applied, and the notes can be filtered to", () => {
      const html = render(loaded(inSeason()))
      expect(html).toContain("live projection of Nov 16")
      const jokic = html.split("<tr").find((cells) => cells.includes("Nikola Jokić"))!
      expect(jokic).toContain(">Note</span>")
      expect(jokic).toContain("back Dec 8 (Dec 1 to Dec 20)")
      // In season the third tile is the notes, where the disagreements with ESPN were
      const tile = html.split("<dt").find((cells) => cells.includes("Season notes"))!
      expect(tile).toContain(">1</dd>")
      expect(tile).toContain('data-tone="up"')
      expect(html).not.toMatch(/<dt[^>]*>±25 vs ESPN/)
      expect(names(render(loaded(inSeason()), "?filter=notes"))).toEqual(["Nikola Jokić"])
      expect(render(loaded(inSeason()), "?filter=review")).toContain("Nobody matches.")
    })

    test("a note nobody has read again is marked, counted and filtered to", () => {
      const due = { ...NOTE, standing: { state: "review_due", games: false, role: false, reasons: ["due to be read again since 2026-11-30"] } }
      const client = loaded(inSeason({ players: [row({ note: due }), KESSLER] }))
      const html = render(client, "?filter=review&open=1")
      expect(names(html)).toEqual(["Nikola Jokić"])
      expect(html).toContain(">Note: read again</span>")
      const tile = html.split("<dt").find((cells) => cells.includes("Season notes"))!
      expect(tile).toContain('data-tone="warn"')
      expect(tile).toContain(">1 to read again</dd>")
      // Opened, the form says why and what to do about it
      expect(html).toContain("due to be read again since Nov 30. It is not applied until it is saved again.")
    })

    test("an open row shows the live line and the note form, filled from his note, with the news it can cite", () => {
      const client = loaded(inSeason())
      client.setQueryData(playerNewsQuery(1).queryKey, {
        player_id: 1,
        items: [REPORT, { ...REPORT, id: 502, kind: "availability", headline: "Jokić out Friday", url: null, published_at: null }],
      })
      client.setQueryData(adjustmentHistoryQuery(1).queryKey, {
        player_id: 1, season: "2026-27",
        versions: [NOTE, { ...NOTE, id: 80, return_date: "2026-11-28", return_earliest: null, return_latest: null, evidence: [], standing: null, state: "superseded" }],
      })

      const html = render(client, "?open=1")

      // The live line under the four, its games the ones left
      const live = html.split("<tr").find((cells) => cells.includes("Live, from here"))!
      expect(live).toContain(">31.4</td>")
      expect(live).toContain(">27.9</td>")
      // In season a row opens on the note
      expect(html).toMatch(pressed("Season note"))
      expect(html).toContain("Edit the season note")
      expect(html).not.toContain("Edit the adjustment")
      expect(html).toMatch(/<option value="injury_current" selected="">Injured now<\/option>/)
      expect(html).toMatch(/type="date"[^>]*value="2026-12-08"/)
      expect(html).toMatch(/type="date"[^>]*value="2026-12-01"/)
      expect(html).toMatch(/<option value="likely" selected="">likely<\/option>/)
      expect(html).toContain("Read again by Nov 30.")
      expect(html).toContain("Blank is 14 days from the save.")
      // His minutes can wait on a teammate: the other Nugget on the page, not a Laker
      expect(html).toContain('<option value="4">Jamal Murray</option>')
      expect(html).not.toContain('<option value="2">Walker Kessler</option>')
      // The report his note rests on is ticked; the other item is offered
      expect(html).toMatch(/<input[^>]*type="checkbox"[^>]*checked=""[^>]*\/><span[^>]*>.*?Jokić \(knee\) to be re-evaluated in two weeks/s)
      expect(html).toContain("Jokić out Friday")
      expect(html).toContain('href="https://example.test/jokic-knee"')
      // Not previewed yet, so not saved yet
      expect(html).toContain("Preview this note first.")
      expect(html).toContain(">Retire</button>")
      // Both versions, each marked as a season note, with what the live one rests on
      expect(html).toContain(">Season note</span>")
      expect(html).toContain("likely · read again by Nov 30")
      expect(html.indexOf("back Dec 8 (Dec 1 to Dec 20)")).toBeLessThan(html.indexOf("back Nov 28"))
    })

    test("the other form is one switch away, and the switch is in the URL", () => {
      const html = render(loaded(inSeason()), "?open=2&edit=adjustment")
      expect(html).toMatch(pressed("Preseason adjustment"))
      expect(html).toContain("Edit the adjustment")
      expect(html).not.toContain("season note for")
      // Kessler has no note: his note form is blank, and offers nothing to retire
      const blank = render(loaded(inSeason()), "?open=2")
      expect(blank).toContain("Add a season note")
      expect(blank).toContain("Set a return date, a season written off, minutes or usage.")
      expect(blank).not.toContain(">Retire</button>")
      // No game this season and no preseason line: the live projection does not have him
      expect(render(loaded(inSeason()), "?open=3")).toContain("Not in the live projection of Nov 16")
    })
  })
})
