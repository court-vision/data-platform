import { describe, expect, test } from "bun:test"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { renderToStaticMarkup } from "react-dom/server"
import { MemoryRouter } from "react-router"

import { adjustmentHistoryQuery, analystDigestQuery, playerNewsQuery, projectionsQuery } from "@/hooks/useProjections"
import type { AnalystDigest, DigestRead, DigestSide } from "@/lib/analyst"
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

// ---- the analyst's morning -------------------------------------------------------------------

function side(overrides: Partial<DigestSide> = {}): DigestSide {
  return { games: 38.2, minutes: 33.5, points: 27.9, points_rank: 3, categories_rank: 2, ...overrides }
}

/** The note the analyst left for Jokić: the same facts as NOTE, in its own scope. */
const ANALYST_NOTE: AdjustmentEntry = {
  ...NOTE, id: 120, scope: "analyst", author: "analyst", created_at: "2026-11-20T11:02:10",
  note: "Re-evaluated in two weeks is not back in two weeks.",
}

function read(overrides: Partial<DigestRead> = {}): DigestRead {
  return {
    id: 1, player_id: 1, name: "Nikola Jokić", team: "DEN", as_of: "2026-11-20", read_at: "2026-11-20T11:02:10",
    reasons: [
      { rule: "status_changed", text: "the availability feed moved him to out on 2026-11-19" },
      { rule: "new_item", text: "an injury item stored since his last read" },
    ],
    outcome: "note", summary: "Re-evaluated in two weeks is not back in two weeks.", disagrees: null,
    note: ANALYST_NOTE, note_live: true, owner_note: false,
    plain: side(), noted: side({ games: 31, points_rank: 9, categories_rank: 7 }), cost_usd: 0.0431,
    ...overrides,
  }
}

const QUIET = read({
  id: 2, player_id: 4, name: "Jamal Murray", reasons: [{ rule: "teammate_change", text: "Nikola Jokić went out" }],
  outcome: "none", summary: "His role with Jokić out is already what the last ten games show.", note: null, note_live: false,
  plain: side({ points_rank: 30, categories_rank: 33 }), noted: side({ points_rank: 30, categories_rank: 33 }), cost_usd: 0.0388,
})
const REFUSED = read({
  id: 3, player_id: 2, name: "Walker Kessler", team: "LAL", reasons: [{ rule: "minutes_gap", text: "24.1 minutes a game against 30.0 projected" }],
  outcome: "refused", summary: "Refused: it cites no stored item", note: null, note_live: false,
  plain: side({ points_rank: 57, categories_rank: 43 }), noted: side({ points_rank: 57, categories_rank: 43 }), cost_usd: 0.0402,
})

function digest(overrides: Partial<AnalystDigest> = {}): AnalystDigest {
  return {
    season: "2026-27", as_of: "2026-11-20", days: ["2026-11-20", "2026-11-19"], in_shadow: true,
    reads: [read(), QUIET, REFUSED], counts: { note: 1, none: 1, refused: 1 }, cost_usd: 0.1221, notes_live: 1,
    second_as_of: "2026-11-20",
    moved: [
      { player_id: 1, name: "Nikola Jokić", team: "DEN", plain: side(), noted: side({ games: 31, points_rank: 9, categories_rank: 7 }) },
      { player_id: 4, name: "Jamal Murray", team: "DEN", plain: side({ points_rank: 30 }), noted: side({ points_rank: 26 }) },
    ],
    spend: { available: true, enabled: true, month_spent_usd: 1.84, month_cap_usd: 40, model: "claude-opus-5-5", prompt_version: "analyst-1" },
    ...overrides,
  }
}

/** The analyst's morning in the cache, under the morning the address asks for (the newest when none). */
function morning(data: AnalystDigest, day: string | null = null): QueryClient {
  const client = new QueryClient()
  client.setQueryData(analystDigestQuery(day).queryKey, data)
  return client
}

/** One read's markup, from the list of reads alone: the board's moves name the same players. */
function readOf(html: string, name: string): string {
  const reads = html.split('<ol class="reads">')[1]?.split("</ol>")[0] ?? ""
  return reads.split('<li class="read"').find((item) => item.includes(`>${name}</a>`))!
}

describe("Projections: the analyst's morning", () => {
  test("the players are the page's first view, and the analyst's morning is one switch away", () => {
    const players = render(loaded(payload()))
    expect(players).toMatch(pressed("Players"))
    expect(players).not.toContain("Moved on the second board")

    const html = render(morning(digest()), "?view=analyst")
    expect(html).toMatch(pressed("Analyst"))
    // Its own head: what it is, what it runs on, the month against its cap, and where its runs are
    expect(html).toContain("The analyst&#x27;s morning for 2026-27, in shadow.")
    expect(html).toContain("in shadow: its notes reach only the second projection and boards")
    expect(html).toContain("claude-opus-5-5, prompt analyst-1")
    expect(html).toContain("$1.84 of $40.00 this month")
    expect(html).toContain('href="/pipelines/analyst"')
    // Republish belongs to the players' projection
    expect(html).not.toContain(">Republish</button>")
    expect(names(html)).toEqual([])
  })

  test("a read says why he was picked, what it came to, the note it left and what that rests on", () => {
    const html = render(morning(digest()), "?view=analyst")
    const jokic = readOf(html, "Nikola Jokić")
    expect(jokic).toContain('data-outcome="note"')
    expect(jokic).toContain(">note written</span>")
    expect(jokic).toContain(">status changed</span>")
    expect(jokic).toContain("the availability feed moved him to out on Nov 19")
    expect(jokic).toContain(">new item</span>")
    expect(jokic).toContain("$0.04 · Nov 20, 5:02 AM CT")
    // The note as stored: what it states, how sure, when to read it again, and the report under it
    expect(jokic).toContain(">its note</span>")
    expect(jokic).toContain("back Dec 8 (Dec 1 to Dec 20)")
    expect(jokic).toContain("likely · read again by Nov 30")
    expect(jokic).toContain('href="https://example.test/jokic-knee"')
    expect(jokic).toContain("Jokić (knee) to be re-evaluated in two weeks")
    // The read's reason is the note's text: said once
    expect(jokic.split("Re-evaluated in two weeks is not back in two weeks.")).toHaveLength(2)
    // His name opens his row among the players, on his season note
    expect(jokic).toContain('href="/projections?open=1&amp;edit=note"')
  })

  test("he is shown on both projections, and what the note moved is marked", () => {
    const jokic = readOf(render(morning(digest()), "?view=analyst"), "Nikola Jokić")
    const [, plain, noted] = jokic.split("<tr><th scope=\"row\">")
    expect(plain).toContain("Live</th><td>3</td><td>2</td><td>38.2</td><td>33.5</td><td>27.9</td>")
    expect(noted).toContain('With its notes</th><td data-moved="true">9</td><td data-moved="true">7</td><td data-moved="true">31.0</td><td>33.5</td>')
    // A read that wrote nothing moved nothing: one line where the table would be
    const murray = readOf(render(morning(digest()), "?view=analyst"), "Jamal Murray")
    expect(murray).toContain("The same with its notes applied: points rank 30 · 9-cat rank 33 · 38.2 games left.")
    expect(murray).not.toContain("<table")
  })

  test("the morning is counted, and the reads can be narrowed from the URL", () => {
    const html = render(morning(digest()), "?view=analyst")
    expect(html).toContain("1 note written · 1 with nothing to write · 1 refused")
    const tile = html.split("<dt").find((cells) => cells.includes(">Reads</dt>"))!
    expect(tile).toContain(">3</dd>")
    expect(tile).toContain("1 went wrong")
    expect(html.split("<dt").find((cells) => cells.includes("Morning&#x27;s cost"))).toContain("$0.04 a read")

    const trouble = render(morning(digest()), "?view=analyst&reads=trouble")
    expect(trouble).toMatch(pressed("Went wrong"))
    expect(trouble).toContain(">1 of 3</span>")
    const kessler = readOf(trouble, "Walker Kessler")
    expect(kessler).toContain(">refused</span>")
    expect(kessler).toContain("Refused: it cites no stored item")
    expect(readOf(trouble, "Jamal Murray")).toBeUndefined()
    expect(readOf(trouble, "Nikola Jokić")).toBeUndefined()

    const clean = render(morning(digest({ reads: [QUIET], counts: { none: 1 } })), "?view=analyst&reads=trouble")
    expect(clean).toContain("No read of that kind that morning.")
  })

  test("a note written by hand is said to stand, with what the analyst says against it", () => {
    const html = render(
      morning(digest({ reads: [read({ owner_note: true, disagrees: "The team now says four weeks; your note has him back Dec 8." })] })),
      "?view=analyst",
    )
    const jokic = readOf(html, "Nikola Jokić")
    expect(jokic).toContain(">your note stands</span>")
    expect(jokic).toContain(">against your note</span>")
    expect(jokic).toContain("The team now says four weeks; your note has him back Dec 8.")
  })

  test("a note a later read replaced is not shown as its live one", () => {
    const html = render(morning(digest({ reads: [read({ note_live: false, note: { ...ANALYST_NOTE, standing: null } })] })), "?view=analyst")
    const jokic = readOf(html, "Nikola Jokić")
    expect(jokic).toContain(">replaced since</span>")
    expect(jokic).not.toContain(">in force</span>")
  })

  test("the widest moves between the two points boards are listed, with places gained and lost", () => {
    const html = render(morning(digest()), "?view=analyst")
    expect(html).toContain("the two boards of Nov 20")
    const moved = html.split('<table class="table moved">')[1]
    const jokic = moved.split("<tr").find((cells) => cells.includes("Nikola Jokić"))!
    expect(jokic).toMatch(/>3<\/td><td[^>]*>9<\/td><td class="num bad">−6<\/td>/)
    expect(jokic).toContain(">38.2 → 31.0</td>")
    const murray = moved.split("<tr").find((cells) => cells.includes("Jamal Murray"))!
    expect(murray).toMatch(/>30<\/td><td[^>]*>26<\/td><td class="num good">\+4<\/td>/)
    // A number the notes left alone is shown once
    expect(murray).toContain(">38.2</td>")

    const still = render(morning(digest({ moved: [] })), "?view=analyst")
    expect(still).toContain("Nobody&#x27;s place differs between the two boards.")
  })

  test("a live morning says so, and calls the side with the notes the live one", () => {
    const html = render(morning(digest({ in_shadow: false })), "?view=analyst")
    expect(html).toContain("The analyst&#x27;s morning for 2026-27, live.")
    expect(html).toContain("live: its notes are on the projection and boards everybody sees")
    expect(html).not.toContain("in shadow")
    // The same numbers in the same order, without the notes and then with them: only the names change
    const jokic = readOf(html, "Nikola Jokić")
    const [, plain, noted] = jokic.split("<tr><th scope=\"row\">")
    expect(plain).toContain("Without its notes</th><td>3</td><td>2</td><td>38.2</td><td>33.5</td><td>27.9</td>")
    expect(noted).toContain('Live, with its notes</th><td data-moved="true">9</td><td data-moved="true">7</td><td data-moved="true">31.0</td>')
    expect(readOf(html, "Jamal Murray")).toContain("The same without its notes: points rank 30 · 9-cat rank 33 · 38.2 games left.")

    expect(html).toContain("Moved by its notes")
    expect(html).not.toContain("Moved on the second board")
    const moved = html.split('<table class="table moved">')[1]
    const [without, live] = moved.split("</thead>")[0].split("<th ").slice(2, 4)
    expect(without).toContain("without the analyst&#x27;s notes")
    expect(without).toMatch(/>Without<\/th>$/)
    expect(live).toMatch(/>Live<\/th>$/)
    const row = moved.split("<tr").find((cells) => cells.includes("Nikola Jokić"))!
    expect(row).toMatch(/>3<\/td><td[^>]*>9<\/td><td class="num bad">−6<\/td>/)
  })

  test("an older morning's moves are dated by that morning, not by the newest second boards", () => {
    // second_as_of stays the newest day with second boards, whichever morning is shown
    const yesterday = render(morning(digest({ as_of: "2026-11-19" }), "2026-11-19"), "?view=analyst&day=2026-11-19")
    expect(yesterday).toContain("the two boards of Nov 19")
    expect(yesterday).not.toContain("boards of Nov 20")

    // A morning read before its second boards were stored has nothing to compare, and says so
    const early = render(morning(digest({ second_as_of: "2026-11-19", moved: [] })), "?view=analyst")
    expect(early).toContain("no second boards stored for that morning yet")
    expect(early).toContain("The second boards of that morning are not stored yet, so there is nothing to compare.")
    expect(early).not.toContain("Nobody&#x27;s place differs")
  })

  test("another morning is asked for by the address, and the picker lists the mornings with reads", () => {
    const yesterday = digest({ as_of: "2026-11-19", reads: [QUIET], counts: { none: 1 }, cost_usd: 0.0388 })
    const html = render(morning(yesterday, "2026-11-19"), "?view=analyst&day=2026-11-19")
    expect(html).toContain('<option value="2026-11-20">Nov 20 (newest)</option>')
    expect(html).toContain('<option value="2026-11-19" selected="">Nov 19</option>')
    expect(readOf(html, "Jamal Murray")).toContain(">nothing to write</span>")
    // Not a date: the newest morning, not a request for nonsense
    expect(render(morning(digest()), "?view=analyst&day=yesterday")).toContain(">note written</span>")
    // One morning needs no picker
    expect(render(morning(digest({ days: ["2026-11-20"] })), "?view=analyst")).not.toContain("<select")
    // A day nobody was read, asked for by its address: the picker still offers the one morning with reads
    const nobody = digest({ as_of: "2026-11-10", days: ["2026-11-20"], reads: [], counts: {}, cost_usd: 0, moved: [] })
    const empty = render(morning(nobody, "2026-11-10"), "?view=analyst&day=2026-11-10")
    expect(empty).toContain("Nobody was read that morning.")
    expect(empty).toContain('<option value="2026-11-20">Nov 20 (newest)</option>')
    expect(empty).toContain('<option value="2026-11-10" selected="">Nov 10</option>')
  })

  test("before its first run, switched off, or with the backend silent, the page says so", () => {
    const never = render(
      morning(digest({ as_of: null, days: [], reads: [], counts: {}, cost_usd: 0, notes_live: 0, second_as_of: null, moved: [] })),
      "?view=analyst",
    )
    expect(never).toContain("The analyst has not read anybody yet.")
    expect(never).not.toContain("Moved on the second board")
    expect(never).toContain("$1.84 of $40.00 this month")

    const off = render(morning(digest({ spend: { ...digest().spend, enabled: false } })), "?view=analyst")
    expect(off).toContain("switched off on the backend · $1.84 of $40.00 this month")
    const silent = render(
      morning(digest({ spend: { available: false, enabled: false, month_spent_usd: 0, month_cap_usd: 0, model: null, prompt_version: null } })),
      "?view=analyst",
    )
    expect(silent).toContain("the backend could not be asked about the month&#x27;s spend")
    expect(silent.split("<dt").find((cells) => cells.includes(">This month</dt>"))).toContain("the backend did not answer")
  })

  test("a failed load says so; a failed refresh keeps the last good morning", () => {
    const empty = new QueryClient()
    const failed = empty.getQueryCache().build(empty, { queryKey: analystDigestQuery(null).queryKey })
    failed.setState({ status: "error", error: new ApiError(502, "Bad Gateway"), fetchStatus: "idle" })
    const html = render(empty, "?view=analyst")
    expect(html).toContain("Could not load the analyst&#x27;s morning: Bad Gateway")
    expect(html).not.toContain("<table")

    const stale = morning(digest())
    stale.getQueryCache().find({ queryKey: analystDigestQuery(null).queryKey })!
      .setState({ error: new ApiError(502, "Bad Gateway"), status: "error" })
    const kept = render(stale, "?view=analyst")
    expect(kept).toContain("Showing the last good data.")
    expect(kept).toContain(">note written</span>")
  })
})
