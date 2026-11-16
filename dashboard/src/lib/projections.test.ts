import { describe, expect, test } from "bun:test"

import {
  changeKey,
  changesSomething,
  defaultEditMode,
  describeChange,
  describeEntry,
  describeLive,
  describeNote,
  describePublished,
  describeRanks,
  describeStretch,
  formFrom,
  formatStat,
  gapIsWide,
  LIVE_COLUMNS,
  matchesQuery,
  noteFormFrom,
  noteKey,
  noteNeedsReading,
  noteSaveBlocker,
  noteStatesSomething,
  parseChange,
  parseNote,
  rankGap,
  rankMove,
  saveBlocker,
  standingLabel,
  STAT_COLUMNS,
  statDiffers,
  summaryTiles,
  teammatesOf,
  visibleRows,
  type AdjustmentEntry,
  type AdjustmentForm,
  type NoteForm,
  type ProjectionLine,
  type ProjectionRow,
  type ProjectionsData,
} from "@/lib/projections"

function line(overrides: Partial<ProjectionLine> = {}): ProjectionLine {
  return {
    games: 70, min: 32, pts: 20, reb: 5, ast: 4, stl: 1, blk: 0.5, tov: 2,
    fgm: 7.5, fga: 16, fg3m: 2, fg3a: 5.5, ftm: 3, fta: 3.75,
    ...overrides,
  }
}

function adjustment(overrides: Partial<AdjustmentEntry> = {}): AdjustmentEntry {
  return {
    id: 7, scope: "preseason", kind: "role", minutes: 32, games: null, return_date: null, usage: null, rates: null,
    note: "starting now", source_url: null, author: "jp", created_at: "2026-10-01T12:00:00", state: "live",
    return_earliest: null, return_latest: null, until_date: null, depends_on_player_id: null, depends_on_name: null,
    evidence: [], confidence: null, review_date: null, standing: null,
    ...overrides,
  }
}

const IN_FORCE = { state: "in_force", games: true, role: false, reasons: [] }

/** A live season note: out until Dec 8, read again by Nov 30. */
function note(overrides: Partial<AdjustmentEntry> = {}): AdjustmentEntry {
  return adjustment({
    id: 31, scope: "season", kind: "injury_current", minutes: null, return_date: "2026-12-08", note: "knee, re-evaluated in two weeks",
    created_at: "2026-11-16T14:00:00", confidence: "likely", review_date: "2026-11-30", standing: IN_FORCE,
    ...overrides,
  })
}

function row(overrides: Partial<ProjectionRow> = {}): ProjectionRow {
  return {
    player_id: 1, name: "Player", team: "DEN", position: "G", age: 27, seasons: [2025], espn_weight: 0.5,
    statistical: line(), espn: line(), blended: line(), final: line(),
    ranks: { points: 10, categories: 12 }, espn_ranks: { points: 14, categories: 9 }, adjustment: null,
    live: null, note: null,
    ...overrides,
  }
}

function data(players: ProjectionRow[], overrides: Partial<ProjectionsData> = {}): ProjectionsData {
  return {
    season: "2026-27", coefficients_version: "cv-1", espn_weight: 0.5, espn_as_of: "2026-09-30",
    published_as_of: "2026-10-01", unpublished: 0, ranks_available: true, ranks_reason: null,
    league: { league_size: 12, rounds: 13, playoff_weight: 2, playoff_weeks: [20, 21, 22, 23] },
    kinds: ["year2", "role"], live_as_of: null, confidences: ["confirmed", "likely", "possible"], review_days: 14,
    players, fetched_at: "2026-10-01T15:00:00Z",
    ...overrides,
  }
}

describe("ranks against ESPN's", () => {
  test("the gap is ESPN's rank minus ours: positive when we are higher on him", () => {
    expect(rankGap(row(), "points")).toBe(4)
    expect(rankGap(row(), "categories")).toBe(-3)
    expect(rankGap(row({ espn_ranks: { points: null, categories: 9 } }), "points")).toBeNull()
    expect(rankGap(row({ ranks: { points: null, categories: null } }), "categories")).toBeNull()
  })

  test("a wide gap counts only where one side would draft him", () => {
    expect(gapIsWide(row({ ranks: { points: 40, categories: 40 }, espn_ranks: { points: 70, categories: 41 } }), "points")).toBe(true)
    expect(gapIsWide(row({ ranks: { points: 40, categories: 40 }, espn_ranks: { points: 70, categories: 41 } }), "categories")).toBe(false)
    // Thirty places apart at the bottom of the pool is noise.
    expect(gapIsWide(row({ ranks: { points: 300, categories: 300 }, espn_ranks: { points: 340, categories: 340 } }), "points")).toBe(false)
    // One side inside the draftable range is enough.
    expect(gapIsWide(row({ ranks: { points: 140, categories: 1 }, espn_ranks: { points: 260, categories: 1 } }), "points")).toBe(true)
  })
})

describe("visibleRows", () => {
  const rows = [
    row({ player_id: 1, name: "Luka Dončić", team: "LAL", ranks: { points: 3, categories: 4 }, espn_ranks: { points: 2, categories: 5 } }),
    row({ player_id: 2, name: "Zach Edey", team: "MEM", ranks: { points: 86, categories: 63 }, espn_ranks: { points: 116, categories: 80 }, adjustment: adjustment() }),
    row({ player_id: 3, name: "AJ Dybantsa", team: "WAS", statistical: null, ranks: { points: 50, categories: 90 }, espn_ranks: { points: 45, categories: null } }),
    row({ player_id: 4, name: "Deep Bench", team: "MEM", ranks: { points: null, categories: null }, espn_ranks: { points: null, categories: null } }),
  ]
  const names = (out: ProjectionRow[]) => out.map((r) => r.name)
  const base = { query: "", filter: "all", sort: "cv", format: "points" } as const

  test("orders by our rank in the chosen format, unranked last", () => {
    expect(names(visibleRows(rows, base))).toEqual(["Luka Dončić", "AJ Dybantsa", "Zach Edey", "Deep Bench"])
    expect(names(visibleRows(rows, { ...base, format: "categories" }))).toEqual(["Luka Dončić", "Zach Edey", "AJ Dybantsa", "Deep Bench"])
  })

  test("the other orders: ESPN's, the biggest disagreement, the name", () => {
    expect(names(visibleRows(rows, { ...base, sort: "espn" }))).toEqual(["Luka Dončić", "AJ Dybantsa", "Zach Edey", "Deep Bench"])
    expect(names(visibleRows(rows, { ...base, sort: "gap" }))).toEqual(["Zach Edey", "AJ Dybantsa", "Luka Dončić", "Deep Bench"])
    expect(names(visibleRows(rows, { ...base, sort: "name" }))).toEqual(["AJ Dybantsa", "Deep Bench", "Luka Dončić", "Zach Edey"])
  })

  test("filters: adjusted, no NBA history, far from ESPN", () => {
    expect(names(visibleRows(rows, { ...base, filter: "adjusted" }))).toEqual(["Zach Edey"])
    expect(names(visibleRows(rows, { ...base, filter: "rookies" }))).toEqual(["AJ Dybantsa"])
    expect(names(visibleRows(rows, { ...base, filter: "gap" }))).toEqual(["Zach Edey"])
  })

  test("search reads names without their accents, and a team by its code", () => {
    expect(names(visibleRows(rows, { ...base, query: "doncic" }))).toEqual(["Luka Dončić"])
    expect(names(visibleRows(rows, { ...base, query: " mem " }))).toEqual(["Zach Edey", "Deep Bench"])
    expect(matchesQuery(rows[0], "me")).toBe(false) // a team is matched whole, not as a fragment
    expect(names(visibleRows(rows, { ...base, query: "nobody" }))).toEqual([])
  })

  test("does not mutate its input", () => {
    const input = [...rows].reverse()
    visibleRows(input, base)
    expect(input[0].name).toBe("Deep Bench")
  })
})

describe("the stat columns", () => {
  const column = (key: string) => STAT_COLUMNS.find((c) => c.key === key)!

  test("percentages come from makes and attempts, never from a stored rate", () => {
    expect(formatStat(column("fg_pct"), line())).toBe("46.9")
    expect(formatStat(column("ft_pct"), line())).toBe("80.0")
    expect(formatStat(column("ft_pct"), line({ fta: 0, ftm: 0 }))).toBe("—")
  })

  test("games are whole, a missing line or a missing number is a dash", () => {
    expect(formatStat(column("games"), line({ games: 66.4 }))).toBe("66")
    expect(formatStat(column("games"), line({ games: null }))).toBe("—")
    expect(formatStat(column("pts"), null)).toBe("—")
  })

  test("two lines differ only when the number shown differs", () => {
    expect(statDiffers(column("pts"), line({ pts: 20.01 }), line({ pts: 20.04 }))).toBe(false)
    expect(statDiffers(column("pts"), line({ pts: 20.0 }), line({ pts: 21.4 }))).toBe(true)
    expect(statDiffers(column("min"), line(), null)).toBe(true)
  })
})

describe("the adjustment form", () => {
  const blank: AdjustmentForm = formFrom(null)

  test("a live adjustment fills the form, and a blank one starts as a role change", () => {
    expect(blank).toEqual({ kind: "role", minutes: "", games: "", return_date: "", usage: "", rates: [], note: "", source_url: "" })
    const form = formFrom(adjustment({ kind: "injury_return", games: 62, usage: 0.97, rates: { ast: 0.92 }, return_date: "2027-01-05" }))
    expect(form).toMatchObject({ kind: "injury_return", minutes: "32", games: "62", usage: "0.97", return_date: "2027-01-05", note: "starting now" })
    expect(form.rates).toEqual([{ key: "ast", multiplier: "0.92" }])
  })

  test("blank fields are absent from the change, not zero", () => {
    expect(parseChange(blank)).toEqual({ change: {}, errors: {} })
    expect(changesSomething(parseChange(blank).change)).toBe(false)
    const parsed = parseChange({ ...blank, minutes: " 32 ", games: "66", usage: "0.97", return_date: "2027-01-05", rates: [{ key: "blk", multiplier: "1.1" }] })
    expect(parsed.errors).toEqual({})
    expect(parsed.change).toEqual({ minutes: 32, games: 66, usage: 0.97, return_date: "2027-01-05", rates: { blk: 1.1 } })
    expect(changesSomething(parsed.change)).toBe(true)
  })

  test("the API's limits are said next to the field", () => {
    expect(parseChange({ ...blank, minutes: "60" }).errors).toEqual({ minutes: "0 to 48" })
    expect(parseChange({ ...blank, minutes: "abc" }).errors).toEqual({ minutes: "0 to 48" })
    expect(parseChange({ ...blank, games: "66.5" }).errors).toEqual({ games: "a whole number, 0 to 82" })
    expect(parseChange({ ...blank, games: "90" }).errors).toEqual({ games: "a whole number, 0 to 82" })
    expect(parseChange({ ...blank, usage: "0" }).errors).toEqual({ usage: "above 0, at most 2" })
    expect(parseChange({ ...blank, return_date: "soon" }).errors).toEqual({ return_date: "a date" })
  })

  test("a multiplier needs a stat, once, and a sane size", () => {
    const rates = (...rows: [string, string][]) => parseChange({ ...blank, rates: rows.map(([key, multiplier]) => ({ key, multiplier })) })
    expect(rates(["", ""]).change).toEqual({}) // a row not filled in yet is ignored
    expect(rates(["", "1.1"]).errors).toEqual({ rates: "pick a stat for every multiplier" })
    expect(rates(["blk", "1.1"], ["blk", "1.2"]).errors).toEqual({ rates: "blk is listed twice" })
    expect(rates(["blk", "0"]).errors).toEqual({ rates: "blk: above 0, at most 3" })
    expect(rates(["blk", ""]).errors).toEqual({ rates: "blk: above 0, at most 3" })
    expect(rates(["blk", "1.1"], ["ast", "0.9"]).change).toEqual({ rates: { blk: 1.1, ast: 0.9 } })
  })

  test("a preview belongs to the numbers it was computed for", () => {
    const a = parseChange({ ...blank, minutes: "32", rates: [{ key: "blk", multiplier: "1.1" }, { key: "ast", multiplier: "0.9" }] }).change
    const b = parseChange({ ...blank, minutes: "32.0", rates: [{ key: "ast", multiplier: "0.90" }, { key: "blk", multiplier: "1.1" }] }).change
    expect(changeKey(a)).toBe(changeKey(b)) // the same numbers, typed differently
    expect(changeKey(a)).not.toBe(changeKey({ ...a, minutes: 33 }))
  })

  test("save waits for valid numbers, a reason, and a preview of exactly these numbers", () => {
    const form = { ...blank, minutes: "32" }
    const parsed = parseChange(form)
    const key = changeKey(parsed.change)
    expect(saveBlocker({ ...blank, minutes: "99" }, parseChange({ ...blank, minutes: "99" }), null)).toBe("Fix the highlighted fields")
    expect(saveBlocker({ ...blank, note: "why" }, parseChange(blank), null)).toBe("Set minutes, games, a return date, usage or a stat multiplier")
    expect(saveBlocker(form, parsed, key)).toBe("Say why: a note is required")
    expect(saveBlocker({ ...form, note: "  " }, parsed, key)).toBe("Say why: a note is required")
    expect(saveBlocker({ ...form, note: "starting" }, parsed, null)).toBe("Preview these numbers first")
    expect(saveBlocker({ ...form, note: "starting" }, parsed, changeKey({ minutes: 31 }))).toBe("Preview these numbers first")
    expect(saveBlocker({ ...form, note: "starting" }, parsed, key)).toBeNull()
  })

  test("a change reads as what it sets", () => {
    expect(describeChange(adjustment({ games: 66, return_date: "2027-01-05", usage: 0.97, rates: { blk: 1.1 } }))).toBe(
      "32 min · 66 games · back Jan 5 · usage ×0.97 · blk ×1.1",
    )
    expect(describeChange(adjustment({ minutes: null }))).toBe("nothing")
  })
})

describe("the page's summary", () => {
  const players = [
    row({ adjustment: adjustment() }),
    row({ player_id: 2, ranks: { points: 40, categories: 40 }, espn_ranks: { points: 80, categories: 42 } }),
    row({ player_id: 3 }),
  ]

  test("tiles: a zero is quiet, and unpublished lines are a warning", () => {
    expect(summaryTiles(data(players), "points")).toEqual([
      { label: "Players", value: 3, tone: "plain" },
      { label: "Adjusted", value: 1, tone: "good" },
      { label: "±25 vs ESPN", value: 1, tone: "plain" },
      { label: "Unpublished", value: 0, tone: "quiet" },
    ])
    const behind = summaryTiles(data([row()], { unpublished: 3 }), "categories")
    expect(behind[1]).toEqual({ label: "Adjusted", value: 0, tone: "quiet" })
    expect(behind[2]).toEqual({ label: "±25 vs ESPN", value: 0, tone: "quiet" })
    expect(behind[3]).toEqual({ label: "Unpublished", value: 3, tone: "warn" })
  })

  test("tiles: in season the third is the season's notes, and the ones nobody has read again", () => {
    const due = note({ standing: { state: "review_due", games: false, role: false, reasons: ["due to be read again since 2026-11-30"] } })
    const live = { live_as_of: "2026-11-16" }
    const tiles = summaryTiles(data([row({ note: note() }), row({ player_id: 2, note: due }), row({ player_id: 3 })], live), "points")
    expect(tiles.map((tile) => tile.label)).toEqual(["Players", "Adjusted", "Season notes", "Unpublished"])
    expect(tiles[2]).toEqual({ label: "Season notes", value: 2, tone: "warn", note: "1 to read again" })
    expect(summaryTiles(data([row({ note: note() })], live), "points")[2]).toEqual({ label: "Season notes", value: 1, tone: "good" })
    expect(summaryTiles(data([row()], live), "points")[2]).toEqual({ label: "Season notes", value: 0, tone: "quiet" })
  })

  test("says which morning the live projection is from, and which form a row opens on", () => {
    expect(describeLive({ live_as_of: "2026-11-16" })).toBe("live projection of Nov 16")
    expect(describeLive({ live_as_of: null })).toBe("no live projection yet: season notes wait for the first one")
    expect(defaultEditMode({ live_as_of: "2026-11-16" })).toBe("note")
    expect(defaultEditMode({ live_as_of: null })).toBe("adjustment")
  })

  test("says whether the board is reading this page", () => {
    expect(describePublished({ published_as_of: "2026-10-01", unpublished: 0 })).toBe(
      "published Oct 1 · the board reads what this page shows",
    )
    expect(describePublished({ published_as_of: "2026-10-01", unpublished: 1 })).toBe(
      "published Oct 1 · 1 player differs from the published snapshot",
    )
    expect(describePublished({ published_as_of: "2026-10-01", unpublished: 4 })).toBe(
      "published Oct 1 · 4 players differ from the published snapshot",
    )
    expect(describePublished({ published_as_of: null, unpublished: 517 })).toBe(
      "never published: the board is still on ESPN's projection",
    )
  })

  test("says what the ranks were measured in, or why there are none", () => {
    expect(describeRanks(data([]))).toBe("ranks: standard 12-team, 13-round league · playoff weeks 20–23 count ×2")
    expect(describeRanks(data([], { league: { league_size: 12, rounds: 13, playoff_weight: 2, playoff_weeks: [] } }))).toBe(
      "ranks: standard 12-team, 13-round league · no playoff weeks on the calendar",
    )
    expect(describeRanks(data([], { ranks_available: false, ranks_reason: "timeout", league: null }))).toBe(
      "ranks unavailable (timeout): the lines below are still current",
    )
  })

  test("a rank move is places gained or lost", () => {
    expect(rankMove(44, 38)).toBe("+6")
    expect(rankMove(19, 22)).toBe("−3")
    expect(rankMove(10, 10)).toBe("")
    expect(rankMove(null, 10)).toBe("")
  })
})

describe("the season note", () => {
  function form(overrides: Partial<NoteForm> = {}): NoteForm {
    return { ...noteFormFrom(null), ...overrides }
  }

  test("reads as what it states", () => {
    expect(describeNote(note({ return_earliest: "2026-12-01", return_latest: "2026-12-20" }))).toBe("back Dec 8 (Dec 1 to Dec 20)")
    expect(describeNote(note({ return_latest: "2026-12-20" }))).toBe("back Dec 8 (… to Dec 20)")
    expect(describeNote(note({ return_date: null, games: 0 }))).toBe("out for the season")
    expect(
      describeNote(note({ return_date: null, minutes: 30, usage: 1.1, until_date: "2026-12-20", depends_on_player_id: 9, depends_on_name: "Nikola Jokić" })),
    ).toBe("30 min · usage ×1.1 · through Dec 20 · until Nikola Jokić plays")
    expect(describeNote(note({ return_date: null, minutes: 30, depends_on_player_id: 9 }))).toBe("30 min · until player 9 plays")
    // A version of either scope reads in its own terms
    expect(describeEntry(note())).toBe("back Dec 8")
    expect(describeEntry(adjustment({ games: 66 }))).toBe("32 min · 66 games")
  })

  test("its standing is a chip: applied, applied in part, to be read again, or ended", () => {
    expect(standingLabel(null)).toBeNull()
    expect(standingLabel(IN_FORCE)).toEqual({ label: "in force", tone: "up", detail: "" })
    // The API's reasons carry ISO days; the page writes them as it writes every other day
    expect(standingLabel({ state: "in_force", games: false, role: true, reasons: ["he played on 2026-11-20"] })).toEqual({
      label: "in force in part", tone: "up", detail: "he played on Nov 20",
    })
    expect(standingLabel({ state: "review_due", games: false, role: false, reasons: ["due to be read again since 2026-11-30"] })).toEqual({
      label: "read again", tone: "warn", detail: "due to be read again since Nov 30",
    })
    expect(
      standingLabel({ state: "ended", games: false, role: false, reasons: ["he played on 2026-11-20", "it ran through 2026-11-22"] })?.detail,
    ).toBe("he played on Nov 20; it ran through Nov 22")
    expect(standingLabel({ state: "ended", games: false, role: false, reasons: [] })).toEqual({
      label: "ended", tone: "quiet", detail: "nothing of it applies any more",
    })
    expect(noteNeedsReading(row({ note: note() }))).toBe(false)
    expect(noteNeedsReading(row({ note: note({ standing: { state: "ended", games: false, role: false, reasons: [] } }) }))).toBe(true)
    expect(noteNeedsReading(row())).toBe(false)
  })

  test("the filters find the noted and the ones to read again", () => {
    const due = note({ standing: { state: "review_due", games: false, role: false, reasons: [] } })
    const rows = [row({ note: note() }), row({ player_id: 2, name: "Due", note: due }), row({ player_id: 3, name: "Plain" })]
    const view = { query: "", sort: "name", format: "points" } as const
    expect(visibleRows(rows, { ...view, filter: "notes" }).map((r) => r.player_id)).toEqual([2, 1])
    expect(visibleRows(rows, { ...view, filter: "review" }).map((r) => r.player_id)).toEqual([2])
  })

  test("the live line shows the games left to a decimal", () => {
    const live = line({ games: 31.44, min: 24.6 })
    expect(formatStat(LIVE_COLUMNS[0], live)).toBe("31.4")
    expect(formatStat(STAT_COLUMNS[0], live)).toBe("31")
    expect(LIVE_COLUMNS[0].label).toBe("GP left")
    expect(LIVE_COLUMNS.slice(1)).toEqual(STAT_COLUMNS.slice(1))
  })

  test("a stretch of the games ahead reads as days, games and what he does in them", () => {
    expect(describeStretch({ first: "2026-11-16", last: "2026-11-22", games: 4, chance: 0, minutes: 28, usage: 0.983 })).toBe(
      "Nov 16 – Nov 22 · 4 games · out",
    )
    expect(describeStretch({ first: "2026-11-24", last: "2026-11-24", games: 1, chance: 0.837, minutes: 28, usage: 0.985 })).toBe(
      "Nov 24 · 1 game · plays 84% · 28.0 min · usage ×0.98",
    )
  })

  test("the form starts from the live note, with the review date left to the default", () => {
    expect(noteFormFrom(null)).toEqual({
      kind: "injury_current", out_for_season: false, return_date: "", return_earliest: "", return_latest: "",
      minutes: "", usage: "", until_date: "", depends_on: "", confidence: "", review_date: "", note: "", source_url: "", evidence: [],
    })
    const stored = note({
      kind: "injury_return", return_earliest: "2026-12-01", return_latest: "2026-12-20", minutes: 24, usage: 0.9,
      until_date: "2026-12-22", depends_on_player_id: 9, source_url: "https://example.test/report",
      evidence: [
        { type: "news", id: 5, source: "rotowire_rss", kind: "injury", headline: "Out two weeks", url: null, published_at: null },
        { type: "news", id: null, source: null, kind: null, headline: "no id", url: null, published_at: null },
      ],
    })
    expect(noteFormFrom(stored)).toEqual({
      kind: "injury_return", out_for_season: false, return_date: "2026-12-08", return_earliest: "2026-12-01",
      return_latest: "2026-12-20", minutes: "24", usage: "0.9", until_date: "2026-12-22", depends_on: "9", confidence: "likely",
      // Saved again, it is read again two weeks from that save
      review_date: "", note: "knee, re-evaluated in two weeks", source_url: "https://example.test/report", evidence: [5],
    })
    expect(noteFormFrom(note({ return_date: null, games: 0 })).out_for_season).toBe(true)
  })

  test("parses what was typed into the API's note", () => {
    const parsed = parseNote(form({
      return_date: "2026-12-08", return_earliest: "2026-12-01", return_latest: "2026-12-20", minutes: " 24 ", usage: "0.9",
      until_date: "2026-12-22", depends_on: "9", review_date: "2026-11-25",
    }))
    expect(parsed).toEqual({
      change: {
        return_date: "2026-12-08", return_earliest: "2026-12-01", return_latest: "2026-12-20", minutes: 24, usage: 0.9,
        until_date: "2026-12-22", depends_on_player_id: 9,
      },
      review_date: "2026-11-25",
      errors: {},
    })
    expect(parseNote(form({ out_for_season: true }))).toEqual({ change: { out_for_season: true }, review_date: null, errors: {} })
    expect(parseNote(form())).toEqual({ change: {}, review_date: null, errors: {} })
    expect(noteStatesSomething(parseNote(form()).change)).toBe(false)
    expect(noteStatesSomething(parseNote(form({ usage: "1.1" })).change)).toBe(true)
  })

  test("says what is wrong next to the field, as the API would refuse it", () => {
    expect(parseNote(form({ minutes: "60", usage: "0" })).errors).toEqual({ minutes: "0 to 48", usage: "above 0, at most 2" })
    expect(parseNote(form({ out_for_season: true, return_date: "2026-12-08" })).errors).toEqual({
      return_date: "a season written off has no return date",
    })
    expect(parseNote(form({ return_earliest: "2026-12-01" })).errors).toEqual({
      return_date: "the range is around a date: give the likely one",
    })
    expect(parseNote(form({ return_date: "2026-12-08", return_earliest: "2026-12-09", return_latest: "2026-12-07" })).errors).toEqual({
      return_earliest: "after the likely date", return_latest: "before the likely date",
    })
    // An end with nothing to end
    expect(parseNote(form({ return_date: "2026-12-08", until_date: "2026-12-20", depends_on: "9" })).errors).toEqual({
      until_date: "it ends the minutes or usage: set one", depends_on: "he ends the minutes or usage: set one",
    })
    expect(parseNote(form({ minutes: "30", review_date: "soon" })).errors).toEqual({ review_date: "a date" })
  })

  test("a note is saved only once the note on the form has been previewed", () => {
    const filled = form({ return_date: "2026-12-08", note: "knee" })
    const parsed = parseNote(filled)
    expect(noteSaveBlocker(form({ minutes: "60" }), parseNote(form({ minutes: "60" })), null)).toBe("Fix the highlighted fields")
    expect(noteSaveBlocker(form(), parseNote(form()), null)).toBe("Set a return date, a season written off, minutes or usage")
    expect(noteSaveBlocker({ ...filled, note: " " }, parsed, noteKey(parsed))).toBe("Say why: a note is required")
    expect(noteSaveBlocker(filled, parsed, null)).toBe("Preview this note first")
    expect(noteSaveBlocker(filled, parsed, noteKey(parsed))).toBeNull()
    // The review date decides whether it would apply, so changing it asks for another preview;
    // the range and the reason do not move the price
    expect(noteKey(parseNote({ ...filled, review_date: "2026-11-20" }))).not.toBe(noteKey(parsed))
    expect(noteKey(parseNote({ ...filled, return_latest: "2026-12-20", note: "other", confidence: "likely" }))).toBe(noteKey(parsed))
  })

  test("his minutes can wait on anyone else on his team", () => {
    const rows = [
      row({ player_id: 1, name: "Murray", team: "DEN" }), row({ player_id: 2, name: "Jokić", team: "DEN" }),
      row({ player_id: 3, name: "Brown", team: "BOS" }), row({ player_id: 4, name: "Gordon", team: "DEN" }),
    ]
    expect(teammatesOf(rows[0], rows).map((r) => r.name)).toEqual(["Gordon", "Jokić"])
    expect(teammatesOf(row({ team: null }), rows)).toEqual([])
  })
})
