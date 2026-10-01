import { describe, expect, test } from "bun:test"

import { daysBehind, describeSeason, sortByUrgency, splitTable, summarizeFreshness, summaryTiles, type TableFreshness } from "@/lib/freshness"

function table(overrides: Partial<TableFreshness> = {}): TableFreshness {
  return {
    table: "nba.player_game_stats",
    pipelines: [{ name: "player_game_stats", display_name: "Player Game Stats" }],
    category: "post_game",
    date_column: "game_date",
    latest_date: "2026-03-04",
    write_column: "updated_at",
    latest_written_at: "2026-03-05T07:11:00",
    rows_estimate: 24180,
    expected_date: "2026-03-04",
    state: "fresh",
    error: null,
    ...overrides,
  }
}

describe("sortByUrgency", () => {
  test("stale first, then errors and empties, then the rest", () => {
    const sorted = sortByUrgency([
      table({ table: "nba.a", state: "unjudged" }),
      table({ table: "nba.b", state: "fresh" }),
      table({ table: "nba.c", state: "stale" }),
      table({ table: "nba.d", state: "empty" }),
      table({ table: "nba.e", state: "error" }),
      table({ table: "nba.f", state: "idle" }),
    ])
    expect(sorted.map((t) => t.table)).toEqual(["nba.c", "nba.e", "nba.d", "nba.b", "nba.f", "nba.a"])
  })

  test("within a state, the most time-critical category first, then the name", () => {
    const sorted = sortByUrgency([
      table({ table: "nba.z", category: "post_game" }),
      table({ table: "nba.y", category: "scheduled" }),
      table({ table: "nba.x", category: "pre_game" }),
      table({ table: "nba.a", category: "post_game" }),
      table({ table: "nba.w", category: "live" }),
    ])
    expect(sorted.map((t) => t.table)).toEqual(["nba.w", "nba.x", "nba.a", "nba.z", "nba.y"])
  })

  test("does not mutate its input", () => {
    const input = [table({ state: "fresh" }), table({ state: "stale" })]
    sortByUrgency(input)
    expect(input[0].state).toBe("fresh")
  })
})

describe("summarizeFreshness", () => {
  test("counts every state and the total", () => {
    const summary = summarizeFreshness([
      table({ state: "fresh" }),
      table({ state: "fresh" }),
      table({ state: "stale" }),
      table({ state: "empty" }),
      table({ state: "unjudged" }),
    ])
    expect(summary).toEqual({ total: 5, fresh: 2, stale: 1, idle: 0, empty: 1, unjudged: 1, error: 0 })
  })
})

describe("summaryTiles", () => {
  const tiles = (...states: TableFreshness["state"][]) => summaryTiles(summarizeFreshness(states.map((state) => table({ state }))))

  test("a table whose query failed is counted in the red tile, and the tile says so", () => {
    expect(tiles("fresh", "stale", "error", "error")).toEqual([
      { label: "Tables", value: 4, tone: "plain" },
      { label: "Fresh", value: 1, tone: "good" },
      { label: "Stale / error", value: 3, tone: "bad" },
      { label: "Empty", value: 0, tone: "quiet" },
    ])
  })

  test("every table errored is red, not a row of quiet zeros", () => {
    expect(tiles("error", "error").slice(1)).toEqual([
      { label: "Fresh", value: 0, tone: "quiet" },
      { label: "Stale / error", value: 2, tone: "bad" },
      { label: "Empty", value: 0, tone: "quiet" },
    ])
  })

  test("without errors the tile is plain Stale, quiet at zero", () => {
    expect(tiles("fresh", "stale", "empty").slice(1)).toEqual([
      { label: "Fresh", value: 1, tone: "good" },
      { label: "Stale", value: 1, tone: "bad" },
      { label: "Empty", value: 1, tone: "warn" },
    ])
    expect(tiles("idle", "unjudged").slice(1)).toEqual([
      { label: "Fresh", value: 0, tone: "quiet" },
      { label: "Stale", value: 0, tone: "quiet" },
      { label: "Empty", value: 0, tone: "quiet" },
    ])
  })
})

describe("daysBehind", () => {
  test("game days between what the table has and what was expected", () => {
    expect(daysBehind(table({ state: "stale", latest_date: "2026-03-02", expected_date: "2026-03-04" }))).toBe(2)
  })

  test("at least one day when judged stale", () => {
    expect(daysBehind(table({ state: "stale", latest_date: "2026-03-04", expected_date: "2026-03-04" }))).toBe(1)
  })

  test("null unless stale", () => {
    expect(daysBehind(table({ state: "fresh" }))).toBeNull()
    expect(daysBehind(table({ state: "stale", latest_date: null }))).toBeNull()
  })
})

describe("describeSeason", () => {
  test("in season, before today's first tip: both cadences due through last night", () => {
    expect(describeSeason({ season: "2025-26", phase: "regular", post_game_due: "2026-03-04", pre_game_due: "2026-03-04", next_game_date: "2026-03-05" }))
      .toBe("Regular season 2025-26 · post-game and pre-game due through Mar 4 · next game Mar 5")
  })

  test("after today's first tip the pre-game cadence is a day ahead", () => {
    expect(describeSeason({ season: "2025-26", phase: "regular", post_game_due: "2026-03-04", pre_game_due: "2026-03-05", next_game_date: "2026-03-06" }))
      .toBe("Regular season 2025-26 · post-game due through Mar 4 · pre-game through Mar 5 · next game Mar 6")
  })

  test("opening night after tip: only pre-game is due", () => {
    expect(describeSeason({ season: "2026-27", phase: "regular", post_game_due: null, pre_game_due: "2026-10-21", next_game_date: "2026-10-22" }))
      .toBe("Regular season 2026-27 · pre-game due through Oct 21 · next game Oct 22")
  })

  test("opening day before anything is due", () => {
    expect(describeSeason({ season: "2026-27", phase: "regular", post_game_due: null, pre_game_due: null, next_game_date: "2026-10-21" }))
      .toBe("Regular season 2026-27 · nothing due until the first night settles · next game Oct 21")
  })

  test("preseason: nothing nightly is due yet", () => {
    expect(describeSeason({ season: "2026-27", phase: "preseason", post_game_due: null, pre_game_due: null, next_game_date: "2026-10-03" }))
      .toBe("Preseason 2026-27 · nothing nightly is due yet · next game Oct 3")
  })

  test("offseason keeps judging through the season's last night", () => {
    expect(describeSeason({ season: "2025-26", phase: "offseason", post_game_due: "2026-04-12", pre_game_due: "2026-04-12", next_game_date: null }))
      .toBe("Offseason 2025-26 · judged through the season's last night, Apr 12")
    expect(describeSeason({ season: "2026-27", phase: "offseason", post_game_due: null, pre_game_due: null, next_game_date: null }))
      .toBe("Offseason 2026-27 · nothing nightly is due · no game scheduled")
  })
})

describe("splitTable", () => {
  test("schema and name", () => {
    expect(splitTable("stats_s2.daily_matchup_scores")).toEqual({ schema: "stats_s2", name: "daily_matchup_scores" })
    expect(splitTable("bare")).toEqual({ schema: "", name: "bare" })
  })
})
