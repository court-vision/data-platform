import { describe, expect, test } from "bun:test"

import { daysBehind, describeSeason, sortByUrgency, splitTable, summarizeFreshness, type TableFreshness } from "@/lib/freshness"

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
  test("in season: what the verdicts are judged against", () => {
    expect(describeSeason({ season: "2025-26", phase: "regular", last_game_date: "2026-03-04", next_game_date: "2026-03-06" }))
      .toBe("Regular season 2025-26 · last settled game Mar 4 · next game Mar 6")
  })

  test("opening week has no settled game yet", () => {
    expect(describeSeason({ season: "2026-27", phase: "regular", last_game_date: null, next_game_date: "2026-10-21" }))
      .toBe("Regular season 2026-27 · no game settled yet · next game Oct 21")
  })

  test("outside the season nothing nightly is due", () => {
    expect(describeSeason({ season: "2026-27", phase: "preseason", last_game_date: null, next_game_date: "2026-10-03" }))
      .toBe("Preseason 2026-27 · nothing nightly is due · next game Oct 3")
    expect(describeSeason({ season: "2025-26", phase: "offseason", last_game_date: "2026-04-12", next_game_date: null }))
      .toBe("Offseason 2025-26 · nothing nightly is due · no game scheduled")
  })
})

describe("splitTable", () => {
  test("schema and name", () => {
    expect(splitTable("stats_s2.daily_matchup_scores")).toEqual({ schema: "stats_s2", name: "daily_matchup_scores" })
    expect(splitTable("bare")).toEqual({ schema: "", name: "bare" })
  })
})
