import { describe, expect, test } from "bun:test"

import {
  checksForPipeline,
  countOutcomes,
  extraDetails,
  failingStreak,
  groupChecks,
  latestResult,
  parseQualityLimit,
  resultLabel,
  sampleCell,
  sampleRows,
  summarizeLatest,
  type QualityCheckRow,
  type QualityOutcome,
} from "@/lib/quality"

function check(overrides: Partial<QualityCheckRow> = {}): QualityCheckRow {
  return {
    name: "player_game_stats_stat_ranges_valid",
    severity: "critical",
    group: "structural",
    table: "nba.player_game_stats",
    against: [],
    pipelines: ["player_game_stats"],
    failure_message: "out-of-range values",
    sql: "SELECT COUNT(*) FROM nba.player_game_stats WHERE pts < 0",
    results: ["passed"],
    ...overrides,
  }
}

function outcome(overrides: Partial<QualityOutcome> = {}): QualityOutcome {
  return {
    check_name: "c",
    status: "passed",
    severity: "critical",
    failures: 0,
    message: null,
    details: null,
    duration_ms: 3,
    definition: null,
    ...overrides,
  }
}

describe("failingStreak", () => {
  test("counts back from the newest run until a pass", () => {
    expect(failingStreak(["failed", "error", "failed", "passed", "failed"])).toBe(3)
    expect(failingStreak(["passed", "failed"])).toBe(0)
    expect(failingStreak([])).toBe(0)
  })

  test("a run that left the check out neither counts nor breaks the streak", () => {
    expect(failingStreak([null, "failed", null, "failed", "passed"])).toBe(2)
    expect(failingStreak([null, null])).toBe(0)
  })
})

test("latestResult skips runs that left the check out", () => {
  expect(latestResult([null, "failed", "passed"])).toBe("failed")
  expect(latestResult([null, null])).toBeNull()
})

test("resultLabel says it in words", () => {
  expect(["passed", "failed", "error", null].map(resultLabel)).toEqual(["passed", "failed", "could not run", "not in this run"])
})

describe("groupChecks", () => {
  test("structural, consistency, then timing, dropping an empty group", () => {
    const groups = groupChecks([check({ name: "t", group: "timing" }), check({ name: "c", group: "consistency" }), check({ name: "s" })])
    expect(groups.map((group) => [group.label, group.checks.map((c) => c.name)])).toEqual([
      ["Structural", ["s"]],
      ["Consistency", ["c"]],
      ["Timing", ["t"]],
    ])
    expect(groupChecks([check()]).map((group) => group.key)).toEqual(["structural"])
  })

  test("a group this UI does not know is still shown, last", () => {
    const groups = groupChecks([check({ name: "x", group: "volume" }), check({ name: "s" })])
    expect(groups.map((group) => group.label)).toEqual(["Structural", "volume"])
  })
})

test("summarizeLatest reads the newest run and splits failures by severity", () => {
  expect(
    summarizeLatest([
      check({ results: ["passed", "failed"] }),
      check({ results: ["failed"] }),
      check({ severity: "warning", results: ["error"] }),
      check({ results: [null, "failed"] }), // not in the newest run
    ]),
  ).toEqual({ total: 3, passed: 1, criticalFailing: 1, warningFailing: 1 })
})

test("countOutcomes splits one run's failures by severity", () => {
  expect(
    countOutcomes([outcome(), outcome({ status: "failed" }), outcome({ status: "error", severity: "warning" })]),
  ).toEqual({ total: 3, passed: 1, criticalFailing: 1, warningFailing: 1 })
})

test("extraDetails drops what the row already shows", () => {
  expect(extraDetails(outcome({ details: { failures: 37 } }))).toBeNull()
  expect(extraDetails(outcome({ details: { error: "relation does not exist" } }))).toEqual({ error: "relation does not exist" })
  expect(extraDetails(outcome())).toBeNull()
})

describe("sampleRows", () => {
  const SAMPLE = [
    { player: "Devin Booker", season_row: "2026-04-08", season_gp: 63, games_logged: 64, gap: -1 },
    { player: "New Guy", season_row: null, season_gp: 0, games_logged: 1, gap: -1 },
  ]

  test("the offending rows a failed check kept, columns in the query's order", () => {
    const sample = sampleRows(outcome({ details: { failures: 24, sample: SAMPLE } }))
    expect(sample?.columns).toEqual(["player", "season_row", "season_gp", "games_logged", "gap"])
    expect(sample?.rows).toHaveLength(2)
  })

  test("nothing when there is no sample, or it is not a list of records", () => {
    expect(sampleRows(outcome({ details: { failures: 3 } }))).toBeNull()
    expect(sampleRows(outcome({ details: { failures: 3, sample: [] } }))).toBeNull()
    expect(sampleRows(outcome({ details: { failures: 3, sample: "oops" } }))).toBeNull()
    expect(sampleRows(outcome({ details: { failures: 3, sample: [1, [2]] } }))).toBeNull()
    expect(sampleRows(outcome())).toBeNull()
  })

  test("the sample is the table's to show, so the key/value details leave it out", () => {
    expect(extraDetails(outcome({ details: { failures: 24, sample: SAMPLE } }))).toBeNull()
    expect(extraDetails(outcome({ details: { failures: 24, sample_error: "timeout" } }))).toEqual({ sample_error: "timeout" })
  })

  test("a cell is shown as text, a missing one as a dash", () => {
    expect([sampleCell("PHX"), sampleCell(-1), sampleCell(0), sampleCell(null), sampleCell(undefined)]).toEqual(["PHX", "-1", "0", "—", "—"])
  })
})

test("checksForPipeline keeps the checks that judge that pipeline's output", () => {
  const checks = [
    check({ name: "season_vs_log", group: "consistency", pipelines: ["player_season_stats", "player_game_stats"] }),
    check({ name: "ranges", pipelines: ["player_game_stats"] }),
    check({ name: "team_ran", group: "timing", pipelines: ["team_stats"] }),
  ]
  expect(checksForPipeline(checks, "player_game_stats").map((c) => c.name)).toEqual(["season_vs_log", "ranges"])
  expect(checksForPipeline(checks, "player_season_stats").map((c) => c.name)).toEqual(["season_vs_log"])
  expect(checksForPipeline(checks, "breakout_detection")).toEqual([])
})

test("parseQualityLimit accepts only the offered sizes", () => {
  expect([parseQualityLimit("50"), parseQualityLimit("7"), parseQualityLimit(null)]).toEqual([50, 20, 20])
})
