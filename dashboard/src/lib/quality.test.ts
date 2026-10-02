import { describe, expect, test } from "bun:test"

import {
  countOutcomes,
  extraDetails,
  failedThroughout,
  failingStreak,
  groupChecks,
  latestResult,
  parseQualityLimit,
  resultLabel,
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

describe("failedThroughout", () => {
  test("a streak that reaches the oldest result has no pass to end it", () => {
    expect(failedThroughout(["failed", "error", "failed"])).toBe(true)
    expect(failedThroughout(["failed"])).toBe(true)
  })

  test("runs that left the check out do not end it either", () => {
    expect(failedThroughout([null, "failed", null, "failed", null])).toBe(true)
  })

  test("a pass anywhere in the window ends the streak there, or means there is none", () => {
    expect(failedThroughout(["failed", "failed", "passed"])).toBe(false)
    expect(failedThroughout(["failed", "passed", "failed"])).toBe(false)
    expect(failedThroughout(["passed", "failed"])).toBe(false)
  })

  test("a check with no results is not failing", () => {
    expect(failedThroughout([null, null])).toBe(false)
    expect(failedThroughout([])).toBe(false)
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
  test("structural, then timing, dropping an empty group", () => {
    const groups = groupChecks([check({ name: "t", group: "timing" }), check({ name: "s" })])
    expect(groups.map((group) => [group.label, group.checks.map((c) => c.name)])).toEqual([
      ["Structural", ["s"]],
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

test("parseQualityLimit accepts only the offered sizes", () => {
  expect([parseQualityLimit("50"), parseQualityLimit("7"), parseQualityLimit(null)]).toEqual([50, 20, 20])
})
