import { describe, expect, test } from "bun:test"

import {
  analystTiles,
  dayLabel,
  describeCounts,
  describeSide,
  describeSpend,
  formatSide,
  matchesRead,
  outcomeOf,
  pointsMove,
  ruleLabel,
  SIDE_COLUMNS,
  sideDiffers,
  sidesDiffer,
  usd,
  visibleReads,
  type AnalystDigest,
  type DigestRead,
  type DigestSide,
  type DigestSpend,
} from "@/lib/analyst"

function side(overrides: Partial<DigestSide> = {}): DigestSide {
  return { games: 38.2, minutes: 33.5, points: 27.9, points_rank: 3, categories_rank: 2, ...overrides }
}

function read(overrides: Partial<DigestRead> = {}): DigestRead {
  return {
    id: 1, player_id: 1, name: "Nikola Jokić", team: "DEN", as_of: "2026-11-20", read_at: "2026-11-20T11:02:10",
    reasons: [], outcome: "none", summary: null, disagrees: null, note: null, note_live: false, owner_note: false,
    live: side(), shadow: side(), cost_usd: 0.04,
    ...overrides,
  }
}

function spend(overrides: Partial<DigestSpend> = {}): DigestSpend {
  return { available: true, enabled: true, month_spent_usd: 1.84, month_cap_usd: 40, model: "claude-opus-5-5", prompt_version: "analyst-1", ...overrides }
}

function digest(overrides: Partial<AnalystDigest> = {}): AnalystDigest {
  const reads = overrides.reads ?? [read({ outcome: "note" }), read({ id: 2, outcome: "none" }), read({ id: 3, outcome: "refused" })]
  return {
    season: "2026-27", as_of: "2026-11-20", days: ["2026-11-20"], in_shadow: true, reads,
    counts: { note: 1, none: 1, refused: 1 }, cost_usd: 0.12, notes_live: 1, shadow_as_of: "2026-11-20", moved: [], spend: spend(),
    ...overrides,
  }
}

describe("what a read came to", () => {
  test("each outcome has its own words and tone, and an unknown one is shown as stored", () => {
    expect(outcomeOf("note")).toMatchObject({ label: "note written", tone: "up" })
    expect(outcomeOf("retired")).toMatchObject({ label: "note withdrawn", tone: "warn" })
    expect(outcomeOf("failed").tone).toBe("down")
    expect(outcomeOf("failed").meaning).toContain("the next run reads him again")
    expect(outcomeOf("postponed")).toMatchObject({ label: "postponed", tone: "quiet" })
  })

  test("a morning is counted in one line, notes first and trouble last", () => {
    expect(describeCounts({ refused: 1, none: 5, note: 2 })).toBe("2 notes written · 5 with nothing to write · 1 refused")
    expect(describeCounts({ note: 1, unchanged: 3, retired: 1, failed: 2, declined: 1 })).toBe(
      "1 note written · 1 withdrawn · 3 kept · 1 declined · 2 failed",
    )
    // Nothing is counted for an outcome with no reads, and one this page does not know is still said
    expect(describeCounts({ note: 0, postponed: 2 })).toBe("2 postponed")
    expect(describeCounts({})).toBe("")
  })

  test("the reads can be narrowed to those that changed a note, or went wrong", () => {
    const reads = ["note", "unchanged", "none", "retired", "refused", "declined", "failed"].map((outcome, id) => read({ id, outcome }))
    expect(visibleReads(reads, "all")).toHaveLength(7)
    expect(visibleReads(reads, "changed").map((r) => r.outcome)).toEqual(["note", "retired"])
    expect(visibleReads(reads, "trouble").map((r) => r.outcome)).toEqual(["refused", "declined", "failed"])
    expect(matchesRead({ outcome: "unchanged" }, "changed")).toBe(false)
  })

  test("a screen rule reads as a short tag", () => {
    expect(ruleLabel("status_changed")).toBe("status changed")
    expect(ruleLabel("teammate_change")).toBe("teammate")
    expect(ruleLabel("something_new")).toBe("something new")
  })
})

describe("money", () => {
  test("a cost is dollars and cents, and a read under a cent is not shown as free", () => {
    expect(usd(1.2934)).toBe("$1.29")
    expect(usd(40)).toBe("$40.00")
    expect(usd(0.0043)).toBe("$0.004")
    expect(usd(0)).toBe("$0.00")
  })

  test("the month is said against its cap, and toned as it nears and reaches it", () => {
    expect(describeSpend(spend())).toEqual({ text: "$1.84 of $40.00 this month", tone: "quiet" })
    expect(describeSpend(spend({ month_spent_usd: 33 }))).toEqual({ text: "$33.00 of $40.00 this month", tone: "warn" })
    const reached = describeSpend(spend({ month_spent_usd: 40 }))
    expect(reached.tone).toBe("down")
    expect(reached.text).toContain("the cap is reached, so nothing more is read this month")
  })

  test("switched off and unreachable are said, not shown as a bill of nothing", () => {
    expect(describeSpend(spend({ enabled: false }))).toEqual({ text: "switched off on the backend · $1.84 of $40.00 this month", tone: "warn" })
    expect(describeSpend(spend({ available: false, month_spent_usd: 0, month_cap_usd: 0 }))).toEqual({
      text: "the backend could not be asked about the month's spend", tone: "warn",
    })
  })
})

describe("the morning's tiles", () => {
  test("reads, what they cost and what one cost, the notes standing, and the month", () => {
    const tiles = analystTiles(digest())
    expect(tiles.map((tile) => tile.label)).toEqual(["Reads", "Morning's cost", "Notes standing", "This month"])
    // One of the three was refused: the reads tile says so before it says the day
    expect(tiles[0]).toMatchObject({ value: "3", tone: "warn", note: "1 went wrong" })
    expect(tiles[1]).toMatchObject({ value: "$0.12", note: "$0.04 a read" })
    expect(tiles[2]).toMatchObject({ value: "1", tone: "up" })
    expect(tiles[3]).toMatchObject({ value: "$1.84", tone: "plain", note: "of $40.00" })
  })

  test("a clean morning names its day, and an empty one divides nothing", () => {
    const clean = analystTiles(digest({ reads: [read({ outcome: "note" })] }))
    expect(clean[0]).toMatchObject({ value: "1", tone: "plain", note: "Nov 20" })
    const empty = analystTiles(digest({ reads: [], cost_usd: 0, notes_live: 0 }))
    expect(empty[0]).toMatchObject({ value: "0", tone: "quiet" })
    expect(empty[1]).toEqual({ label: "Morning's cost", value: "$0.00", tone: "quiet" })
    expect(empty[2].tone).toBe("quiet")
  })

  test("the month tile says when the analyst is off or the backend did not answer", () => {
    expect(analystTiles(digest({ spend: spend({ enabled: false }) }))[3]).toMatchObject({ value: "$1.84", tone: "warn", note: "switched off" })
    expect(analystTiles(digest({ spend: spend({ available: false }) }))[3]).toMatchObject({ value: "?", tone: "warn", note: "the backend did not answer" })
  })
})

describe("the two projections", () => {
  const [pointsRank, , games, minutes] = SIDE_COLUMNS

  test("a number is shown to its column's precision, and a missing one as a dash", () => {
    expect(formatSide(pointsRank, side())).toBe("3")
    expect(formatSide(games, side({ games: 31 }))).toBe("31.0")
    expect(formatSide(minutes, side({ minutes: null }))).toBe("—")
    expect(formatSide(games, null)).toBe("—")
  })

  test("a number differs only when both sides have it and show it differently", () => {
    const live = side()
    expect(sideDiffers(games, live, side({ games: 31 }))).toBe(true)
    expect(sideDiffers(games, live, side({ games: 38.24 }))).toBe(false)
    // An older morning keeps its ranks but not the second projection's lines
    expect(sideDiffers(games, live, null)).toBe(false)
    expect(sidesDiffer(live, side())).toBe(false)
    expect(sidesDiffer(live, side({ categories_rank: 7 }))).toBe(true)
  })

  test("a player the notes left where he was is one line: his ranks and the games ahead of him", () => {
    expect(describeSide(side({ points_rank: 14, categories_rank: 9, games: 58.1 }))).toBe("points rank 14 · 9-cat rank 9 · 58.1 games left")
    expect(describeSide(side({ points_rank: null, categories_rank: null, games: 12 }))).toBe("12.0 games left")
    expect(describeSide(side({ points_rank: null, categories_rank: null, games: null }))).toBe("on neither board")
  })

  test("a move on the points board is places gained or lost with the notes applied", () => {
    expect(pointsMove({ live: side({ points_rank: 3 }), shadow: side({ points_rank: 9 }) })).toBe("−6")
    expect(pointsMove({ live: side({ points_rank: 41 }), shadow: side({ points_rank: 37 }) })).toBe("+4")
    expect(pointsMove({ live: side(), shadow: side() })).toBe("")
    expect(pointsMove({ live: side({ points_rank: null }), shadow: side() })).toBe("")
  })

  test("the newest morning is marked in the picker", () => {
    const days = ["2026-11-20", "2026-11-19"]
    expect(dayLabel("2026-11-20", days)).toBe("Nov 20 (newest)")
    expect(dayLabel("2026-11-19", days)).toBe("Nov 19")
  })
})
