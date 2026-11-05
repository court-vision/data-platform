import { describe, expect, test } from "bun:test"

import {
  formatLead,
  ownFalseOuts,
  parseDays,
  parseShow,
  shownCalls,
  sourceLabel,
  type AvailabilityCall,
  type AvailabilitySummary,
  type Outcome,
} from "@/lib/availability"

function call(outcome: Outcome, player_id = 1): AvailabilityCall {
  return {
    game_date: "2026-11-04", player_id, name: `P${player_id}`, team: "NYK", game_id: "0022600151",
    tip: "2026-11-05T00:30:00Z", outcome, ours_at: null, ours_by: null, espn_at: null, lead_minutes: null,
    verdict_at_tip: null, verdict_by: null, espn_at_tip: null, box: null, reason: null, detail: null,
  }
}

describe("availability", () => {
  test("the window and the rows shown fall back to a week of races", () => {
    expect(parseDays("14")).toBe("14")
    expect(parseDays("5")).toBe("7")
    expect(parseDays(null)).toBe("7")
    expect(parseShow("false_outs")).toBe("false_outs")
    expect(parseShow("nope")).toBe("races")
  })

  test("a lead reads as hours and minutes, signed for who was first", () => {
    expect(formatLead(125)).toBe("+2h 5m")
    expect(formatLead(60)).toBe("+1h")
    expect(formatLead(-10)).toBe("−10m")
    expect(formatLead(0.2)).toBe("0m")
    expect(formatLead(null)).toBe("—")
  })

  test("each choice shows its own outcomes", () => {
    const calls = (["us_first", "espn_only", "false_out", "carried", "pending"] as const).map((o, i) => call(o, i))

    expect(shownCalls(calls, "races").map((c) => c.outcome)).toEqual(["us_first", "espn_only"])
    expect(shownCalls(calls, "false_outs").map((c) => c.outcome)).toEqual(["false_out"])
    expect(shownCalls(calls, "others").map((c) => c.outcome)).toEqual(["carried", "pending"])
    expect(shownCalls(calls, "all")).toHaveLength(5)
  })

  test("the floor's false outs are not ours", () => {
    expect(ownFalseOuts({ false_outs: 3, false_outs_floor: 2 } as AvailabilitySummary)).toBe(1)
  })

  test("sources read as their names, an unknown one as itself", () => {
    expect(sourceLabel("espn_injuries")).toBe("ESPN injuries")
    expect(sourceLabel("rotowire")).toBe("rotowire")
    expect(sourceLabel(null)).toBe("—")
  })
})
