import { describe, expect, test } from "bun:test"

import {
  byDay,
  byFeature,
  byModel,
  byTeam,
  cacheHitRate,
  capInput,
  capShare,
  featuresUsed,
  formatTokens,
  formatUsd,
  niceTop,
  parseCap,
  parseWindow,
  type SpendRow,
  type UsageRow,
} from "@/lib/aiSpend"

function row(overrides: Partial<SpendRow>): SpendRow {
  return {
    day: "2026-11-18", line: "desk", feature: "chat", detail: null,
    user_id: 1, team_id: null, cost_micros: 0, open_micros: 0, entries: 1,
    ...overrides,
  }
}

function usage(overrides: Partial<UsageRow>): UsageRow {
  return {
    day: "2026-11-18", feature: "chat", model: "claude-sonnet-5-5", records: 1, model_calls: 1,
    input_tokens: 0, output_tokens: 0, cache_read_input_tokens: 0, cache_creation_input_tokens: 0, cost_micros: 0,
    ...overrides,
  }
}

describe("money", () => {
  test("dollars from micros, a sliver of a cent shown as one", () => {
    expect(formatUsd(12_345_678)).toBe("$12.35")
    expect(formatUsd(1_234_000_000)).toBe("$1,234.00")
    expect(formatUsd(0)).toBe("$0.00")
    expect(formatUsd(4_999)).toBe("<$0.01")
    expect(formatUsd(5_000)).toBe("$0.01")
    expect(formatUsd(null)).toBe("—")
  })

  test("the editor shows a cap in plain dollars", () => {
    expect(capInput(25_000_000)).toBe("25")
    expect(capInput(12_500_000)).toBe("12.5")
    expect(capInput(0)).toBe("0")
    expect(capInput(null)).toBe("")
  })

  test("a typed cap is dollars with at most two decimals, up to $1,000", () => {
    expect(parseCap(" $25 ")).toEqual({ usd: "25" })
    expect(parseCap("12.50")).toEqual({ usd: "12.50" })
    expect(parseCap("0")).toEqual({ usd: "0" })
    expect(parseCap("1000")).toEqual({ usd: "1000" })
    expect(parseCap("")).toEqual({ error: "Enter a monthly cap in dollars" })
    expect(parseCap("1.234")).toEqual({ error: "Dollars, with at most two decimals" })
    expect(parseCap("-5")).toEqual({ error: "Dollars, with at most two decimals" })
    expect(parseCap("ten")).toEqual({ error: "Dollars, with at most two decimals" })
    expect(parseCap("1000.01")).toEqual({ error: "At most $1000 a month" })
  })

  test("a cap warns at 80% and is down once used, and a zero cap is down as soon as anything is spent", () => {
    expect(capShare(10, null)).toEqual({ share: null })
    expect(capShare(79, 100)).toEqual({ share: 0.79, tone: undefined })
    expect(capShare(80, 100)).toEqual({ share: 0.8, tone: "warn" })
    expect(capShare(100, 100)).toEqual({ share: 1, tone: "down" })
    expect(capShare(0, 0)).toEqual({ share: 0, tone: "down" })
    expect(capShare(1, 0)).toEqual({ share: 1, tone: "down" })
  })

  test("the window is 7, 30 or 90 days, 30 when the URL says otherwise", () => {
    expect(parseWindow("7")).toBe("7")
    expect(parseWindow("90")).toBe("90")
    expect(parseWindow("30")).toBe("30")
    expect(parseWindow("365")).toBe("30")
    expect(parseWindow(null)).toBe("30")
  })
})

describe("by day", () => {
  test("every day of the window is there, oldest first, empty days included", () => {
    const days = byDay({
      since: "2026-10-30",
      days: 4,
      rows: [
        row({ day: "2026-10-31", feature: "chat", cost_micros: 300 }),
        row({ day: "2026-10-31", feature: "checkin", line: "autopilot", cost_micros: 200, open_micros: 200 }),
        row({ day: "2026-11-02", feature: "analyst", line: "analyst", cost_micros: 50, user_id: null }),
        row({ day: "2026-10-31", feature: "chat", team_id: 4, cost_micros: 100 }),
        // Outside the window: dropped, not folded into the last day
        row({ day: "2026-11-03", cost_micros: 999 }),
      ],
    })
    expect(days.map((day) => day.day)).toEqual(["2026-10-30", "2026-10-31", "2026-11-01", "2026-11-02"])
    expect(days.map((day) => day.total)).toEqual([0, 600, 0, 50])
    expect(days[1]).toEqual({ day: "2026-10-31", total: 600, open: 200, parts: { chat: 400, checkin: 200 } })
    // The key follows the palette, not the order spend arrived in
    expect(featuresUsed(days).map((feature) => feature.key)).toEqual(["chat", "checkin", "analyst"])
  })

  test("the scale tops out at 1, 2 or 5 times a power of ten", () => {
    expect(niceTop(0)).toBe(1_000_000)
    expect(niceTop(730_000)).toBe(1_000_000)
    expect(niceTop(1_000_001)).toBe(2_000_000)
    expect(niceTop(3_400_000)).toBe(5_000_000)
    expect(niceTop(6_000_000)).toBe(10_000_000)
    expect(niceTop(12_000_000)).toBe(20_000_000)
  })
})

describe("by feature, model and team", () => {
  test("a feature's spend is the ledger's and its tokens are the records', costliest first", () => {
    const features = byFeature({
      rows: [
        row({ feature: "chat", cost_micros: 1_000, entries: 3 }),
        row({ feature: "chat", day: "2026-11-19", cost_micros: 500, open_micros: 500, entries: 1 }),
        row({ feature: "checkin", line: "autopilot", detail: "morning", cost_micros: 2_000, entries: 2 }),
        row({ feature: "checkin", line: "autopilot", detail: "night", cost_micros: 700, entries: 1 }),
        row({ feature: "checkin", line: "autopilot", detail: "morning", day: "2026-11-19", cost_micros: 300, entries: 1 }),
        // Spend whose chat was deleted: it has no record
        row({ feature: "unlinked", cost_micros: 100 }),
      ],
      usage: [
        usage({ feature: "chat", records: 3, input_tokens: 100, cache_read_input_tokens: 300, output_tokens: 50, cost_micros: 1_000 }),
        usage({ feature: "checkin", records: 3, model_calls: 7, input_tokens: 10, cache_creation_input_tokens: 90, cost_micros: 3_000 }),
      ],
    })
    expect(features.map((f) => [f.feature.key, f.cost, f.open, f.entries, f.records])).toEqual([
      ["checkin", 3_000, 0, 4, 3],
      ["chat", 1_500, 500, 4, 3],
      ["unlinked", 100, 0, 1, 0],
    ])
    expect(features[0].kinds).toEqual([{ kind: "morning", cost: 2_300 }, { kind: "night", cost: 700 }])
    expect(features[0].modelCalls).toBe(7)
    expect(cacheHitRate(features[1])).toBe(0.75)
    expect(cacheHitRate(features[2])).toBeNull()
  })

  test("models come from the records alone, costliest first", () => {
    const models = byModel([
      usage({ model: "claude-sonnet-5-5", records: 2, cost_micros: 400, output_tokens: 20 }),
      usage({ model: "claude-opus-5-5", records: 1, cost_micros: 900 }),
      usage({ model: "claude-sonnet-5-5", feature: "analyst", records: 1, cost_micros: 100, output_tokens: 5 }),
    ])
    expect(models.map((m) => [m.model, m.records, m.recordCost, m.output])).toEqual([
      ["claude-opus-5-5", 1, 900, 0],
      ["claude-sonnet-5-5", 3, 500, 25],
    ])
  })

  test("teams, then each user's spend with no team, then the analyst's, which is no one's", () => {
    const teams = byTeam({
      teams: [{ team_id: 4, user_id: 1, name: "Dime's Delectable Team" }],
      users: [
        { user_id: 1, email: "one@example.com", allowed: true, desk: line(), autopilot: line() },
        { user_id: 2, email: "two@example.com", allowed: true, desk: line(), autopilot: line() },
      ],
      rows: [
        row({ team_id: 4, user_id: 1, feature: "chat", cost_micros: 400 }),
        row({ team_id: 4, user_id: 1, feature: "checkin", line: "autopilot", cost_micros: 600 }),
        row({ team_id: null, user_id: 2, feature: "draft", cost_micros: 300 }),
        row({ team_id: null, user_id: null, feature: "analyst", line: "analyst", cost_micros: 200 }),
        // A team the report could not name, for a user it does not list
        row({ team_id: 9, user_id: 8, feature: "chat", cost_micros: 50 }),
      ],
    })
    expect(teams.map((t) => [t.team, t.user, t.cost])).toEqual([
      ["Dime's Delectable Team", "one@example.com", 1_000],
      ["No team", "two@example.com", 300],
      ["No one's", null, 200],
      ["Team 9", "user 8", 50],
    ])
    expect(teams[0].parts).toEqual({ chat: 400, checkin: 600 })
  })

  test("token counts at a glance", () => {
    expect(formatTokens(9_999)).toBe("9,999")
    expect(formatTokens(12_345)).toBe("12.3k")
    expect(formatTokens(456_789)).toBe("457k")
    expect(formatTokens(4_500_000)).toBe("4.5M")
  })
})

function line() {
  return { spent_micros: 0, cap_micros: null, default_cap_micros: null, cap_set: false, updated_at: null, updated_by: null }
}
