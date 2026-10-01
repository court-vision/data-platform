import { describe, expect, test } from "bun:test"

import { chartGeometry, columnPath, formatRate, niceMax, parseLimit, roveIndex, runSeconds, tickLabel, ticks, type PipelineRun } from "@/lib/runs"

const now = Date.parse("2026-03-05T12:00:30Z")

function run(overrides: Partial<PipelineRun> = {}): PipelineRun {
  return {
    id: "r",
    started_at: "2026-03-05T12:00:00",
    completed_at: "2026-03-05T12:00:12",
    status: "success",
    duration_seconds: 12,
    records_processed: 240,
    error_message: null,
    ...overrides,
  }
}

describe("niceMax", () => {
  test.each([
    [0, 1],
    [0.4, 0.5],
    [3, 5],
    [12, 20],
    [50, 50],
    [51, 60],
    [60, 60],
    [61, 120],
    [570, 600],
    [730, 1200],
    [3600, 3600],
    [3601, 7200],
    [30 * 3600, 40 * 3600],
  ])("%p -> %p", (max, expected) => {
    expect(niceMax(max)).toBe(expected)
  })

  test("every tick label fits the axis, from a millisecond to a week", () => {
    // The axis is 44px with a 6px gap, and 10px mono is 6px a character: six
    // fit. A 570s run used to get a "16m 40s" ceiling, which lost its "1".
    for (let seconds = 0.001; seconds < 7 * 86_400; seconds *= 1.07) {
      const top = niceMax(seconds)
      expect(top).toBeGreaterThanOrEqual(seconds)
      for (const label of ticks(top).map(tickLabel)) {
        expect(label.length).toBeLessThanOrEqual(6)
      }
    }
  })

  test("past a minute the ticks are whole minutes, past an hour whole hours", () => {
    expect(ticks(niceMax(570)).map(tickLabel)).toEqual(["0s", "5m", "10m"])
    expect(ticks(niceMax(55)).map(tickLabel)).toEqual(["0s", "30s", "1m"])
    expect(ticks(niceMax(2000)).map(tickLabel)).toEqual(["0s", "30m", "1h"])
    expect(ticks(niceMax(7000)).map(tickLabel)).toEqual(["0s", "1h", "2h"])
    expect(ticks(niceMax(71 * 3600)).map(tickLabel)).toEqual(["0s", "50h", "100h"])
  })
})

test("ticks are 0, half, top", () => {
  expect(ticks(20)).toEqual([0, 10, 20])
})

describe("runSeconds", () => {
  test("a finished run is its duration", () => {
    expect(runSeconds(run(), now)).toBe(12)
  })

  test("a running run is the time elapsed so far", () => {
    expect(runSeconds(run({ status: "running", completed_at: null, duration_seconds: null }), now)).toBe(30)
  })

  test("a finished run with no duration is zero, not NaN", () => {
    expect(runSeconds(run({ duration_seconds: null }), now)).toBe(0)
  })

  test("a stuck run has no length: it is not still growing", () => {
    const stuck = run({ status: "stuck", started_at: "2026-03-02T12:00:00", completed_at: null, duration_seconds: null })
    expect(runSeconds(stuck, now)).toBe(0)
  })
})

describe("chartGeometry", () => {
  const runs = [
    run({ id: "newest", started_at: "2026-03-05T12:00:00", duration_seconds: 20 }),
    run({ id: "middle", started_at: "2026-03-04T12:00:00", duration_seconds: 5 }),
    run({ id: "oldest", started_at: "2026-03-03T12:00:00", duration_seconds: 10 }),
  ]

  test("oldest run on the left, newest on the right", () => {
    const { bars } = chartGeometry(runs, now, 300, 100)
    expect(bars.map((bar) => bar.run.id)).toEqual(["oldest", "middle", "newest"])
  })

  test("heights are proportional to a nice ceiling", () => {
    const { bars, top } = chartGeometry(runs, now, 300, 100)
    expect(top).toBe(20)
    expect(bars.map((bar) => bar.height)).toEqual([50, 25, 100])
    expect(bars[2].y).toBe(0)
  })

  test("bars stay 24px or thinner with a 2px gap and never overlap", () => {
    const { bars } = chartGeometry(runs, now, 300, 100)
    expect(bars.every((bar) => bar.width <= 24)).toBe(true)
    for (let i = 1; i < bars.length; i++) {
      expect(bars[i].x).toBeGreaterThanOrEqual(bars[i - 1].x + bars[i - 1].width + 2)
    }
    const many = chartGeometry(Array.from({ length: 200 }, (_, i) => run({ id: String(i) })), now, 800, 100)
    expect(many.bars[0].width).toBeGreaterThanOrEqual(1)
    expect(many.bars[1].x - many.bars[0].x).toBeCloseTo(4)
  })

  test("no runs is an empty chart with a unit ceiling", () => {
    expect(chartGeometry([], now, 300, 100)).toEqual({ bars: [], top: 1, plotHeight: 100, width: 300 })
  })

  const thirty = Array.from({ length: 5 }, (_, i) => run({ id: `ok${i}`, duration_seconds: 30 }))

  test("a stuck run does not set the ceiling: it stands full height, off the scale", () => {
    const stuck = run({ id: "stuck", status: "stuck", started_at: "2026-03-02T12:00:00", completed_at: null, duration_seconds: null })
    const { bars, top } = chartGeometry([...thirty, stuck], now, 600, 100)
    expect(top).toBe(50) // three days of "running so far" made it 500000
    expect(bars.find((bar) => bar.run.id === "ok0")?.height).toBe(60)
    expect(bars.find((bar) => bar.run.id === "stuck")).toMatchObject({ seconds: 0, height: 100, y: 0, over: true })
  })

  test("a live running run still does: it has taken that long so far", () => {
    const live = run({ id: "live", status: "running", started_at: "2026-03-05T11:58:30", completed_at: null, duration_seconds: null })
    const { bars, top } = chartGeometry([live, ...thirty], now, 600, 100)
    expect(top).toBe(120)
    expect(bars.find((bar) => bar.run.id === "live")).toMatchObject({ seconds: 120, height: 100, over: false })
  })

  test("a failed run past the ceiling is drawn over it, and the rest keep their scale", () => {
    // What a restart leaves of a hung run: failed, and as long as the hang.
    const swept = run({ id: "swept", status: "failed", duration_seconds: 10_800, error_message: "Interrupted by service restart" })
    const slow = run({ id: "slow", status: "failed", duration_seconds: 45, error_message: "ESPN 503" })
    const { bars, top } = chartGeometry([swept, slow, ...thirty], now, 700, 100)
    expect(top).toBe(50)
    expect(bars.find((bar) => bar.run.id === "swept")).toMatchObject({ seconds: 10_800, height: 100, over: true })
    expect(bars.find((bar) => bar.run.id === "slow")).toMatchObject({ height: 90, over: false })
  })

  test("with nothing but failures the axis is scaled to those", () => {
    const failures = [4, 8].map((seconds) => run({ id: String(seconds), status: "failed", duration_seconds: seconds }))
    const { bars, top } = chartGeometry(failures, now, 300, 100)
    expect(top).toBe(10)
    expect(bars.map((bar) => [bar.height, bar.over])).toEqual([[80, false], [40, false]])
  })

  test("a bar too short to see stands 2px above the baseline, not below it", () => {
    const fast = run({ id: "fast", status: "failed", duration_seconds: 0.05 })
    const none = run({ id: "none", status: "failed", duration_seconds: null })
    const { bars } = chartGeometry([fast, none, run({ id: "slow", duration_seconds: 60 })], now, 300, 120)
    for (const id of ["fast", "none"]) {
      const bar = bars.find((candidate) => candidate.run.id === id)!
      expect([bar.height, bar.y]).toEqual([2, 118])
      // The top edge is drawn at 118 and the path closes on the baseline.
      expect(columnPath(bar.x, bar.y, bar.width, bar.height, 120)).toBe(
        `M${bar.x},120 V120 Q${bar.x},118 ${bar.x + 2},118 H${bar.x + bar.width - 2} Q${bar.x + bar.width},118 ${bar.x + bar.width},120 V120 Z`,
      )
    }
  })
})

test("columnPath rounds the top only", () => {
  const path = columnPath(10, 20, 12, 30, 50)
  expect(path.startsWith("M10,50 V24 Q10,20 14,20 H18 Q22,20 22,24 V50 Z")).toBe(true)
  expect(columnPath(10, 50, 12, 0, 50)).toBe("")
})

test("tickLabel keeps whole seconds whole, turns to minutes past 60 and hours past 60 of those", () => {
  expect([0, 0.5, 25, 12.5, 60, 90].map(tickLabel)).toEqual(["0s", "500ms", "25s", "12.5s", "1m", "1m 30s"])
  expect([3600, 5400, 43_200].map(tickLabel)).toEqual(["1h", "1h 30m", "12h"])
})

describe("roveIndex", () => {
  test("the arrows step one run and stop at the ends", () => {
    expect(roveIndex("ArrowLeft", 3, 50)).toBe(2)
    expect(roveIndex("ArrowRight", 3, 50)).toBe(4)
    expect(roveIndex("ArrowLeft", 0, 50)).toBe(0)
    expect(roveIndex("ArrowRight", 49, 50)).toBe(49)
  })

  test("Home and End jump to the oldest and the newest", () => {
    expect(roveIndex("Home", 30, 50)).toBe(0)
    expect(roveIndex("End", 30, 50)).toBe(49)
  })

  test("any other key is not the chart's", () => {
    expect(roveIndex("Tab", 3, 50)).toBeNull()
    expect(roveIndex("a", 3, 50)).toBeNull()
  })
})

test("parseLimit accepts only the offered sizes", () => {
  expect(parseLimit("100")).toBe(100)
  expect(parseLimit("7")).toBe(50)
  expect(parseLimit(null)).toBe(50)
})

test("formatRate", () => {
  expect(formatRate(null)).toBe("—")
  expect(formatRate(2 / 3)).toBe("67%")
})
