import { describe, expect, test } from "bun:test"

import { chartGeometry, columnPath, formatRate, niceMax, parseLimit, runSeconds, tickLabel, ticks, type PipelineRun } from "@/lib/runs"

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
    [51, 100],
    [730, 1000],
  ])("%p -> %p", (max, expected) => {
    expect(niceMax(max)).toBe(expected)
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
})

test("columnPath rounds the top only", () => {
  const path = columnPath(10, 20, 12, 30, 50)
  expect(path.startsWith("M10,50 V24 Q10,20 14,20 H18 Q22,20 22,24 V50 Z")).toBe(true)
  expect(columnPath(10, 50, 12, 0, 50)).toBe("")
})

test("tickLabel keeps whole seconds whole and turns to minutes past 60", () => {
  expect([0, 0.5, 25, 12.5, 60, 90].map(tickLabel)).toEqual(["0s", "500ms", "25s", "12.5s", "1m", "1m 30s"])
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
