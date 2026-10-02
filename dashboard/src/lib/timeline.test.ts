import { describe, expect, test } from "bun:test"

import {
  axisTicks,
  buildLanes,
  clusterRuns,
  markerPosition,
  parseRange,
  rangeMs,
  RANGES,
  runTone,
  WINDOW_MS,
  worstTone,
  type CronRun,
  type RunTone,
} from "@/lib/timeline"

const now = Date.parse("2026-09-24T18:00:00Z")

function run(overrides: Partial<CronRun> = {}): CronRun {
  return {
    id: "r1",
    job_name: "post-game",
    triggered_at: "2026-09-24T15:00:00",
    completed_at: "2026-09-24T15:04:00",
    duration_ms: 240_000,
    duration_seconds: 240,
    result: "success",
    http_status: 200,
    attempts: 1,
    error_message: null,
    response_snippet: null,
    ...overrides,
  }
}

describe("markerPosition", () => {
  test("window start is 0, now is 100, halfway is 50", () => {
    expect(markerPosition("2026-09-24T12:00:00", now)).toBe(0)
    expect(markerPosition("2026-09-24T18:00:00", now)).toBe(100)
    expect(markerPosition("2026-09-24T15:00:00", now)).toBe(50)
  })

  test("outside the window is null, either side", () => {
    expect(markerPosition("2026-09-24T11:59:59", now)).toBeNull()
    expect(markerPosition("2026-09-24T18:00:01", now)).toBeNull()
    expect(markerPosition("garbage", now)).toBeNull()
  })

  test("reads offset-less timestamps as UTC", () => {
    expect(markerPosition("2026-09-24T15:00:00", now)).toBe(markerPosition("2026-09-24T15:00:00Z", now))
  })
})

describe("runTone", () => {
  const cases: Array<[{ result: string; attempts: number }, RunTone]> = [
    [{ result: "success", attempts: 1 }, "success"],
    [{ result: "success", attempts: 3 }, "retried"],
    [{ result: "failure", attempts: 3 }, "failure"],
  ]
  test.each(cases)("%o -> %s", (input, expected) => {
    expect(runTone(input)).toBe(expected)
  })
})

describe("buildLanes", () => {
  test("the batch jobs always have a lane, in order, even with no runs", () => {
    expect(buildLanes([], now).map((lane) => lane.job)).toEqual(["pre-game", "live-stats", "post-game"])
  })

  test("other known jobs appear only with runs, after the batch three", () => {
    const lanes = buildLanes([run({ job_name: "playoffs" })], now)
    expect(lanes.map((lane) => lane.job)).toEqual(["pre-game", "live-stats", "post-game", "playoffs"])
    expect(lanes[3].runs).toHaveLength(1)
  })

  test("runs outside the window are dropped", () => {
    const lanes = buildLanes([run({ triggered_at: "2026-09-24T11:00:00" })], now)
    expect(lanes.find((lane) => lane.job === "post-game")?.runs).toHaveLength(0)
  })

  test("a job this UI does not know still gets a lane, last", () => {
    const lanes = buildLanes([run({ job_name: "alert-test" })], now)
    expect(lanes.at(-1)?.job).toBe("alert-test")
  })
})

test("axisTicks spans the window in 30-minute steps, inclusive", () => {
  const ticks = axisTicks(now)
  expect(ticks).toHaveLength(13)
  expect(ticks[0]).toBe(now - WINDOW_MS)
  expect(ticks.at(-1)).toBe(now)
})

describe("ranges", () => {
  test("six hours unless the URL names another offered range", () => {
    expect([parseRange(null), parseRange("12h"), parseRange("")].map((range) => range.key)).toEqual(["6h", "6h", "6h"])
    expect(RANGES.map((range) => parseRange(range.key).hours)).toEqual([6, 24, 72, 168])
  })

  test("the default range is the window the status payload carries", () => {
    expect(rangeMs(parseRange(null))).toBe(WINDOW_MS)
  })

  test("every range draws a readable number of ticks", () => {
    for (const range of RANGES) {
      const ticks = axisTicks(now, rangeMs(range), range.tickMs)
      // Clock labels fit thirteen across; the wider day-and-hour labels get fewer.
      expect(ticks.length).toBe(range.dayTicks ? (range.key === "3d" ? 7 : 8) : 13)
      expect(ticks.at(-1)).toBe(now)
    }
  })

  test("a run from yesterday is inside a longer window and outside the default", () => {
    const yesterday = "2026-09-23T20:00:00"
    expect(markerPosition(yesterday, now)).toBeNull()
    expect(markerPosition(yesterday, now, rangeMs(parseRange("24h")))).toBeCloseTo((2 / 24) * 100)
  })
})

describe("clusterRuns", () => {
  const minute = (m: number, overrides: Partial<CronRun> = {}) =>
    run({ id: `m${m}`, triggered_at: `2026-09-24T17:${String(m).padStart(2, "0")}:00`, ...overrides })

  test("a lone run keeps its own moment", () => {
    const [only] = clusterRuns([run()], now)
    expect(only.runs).toHaveLength(1)
    expect(only.position).toBe(50)
  })

  test("runs sharing a column become one mark at the column's centre, oldest first", () => {
    // 96 columns over 6 hours are 3.75 minutes each: 17:00, 17:01 and 17:02 share one.
    const clusters = clusterRuns([minute(2), minute(0), minute(1)], now)
    expect(clusters).toHaveLength(1)
    expect(clusters[0].runs.map((r) => r.id)).toEqual(["m0", "m1", "m2"])
    expect(clusters[0].position).toBeCloseTo(((clusters[0].slot + 0.5) / 96) * 100)
  })

  test("a group is drawn in its worst tone, so one failure among many still shows", () => {
    expect(clusterRuns([minute(0), minute(1, { result: "failure" }), minute(2)], now)[0].tone).toBe("failure")
    expect(clusterRuns([minute(0), minute(1, { attempts: 3 })], now)[0].tone).toBe("retried")
    expect(worstTone([minute(0), minute(1)])).toBe("success")
  })

  test("marks come left to right, and runs outside the window are dropped", () => {
    const clusters = clusterRuns([minute(30), run({ id: "old", triggered_at: "2026-09-24T11:00:00" }), minute(0)], now)
    expect(clusters.map((c) => c.runs[0].id)).toEqual(["m0", "m30"])
  })

  test("the run at this very moment falls in the last column, not past it", () => {
    const [last] = clusterRuns([run({ triggered_at: "2026-09-24T18:00:00" })], now)
    expect(last.slot).toBe(95)
  })

  test("a week of minute-by-minute polls is at most one mark per column", () => {
    const week = rangeMs(parseRange("7d"))
    const polls = Array.from({ length: 2000 }, (_, i) =>
      run({ id: `p${i}`, triggered_at: new Date(now - i * 60_000).toISOString().slice(0, 19) }),
    )
    const clusters = clusterRuns(polls, now, week)
    expect(clusters.length).toBeLessThanOrEqual(96)
    expect(clusters.reduce((count, c) => count + c.runs.length, 0)).toBe(2000)
  })
})
