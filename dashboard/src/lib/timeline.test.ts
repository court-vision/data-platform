import { describe, expect, test } from "bun:test"

import {
  axisTicks,
  buildLanes,
  clusterBuckets,
  clusterRuns,
  markerPosition,
  parseRange,
  rangeMs,
  RANGES,
  runTone,
  WINDOW_MS,
  worstTone,
  type CronRun,
  type RunBucket,
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

describe("counted columns", () => {
  // Past a day the server counts the runs per job and column. A week in 96
  // columns is 105 minutes a column, cut from the epoch.
  const week = rangeMs(parseRange("7d"))
  const bucketMs = week / 96
  const column = (iso: string) => Math.floor(Date.parse(`${iso}Z`) / bucketMs) * bucketMs
  const naive = (ms: number) => new Date(ms).toISOString().slice(0, 19)

  function bucket(at: string, overrides: Partial<RunBucket> = {}): RunBucket {
    return {
      job_name: "live-stats",
      start: naive(column(at)),
      runs: 210,
      failed: 0,
      retried: 0,
      first_triggered_at: at,
      last_triggered_at: at,
      ...overrides,
    }
  }

  test("a lane is its columns, and holds as many runs as they count", () => {
    const counted = { buckets: [bucket("2026-09-20T03:00:00"), bucket("2026-09-22T03:00:00", { runs: 90 })], bucketMs }
    const lanes = buildLanes([], now, week, counted)
    expect(lanes.map((lane) => [lane.job, lane.count])).toEqual([["pre-game", 0], ["live-stats", 300], ["post-game", 0]])
    expect(lanes[1].counted?.buckets).toHaveLength(2)
  })

  test("a column older than the window is neither drawn nor counted", () => {
    const counted = { buckets: [bucket("2026-09-10T03:00:00"), bucket("2026-09-22T03:00:00")], bucketMs }
    expect(buildLanes([], now, week, counted)[1].count).toBe(210)
    expect(clusterBuckets(counted, [], now, week)).toHaveLength(1)
  })

  test("a column is one mark at its centre, a column wide, in the worst tone it counts", () => {
    const at = "2026-09-22T03:00:00"
    const [calm, retried, failed] = [{}, { retried: 2 }, { retried: 2, failed: 1 }].map(
      (counts) => clusterBuckets({ buckets: [bucket(at, counts)], bucketMs }, [], now, week)[0],
    )
    expect([calm.tone, retried.tone, failed.tone]).toEqual(["success", "retried", "failure"])
    expect(calm.count).toBe(210)
    expect(calm.width).toBeCloseTo(100 / 96)
    expect(calm.position).toBeCloseTo(((column(at) + bucketMs / 2 - (now - week)) / week) * 100)
  })

  test("the runs that came with a column are its own, oldest first", () => {
    const at = "2026-09-22T03:00:00"
    const mine = [
      run({ id: "newest", job_name: "live-stats", triggered_at: "2026-09-22T03:20:00" }),
      run({ id: "failed", job_name: "live-stats", triggered_at: "2026-09-22T03:05:00", result: "failure" }),
    ]
    const elsewhere = run({ id: "other", job_name: "live-stats", triggered_at: "2026-09-23T03:05:00" })
    const [mark] = clusterBuckets({ buckets: [bucket(at, { failed: 1 })], bucketMs }, [...mine, elsewhere], now, week)
    expect(mark.runs.map((r) => r.id)).toEqual(["failed", "newest"])
    expect([mark.count, mark.failed]).toEqual([210, 1])
  })

  test("a run alone in its column keeps its own moment", () => {
    const at = "2026-09-22T06:00:00"
    const only = run({ id: "only", job_name: "playoffs", triggered_at: at })
    const [mark] = clusterBuckets({ buckets: [bucket(at, { job_name: "playoffs", runs: 1 })], bucketMs }, [only], now, week)
    expect([mark.count, mark.runs.length]).toEqual([1, 1])
    expect(mark.position).toBe(markerPosition(at, now, week) as number)
  })

  test("the columns at the window's edges stay inside the lane", () => {
    const edges = [bucket(naive(now - week + 60_000)), bucket(naive(now - 60_000))]
    const marks = clusterBuckets({ buckets: edges, bucketMs }, [], now, week)
    expect(marks).toHaveLength(2)
    for (const mark of marks) {
      expect(mark.position - mark.width / 2).toBeGreaterThanOrEqual(0)
      expect(mark.position + mark.width / 2).toBeLessThanOrEqual(100)
    }
  })

  test("a wider range's columns are drawn as wide as they are in a narrower window", () => {
    // The 7d reply kept on screen while 3d loads: each column still spans 105 minutes.
    const threeDays = rangeMs(parseRange("3d"))
    const [mark] = clusterBuckets({ buckets: [bucket("2026-09-23T03:00:00")], bucketMs }, [], now, threeDays)
    expect(mark.width).toBeCloseTo((bucketMs / threeDays) * 100)
  })
})
