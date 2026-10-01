import { describe, expect, test } from "bun:test"

import { axisTicks, buildLanes, markerPosition, runTone, WINDOW_MS, type CronRun, type RunTone } from "@/lib/timeline"

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
