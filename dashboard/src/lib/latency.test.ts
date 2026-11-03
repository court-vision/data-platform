import { describe, expect, test } from "bun:test"

import {
  bucketRows,
  externalShare,
  formatCalls,
  formatMs,
  formatShare,
  formatSlice,
  groupOf,
  groupsUsed,
  msTick,
  niceMs,
  parseSort,
  parseWindow,
  routeHref,
  segments,
  sortRoutes,
  type RouteLatency,
} from "@/lib/latency"

function route(overrides: Partial<RouteLatency>): RouteLatency {
  return {
    method: "GET", route: "/v1/x", requests: 1, errors: 0,
    p50_ms: 10, p95_ms: 20, p99_ms: 30, mean_ms: 12, max_ms: 40, parts: {}, calls: {},
    ...overrides,
  }
}

describe("latency", () => {
  test("buckets fold into eight coloured groups and Other, in the palette's order", () => {
    expect(groupOf("fantasy_writer")).toBe("services")
    expect(groupOf("clerk")).toBe("other")
    expect(groupOf("a_bucket_from_next_month")).toBe("other")
    const segs = segments({ app: 40, espn: 200, redis: 1, clerk: 3, sqlmate: 5, postgres: 0 })
    expect(segs.map((s) => [s.group.key, s.ms])).toEqual([["espn", 200], ["app", 40], ["services", 5], ["other", 4]])
    expect(groupsUsed([{ app: 1 }, { espn: 2 }]).map((g) => g.key)).toEqual(["espn", "app"])
  })

  test("outside our code is everything but our own code and the queues", () => {
    expect(externalShare({ espn: 300, postgres: 50, wait: 25, app: 25 })).toBeCloseTo(0.875)
    expect(externalShare({})).toBeNull()
  })

  test("routes order by total time, p95 or requests", () => {
    const busy = route({ route: "/v1/rankings", requests: 1000, mean_ms: 20, p95_ms: 40 })
    const slow = route({ route: "/v1/matchups", requests: 50, mean_ms: 900, p95_ms: 2000 })
    const many = route({ route: "/v1/health-ish", requests: 5000, mean_ms: 1, p95_ms: 2 })
    expect(sortRoutes([busy, many, slow], "time").map((r) => r.route)).toEqual(["/v1/matchups", "/v1/rankings", "/v1/health-ish"])
    expect(sortRoutes([busy, many, slow], "requests")[0].route).toBe("/v1/health-ish")
    expect(sortRoutes([busy, many, slow], "p95")[0].route).toBe("/v1/matchups")
  })

  test("formats milliseconds, shares and calls for reading", () => {
    expect([formatMs(4.25), formatMs(86.4), formatMs(1240), formatMs(12400), formatMs(null)]).toEqual(["4.3 ms", "86 ms", "1.24 s", "12.4 s", "—"])
    expect([formatShare(0.004), formatShare(0.42), formatShare(null)]).toEqual(["<1%", "42%", "—"])
    expect([formatCalls(1), formatCalls(2.25), formatCalls(0)]).toEqual(["1 call", "2.3 calls", "—"])
  })

  test("an axis tops out at a round number whose half is round", () => {
    expect([niceMs(0), niceMs(87), niceMs(130), niceMs(240), niceMs(2600)]).toEqual([10, 100, 200, 250, 5000])
    expect([msTick(0), msTick(125), msTick(1500), msTick(2000)]).toEqual(["0", "125ms", "1.5s", "2s"])
  })

  test("the window and order come from the URL, unknown values fall back", () => {
    expect([parseWindow("7d"), parseWindow("2d"), parseWindow(null)]).toEqual(["7d", "24h", "24h"])
    expect([parseSort("p95"), parseSort("vibes")]).toEqual(["p95", "time"])
    expect(routeHref({ method: "GET", route: "/v1/players/{player_id}/stats" }, "7d")).toBe(
      "/latency/route?method=GET&route=%2Fv1%2Fplayers%2F%7Bplayer_id%7D%2Fstats&window=7d",
    )
    expect(routeHref({ method: "POST", route: "/v1/x" }, "24h")).toBe("/latency/route?method=POST&route=%2Fv1%2Fx")
  })

  test("the bucket table is biggest first, with each bucket's calls", () => {
    const rows = bucketRows({ app: 40, espn: 200, postgres: 10 }, { espn: 1, postgres: 3 })
    expect(rows.map((r) => [r.bucket, r.ms, Math.round(r.share * 100), r.calls])).toEqual([
      ["espn", 200, 80, 1],
      ["app", 40, 16, 0],
      ["postgres", 10, 4, 3],
    ])
  })
})

describe("formatSlice", () => {
  test("names a slice at its grain, so a day's two ends never read alike", () => {
    expect(formatSlice("2026-10-20T19:05:00Z", 300)).toBe("2:05 PM")
    expect(formatSlice("2026-10-20T06:00:00Z", 3600)).toBe("Tue 1 AM")
    expect(formatSlice("2026-10-19T06:00:00Z", 3600)).toBe("Mon 1 AM")
    expect(formatSlice("2026-10-20T00:00:00Z", 86400)).toBe("Oct 20 UTC")
  })
})
