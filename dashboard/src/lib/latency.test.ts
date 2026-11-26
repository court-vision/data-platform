import { describe, expect, test } from "bun:test"

import {
  bucketRows,
  externalShare,
  formatCalls,
  formatMs,
  formatShare,
  formatTotal,
  formatSlice,
  groupOf,
  groupsUsed,
  msTick,
  niceMs,
  parseDirection,
  parseSort,
  parseWindow,
  partRows,
  routeHref,
  routeTag,
  routeTagTitle,
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

  test("any column orders either way; routes go A to Z first, numbers biggest first", () => {
    const busy = route({ route: "/v1/rankings", requests: 1000, mean_ms: 20, p50_ms: 15, errors: 0, parts: { app: 20 } })
    const slow = route({ route: "/v1/matchups", requests: 50, mean_ms: 900, p50_ms: 700, errors: 3, parts: { espn: 800, app: 100 } })
    const quiet = route({ route: "/v1/health", requests: 5, mean_ms: 2, p50_ms: null, errors: 0, parts: {} })
    const names = (sort: Parameters<typeof sortRoutes>[1], direction?: "asc" | "desc") =>
      sortRoutes([busy, slow, quiet], sort, direction).map((r) => r.route)

    expect(names("route")).toEqual(["/v1/health", "/v1/matchups", "/v1/rankings"])
    expect(names("route", "desc")).toEqual(["/v1/rankings", "/v1/matchups", "/v1/health"])
    expect(names("time", "asc")).toEqual(["/v1/health", "/v1/rankings", "/v1/matchups"])
    // No p50 and no split: last, whichever way.
    expect(names("p50")).toEqual(["/v1/matchups", "/v1/rankings", "/v1/health"])
    expect(names("p50", "asc")).toEqual(["/v1/rankings", "/v1/matchups", "/v1/health"])
    expect(names("outside")).toEqual(["/v1/matchups", "/v1/rankings", "/v1/health"])
    // A tie goes A to Z in both directions.
    expect(names("errors")).toEqual(["/v1/matchups", "/v1/health", "/v1/rankings"])
    expect(names("errors", "asc")).toEqual(["/v1/health", "/v1/rankings", "/v1/matchups"])
  })

  test("a route's tag is what it calls: AI, then ESPN, then NBA, then Other, then SVC, else DB", () => {
    const tag = (parts: Record<string, number>) => routeTag(parts).label
    expect(tag({ claude: 4000, espn: 300, postgres: 20, app: 50 })).toBe("AI")
    expect(tag({ espn: 200, yahoo: 60, nba: 5, app: 30 })).toBe("ESPN")
    expect(tag({ nba: 30, other_http: 4, postgres: 9 })).toBe("NBA")
    expect(tag({ yahoo: 120, postgres: 10, app: 20 })).toBe("Other")
    expect(tag({ other_http: 40, app: 5 })).toBe("Other")
    expect(tag({ a_bucket_from_next_month: 3, app: 5 })).toBe("Other")
    // An outside call outranks our own services.
    expect(tag({ yahoo: 50, sqlmate: 300 })).toBe("Other")
    expect(tag({ sqlmate: 40, postgres: 6, app: 4 })).toBe("SVC")
    expect(tag({ features: 12 })).toBe("SVC")
    expect(tag({ fantasy_writer: 90, redis: 1 })).toBe("SVC")
    // Postgres, Redis and our own code are DB; Clerk (any signed-in route can fetch its keys) changes nothing.
    expect(tag({ postgres: 6, redis: 1, clerk: 80, wait: 3, app: 4 })).toBe("DB")
    expect(tag({ clerk: 80, sqlmate: 5 })).toBe("SVC")
    expect(tag({})).toBe("DB")
    // A bucket with no time is not a call.
    expect(tag({ claude: 0, sqlmate: 0, postgres: 3 })).toBe("DB")

    expect(routeTagTitle({ espn: 200, postgres: 20, app: 35, wait: 6 })).toBe("ESPN: calls ESPN. Waits on ESPN, Postgres.")
    expect(routeTagTitle({ app: 4 })).toBe("DB: only Postgres and Redis. Only our own code.")
  })

  test("ordered by tag, routes group as AI, ESPN, NBA, SVC, DB, Other, the costliest first within each", () => {
    const chat = route({ route: "/v1/ai/chat", requests: 10, mean_ms: 5000, parts: { claude: 4800, app: 200 } })
    const lineup = route({ route: "/v1/lineup", requests: 100, mean_ms: 300, parts: { espn: 250, app: 50 } })
    const matchup = route({ route: "/v1/matchup", requests: 1000, mean_ms: 200, parts: { espn: 150, app: 50 } })
    const games = route({ route: "/v1/games", requests: 50, mean_ms: 40, parts: { nba: 30, app: 10 } })
    const rankings = route({ route: "/v1/rankings", requests: 900, mean_ms: 8, parts: { postgres: 3, app: 5 } })
    const yahoo = route({ route: "/v1/yahoo", requests: 3, mean_ms: 400, parts: { yahoo: 380, app: 20 } })
    const query = route({ route: "/v1/query", requests: 20, mean_ms: 90, parts: { sqlmate: 70, postgres: 10, app: 10 } })
    const all = [rankings, yahoo, query, games, matchup, chat, lineup]
    expect(sortRoutes(all, "tag").map((r) => r.route)).toEqual([
      "/v1/ai/chat", "/v1/matchup", "/v1/lineup", "/v1/games", "/v1/query", "/v1/rankings", "/v1/yahoo",
    ])
    expect(sortRoutes(all, "tag", "desc").map((r) => r.route)).toEqual([
      "/v1/yahoo", "/v1/rankings", "/v1/query", "/v1/games", "/v1/matchup", "/v1/lineup", "/v1/ai/chat",
    ])
  })

  test("formats milliseconds, shares and calls for reading", () => {
    expect([formatMs(4.25), formatMs(86.4), formatMs(1240), formatMs(12400), formatMs(null)]).toEqual(["4.3 ms", "86 ms", "1.24 s", "12.4 s", "—"])
    expect([formatShare(0.004), formatShare(0.42), formatShare(null)]).toEqual(["<1%", "42%", "—"])
    expect([formatCalls(1), formatCalls(2.25), formatCalls(0)]).toEqual(["1 call", "2.3 calls", "—"])
    expect([formatTotal(10_400), formatTotal(252_000), formatTotal(11_160_000), formatTotal(511_200_000), formatTotal(null)]).toEqual([
      "10.4 s", "4.2 min", "3.1 h", "142 h", "—",
    ])
  })

  test("an axis tops out at a round number whose half is round", () => {
    expect([niceMs(0), niceMs(87), niceMs(130), niceMs(240), niceMs(2600)]).toEqual([10, 100, 200, 250, 5000])
    expect([msTick(0), msTick(125), msTick(1500), msTick(2000)]).toEqual(["0", "125ms", "1.5s", "2s"])
  })

  test("the window and order come from the URL, unknown values fall back", () => {
    expect([parseWindow("7d"), parseWindow("2d"), parseWindow(null)]).toEqual(["7d", "24h", "24h"])
    // "time", "p95" and "requests" were the picker's: their addresses still work.
    expect([parseSort("p95"), parseSort("requests"), parseSort("outside"), parseSort("vibes"), parseSort(null)]).toEqual([
      "p95", "requests", "outside", "time", "time",
    ])
    expect([parseDirection(null, "time"), parseDirection(null, "route"), parseDirection(null, "tag"), parseDirection("asc", "p95"), parseDirection("up", "p95")]).toEqual([
      "desc", "asc", "asc", "asc", "desc",
    ])
    expect(routeHref({ method: "GET", route: "/v1/players/{player_id}/stats" }, "7d")).toBe(
      "/latency/route?method=GET&route=%2Fv1%2Fplayers%2F%7Bplayer_id%7D%2Fstats&window=7d",
    )
    expect(routeHref({ method: "POST", route: "/v1/x" }, "24h")).toBe("/latency/route?method=POST&route=%2Fv1%2Fx")
  })

  test("a bar's tooltip lists its groups in the bar's order, with the buckets a folded group holds", () => {
    const rows = partRows({ app: 40, espn: 150, clerk: 6, redis: 4, sqlmate: 0 })
    expect(rows.map((r) => [r.group.key, r.ms, Math.round(r.share * 100)])).toEqual([["espn", 150, 75], ["app", 40, 20], ["other", 10, 5]])
    // A group of one bucket needs no lines under it; Other names what it folded, biggest first.
    expect(rows[0].buckets).toEqual([])
    expect(rows[2].buckets.map((b) => [b.label, b.ms, Math.round(b.share * 100)])).toEqual([["Clerk", 6, 3], ["Redis", 4, 2]])
    expect(partRows({})).toEqual([])
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
