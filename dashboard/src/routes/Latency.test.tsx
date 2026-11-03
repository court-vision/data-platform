import { describe, expect, test } from "bun:test"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import type { ReactElement } from "react"
import { renderToStaticMarkup } from "react-dom/server"
import { MemoryRouter } from "react-router"

import { latencyQuery, routeLatencyQuery } from "@/hooks/useLatency"
import type { LatencyBin, LatencyOverview, LatencyWindow, RouteLatency, RouteLatencyDetail } from "@/lib/latency"
import { Latency } from "@/routes/Latency"
import { LatencyRoute } from "@/routes/LatencyRoute"
import { pressed } from "@/test-helpers"

// No DOM and no network: each page renders to markup from a query cache filled
// by hand, and is read as text.

const MATCHUP: RouteLatency = {
  method: "GET", route: "/v1/matchups/live/{team_id}", requests: 40, errors: 2,
  p50_ms: 180, p95_ms: 900, p99_ms: 1400, mean_ms: 260, max_ms: 1500,
  parts: { espn: 200, postgres: 20, app: 35, wait: 5 }, calls: { espn: 1, postgres: 3 },
}
const RANKINGS: RouteLatency = {
  method: "GET", route: "/v1/rankings", requests: 1200, errors: 0,
  p50_ms: 7, p95_ms: 30, p99_ms: 45, mean_ms: 8, max_ms: 80,
  parts: { postgres: 3, app: 5 }, calls: { postgres: 2 },
}

function overview(overrides: Partial<LatencyOverview> = {}): LatencyOverview {
  return {
    service: "api", window: "24h", since: "2026-10-19T18:00:00Z", until: "2026-10-20T18:00:00Z", available: true,
    totals: { requests: 1240, errors: 2, p50_ms: 9, p95_ms: 400, p99_ms: 1100, mean_ms: 18, max_ms: 1500 },
    routes: [MATCHUP, RANKINGS],
    ...overrides,
  }
}

function render(element: ReactElement, path: string, fill: (client: QueryClient) => void): string {
  const client = new QueryClient()
  fill(client)
  return renderToStaticMarkup(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>{element}</MemoryRouter>
    </QueryClientProvider>,
  )
}

function routeNames(html: string): string[] {
  return [...html.matchAll(/class="nameLink path"[^>]*>([^<]+)</g)].map((match) => match[1])
}

describe("Latency", () => {
  test("lists the routes costliest first, each with its split and a link to its page", () => {
    const html = render(<Latency />, "/latency", (client) => client.setQueryData(latencyQuery("24h").queryKey, overview()))

    // 40 × 260 ms beats 1,200 × 8 ms.
    expect(routeNames(html)).toEqual(["/v1/matchups/live/{team_id}", "/v1/rankings"])
    expect(html).toContain('href="/latency/route?method=GET&amp;route=%2Fv1%2Fmatchups%2Flive%2F%7Bteam_id%7D"')
    expect(html).toContain("Average request: 260 ms · ESPN 200 ms · Postgres 20 ms · Our code 35 ms · Queued 5.0 ms")
    // ESPN and Postgres are outside our code: 220 of 260 ms.
    const matchup = html.split("<tr").find((row) => row.includes("matchups/live"))!
    expect(matchup).toContain(">85%</td>")
    expect(matchup).toContain(">2</td>")
    expect(html).toContain(">1,240</dd>")
    expect(html).toMatch(pressed("24h"))
    expect(html).toMatch(pressed("Total time"))
  })

  test("the order and the window come from the URL", () => {
    const html = render(<Latency />, "/latency?window=7d&sort=requests", (client) =>
      client.setQueryData(latencyQuery("7d").queryKey, overview({ window: "7d" })),
    )
    expect(routeNames(html)).toEqual(["/v1/rankings", "/v1/matchups/live/{team_id}"])
    expect(html).toMatch(pressed("7d"))
  })

  test("while another window loads, the text names the window of the numbers on screen", () => {
    // The 30d answer has not landed: the 24h one stands in for it.
    const html = render(<Latency />, "/latency?window=30d", (client) =>
      client.setQueryData(latencyQuery("30d").queryKey, overview({ window: "24h" })),
    )
    expect(html).toContain("in the last 24h")
    expect(html).not.toContain("in the last 30d")
    expect(html).toMatch(pressed("30d"))
  })

  test("before the backend writes any timings, the page says so", () => {
    const html = render(<Latency />, "/latency", (client) =>
      client.setQueryData(latencyQuery("24h").queryKey, overview({ available: false, routes: [] })),
    )
    expect(html).toContain("No request timings yet.")
    expect(routeNames(html)).toEqual([])
  })
})

function bin(at: string, overrides: Partial<LatencyBin> = {}): LatencyBin {
  return { at, requests: 0, errors: 0, p50_ms: null, p95_ms: null, parts: {}, ...overrides }
}

function detail(window: LatencyWindow, overrides: Partial<RouteLatencyDetail> = {}): RouteLatencyDetail {
  return {
    service: "api", window, since: "2026-10-20T15:00:00Z", until: "2026-10-20T18:00:00Z", available: true,
    bin_seconds: 3600, route: MATCHUP,
    series: [
      bin("2026-10-20T15:00:00Z", { requests: 10, p50_ms: 150, p95_ms: 600, parts: { espn: 150, app: 30 } }),
      bin("2026-10-20T16:00:00Z"),
      bin("2026-10-20T17:00:00Z", { requests: 30, errors: 2, p50_ms: 190, p95_ms: 950, parts: { espn: 210, postgres: 20, app: 40 } }),
    ],
    slowest: [
      {
        started_at: "2026-10-20T17:12:00Z", duration_ms: 1500, status_code: 502,
        parts: { espn: 1400, app: 100 }, calls: { espn: 1 }, trace_id: "0af7651916cd43dd8448eb211c80319c", version: "abc1234def",
      },
    ],
    ...overrides,
  }
}

describe("LatencyRoute", () => {
  const PATH = "/latency/route?method=GET&route=%2Fv1%2Fmatchups%2Flive%2F%7Bteam_id%7D"

  test("shows the route's times, its split per bucket and its slowest requests", () => {
    const html = render(<LatencyRoute />, PATH, (client) =>
      client.setQueryData(routeLatencyQuery("GET", MATCHUP.route, "24h").queryKey, detail("24h")),
    )

    expect(html).toContain("/v1/matchups/live/{team_id}</h1>")
    expect(html).toContain(">900 ms</dd>")
    // The bucket table: biggest first, with calls per request.
    const table = html.split('aria-label="Average request"')[1].split("</table>")[0]
    const rows = table.split("<tr").slice(2)
    expect(rows[0]).toContain(">ESPN</span>")
    expect(rows[0]).toContain(">77%</td>")
    expect(rows[0]).toContain(">1 call</td>")
    // The slowest request: its status, its split, its trace id and version.
    expect(html).toContain("0af7651916cd43dd8448eb211c80319c")
    expect(html).toContain(">abc1234</span>")
    expect(html).toContain("1.50 s · ESPN 1.40 s · Our code 100 ms")
    // Two lines on the percentile chart, and a stack per slice that had requests.
    expect(html.match(/class="line p(50|95)"/g)).toHaveLength(2)
    expect(html.match(/class="stack"/g)).toHaveLength(5)
  })

  test("a route with no requests in the window says so, and keeps the window choice", () => {
    const html = render(<LatencyRoute />, `${PATH}&window=1h`, (client) =>
      client.setQueryData(routeLatencyQuery("GET", MATCHUP.route, "1h").queryKey, detail("1h", { route: null, series: [], slowest: [] })),
    )
    expect(html).toContain("No GET requests to this route in the last 1h.")
    expect(html).toMatch(pressed("1h"))
    expect(html).toContain('href="/latency?window=1h"')
  })

  test("without a route there is nothing to show", () => {
    const html = render(<LatencyRoute />, "/latency/route", () => {})
    expect(html).toContain("No route given")
  })
})
