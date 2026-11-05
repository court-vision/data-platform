import { describe, expect, test } from "bun:test"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { renderToStaticMarkup } from "react-dom/server"
import { MemoryRouter } from "react-router"

import { availabilityQuery } from "@/hooks/useAvailability"
import type { AvailabilityCall, AvailabilityScorecard, AvailabilitySummary, Days } from "@/lib/availability"
import { Availability } from "@/routes/Availability"
import { pressed } from "@/test-helpers"

// No DOM and no network: the page renders to markup from a query cache filled
// by hand, and is read as text.

const SUMMARY: AvailabilitySummary = {
  races: 4, us_first: 2, same_poll: 0, espn_first: 1, us_only: 1, espn_only: 0,
  carried: 6, unannounced: 1, not_in_box: 0, pending: 2,
  first_share: 0.75, median_lead_minutes: 45, first_by: { espn_injuries: 3 },
  false_outs: 1, false_outs_floor: 1, espn_false_outs: 1, gate_met: true,
}

function call(overrides: Partial<AvailabilityCall>): AvailabilityCall {
  return {
    game_date: "2026-11-04", player_id: 1628973, name: "Jalen Brunson", team: "NYK", game_id: "0022600151",
    tip: "2026-11-05T00:30:00+00:00", outcome: "us_first",
    ours_at: "2026-11-04T20:00:00+00:00", ours_by: "espn_injuries",
    espn_at: "2026-11-04T21:15:00+00:00", lead_minutes: 75,
    verdict_at_tip: "out", verdict_by: "espn_injuries", espn_at_tip: "out", box: "inactive",
    reason: "Ankle", detail: "Brunson won't play Wednesday against Boston.",
    ...overrides,
  }
}

function scorecard(overrides: Partial<AvailabilityScorecard> = {}): AvailabilityScorecard {
  return {
    days: 7, since: "2026-10-29", until: "2026-11-04", generated_at: "2026-11-05T01:00:00Z", available: true,
    summary: SUMMARY,
    dates: [
      { game_date: "2026-11-04", games: 8, first_tip: "2026-11-05T00:00:00Z", first_poll: "2026-11-04T11:00:00Z", covered: true, calls: 2 },
      { game_date: "2026-11-02", games: 10, first_tip: "2026-11-03T00:00:00Z", first_poll: "2026-11-03T08:30:00Z", covered: false, calls: 0 },
    ],
    calls: [
      call({}),
      call({
        player_id: 1628369, name: "Jayson Tatum", team: "BOS", outcome: "false_out", ours_at: null, ours_by: null,
        espn_at: "2026-11-04T20:00:00+00:00", lead_minutes: null, verdict_by: "espn_fantasy", box: "active",
        reason: null, detail: null,
      }),
    ],
    ...overrides,
  }
}

function render(path: string, days: Days, data: AvailabilityScorecard): string {
  const client = new QueryClient()
  client.setQueryData(availabilityQuery(days).queryKey, data)
  return renderToStaticMarkup(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>
        <Availability />
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

function players(html: string): string[] {
  const body = html.split('aria-label="Players"')[1] ?? ""
  return [...body.matchAll(/<th scope="row" class="nowrap">([^<]+)</g)].map((match) => match[1])
}

describe("Availability", () => {
  test("the tiles read the gate: first share, lead, our false outs and the floor's", () => {
    const html = render("/availability", "7", scorecard())

    expect(html).toContain(">75%</dd>")
    expect(html).toContain("3 of 4 scratches, first or only")
    expect(html).toContain(">+45m</dd>")
    expect(html).toContain("ours; 1 more from ESPN fantasy&#x27;s floor")
    expect(html).toContain(">Met</dd>")
    expect(html).toContain("Our firsts came from ESPN injuries (3).")
    expect(html).toMatch(pressed("7d"))
  })

  test("races are shown by default, each side's time in Central", () => {
    const html = render("/availability", "7", scorecard())

    expect(players(html)).toEqual(["Jalen Brunson"])
    // 20:00 and 21:15 UTC are 2:00 and 3:15 PM Central in November.
    expect(html).toContain(">2:00 PM</span>")
    expect(html).toContain(">3:15 PM</span>")
    expect(html).toContain(">+1h 15m</td>")
    expect(html).toMatch(pressed("Races"))
  })

  test("the false outs and the window come from the URL", () => {
    const html = render("/availability?days=14&show=false_outs", "14", scorecard({ days: 14 }))

    expect(players(html)).toEqual(["Jayson Tatum"])
    expect(html).toMatch(pressed("14d"))
    expect(html).toMatch(pressed("False outs"))
  })

  test("a date the pipeline first polled after its first tip is not scored", () => {
    const html = render("/availability", "7", scorecard())

    expect(html).toContain("1 date scored")
    const nov2 = html.split("<tr").find((row) => row.includes(">Nov 2<"))!
    expect(nov2).toContain(">no</td>")
  })

  test("before the pipeline writes anything, the page says so", () => {
    const html = render("/availability", "7", scorecard({ available: false, dates: [], calls: [], summary: { ...SUMMARY, races: 0 } }))

    expect(html).toContain("No availability reports in the last 7 days.")
    expect(players(html)).toEqual([])
  })
})
