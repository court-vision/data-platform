import { describe, expect, test } from "bun:test"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { renderToStaticMarkup } from "react-dom/server"
import { MemoryRouter } from "react-router"

import { aiSpendQuery } from "@/hooks/useAiSpend"
import type { AiSpendReport, SpendWindow, UserLine } from "@/lib/aiSpend"
import { AiSpend } from "@/routes/AiSpend"
import { pressed } from "@/test-helpers"

// No DOM and no network: the page renders to markup from a query cache filled
// by hand, and is read as text.

function userLine(overrides: Partial<UserLine> = {}): UserLine {
  return { spent_micros: 0, cap_micros: null, default_cap_micros: null, cap_set: false, updated_at: null, updated_by: null, ...overrides }
}

function report(overrides: Partial<AiSpendReport> = {}): AiSpendReport {
  return {
    as_of: "2026-11-20T18:00:00Z",
    month_start: "2026-11-01",
    since: "2026-11-18",
    days: 3,
    turn_max_micros: 500_000,
    read_max_micros: 200_000,
    lines: [
      {
        line: "desk", spent_micros: 8_000_000, open_micros: 500_000, cap_micros: 25_000_000, default_cap_micros: 25_000_000,
        cap_set: false, updated_at: null, updated_by: null,
      },
      {
        line: "autopilot", spent_micros: 9_000_000, open_micros: 0, cap_micros: 10_000_000, default_cap_micros: 30_000_000,
        cap_set: true, updated_at: "2026-11-19T15:00:00Z", updated_by: "james",
      },
      {
        line: "analyst", spent_micros: 1_000_000, open_micros: 0, cap_micros: 40_000_000, default_cap_micros: 40_000_000,
        cap_set: false, updated_at: null, updated_by: null,
      },
    ],
    users: [
      {
        user_id: 1, email: "one@example.com", allowed: true,
        desk: userLine({ spent_micros: 6_000_000, cap_micros: 12_500_000, default_cap_micros: 12_500_000 }),
        autopilot: userLine({ spent_micros: 9_000_000, cap_micros: 5_000_000, cap_set: true, updated_at: "2026-11-19T15:00:00Z", updated_by: "james" }),
      },
      {
        user_id: 7, email: "gone@example.com", allowed: false,
        desk: userLine({ spent_micros: 2_500_000, cap_micros: 12_500_000, default_cap_micros: 12_500_000 }),
        autopilot: userLine(),
      },
    ],
    teams: [{ team_id: 4, user_id: 1, name: "Dime's Delectable Team" }],
    rows: [
      { day: "2026-11-18", line: "desk", feature: "chat", detail: null, user_id: 1, team_id: 4, cost_micros: 1_200_000, open_micros: 0, entries: 4 },
      { day: "2026-11-19", line: "autopilot", feature: "checkin", detail: "morning", user_id: 1, team_id: 4, cost_micros: 2_000_000, open_micros: 0, entries: 1 },
      { day: "2026-11-19", line: "analyst", feature: "analyst", detail: null, user_id: null, team_id: null, cost_micros: 300_000, open_micros: 0, entries: 1 },
    ],
    usage: [
      {
        day: "2026-11-18", feature: "chat", model: "claude-sonnet-5-5", records: 4, model_calls: 9,
        input_tokens: 2_000, output_tokens: 1_500, cache_read_input_tokens: 6_000, cache_creation_input_tokens: 0, cost_micros: 1_200_000,
      },
    ],
    ...overrides,
  }
}

function render(path: string, window: SpendWindow, data: AiSpendReport): string {
  const client = new QueryClient()
  client.setQueryData(aiSpendQuery(window).queryKey, data)
  return renderToStaticMarkup(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>
        <AiSpend />
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

function rowOf(html: string, text: string): string {
  const row = html
    .split("<tr")
    .map((part) => part.split("</tr>")[0])
    .find((part) => part.includes(text))
  if (!row) throw new Error(`no row with ${text}`)
  return row
}

describe("AiSpend", () => {
  test("each line's month against its cap, open spend counted, with an editor for its cap", () => {
    const html = render("/ai-spend", "30", report())

    // Tiles: 8 + 0.5 + 9 + 1 of 25 + 10 + 40
    expect(html).toContain("$18.50")
    expect(html).toContain("of $75.00 across every cap")
    expect(html).toContain("34% of $25.00 · $0.50 open")
    expect(html).toContain("90% of $10.00")

    const desk = rowOf(html, "Dime desk<span")
    expect(desk).toContain('aria-label="$8.50 of $25.00 (34%)"')
    expect(desk).toContain('value="25"')
    // Nothing set here, so nothing to hand back
    expect(desk).not.toContain(">Default</button>")
    expect(desk).toContain(">default</span>")

    const checkins = rowOf(html, "The check-ins Dime starts itself")
    expect(checkins).toContain('data-tone="warn"')
    expect(checkins).toContain('value="10"')
    expect(checkins).toContain('title="Back to $30.00"')
    expect(checkins).toContain(">Default</button>")
    expect(checkins).toContain("james · ")

    // The ceilings a cap has to leave room for
    expect(html).toContain("$0.50 a desk turn or check-in, $0.20 an analyst read")
  })

  test("each user's desk share and check-in cap, and who has no access", () => {
    const html = render("/ai-spend", "30", report())

    const one = rowOf(html, "one@example.com<span")
    expect(one).toContain("share: $12.50")
    // $6 of a $12.50 share; $9 of a $5 check-in cap is over it
    expect(one).toContain("48% of $12.50")
    expect(one).toMatch(/class="tag[^"]*"[^>]*>180%<\/span> of \$5\.00/)
    expect(one).toContain('aria-label="one@example.com check-in cap"')
    expect(one).toContain('value="5"')
    expect(one).toContain(">Default</button>")

    const gone = rowOf(html, "gone@example.com")
    expect(gone).toContain("no access")
    expect(gone).toContain("the line&#x27;s cap only")
    expect(gone).toContain('placeholder="none"')
  })

  test("the window comes from the URL, and the chart has a column for every day", () => {
    const html = render("/ai-spend?days=7", "7", report())
    expect(html).toMatch(pressed("7d"))
    expect(html).toContain('aria-label="Nov 18 · $1.20 · Chat $1.20"')
    expect(html).toContain('aria-label="Nov 19 · $2.30 · Check-ins $2.00 · Analyst $0.30"')
    expect(html).toContain('aria-label="Nov 20 · nothing spent"')
    expect(html).toContain("Nov 18 → Nov 20 · $3.50 in all")
  })

  test("the tables split the window by feature, model and team", () => {
    const html = render("/ai-spend", "30", report())

    const chat = rowOf(html, 'data-feature="chat"')
    expect(chat).toContain(">$1.20<")
    expect(chat).toContain(">34%</td>")
    // 6,000 of 8,000 input tokens came from the cache
    expect(chat).toContain(">75%</td>")
    expect(rowOf(html, 'data-feature="checkin"')).toContain("Morning $2.00")

    const model = rowOf(html, "claude-sonnet-5-5</th>")
    expect(model).toContain(">9</td>")

    expect(rowOf(html, "Dime&#x27;s Delectable Team")).toContain(">$3.20</td>")
    expect(rowOf(html, "No one&#x27;s")).toContain(">$0.30</td>")
  })

  test("an empty window says so instead of drawing nothing", () => {
    const html = render("/ai-spend", "30", report({ rows: [], usage: [], teams: [] }))
    expect(html).toContain("Nothing spent in this window.")
    expect(html).toContain("Nothing spent in the last 3 days.")
    expect(html).toContain("No calls recorded in the last 3 days.")
  })
})
