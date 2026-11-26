import { describe, expect, test } from "bun:test"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { renderToStaticMarkup } from "react-dom/server"
import { MemoryRouter } from "react-router"

import { aiBillQuery, aiSpendQuery } from "@/hooks/useAiSpend"
import type { AiBill, AiSpendReport, SpendWindow, UserLine } from "@/lib/aiSpend"
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
    // A backend with no whole-app cap: the month is against the lines' caps summed
    app: null,
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

function render(path: string, window: SpendWindow, data: AiSpendReport, bill?: AiBill): string {
  const client = new QueryClient()
  client.setQueryData(aiSpendQuery(window).queryKey, data)
  if (bill) client.setQueryData(aiBillQuery().queryKey, bill)
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

  test("the whole app's cap tops the month and the caps, editable like a line's", () => {
    const app = {
      spent_micros: 18_000_000, open_micros: 500_000, cap_micros: 20_000_000, default_cap_micros: 100_000_000,
      cap_set: true, updated_at: "2026-11-23T05:00:00Z", updated_by: "james",
    }
    const html = render("/ai-spend", "30", report({ app }))

    expect(html).toContain("93% of $20.00 in all")
    expect(html).not.toContain("across every cap")

    const whole = rowOf(html, "Whole app<span")
    expect(whole).toContain("Every line together")
    expect(whole).toContain('aria-label="$18.50 of $20.00 (93%)"')
    expect(whole).toContain('data-tone="warn"')
    expect(whole).toContain('aria-label="Whole app monthly cap"')
    expect(whole).toContain('value="20"')
    expect(whole).toContain('title="Back to $100.00"')
    expect(whole).toContain("james · ")
    // It comes before the lines it caps
    expect(html.indexOf("Whole app<span")).toBeLessThan(html.indexOf("Dime desk<span"))
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

function bill(overrides: Partial<AiBill> = {}): AiBill {
  return {
    state: "ok",
    message: null,
    read_at: "2026-11-26T12:30:00Z",
    refreshed_at: "2026-11-26T08:00:00Z",
    months: [
      { month: "2026-10-01", billed_micros: 10_255_000, ours_micros: 10_000_000, ledger_micros: 9_800_000, open_micros: 0 },
      { month: "2026-11-01", billed_micros: 7_710_000, ours_micros: 7_300_000, ledger_micros: 3_950_000, open_micros: 1_000_000 },
    ],
    days: [
      { day: "2026-11-24", billed_micros: 3_000_000, ours_micros: 3_000_000, ledger_micros: 3_000_000, open_micros: 0 },
      { day: "2026-11-25", billed_micros: 3_210_000, ours_micros: 2_800_000, ledger_micros: 2_750_000, open_micros: 0 },
      { day: "2026-11-26", billed_micros: 1_500_000, ours_micros: 1_500_000, ledger_micros: 1_200_000, open_micros: 1_000_000 },
    ],
    keys: [
      { api_key_id: "apikey_app", name: "court-vision-prod", hint: "sk-ant-api03-R2D...igAA", ours: true, billed_micros: 7_300_000 },
      { api_key_id: "apikey_laptop", name: "laptop evals", hint: null, ours: false, billed_micros: 410_000 },
    ],
    ...overrides,
  }
}

describe("AiSpend's bill check", () => {
  test("each month and recent day beside the ledger, held to the app's own key", () => {
    const text = render("/ai-spend", "30", report(), bill())
    expect(text).toContain("Against Anthropic&#x27;s bill")
    expect(text).toContain("October 2026")
    expect(text).toContain("November 2026")
    // November: $3.95 priced against $7.30 on the app's key
    expect(text).toContain("$7.71")
    expect(text).toContain("−$3.35")
    // Only today runs past Anthropic's refresh (08:00 UTC); November 25 ended before it
    expect(text.match(/may still read short/g)?.length).toBe(1)
    expect(text).toContain("court-vision-prod")
    expect(text).toContain("this app")
    expect(text).toContain("laptop evals")
  })

  test("a key Anthropic refused says so, and what the check needs", () => {
    const text = render("/ai-spend", "30", report(), bill({ state: "refused", message: "Anthropic answered 401: invalid x-api-key", months: [], days: [], keys: [] }))
    expect(text).toContain("Anthropic refused the key (Anthropic answered 401: invalid x-api-key)")
    expect(text).toContain("sk-ant-admin")
    expect(text).not.toContain("October 2026")
  })

  test("the rest of the page doesn't wait for the bill", () => {
    const text = render("/ai-spend", "30", report())
    expect(text).toContain("Reading Anthropic&#x27;s bill…")
    expect(text).toContain("By model")
  })
})
