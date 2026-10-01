import { describe, expect, test } from "bun:test"
import { renderToStaticMarkup } from "react-dom/server"

import { FreshnessTable } from "@/components/FreshnessTable"
import type { TableFreshness } from "@/lib/freshness"

const errored: TableFreshness = {
  table: "nba.player_game_stats",
  pipelines: [{ name: "player_game_stats", display_name: "Player Game Stats" }],
  category: "post_game",
  date_column: "game_date",
  latest_date: null,
  write_column: "updated_at",
  latest_written_at: null,
  rows_estimate: 24180,
  expected_date: null,
  state: "error",
  error: "UndefinedColumn",
}

const cells = (html: string, tag: string) =>
  [...html.matchAll(new RegExp(`<${tag}[^>]*>(.*?)</${tag}>`, "g"))].map((match) => match[1].replace(/<[^>]+>/g, ""))

describe("FreshnessTable", () => {
  const html = renderToStaticMarkup(<FreshnessTable tables={[errored]} now={Date.UTC(2026, 2, 5, 12)} />)
  const row = html.slice(html.indexOf("<tbody>"))

  test("the verdict is the column beside the name, where a phone shows it without scrolling", () => {
    // The table is 52rem wide at least, and a phone shows its first 340 px.
    expect(cells(html.slice(0, html.indexOf("<tbody>")), "th")).toEqual(["Table", "State", "Written by", "Runs through", "Last write", "Rows"])
    expect(cells(row, "th")).toEqual(["nba.player_game_stats"])
    expect(cells(row, "td")[0]).toBe("ErrorUndefinedColumn")
  })

  test("a long name can wrap after its schema instead of running over the verdict", () => {
    expect(row).toContain("nba.</span><wbr/>player_game_stats")
  })
})
