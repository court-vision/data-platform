import type { Schemas } from "@/lib/api"
import { CATEGORIES } from "@/lib/pipelines"
import { daysBetween, formatDay } from "@/lib/time"

export type TableFreshness = Schemas["TableFreshness"]
export type FreshnessData = Schemas["FreshnessData"]
export type FreshnessState = TableFreshness["state"]

/** What needs a look first. */
export const STATE_ORDER: readonly FreshnessState[] = ["stale", "error", "empty", "fresh", "idle", "unjudged"]

const categoryRank = new Map<string, number>(CATEGORIES.map((category, i) => [category.key, i]))

/** Stale first, then by the writer's category (live → scheduled), then by table name. */
export function sortByUrgency(tables: TableFreshness[]): TableFreshness[] {
  return [...tables].sort(
    (a, b) =>
      STATE_ORDER.indexOf(a.state) - STATE_ORDER.indexOf(b.state) ||
      (categoryRank.get(a.category) ?? 99) - (categoryRank.get(b.category) ?? 99) ||
      a.table.localeCompare(b.table),
  )
}

export type FreshnessSummary = Record<FreshnessState, number> & { total: number }

export function summarizeFreshness(tables: TableFreshness[]): FreshnessSummary {
  const summary: FreshnessSummary = { total: tables.length, fresh: 0, stale: 0, idle: 0, empty: 0, unjudged: 0, error: 0 }
  for (const table of tables) summary[table.state] += 1
  return summary
}

/** How many game days a stale table is behind, or null when it is not judged stale. */
export function daysBehind(table: TableFreshness): number | null {
  if (table.state !== "stale" || !table.expected_date) return null
  if (!table.latest_date) return null
  return Math.max(1, daysBetween(table.latest_date, table.expected_date))
}

/** One line on where the season is, and so on what the page is judging against. */
export function describeSeason(data: Pick<FreshnessData, "season" | "phase" | "last_game_date" | "next_game_date">): string {
  const next = data.next_game_date ? `next game ${formatDay(data.next_game_date)}` : "no game scheduled"
  if (data.phase === "regular") {
    const last = data.last_game_date ? `last settled game ${formatDay(data.last_game_date)}` : "no game settled yet"
    return `Regular season ${data.season} · ${last} · ${next}`
  }
  const phase = data.phase === "preseason" ? "Preseason" : "Offseason"
  return `${phase} ${data.season} · nothing nightly is due · ${next}`
}

/** The table's own name, and the schema it lives in. */
export function splitTable(table: string): { schema: string; name: string } {
  const dot = table.indexOf(".")
  return dot < 0 ? { schema: "", name: table } : { schema: table.slice(0, dot), name: table.slice(dot + 1) }
}
