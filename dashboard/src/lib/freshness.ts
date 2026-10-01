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

export type TileTone = "plain" | "good" | "bad" | "warn" | "quiet"

export interface SummaryTile {
  label: string
  value: number
  tone: TileTone
}

/**
 * The four tiles above the table. A table whose freshness query failed is
 * counted with the stale ones: both need a look, and an error in no tile reads
 * as "nothing stale". A zero is quiet, so a page with nothing judged (or
 * nothing readable) is not green.
 */
export function summaryTiles(summary: FreshnessSummary): SummaryTile[] {
  const broken = summary.stale + summary.error
  return [
    { label: "Tables", value: summary.total, tone: "plain" },
    { label: "Fresh", value: summary.fresh, tone: summary.fresh > 0 ? "good" : "quiet" },
    { label: summary.error > 0 ? "Stale / error" : "Stale", value: broken, tone: broken > 0 ? "bad" : "quiet" },
    { label: "Empty", value: summary.empty, tone: summary.empty > 0 ? "warn" : "quiet" },
  ]
}

/** How many game days a stale table is behind, or null when it is not judged stale. */
export function daysBehind(table: TableFreshness): number | null {
  if (table.state !== "stale" || !table.expected_date) return null
  if (!table.latest_date) return null
  return Math.max(1, daysBetween(table.latest_date, table.expected_date))
}

/** One line on where the season is, and so on what the page is judging against. */
export function describeSeason(
  data: Pick<FreshnessData, "season" | "phase" | "post_game_due" | "pre_game_due" | "next_game_date">,
): string {
  const next = data.next_game_date ? `next game ${formatDay(data.next_game_date)}` : "no game scheduled"
  const due = describeDue(data)
  if (data.phase === "regular") {
    return `Regular season ${data.season} · ${due ?? "nothing due until the first night settles"} · ${next}`
  }
  if (data.phase === "preseason") return `Preseason ${data.season} · nothing nightly is due yet · ${next}`
  // The season's last night stays judged until the calendar rolls over.
  return data.post_game_due
    ? `Offseason ${data.season} · judged through the season's last night, ${formatDay(data.post_game_due)}`
    : `Offseason ${data.season} · nothing nightly is due · ${next}`
}

/** "post-game due through Mar 4 · pre-game through Mar 5", or null while nothing is due. */
function describeDue({ post_game_due: post, pre_game_due: pre }: Pick<FreshnessData, "post_game_due" | "pre_game_due">): string | null {
  if (post && pre && post === pre) return `post-game and pre-game due through ${formatDay(post)}`
  const parts: string[] = []
  if (post) parts.push(`post-game due through ${formatDay(post)}`)
  if (pre) parts.push(`pre-game ${post ? "" : "due "}through ${formatDay(pre)}`)
  return parts.length > 0 ? parts.join(" · ") : null
}

/** The table's own name, and the schema it lives in. */
export function splitTable(table: string): { schema: string; name: string } {
  const dot = table.indexOf(".")
  return dot < 0 ? { schema: "", name: table } : { schema: table.slice(0, dot), name: table.slice(dot + 1) }
}
