import type { Schemas } from "@/lib/api"
import { formatDay } from "@/lib/time"

export type ProjectionsData = Schemas["ProjectionsData"]
export type ProjectionRow = Schemas["ProjectionRow"]
export type ProjectionLine = Schemas["ProjectionLine"]
export type AdjustmentEntry = Schemas["AdjustmentEntry"]
export type AdjustmentChange = Schemas["AdjustmentChange"]
export type AdjustmentSave = Schemas["AdjustmentSave"]
export type ProjectionPreview = Schemas["ProjectionPreview"]
export type AdjustmentKind = AdjustmentSave["kind"]

/** Which of the two standard boards the rank columns read. */
export type Format = "points" | "categories"
export const FORMATS: readonly { key: Format; label: string }[] = [
  { key: "points", label: "Points" },
  { key: "categories", label: "9-cat" },
]

export function rankOf(row: ProjectionRow, format: Format): number | null {
  return row.ranks[format]
}

export function espnRankOf(row: ProjectionRow, format: Format): number | null {
  return row.espn_ranks[format]
}

/**
 * ESPN's rank minus Court Vision's: positive means Court Vision is higher on
 * him than ESPN is. Null when either side has no rank.
 */
export function rankGap(row: ProjectionRow, format: Format): number | null {
  const cv = rankOf(row, format)
  const espn = espnRankOf(row, format)
  return cv == null || espn == null ? null : espn - cv
}

/** A gap of this many places is a disagreement worth a look. */
export const WIDE_GAP = 25
/** Disagreements deep in the pool are noise: only count them where one side drafts him. */
const GAP_WITHIN = 150

export function gapIsWide(row: ProjectionRow, format: Format): boolean {
  const gap = rankGap(row, format)
  if (gap == null || Math.abs(gap) < WIDE_GAP) return false
  return Math.min(rankOf(row, format) ?? Infinity, espnRankOf(row, format) ?? Infinity) <= GAP_WITHIN
}

// ---- filtering and sorting -----------------------------------------------------------------

export type Filter = "all" | "adjusted" | "rookies" | "gap"
export const FILTERS: readonly { key: Filter; label: string }[] = [
  { key: "all", label: "All" },
  { key: "adjusted", label: "Adjusted" },
  { key: "gap", label: `±${WIDE_GAP} vs ESPN` },
  { key: "rookies", label: "No NBA history" },
]

export function matchesFilter(row: ProjectionRow, filter: Filter, format: Format): boolean {
  if (filter === "adjusted") return row.adjustment != null
  if (filter === "rookies") return row.statistical == null
  if (filter === "gap") return gapIsWide(row, format)
  return true
}

function fold(text: string): string {
  return text.normalize("NFD").replace(/\p{Diacritic}/gu, "").toLowerCase()
}

/** Name or team, accents ignored: "doncic" finds Dončić. */
export function matchesQuery(row: ProjectionRow, query: string): boolean {
  const needle = fold(query.trim())
  if (!needle) return true
  return fold(row.name).includes(needle) || (row.team ?? "").toLowerCase() === needle
}

export type Sort = "cv" | "espn" | "gap" | "name"
export const SORTS: readonly { key: Sort; label: string }[] = [
  { key: "cv", label: "CV rank" },
  { key: "espn", label: "ESPN rank" },
  { key: "gap", label: "Biggest gap" },
  { key: "name", label: "Name" },
]

/** Nulls last, whatever the direction of the rest. */
function byNumber(a: number | null, b: number | null): number {
  if (a == null && b == null) return 0
  if (a == null) return 1
  if (b == null) return -1
  return a - b
}

export function sortRows(rows: ProjectionRow[], sort: Sort, format: Format): ProjectionRow[] {
  const name = (a: ProjectionRow, b: ProjectionRow) => a.name.localeCompare(b.name)
  const cv = (a: ProjectionRow, b: ProjectionRow) => byNumber(rankOf(a, format), rankOf(b, format)) || name(a, b)
  const sorted = [...rows]
  if (sort === "name") return sorted.sort(name)
  if (sort === "espn") {
    return sorted.sort((a, b) => byNumber(espnRankOf(a, format), espnRankOf(b, format)) || cv(a, b))
  }
  if (sort === "gap") {
    const size = (row: ProjectionRow) => {
      const gap = rankGap(row, format)
      return gap == null ? null : -Math.abs(gap)
    }
    return sorted.sort((a, b) => byNumber(size(a), size(b)) || cv(a, b))
  }
  return sorted.sort(cv)
}

export function visibleRows(
  rows: ProjectionRow[],
  { query, filter, sort, format }: { query: string; filter: Filter; sort: Sort; format: Format },
): ProjectionRow[] {
  return sortRows(
    rows.filter((row) => matchesFilter(row, filter, format) && matchesQuery(row, query)),
    sort,
    format,
  )
}

// ---- the lines -----------------------------------------------------------------------------

export interface StatColumn {
  key: string
  label: string
  title: string
  value: (line: ProjectionLine) => number | null
  digits: number
}

function pct(makes: number, attempts: number): number | null {
  return attempts > 0 ? (100 * makes) / attempts : null
}

/** What every line is shown as, in table order. Percentages come from makes and attempts. */
export const STAT_COLUMNS: readonly StatColumn[] = [
  { key: "games", label: "GP", title: "Expected games", value: (l) => l.games, digits: 0 },
  { key: "min", label: "MIN", title: "Minutes per game", value: (l) => l.min, digits: 1 },
  { key: "pts", label: "PTS", title: "Points", value: (l) => l.pts, digits: 1 },
  { key: "reb", label: "REB", title: "Rebounds", value: (l) => l.reb, digits: 1 },
  { key: "ast", label: "AST", title: "Assists", value: (l) => l.ast, digits: 1 },
  { key: "stl", label: "STL", title: "Steals", value: (l) => l.stl, digits: 1 },
  { key: "blk", label: "BLK", title: "Blocks", value: (l) => l.blk, digits: 1 },
  { key: "fg3m", label: "3PM", title: "Threes made", value: (l) => l.fg3m, digits: 1 },
  { key: "tov", label: "TOV", title: "Turnovers", value: (l) => l.tov, digits: 1 },
  { key: "fg_pct", label: "FG%", title: "Field goal percentage", value: (l) => pct(l.fgm, l.fga), digits: 1 },
  { key: "ft_pct", label: "FT%", title: "Free throw percentage", value: (l) => pct(l.ftm, l.fta), digits: 1 },
]

export function formatStat(column: StatColumn, line: ProjectionLine | null | undefined): string {
  if (!line) return "—"
  const value = column.value(line)
  return value == null ? "—" : value.toFixed(column.digits)
}

/** Whether two lines show a different number in this column, at the digits shown. */
export function statDiffers(column: StatColumn, a: ProjectionLine | null | undefined, b: ProjectionLine | null | undefined): boolean {
  return formatStat(column, a) !== formatStat(column, b)
}

// ---- the adjustment ------------------------------------------------------------------------

export const KIND_LABELS: Record<string, string> = {
  year2: "Year 2",
  trade: "Trade",
  role: "Role",
  injury_return: "Injury return",
  injury_current: "Injured now",
  age: "Age",
  other: "Other",
}

export function kindLabel(kind: string): string {
  return KIND_LABELS[kind] ?? kind
}

/** The stats a per-stat multiplier may scale, in the order the form offers them. */
export const RATE_KEYS = ["pts", "reb", "ast", "stl", "blk", "tov", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta"] as const

/** "32 min · 66 games · back Jan 5 · usage ×0.97 · blk ×1.1": what an adjustment sets. */
export function describeChange(change: Pick<AdjustmentEntry, "minutes" | "games" | "return_date" | "usage" | "rates">): string {
  const parts: string[] = []
  if (change.minutes != null) parts.push(`${change.minutes} min`)
  if (change.games != null) parts.push(`${change.games} games`)
  if (change.return_date) parts.push(`back ${formatDay(change.return_date)}`)
  if (change.usage != null) parts.push(`usage ×${change.usage}`)
  for (const [key, multiplier] of Object.entries(change.rates ?? {})) parts.push(`${key} ×${multiplier}`)
  return parts.join(" · ") || "nothing"
}

/** The form's own state: everything a string, as the inputs hold it. */
export interface AdjustmentForm {
  kind: string
  minutes: string
  games: string
  return_date: string
  usage: string
  rates: { key: string; multiplier: string }[]
  note: string
  source_url: string
}

export function formFrom(adjustment: AdjustmentEntry | null | undefined): AdjustmentForm {
  return {
    kind: adjustment?.kind ?? "role",
    minutes: adjustment?.minutes != null ? String(adjustment.minutes) : "",
    games: adjustment?.games != null ? String(adjustment.games) : "",
    return_date: adjustment?.return_date ?? "",
    usage: adjustment?.usage != null ? String(adjustment.usage) : "",
    rates: Object.entries(adjustment?.rates ?? {}).map(([key, multiplier]) => ({ key, multiplier: String(multiplier) })),
    note: adjustment?.note ?? "",
    source_url: adjustment?.source_url ?? "",
  }
}

export interface ParsedChange {
  /** The numbers the form sets; fields left blank are absent. */
  change: AdjustmentChange
  /** Field name → what is wrong with it. Empty when the change can be previewed. */
  errors: Record<string, string>
}

function parseNumber(text: string): number | null | "invalid" {
  const trimmed = text.trim()
  if (!trimmed) return null
  const value = Number(trimmed)
  return Number.isFinite(value) ? value : "invalid"
}

/**
 * The form as the API's change, with the same limits the API enforces, so a
 * bad number is said next to its field instead of coming back as a 422.
 */
export function parseChange(form: AdjustmentForm): ParsedChange {
  const errors: Record<string, string> = {}
  const change: AdjustmentChange = {}

  const minutes = parseNumber(form.minutes)
  if (minutes === "invalid" || (minutes != null && (minutes < 0 || minutes > 48))) errors.minutes = "0 to 48"
  else if (minutes != null) change.minutes = minutes

  const games = parseNumber(form.games)
  if (games === "invalid" || (games != null && (!Number.isInteger(games) || games < 0 || games > 82))) {
    errors.games = "a whole number, 0 to 82"
  } else if (games != null) change.games = games

  const day = form.return_date.trim()
  if (day && !/^\d{4}-\d{2}-\d{2}$/.test(day)) errors.return_date = "a date"
  else if (day) change.return_date = day

  const usage = parseNumber(form.usage)
  if (usage === "invalid" || (usage != null && (usage <= 0 || usage > 2))) errors.usage = "above 0, at most 2"
  else if (usage != null) change.usage = usage

  const rates: Record<string, number> = {}
  for (const { key, multiplier } of form.rates) {
    const value = parseNumber(multiplier)
    if (!key && value == null) continue // an empty row the form has not filled in
    if (!(RATE_KEYS as readonly string[]).includes(key)) errors.rates = "pick a stat for every multiplier"
    else if (key in rates) errors.rates = `${key} is listed twice`
    else if (value === "invalid" || value == null || value <= 0 || value > 3) errors.rates = `${key}: above 0, at most 3`
    else rates[key] = value
  }
  if (Object.keys(rates).length > 0) change.rates = rates

  return { change, errors }
}

export function changesSomething(change: AdjustmentChange): boolean {
  return (
    change.minutes != null || change.games != null || change.return_date != null ||
    change.usage != null || Object.keys(change.rates ?? {}).length > 0
  )
}

/** A stable key for a change, so a preview can be matched to the form it was computed for. */
export function changeKey(change: AdjustmentChange): string {
  const rates = Object.entries(change.rates ?? {}).sort(([a], [b]) => a.localeCompare(b))
  return JSON.stringify([change.minutes ?? null, change.games ?? null, change.return_date ?? null, change.usage ?? null, rates])
}

/** What stands between the form and Save, or null when it can be saved. */
export function saveBlocker(form: AdjustmentForm, parsed: ParsedChange, previewedKey: string | null): string | null {
  if (Object.keys(parsed.errors).length > 0) return "Fix the highlighted fields"
  if (!changesSomething(parsed.change)) return "Set minutes, games, a return date, usage or a stat multiplier"
  if (!form.note.trim()) return "Say why: a note is required"
  if (previewedKey !== changeKey(parsed.change)) return "Preview these numbers first"
  return null
}

// ---- the page's summary ---------------------------------------------------------------------

export type TileTone = "plain" | "good" | "warn" | "quiet"

export interface SummaryTile {
  label: string
  value: number
  tone: TileTone
}

export function summaryTiles(data: ProjectionsData, format: Format): SummaryTile[] {
  const adjusted = data.players.filter((row) => row.adjustment != null).length
  const wide = data.players.filter((row) => gapIsWide(row, format)).length
  return [
    { label: "Players", value: data.players.length, tone: "plain" },
    { label: "Adjusted", value: adjusted, tone: adjusted > 0 ? "good" : "quiet" },
    { label: `±${WIDE_GAP} vs ESPN`, value: wide, tone: wide > 0 ? "plain" : "quiet" },
    { label: "Unpublished", value: data.unpublished, tone: data.unpublished > 0 ? "warn" : "quiet" },
  ]
}

/** "published Oct 1 · the board reads what this page shows" or how far behind it is. */
export function describePublished(data: Pick<ProjectionsData, "published_as_of" | "unpublished">): string {
  if (!data.published_as_of) return "never published: the board is still on ESPN's projection"
  if (data.unpublished === 0) return `published ${formatDay(data.published_as_of)} · the board reads what this page shows`
  const players = data.unpublished === 1 ? "1 player differs" : `${data.unpublished} players differ`
  return `published ${formatDay(data.published_as_of)} · ${players} from the published snapshot`
}

/** What the rank columns were measured in, or why there are none. */
export function describeRanks(data: Pick<ProjectionsData, "ranks_available" | "ranks_reason" | "league">): string {
  if (!data.ranks_available || !data.league) {
    return `ranks unavailable (${data.ranks_reason ?? "the backend did not answer"}): the lines below are still current`
  }
  const { league_size, rounds, playoff_weight, playoff_weeks } = data.league
  const playoffs =
    playoff_weeks.length > 0
      ? `playoff weeks ${playoff_weeks[0]}–${playoff_weeks[playoff_weeks.length - 1]} count ×${playoff_weight}`
      : "no playoff weeks on the calendar"
  return `ranks: standard ${league_size}-team, ${rounds}-round league · ${playoffs}`
}

/** "+6" for a rank that improved by six places, "−3" for one that fell, "" for no move. */
export function rankMove(before: number | null, after: number | null): string {
  if (before == null || after == null || before === after) return ""
  const places = before - after
  return places > 0 ? `+${places}` : `−${-places}`
}
