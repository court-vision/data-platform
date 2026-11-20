import type { Schemas } from "@/lib/api"
import { rankMove } from "@/lib/projections"
import { formatDay } from "@/lib/time"

export type AnalystDigest = Schemas["AnalystDigest"]
export type DigestRead = Schemas["DigestRead"]
export type DigestSide = Schemas["DigestSide"]
export type DigestMove = Schemas["DigestMove"]
export type DigestSpend = Schemas["DigestSpend"]

/** The page's two views: everybody's projection, or the analyst's morning. */
export type View = "players" | "analyst"
export const VIEWS: readonly { key: View; label: string }[] = [
  { key: "players", label: "Players" },
  { key: "analyst", label: "Analyst" },
]

// ---- what a read came to -------------------------------------------------------------------

export type OutcomeTone = "up" | "warn" | "down" | "quiet"

interface OutcomeFacts {
  /** On the read's chip. */
  label: string
  tone: OutcomeTone
  /** The chip's title: what the outcome means, as nba.analyst_reads defines it. */
  meaning: string
  /** "3 notes written": the outcome counted, for the morning's summary. */
  counted: (reads: number) => string
}

/** The seven outcomes a read can have, in the order a morning's summary lists them. */
const OUTCOMES: readonly (OutcomeFacts & { key: string })[] = [
  {
    key: "note", label: "note written", tone: "up", meaning: "A new or changed note was stored",
    counted: (n) => (n === 1 ? "1 note written" : `${n} notes written`),
  },
  {
    key: "retired", label: "note withdrawn", tone: "warn", meaning: "The note it had no longer holds, and was withdrawn",
    counted: (n) => `${n} withdrawn`,
  },
  {
    key: "unchanged", label: "note kept", tone: "quiet", meaning: "The note it already had still says it",
    counted: (n) => `${n} kept`,
  },
  {
    key: "none", label: "nothing to write", tone: "quiet", meaning: "Nothing a box score cannot say, or nothing to cite",
    counted: (n) => `${n} with nothing to write`,
  },
  {
    key: "refused", label: "refused", tone: "warn", meaning: "The check refused what came back; the line under it says why",
    counted: (n) => `${n} refused`,
  },
  {
    key: "declined", label: "declined", tone: "warn", meaning: "The model declined the request",
    counted: (n) => `${n} declined`,
  },
  {
    key: "failed", label: "failed", tone: "down", meaning: "The read did not complete; the next run reads him again",
    counted: (n) => `${n} failed`,
  },
]

/** How an outcome reads on the page. One this page has not heard of is shown as it is stored. */
export function outcomeOf(outcome: string): OutcomeFacts {
  return (
    OUTCOMES.find((known) => known.key === outcome) ?? {
      label: outcome, tone: "quiet", meaning: "", counted: (n) => `${n} ${outcome}`,
    }
  )
}

/** "2 notes written · 5 with nothing to write · 1 refused": a morning's reads by what they came to. */
export function describeCounts(counts: Record<string, number>): string {
  const known = OUTCOMES.filter((outcome) => (counts[outcome.key] ?? 0) > 0).map((outcome) => outcome.counted(counts[outcome.key]))
  const others = Object.keys(counts)
    .filter((key) => counts[key] > 0 && !OUTCOMES.some((outcome) => outcome.key === key))
    .sort()
    .map((key) => outcomeOf(key).counted(counts[key]))
  return [...known, ...others].join(" · ")
}

/** Which of a morning's reads are listed. */
export type ReadFilter = "all" | "changed" | "trouble"
export const READ_FILTERS: readonly { key: ReadFilter; label: string }[] = [
  { key: "all", label: "All" },
  { key: "changed", label: "Changed a note" },
  { key: "trouble", label: "Went wrong" },
]

const CHANGED = new Set(["note", "retired"])
const TROUBLE = new Set(["refused", "declined", "failed"])

export function matchesRead(read: Pick<DigestRead, "outcome">, filter: ReadFilter): boolean {
  if (filter === "changed") return CHANGED.has(read.outcome)
  if (filter === "trouble") return TROUBLE.has(read.outcome)
  return true
}

export function visibleReads(reads: DigestRead[], filter: ReadFilter): DigestRead[] {
  return reads.filter((read) => matchesRead(read, filter))
}

// ---- why he was picked ----------------------------------------------------------------------

const RULES: Record<string, string> = {
  note_due: "note due",
  status_changed: "status changed",
  new_item: "new item",
  teammate_change: "teammate",
  minutes_gap: "minutes",
  rank_gap: "rank gap",
}

/** The screen's rule, as a tag on the reason it gave. */
export function ruleLabel(rule: string): string {
  return RULES[rule] ?? rule.replace(/_/g, " ")
}

// ---- money ----------------------------------------------------------------------------------

/** "$1.29", and "$0.004" under a cent, so a cheap read is not shown as a free one. */
export function usd(value: number): string {
  if (value > 0 && value < 0.01) return `$${value.toFixed(3)}`
  return `$${value.toFixed(2)}`
}

/** The share of the month's cap past which the bill is worth a look. */
export const SPEND_WARN_SHARE = 0.8

/** The month's bill beside its cap, or why there is none to show. */
export function describeSpend(spend: DigestSpend): { text: string; tone: OutcomeTone } {
  if (!spend.available) return { text: "the backend could not be asked about the month's spend", tone: "warn" }
  const bill = `${usd(spend.month_spent_usd)} of ${usd(spend.month_cap_usd)} this month`
  if (!spend.enabled) return { text: `switched off on the backend · ${bill}`, tone: "warn" }
  if (spend.month_cap_usd > 0 && spend.month_spent_usd >= spend.month_cap_usd) {
    return { text: `${bill}: the cap is reached, so nothing more is read this month`, tone: "down" }
  }
  if (spend.month_cap_usd > 0 && spend.month_spent_usd >= spend.month_cap_usd * SPEND_WARN_SHARE) return { text: bill, tone: "warn" }
  return { text: bill, tone: "quiet" }
}

// ---- the morning at a glance ----------------------------------------------------------------

export interface AnalystTile {
  label: string
  value: string
  tone: OutcomeTone | "plain"
  note?: string
}

/** Four tiles: the morning's reads, what they cost, the notes standing today, and the month against its cap. */
export function analystTiles(digest: AnalystDigest): AnalystTile[] {
  const reads = digest.reads.length
  const trouble = digest.reads.filter((read) => matchesRead(read, "trouble")).length
  const month = describeSpend(digest.spend)
  return [
    {
      label: "Reads",
      value: String(reads),
      tone: trouble > 0 ? "warn" : reads > 0 ? "plain" : "quiet",
      ...(trouble > 0 ? { note: `${trouble} went wrong` } : digest.as_of ? { note: formatDay(digest.as_of) } : {}),
    },
    {
      label: "Morning's cost",
      value: usd(digest.cost_usd),
      tone: reads > 0 ? "plain" : "quiet",
      ...(reads > 0 ? { note: `${usd(digest.cost_usd / reads)} a read` } : {}),
    },
    { label: "Notes standing", value: String(digest.notes_live), tone: digest.notes_live > 0 ? "up" : "quiet", note: "the analyst's, today" },
    {
      label: "This month",
      value: digest.spend.available ? usd(digest.spend.month_spent_usd) : "?",
      tone: month.tone === "quiet" ? "plain" : month.tone,
      note: !digest.spend.available
        ? "the backend did not answer"
        : !digest.spend.enabled
          ? "switched off"
          : `of ${usd(digest.spend.month_cap_usd)}`,
    },
  ]
}

// ---- the two projections, side by side ------------------------------------------------------

export interface SideColumn {
  key: keyof DigestSide
  label: string
  title: string
  digits: number
  /** A rank: lower is better, and a move is shown as places gained or lost. */
  rank?: boolean
}

export const SIDE_COLUMNS: readonly SideColumn[] = [
  { key: "points_rank", label: "Pts rank", title: "On the standard points board", digits: 0, rank: true },
  { key: "categories_rank", label: "Cat rank", title: "On the standard nine-category board", digits: 0, rank: true },
  { key: "games", label: "GP left", title: "Games he is expected to play from here", digits: 1 },
  { key: "minutes", label: "Min", title: "A game, when he plays", digits: 1 },
  { key: "points", label: "Pts", title: "Points a game, when he plays", digits: 1 },
]

export function formatSide(column: SideColumn, side: DigestSide | null | undefined): string {
  const value = side?.[column.key]
  return value == null ? "—" : value.toFixed(column.digits)
}

/** Whether the analyst's notes changed this number for him. */
export function sideDiffers(column: SideColumn, live: DigestSide | null | undefined, shadow: DigestSide | null | undefined): boolean {
  return live != null && shadow != null && formatSide(column, live) !== formatSide(column, shadow)
}

/** Whether anything about him differs between the two projections and boards. */
export function sidesDiffer(live: DigestSide | null | undefined, shadow: DigestSide | null | undefined): boolean {
  return SIDE_COLUMNS.some((column) => sideDiffers(column, live, shadow))
}

/** "points rank 14 · 9-cat rank 9 · 58.1 games left": where a player the notes did not move stands. */
export function describeSide(side: DigestSide): string {
  const parts: string[] = []
  if (side.points_rank != null) parts.push(`points rank ${side.points_rank}`)
  if (side.categories_rank != null) parts.push(`9-cat rank ${side.categories_rank}`)
  if (side.games != null) parts.push(`${side.games.toFixed(1)} games left`)
  return parts.join(" · ") || "on neither board"
}

/** "+6" for six places gained on the points board with the analyst's notes applied, "−3" for three lost. */
export function pointsMove(move: Pick<DigestMove, "live" | "shadow">): string {
  return rankMove(move.live.points_rank ?? null, move.shadow.points_rank ?? null)
}

/**
 * The day whose two boards a morning's moves are read off: that morning's own.
 * `shadow_as_of` is the newest day with second boards whichever morning is
 * shown, so it says only whether this morning's can be stored yet, never
 * which boards are being compared. Null when they cannot be.
 */
export function movedBoardsDay(digest: Pick<AnalystDigest, "as_of" | "shadow_as_of">): string | null {
  if (digest.as_of == null || digest.shadow_as_of == null || digest.shadow_as_of < digest.as_of) return null
  return digest.as_of
}

/** The morning named in the picker: "Nov 20", and "Nov 20 (newest)" for the first. */
export function dayLabel(day: string, days: readonly string[]): string {
  return day === days[0] ? `${formatDay(day)} (newest)` : formatDay(day)
}
