import type { Tone } from "@/components/blocks"
import type { Schemas } from "@/lib/api"

export type AvailabilityScorecard = Schemas["AvailabilityScorecard"]
export type AvailabilitySummary = Schemas["AvailabilitySummary"]
export type AvailabilityCall = Schemas["AvailabilityCall"]
export type AvailabilityDay = Schemas["AvailabilityDay"]
export type Outcome = AvailabilityCall["outcome"]

export type Days = "3" | "7" | "14" | "30"

export const DAYS: readonly { key: Days; label: string }[] = [
  { key: "3", label: "3d" },
  { key: "7", label: "7d" },
  { key: "14", label: "14d" },
  { key: "30", label: "30d" },
]

export function parseDays(raw: string | null): Days {
  return DAYS.some((option) => option.key === raw) ? (raw as Days) : "7"
}

/** What each outcome is called on the page, what it means, and how it reads. */
export const OUTCOMES: Record<Outcome, { label: string; note: string; tone?: Tone }> = {
  us_first: { label: "Us first", note: "both called him out; our verdict came first", tone: "up" },
  same_poll: { label: "Tie", note: "both called him out within one 2-minute tick", tone: "quiet" },
  espn_first: { label: "ESPN first", note: "both called him out; ESPN fantasy came first", tone: "warn" },
  us_only: { label: "Us only", note: "only our verdict had him out at the tip", tone: "up" },
  espn_only: { label: "ESPN only", note: "only ESPN fantasy had him out at the tip", tone: "warn" },
  carried: { label: "Carried", note: "out at both sides' first read of the date: a long absence, not a race" },
  unannounced: { label: "Unannounced", note: "a source named him, neither called him out, he sat" },
  false_out: { label: "False out", note: "out at the tip, active in the box score", tone: "down" },
  not_in_box: { label: "Not in box", note: "called out, the game tipped, the box score never listed him" },
  pending: { label: "Pending", note: "called out, the game has not tipped", tone: "live" },
}

export const RACES: readonly Outcome[] = ["us_first", "same_poll", "espn_first", "us_only", "espn_only"]
const OTHERS: readonly Outcome[] = ["carried", "unannounced", "pending", "not_in_box"]

export type Show = "races" | "false_outs" | "others" | "all"

export const SHOWS: readonly { key: Show; label: string }[] = [
  { key: "races", label: "Races" },
  { key: "false_outs", label: "False outs" },
  { key: "others", label: "The rest" },
  { key: "all", label: "All" },
]

export function parseShow(raw: string | null): Show {
  return SHOWS.some((option) => option.key === raw) ? (raw as Show) : "races"
}

export function shownCalls(calls: readonly AvailabilityCall[], show: Show): AvailabilityCall[] {
  if (show === "all") return [...calls]
  const wanted: readonly Outcome[] = show === "races" ? RACES : show === "false_outs" ? ["false_out"] : OTHERS
  return calls.filter((call) => wanted.includes(call.outcome))
}

export const SOURCES: Record<string, string> = {
  nba_boxscore: "Box score",
  nba_report: "NBA report",
  espn_injuries: "ESPN injuries",
  espn_fantasy: "ESPN fantasy",
  news_claim: "News",
}

export function sourceLabel(source: string | null | undefined): string {
  if (!source) return "—"
  return SOURCES[source] ?? source
}

/** "+2h 5m", "−10m", "0m": how much earlier our verdict said out than ESPN fantasy. */
export function formatLead(minutes: number | null | undefined): string {
  if (minutes == null) return "—"
  const whole = Math.round(Math.abs(minutes))
  const sign = minutes >= 0.5 ? "+" : minutes <= -0.5 ? "−" : ""
  const hours = Math.floor(whole / 60)
  const rest = whole % 60
  const body = hours > 0 ? `${hours}h${rest > 0 ? ` ${rest}m` : ""}` : `${rest}m`
  return `${sign}${body}`
}

/** "62%", or "50.2%" with `digits`: the gate's share is read against 50%, so it keeps a decimal. */
export function formatPercent(share: number | null | undefined, digits = 0): string {
  return share == null ? "—" : `${(share * 100).toFixed(digits)}%`
}

/** The false outs our own sources made: the floor's are ESPN fantasy's rule, not our call. */
export function ownFalseOuts(summary: AvailabilitySummary): number {
  return summary.false_outs - summary.false_outs_floor
}
