import type { Schemas } from "@/lib/api"
import type { Tone } from "@/components/blocks"

export type AiSpendReport = Schemas["AiSpendReport"]
export type SpendLine = Schemas["AiSpendLine"]
export type SpendRow = Schemas["AiSpendRow"]
export type UsageRow = Schemas["AiUsageRow"]
export type SpendUser = Schemas["AiSpendUser"]
export type UserLine = Schemas["AiUserLine"]
export type LineKey = SpendLine["line"]
export type SpendApp = Schemas["AiSpendApp"]
/** What a cap can be set on: a line, or "app", the whole app's spend on every line together. */
export type CapLineKey = Schemas["AiCapRequest"]["line"]
export type FeatureKey = SpendRow["feature"]

export type SpendWindow = "7" | "30" | "90"
export const WINDOWS: readonly { key: SpendWindow; label: string }[] = [
  { key: "7", label: "7d" },
  { key: "30", label: "30d" },
  { key: "90", label: "90d" },
]

export function parseWindow(value: string | null): SpendWindow {
  return value === "7" || value === "90" ? value : "30"
}

export const LINES: Record<LineKey, { label: string; note: string }> = {
  desk: { label: "Dime desk", note: "Chat, the draft room, and replies in a check-in" },
  autopilot: { label: "Check-ins", note: "The check-ins Dime starts itself" },
  analyst: { label: "Analyst", note: "The live rankings' morning reads" },
}

export const APP_LINE = { label: "Whole app", note: "Every line together: none can spend past it" }

export interface MonthTotal {
  /** Settled and open together, as the caps count it. */
  spent: number
  open: number
  cap: number | null
  /** The cap is the whole app's; false: the lines' caps summed (a backend with no whole-app cap). */
  whole: boolean
}

/** The month across every line, against the whole app's cap when the backend has one. */
export function monthTotal(report: Pick<AiSpendReport, "lines" | "app">): MonthTotal {
  const app = report.app
  if (app) {
    return { spent: app.spent_micros + app.open_micros, open: app.open_micros, cap: app.cap_micros ?? null, whole: true }
  }
  const open = report.lines.reduce((sum, line) => sum + line.open_micros, 0)
  return {
    spent: report.lines.reduce((sum, line) => sum + line.spent_micros, 0) + open,
    open,
    cap: report.lines.reduce((sum, line) => sum + (line.cap_micros ?? 0), 0),
    whole: false,
  }
}

export interface Feature {
  key: FeatureKey
  label: string
  note: string
}

/**
 * Every feature, in the chart's stacking order and the palette's slot order
 * (aiSpend.module.css): the first five hues of the reference palette, then grey.
 */
export const FEATURES: readonly Feature[] = [
  { key: "chat", label: "Chat", note: "Messages to Dime on the AI desk" },
  { key: "checkin", label: "Check-ins", note: "Check-ins Dime started itself (the autopilot budget)" },
  { key: "draft", label: "Draft room", note: "Dime's notes in the draft room (the desk's budget)" },
  { key: "checkin_reply", label: "Check-in replies", note: "Your replies inside a check-in (the desk's budget)" },
  { key: "analyst", label: "Analyst", note: "The live rankings' analyst reads" },
  { key: "unlinked", label: "Unlinked", note: "Desk spend whose chat was deleted, or whose turn never finished" },
]

const FEATURE_BY_KEY = new Map(FEATURES.map((feature) => [feature.key, feature]))

export function featureLabel(key: FeatureKey): string {
  return FEATURE_BY_KEY.get(key)?.label ?? key
}

const CHECKIN_KINDS: Record<string, string> = {
  morning: "Morning",
  weekly: "Weekly",
  pre_lock: "Before lock",
  night: "Night",
  event: "News",
}

export function checkinKindLabel(kind: string | null | undefined): string {
  return kind ? (CHECKIN_KINDS[kind] ?? kind) : "Not recorded"
}

// ---------------------------------------------------------------- money

/** "$12.34"; a sliver of a cent as "<$0.01"; nothing as "$0.00". */
export function formatUsd(micros: number | null | undefined): string {
  if (micros == null) return "—"
  if (micros > 0 && micros < 5_000) return "<$0.01"
  const dollars = micros / 1_000_000
  return `$${dollars.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}

/** A cap as the editor shows it: "25", "12.5", "0". */
export function capInput(micros: number | null | undefined): string {
  if (micros == null) return ""
  return String(Math.round(micros / 10_000) / 100)
}

export const MAX_CAP_USD = 1000

/** What the editor's text means: dollars as a string the API takes, or why not. */
export function parseCap(text: string): { usd: string } | { error: string } {
  const trimmed = text.trim().replace(/^\$/, "")
  if (trimmed === "") return { error: "Enter a monthly cap in dollars" }
  if (!/^\d+(\.\d{1,2})?$/.test(trimmed)) return { error: "Dollars, with at most two decimals" }
  const value = Number(trimmed)
  if (value > MAX_CAP_USD) return { error: `At most $${MAX_CAP_USD} a month` }
  return { usd: trimmed }
}

/** How full a month is against its cap, and the tone that says it. */
export function capShare(spent: number, cap: number | null | undefined): { share: number | null; tone?: Tone } {
  if (cap == null) return { share: null }
  if (cap <= 0) return { share: spent > 0 ? 1 : 0, tone: "down" }
  const share = spent / cap
  return { share, tone: share >= 1 ? "down" : share >= 0.8 ? "warn" : undefined }
}

export function formatShare(share: number | null): string {
  if (share == null) return "—"
  return `${Math.round(share * 100)}%`
}

// ---------------------------------------------------------------- by day

export interface Day {
  day: string
  total: number
  open: number
  parts: Partial<Record<FeatureKey, number>>
}

/** Add days to an ISO date ("2026-11-01"), in UTC. */
function addDays(iso: string, days: number): string {
  const date = new Date(`${iso}T00:00:00Z`)
  date.setUTCDate(date.getUTCDate() + days)
  return date.toISOString().slice(0, 10)
}

/** Every day of the window, oldest first, empty days included, each split by feature. */
export function byDay(report: Pick<AiSpendReport, "rows" | "since" | "days">): Day[] {
  const days: Day[] = Array.from({ length: report.days }, (_, i) => ({
    day: addDays(report.since, i),
    total: 0,
    open: 0,
    parts: {},
  }))
  const index = new Map(days.map((day, i) => [day.day, i]))
  for (const row of report.rows) {
    const at = index.get(row.day)
    if (at == null) continue
    const day = days[at]
    day.total += row.cost_micros
    day.open += row.open_micros
    day.parts[row.feature] = (day.parts[row.feature] ?? 0) + row.cost_micros
  }
  return days
}

/** The features a set of days uses, in palette order: the chart's key. */
export function featuresUsed(days: readonly Day[]): Feature[] {
  return FEATURES.filter((feature) => days.some((day) => (day.parts[feature.key] ?? 0) > 0))
}

// ---------------------------------------------------------------- by feature, model, team

export interface Tokens {
  records: number
  modelCalls: number
  input: number
  output: number
  cacheRead: number
  cacheWrite: number
  recordCost: number
}

const NO_TOKENS: Tokens = { records: 0, modelCalls: 0, input: 0, output: 0, cacheRead: 0, cacheWrite: 0, recordCost: 0 }

function addUsage(into: Tokens, row: UsageRow): Tokens {
  return {
    records: into.records + row.records,
    modelCalls: into.modelCalls + row.model_calls,
    input: into.input + row.input_tokens,
    output: into.output + row.output_tokens,
    cacheRead: into.cacheRead + row.cache_read_input_tokens,
    cacheWrite: into.cacheWrite + row.cache_creation_input_tokens,
    recordCost: into.recordCost + row.cost_micros,
  }
}

/** The share of input read from the cache: what caching saves is the measure of it. */
export function cacheHitRate(tokens: Tokens): number | null {
  const read = tokens.input + tokens.cacheRead + tokens.cacheWrite
  return read > 0 ? tokens.cacheRead / read : null
}

export interface FeatureTotal extends Tokens {
  feature: Feature
  cost: number
  open: number
  entries: number
  /** Check-ins only: spend by kind (morning, night, ...). */
  kinds: { kind: string; cost: number }[]
}

/** Each feature over the window: its spend from the ledger, its tokens from the records. Costliest first. */
export function byFeature(report: Pick<AiSpendReport, "rows" | "usage">): FeatureTotal[] {
  const totals = new Map<FeatureKey, FeatureTotal>()
  const get = (key: FeatureKey) => {
    let total = totals.get(key)
    if (!total) {
      const feature = FEATURE_BY_KEY.get(key) ?? { key, label: key, note: "" }
      total = { feature, cost: 0, open: 0, entries: 0, kinds: [], ...NO_TOKENS }
      totals.set(key, total)
    }
    return total
  }
  const kinds = new Map<string, number>()
  for (const row of report.rows) {
    const total = get(row.feature)
    total.cost += row.cost_micros
    total.open += row.open_micros
    total.entries += row.entries
    if (row.feature === "checkin") kinds.set(row.detail ?? "", (kinds.get(row.detail ?? "") ?? 0) + row.cost_micros)
  }
  for (const row of report.usage) {
    const total = get(row.feature)
    Object.assign(total, addUsage(total, row))
  }
  const checkins = totals.get("checkin")
  if (checkins) {
    checkins.kinds = [...kinds.entries()].map(([kind, cost]) => ({ kind, cost })).sort((a, b) => b.cost - a.cost)
  }
  return [...totals.values()].sort((a, b) => b.cost - a.cost || a.feature.label.localeCompare(b.feature.label))
}

export interface ModelTotal extends Tokens {
  model: string
}

/** Each model over the window, from the records (the ledger does not know models). Costliest first. */
export function byModel(usage: readonly UsageRow[]): ModelTotal[] {
  const totals = new Map<string, ModelTotal>()
  for (const row of usage) {
    const total = totals.get(row.model) ?? { model: row.model, ...NO_TOKENS }
    totals.set(row.model, { ...addUsage(total, row), model: row.model })
  }
  return [...totals.values()].sort((a, b) => b.recordCost - a.recordCost || a.model.localeCompare(b.model))
}

export interface TeamTotal {
  key: string
  team: string
  user: string | null
  cost: number
  parts: Partial<Record<FeatureKey, number>>
}

/**
 * Who spent it over the window: each team, then each user's spend with no team
 * (a chat pinned to none, the draft room), then the analyst, which is no one's.
 */
export function byTeam(report: Pick<AiSpendReport, "rows" | "teams" | "users">): TeamTotal[] {
  const teams = new Map(report.teams.map((team) => [team.team_id, team]))
  const emails = new Map(report.users.map((user) => [user.user_id, user.email]))
  const totals = new Map<string, TeamTotal>()
  for (const row of report.rows) {
    let key: string
    let team: string
    let user: string | null
    if (row.team_id != null) {
      const found = teams.get(row.team_id)
      key = `team:${row.team_id}`
      team = found?.name ?? `Team ${row.team_id}`
      const owner = found?.user_id ?? row.user_id
      user = owner != null ? (emails.get(owner) ?? `user ${owner}`) : null
    } else if (row.user_id != null) {
      key = `user:${row.user_id}`
      team = "No team"
      user = emails.get(row.user_id) ?? `user ${row.user_id}`
    } else {
      key = "none"
      team = "No one's"
      user = null
    }
    const total = totals.get(key) ?? { key, team, user, cost: 0, parts: {} }
    total.cost += row.cost_micros
    total.parts[row.feature] = (total.parts[row.feature] ?? 0) + row.cost_micros
    totals.set(key, total)
  }
  return [...totals.values()].sort((a, b) => b.cost - a.cost || a.team.localeCompare(b.team))
}

/** 1,234 / 12.3k / 4.5M: token counts at a glance. */
export function formatTokens(count: number): string {
  if (count < 10_000) return count.toLocaleString("en-US")
  if (count < 1_000_000) return `${(count / 1000).toFixed(count < 100_000 ? 1 : 0)}k`
  return `${(count / 1_000_000).toFixed(1)}M`
}

/** A round top for the day chart's scale, in micros: 1, 2 or 5 times a power of ten dollars. */
export function niceTop(micros: number): number {
  if (micros <= 0) return 1_000_000
  const power = 10 ** Math.floor(Math.log10(micros))
  for (const step of [1, 2, 5, 10]) {
    if (micros <= step * power) return step * power
  }
  return 10 * power
}
