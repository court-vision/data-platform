import type { Schemas } from "@/lib/api"

export type QualityOverview = Schemas["QualityOverviewData"]
export type QualityCheckRow = Schemas["QualityCheckRow"]
export type QualityCheckInfo = Schemas["QualityCheckInfo"]
export type QualityRun = Schemas["QualityRunEntry"]
export type QualityRunDetail = Schemas["QualityRunDetailData"]
export type QualityOutcome = Schemas["QualityCheckOutcome"]

/** A check's result in one run; null when that run did not include it. */
export type CheckResult = string | null

export const QUALITY_LIMITS = [20, 50, 100] as const
export type QualityLimit = (typeof QUALITY_LIMITS)[number]

export function parseQualityLimit(raw: string | null): QualityLimit {
  const n = Number(raw)
  return (QUALITY_LIMITS as readonly number[]).includes(n) ? (n as QualityLimit) : 20
}

/** What a cell says in words: the matrix never leaves it to colour alone. */
export function resultLabel(result: CheckResult): string {
  if (result === "passed") return "passed"
  if (result === "failed") return "failed"
  if (result === "error") return "could not run"
  return result ?? "not in this run"
}

/**
 * How many runs in a row, counting back from the newest, a check has not
 * passed. A run that left the check out says nothing about it, so it neither
 * counts nor breaks the streak. `results` is newest first, as the API sends it.
 */
export function failingStreak(results: readonly CheckResult[]): number {
  let streak = 0
  for (const result of results) {
    if (result === null) continue
    if (result === "passed") break
    streak += 1
  }
  return streak
}

/**
 * Whether that streak reaches the oldest of `results` without meeting a pass.
 * When those are a full window, the streak is at least what it shows and
 * maybe longer: the runs before the window are not on the page.
 */
export function failedThroughout(results: readonly CheckResult[]): boolean {
  return failingStreak(results) > 0 && !results.includes("passed")
}

/** The newest result a check actually has, skipping runs that left it out. */
export function latestResult(results: readonly CheckResult[]): CheckResult {
  return results.find((result) => result !== null) ?? null
}

export interface CheckGroup {
  key: string
  label: string
  description: string
  checks: QualityCheckRow[]
}

const GROUPS: ReadonlyArray<Omit<CheckGroup, "checks">> = [
  { key: "structural", label: "Structural", description: "Integrity of the rows: required fields, references, ranges" },
  { key: "consistency", label: "Consistency", description: "One table held to account against another, over the last week of game nights that are due: season totals against the game log, team records and scores against the schedule, live against settled" },
  { key: "timing", label: "Timing", description: "Each scheduled pipeline has succeeded in the last 24 hours. Expected to fail on off-days" },
]

/** Structural, consistency, then timing; a group this UI does not know goes last under its own name. */
export function groupChecks(checks: QualityCheckRow[]): CheckGroup[] {
  const known = new Set(GROUPS.map((group) => group.key))
  const groups: CheckGroup[] = GROUPS.map((group) => ({ ...group, checks: checks.filter((check) => check.group === group.key) }))
  for (const key of new Set(checks.map((check) => check.group))) {
    if (!known.has(key)) groups.push({ key, label: key, description: "", checks: checks.filter((check) => check.group === key) })
  }
  return groups.filter((group) => group.checks.length > 0)
}

export interface LatestSummary {
  /** Checks the newest run included. */
  total: number
  passed: number
  criticalFailing: number
  warningFailing: number
}

/** The newest run, column 0 of the matrix, split by severity. */
export function summarizeLatest(checks: QualityCheckRow[]): LatestSummary {
  const summary: LatestSummary = { total: 0, passed: 0, criticalFailing: 0, warningFailing: 0 }
  for (const check of checks) {
    const result = check.results[0] ?? null
    if (result === null) continue
    summary.total += 1
    if (result === "passed") summary.passed += 1
    else if (check.severity === "critical") summary.criticalFailing += 1
    else summary.warningFailing += 1
  }
  return summary
}

export interface OutcomeCounts {
  total: number
  passed: number
  criticalFailing: number
  warningFailing: number
}

/** The same split for one run's own outcomes. */
export function countOutcomes(outcomes: QualityOutcome[]): OutcomeCounts {
  const counts: OutcomeCounts = { total: outcomes.length, passed: 0, criticalFailing: 0, warningFailing: 0 }
  for (const outcome of outcomes) {
    if (outcome.status === "passed") counts.passed += 1
    else if (outcome.severity === "critical") counts.criticalFailing += 1
    else counts.warningFailing += 1
  }
  return counts
}

/** The offending rows a failed check kept: a few of them, each a flat record. */
export interface Sample {
  columns: string[]
  rows: Record<string, unknown>[]
}

/** `details.sample` when it is what the API promises (a list of records), else null. */
export function sampleRows(outcome: Pick<QualityOutcome, "details">): Sample | null {
  const sample = outcome.details?.sample
  if (!Array.isArray(sample) || sample.length === 0) return null
  const rows = sample.filter((row): row is Record<string, unknown> => typeof row === "object" && row !== null && !Array.isArray(row))
  if (rows.length === 0) return null
  // Column order is the query's: the first row carries every column.
  return { columns: Object.keys(rows[0]), rows }
}

/** A sampled cell as text; null is a blank the reader can see. */
export function sampleCell(value: unknown): string {
  if (value === null || value === undefined) return "—"
  return typeof value === "string" ? value : String(value)
}

/** `details` worth showing as key/value: not the count the row has, not the sample the table shows. */
export function extraDetails(outcome: Pick<QualityOutcome, "details">): Record<string, unknown> | null {
  if (!outcome.details) return null
  const rest = Object.fromEntries(Object.entries(outcome.details).filter(([key]) => key !== "failures" && key !== "sample"))
  return Object.keys(rest).length > 0 ? rest : null
}

/** The checks that judge a pipeline's output (or, for a timing check, its runs). */
export function checksForPipeline(checks: QualityCheckRow[], pipeline: string): QualityCheckRow[] {
  return checks.filter((check) => check.pipelines.includes(pipeline))
}
