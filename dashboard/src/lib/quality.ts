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
  { key: "timing", label: "Timing", description: "Each scheduled pipeline has succeeded in the last 24 hours. Expected to fail on off-days" },
]

/** Structural, then timing; a group this UI does not know goes last under its own name. */
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

/** `details` worth showing: anything beyond the failure count the row already has. */
export function extraDetails(outcome: Pick<QualityOutcome, "details">): Record<string, unknown> | null {
  if (!outcome.details) return null
  const rest = Object.fromEntries(Object.entries(outcome.details).filter(([key]) => key !== "failures"))
  return Object.keys(rest).length > 0 ? rest : null
}
