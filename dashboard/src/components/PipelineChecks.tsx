import { Link } from "react-router"

import { StatusBadge } from "@/components/StateBadge"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { useQualityOverview } from "@/hooks/useQuality"
import { checksForPipeline, failingStreak, latestResult, resultLabel, type QualityCheckRow } from "@/lib/quality"

/**
 * The quality checks that judge what one pipeline writes, with each one's
 * newest result. Quiet until the quality page's own query has answered, and
 * absent for a pipeline no check looks at: the runs are this page's subject.
 */
export function PipelineChecks({ pipeline }: { pipeline: string }) {
  const { data } = useQualityOverview(20)
  if (!data) return null
  const checks = checksForPipeline(data.checks, pipeline)
  if (checks.length === 0) return null
  const failing = checks.filter((check) => failingStreak(check.results) > 0).length

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-baseline gap-2 text-base">
          Checks on this data
          <span className="font-mono text-xs font-normal text-muted-foreground">
            {failing > 0 ? `${failing} of ${checks.length} failing` : checks.length}
          </span>
        </CardTitle>
        <CardDescription>
          The quality checks that judge what this pipeline writes, as of the newest run of each.{" "}
          <Link to="/quality" className="text-primary underline-offset-4 hover:underline">All checks</Link>
        </CardDescription>
      </CardHeader>
      <CardContent className="px-0 pb-2">
        <ul>
          {checks.map((check) => (
            <CheckRow key={check.name} check={check} runId={newestRunWith(check, data.runs.map((run) => run.run_id))} />
          ))}
        </ul>
      </CardContent>
    </Card>
  )
}

/** The newest run that included the check: where its result (and offending rows) can be read. */
function newestRunWith(check: QualityCheckRow, runIds: string[]): string | null {
  const index = check.results.findIndex((result) => result !== null)
  return index >= 0 ? (runIds[index] ?? null) : null
}

function CheckRow({ check, runId }: { check: QualityCheckRow; runId: string | null }) {
  const result = latestResult(check.results)
  const streak = failingStreak(check.results)
  return (
    <li className="flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-border/50 px-6 py-2 text-sm last:border-0" data-check={check.name}>
      <div className="min-w-0 flex-1 basis-64">
        <p className="break-all font-mono text-xs font-medium">{check.name}</p>
        <p className="text-xs text-muted-foreground">
          {check.group === "timing" ? "ran in the last 24 hours" : check.table}
          {check.against.length > 0 && ` against ${check.against.join(", ")}`}
        </p>
      </div>
      <Badge variant="neutral">{check.group}</Badge>
      <Badge variant={check.severity === "critical" ? "outline" : "neutral"}>{check.severity}</Badge>
      {streak > 1 && <span className="font-mono text-xs text-status-loss">{streak} runs</span>}
      {runId ? (
        <Link to={`/quality/runs/${runId}`} aria-label={`${check.name}: ${resultLabel(result)}. Open that run`}>
          <StatusBadge status={result} label={resultLabel(result)} />
        </Link>
      ) : (
        <StatusBadge status={null} label="not run yet" />
      )}
    </li>
  )
}
