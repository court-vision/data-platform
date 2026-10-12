import { Link } from "react-router"

import { Block, Tag } from "@/components/blocks"
import { StatusBadge } from "@/components/StateBadge"
import { useQualityOverview } from "@/hooks/useQuality"
import { checksForPipeline, failedThroughout, failingStreak, latestResult, resultLabel, type QualityCheckRow } from "@/lib/quality"
import s from "./sheet.module.css"
import o from "./overview.module.css"

/** How many of the newest quality runs a check's streak is counted over. */
const WINDOW = 20

/**
 * The quality checks that judge what one pipeline writes, with each one's
 * newest result. Quiet until the quality page's own query has answered, and
 * absent for a pipeline no check looks at: the runs are this page's subject.
 */
export function PipelineChecks({ pipeline }: { pipeline: string }) {
  const { data } = useQualityOverview(WINDOW)
  if (!data) return null
  const checks = checksForPipeline(data.checks, pipeline)
  if (checks.length === 0) return null
  const failing = checks.filter((check) => failingStreak(check.results) > 0).length
  // As many runs as were asked for: there are likely older ones, so a streak
  // that fills the window is a floor ("20+ runs"), as on the quality page.
  const windowFull = data.runs.length >= WINDOW

  return (
    <Block
      title="Checks on this data"
      count={failing > 0 ? `${failing} of ${checks.length} failing` : checks.length}
      countTone={failing > 0 ? "down" : undefined}
      text={
        <>
          The quality checks that judge what this pipeline writes, as of the newest run of each.{" "}
          <Link to="/quality" className={s.textLink}>
            All checks
          </Link>
        </>
      }
    >
      <ul className={o.rows}>
        {checks.map((check) => (
          <CheckRow
            key={check.name}
            check={check}
            runId={newestRunWith(check, data.runs.map((run) => run.run_id))}
            orMore={windowFull && failedThroughout(check.results)}
          />
        ))}
      </ul>
    </Block>
  )
}

/** The newest run that included the check: where its result (and offending rows) can be read. */
function newestRunWith(check: QualityCheckRow, runIds: string[]): string | null {
  const index = check.results.findIndex((result) => result !== null)
  return index >= 0 ? (runIds[index] ?? null) : null
}

function CheckRow({ check, runId, orMore }: { check: QualityCheckRow; runId: string | null; orMore: boolean }) {
  const result = latestResult(check.results)
  const streak = failingStreak(check.results)
  return (
    <li className={o.rowLine} data-check={check.name}>
      <div className={o.rowMain}>
        <span className={o.checkName}>{check.name}</span>
        <span className={s.under}>
          {check.group === "timing" ? "ran in the last 24 hours" : check.table}
          {check.against.length > 0 && ` against ${check.against.join(", ")}`}
        </span>
      </div>
      <Tag>{check.group}</Tag>
      <Tag tone={check.severity === "critical" ? "down" : undefined}>{check.severity}</Tag>
      {streak > 1 && (
        <span className={`${s.mono} ${s.bad}`} title={orMore ? "Not passed in any run looked at; older runs are not counted here" : undefined}>
          {`${streak}${orMore ? "+" : ""} runs`}
        </span>
      )}
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
