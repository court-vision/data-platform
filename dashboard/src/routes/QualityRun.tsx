import { ChevronLeft, ChevronRight } from "lucide-react"
import { Link, useParams } from "react-router"

import { Block, Head, Lost, Notice, Page, Sep, SheetSkeleton, Tag, Tiles } from "@/components/blocks"
import { CheckDefinition } from "@/components/CheckDefinition"
import { usePageContext } from "@/components/shell/pageContext"
import { StatusBadge } from "@/components/StateBadge"
import { useNow } from "@/hooks/useNow"
import { useQualityRun } from "@/hooks/useQuality"
import { ApiError } from "@/lib/api"
import { countOutcomes, extraDetails, sampleCell, sampleRows, type QualityOutcome, type QualityRunDetail, type Sample } from "@/lib/quality"
import { formatCentralLong, formatDuration, relativeTime } from "@/lib/time"
import dk from "@/desk/desk.module.css"
import s from "@/components/sheet.module.css"
import q from "@/components/quality.module.css"

export function QualityRun() {
  const { runId = "" } = useParams()
  const now = useNow()
  const query = useQualityRun(runId)
  const data = query.data
  const missing = query.error instanceof ApiError && query.error.status === 404
  usePageContext(missing ? "Not found" : data ? `Quality run · ${formatCentralLong(data.run.started_at)}` : "Quality run")

  if (missing) {
    return <Lost title="No quality run with that id" back={{ to: "/quality", label: "Back to quality" }} />
  }

  return (
    <Page>
      {data ? (
        <Header data={data} now={now} />
      ) : (
        <Head back={{ to: "/quality", label: "Quality" }} title="Quality run" meta={<span>{runId}</span>} />
      )}

      {query.error && (
        <Notice>
          Could not load: {query.error.message}
          {data && " Showing the last good data."}
        </Notice>
      )}

      {!data ? (
        <SheetSkeleton label="Loading quality run" />
      ) : (
        <>
          <OutcomeTiles outcomes={data.checks} />
          <Outcomes outcomes={data.checks} />
        </>
      )}
    </Page>
  )
}

function Header({ data, now }: { data: QualityRunDetail; now: number }) {
  const { run } = data
  return (
    <Head
      back={{ to: "/quality", label: "Quality" }}
      title="Quality run"
      chips={<StatusBadge status={run.status} />}
      meta={
        <>
          <span>{formatCentralLong(run.started_at)}</span>
          <Sep />
          <span>{relativeTime(run.started_at, now)}</span>
          <Sep />
          <span>{run.status === "running" ? "still running" : `took ${formatDuration(run.duration_seconds)}`}</span>
          <Sep />
          <span>triggered by {run.triggered_by ?? "unknown"}</span>
        </>
      }
      sub={
        <>
          <span className={`${s.mono} ${s.muted}`}>{run.run_id}</span>
          {run.error_message && <span className={`${s.bad} ${s.lineBlock}`}>The run itself failed: {run.error_message}</span>}
        </>
      }
      actions={
        <nav className={q.steps} aria-label="Neighbouring runs">
          <Step to={data.older_run_id} label="Older">
            <ChevronLeft size={13} aria-hidden /> Older
          </Step>
          <Step to={data.newer_run_id} label="Newer">
            Newer <ChevronRight size={13} aria-hidden />
          </Step>
        </nav>
      }
    />
  )
}

function Step({ to, label, children }: { to: string | null; label: string; children: React.ReactNode }) {
  if (!to) {
    return (
      <span className={`${dk.btn} ${dk.btnSmall} ${s.disabled}`} aria-disabled="true" aria-label={`${label} run: none`}>
        {children}
      </span>
    )
  }
  return (
    <Link to={`/quality/runs/${to}`} className={`${dk.btn} ${dk.btnSmall}`}>
      {children}
    </Link>
  )
}

function OutcomeTiles({ outcomes }: { outcomes: QualityOutcome[] }) {
  const counts = countOutcomes(outcomes)
  return (
    <Tiles
      tiles={[
        { label: "Checks", value: counts.total },
        { label: "Passed", value: counts.passed, tone: "up" },
        { label: "Critical failing", value: counts.criticalFailing, tone: counts.criticalFailing > 0 ? "down" : "quiet" },
        { label: "Warnings failing", value: counts.warningFailing, tone: counts.warningFailing > 0 ? "warn" : "quiet" },
      ]}
    />
  )
}

/** Every check in the run, what needs a look first. A failing row starts open. */
function Outcomes({ outcomes }: { outcomes: QualityOutcome[] }) {
  return (
    <Block title="Checks" count={outcomes.length} note="failures first · open a check for what it asserts and the query behind it">
      {outcomes.length === 0 ? (
        <p className={s.empty}>This run recorded no checks.</p>
      ) : (
        <div className={s.bleed}>
          <div className={q.outcomes}>
            <div className={`${q.outcomeRow} ${q.outcomeHead}`} aria-hidden>
              <span />
              <span>Check</span>
              <span>Status</span>
              <span>Severity</span>
              <span className={s.alignRight}>Failures</span>
              <span className={s.alignRight}>Took</span>
            </div>
            <ul className={q.outcomeList}>
              {outcomes.map((outcome) => (
                <Outcome key={outcome.check_name} outcome={outcome} />
              ))}
            </ul>
          </div>
        </div>
      )}
    </Block>
  )
}

function Outcome({ outcome }: { outcome: QualityOutcome }) {
  const failing = outcome.status !== "passed"
  const extra = extraDetails(outcome)
  const sample = sampleRows(outcome)
  return (
    <li data-check={outcome.check_name} data-status={outcome.status}>
      <details open={failing}>
        <summary className={q.outcomeRow}>
          <ChevronRight size={12} className={q.chevron} aria-hidden />
          <span className={q.defName}>{outcome.check_name}</span>
          <span>
            <StatusBadge status={outcome.status} />
          </span>
          <span>
            <Tag tone={outcome.severity === "critical" ? "down" : undefined}>{outcome.severity}</Tag>
          </span>
          <span className={`${s.mono} ${s.alignRight} ${failing ? s.bad : s.muted}`}>
            {failing ? outcome.failures.toLocaleString() : "—"}
          </span>
          <span className={`${s.mono} ${s.alignRight} ${s.muted}`}>
            {outcome.duration_ms == null ? "—" : formatDuration(outcome.duration_ms / 1000)}
          </span>
        </summary>
        <div className={q.outcomeBody}>
          {outcome.message && <p className={q.outcomeMessage}>{outcome.message}</p>}
          {sample && <OffendingRows sample={sample} failures={outcome.failures} />}
          {extra && (
            <dl className={`${s.pre} ${s.preWrap} ${q.extra}`}>
              {Object.entries(extra).map(([key, value]) => (
                <div key={key}>
                  <dt>{key}</dt>
                  {/* A string is shown as it is: through JSON its quotes would come out escaped. */}
                  <dd>{typeof value === "string" ? value : JSON.stringify(value)}</dd>
                </div>
              ))}
            </dl>
          )}
          {outcome.definition ? (
            <CheckDefinition definition={outcome.definition} />
          ) : (
            <p className={s.empty}>This check is no longer defined in the code, so only its recorded result remains.</p>
          )}
        </div>
      </details>
    </li>
  )
}

/** The rows a failed check kept: which player, which night, not just how many. */
function OffendingRows({ sample, failures }: { sample: Sample; failures: number }) {
  const shown = sample.rows.length
  return (
    <figure className={q.sample}>
      <figcaption className={q.sampleCaption}>
        {shown < failures ? `The first ${shown} of ${failures.toLocaleString()} offending rows` : shown === 1 ? "The offending row" : `All ${shown} offending rows`}
      </figcaption>
      <div className={q.sampleWrap}>
        <table className={q.sampleTable}>
          <thead>
            <tr>
              {sample.columns.map((column) => (
                <th key={column} scope="col">
                  {column}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sample.rows.map((row, i) => (
              <tr key={i}>
                {sample.columns.map((column) => (
                  <td key={column}>{sampleCell(row[column])}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </figure>
  )
}
