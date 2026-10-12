import { ChevronRight } from "lucide-react"
import { Link, useSearchParams } from "react-router"

import { Block, Head, Notice, Page, Rail, RefreshNote, SheetSkeleton, Tag, Tiles } from "@/components/blocks"
import { CheckDefinition } from "@/components/CheckDefinition"
import { QualityMatrix, QualityMatrixKey } from "@/components/QualityMatrix"
import { RunChecksButton } from "@/components/RunChecksButton"
import { usePageContext } from "@/components/shell/pageContext"
import { StatusBadge } from "@/components/StateBadge"
import { useNow } from "@/hooks/useNow"
import { QUALITY_REFETCH_MS, useQualityOverview } from "@/hooks/useQuality"
import {
  groupChecks,
  parseQualityLimit,
  QUALITY_LIMITS,
  summarizeLatest,
  type QualityLimit,
  type QualityOverview,
  type QualityRun,
} from "@/lib/quality"
import { formatCentral, formatCentralLong, formatDuration, relativeTime } from "@/lib/time"
import s from "@/components/sheet.module.css"
import q from "@/components/quality.module.css"

export function Quality() {
  usePageContext("Quality")
  const [params, setParams] = useSearchParams()
  const limit = parseQualityLimit(params.get("limit"))
  const now = useNow()
  const query = useQualityOverview(limit)
  const data = query.data

  return (
    <Page>
      <Head
        title="Quality"
        sub="What every check asserts, and how each has fared in the newest runs."
        actions={
          <>
            <RunChecksButton />
            <RefreshNote updatedAt={query.dataUpdatedAt} fetching={query.isFetching} now={now} intervalMs={QUALITY_REFETCH_MS} />
          </>
        }
      />

      {query.error && (
        <Notice>
          Could not refresh: {query.error.message}
          {data && " Showing the last good data."}
        </Notice>
      )}

      {!data ? (
        <SheetSkeleton label="Loading quality" />
      ) : (
        <>
          <LatestTiles data={data} now={now} />

          <Block
            title="Checks by run"
            note={
              data.runs.length === 0
                ? "no runs yet"
                : `the last ${data.runs.length} ${data.runs.length === 1 ? "run" : "runs"}, oldest on the left · a cell opens that run`
            }
            right={<LimitRail limit={limit} onChange={(size) => setParams(size === 20 ? {} : { limit: String(size) })} />}
          >
            {data.runs.length === 0 ? (
              <p className={s.empty}>No quality runs yet. Run the checks to start a history.</p>
            ) : (
              <>
                <QualityMatrix checks={data.checks} runs={data.runs} limit={data.limit} />
                <div className={q.matrixFoot}>
                  <span className={`${s.mono} ${s.muted}`}>
                    {formatCentral(data.runs[data.runs.length - 1].started_at)} → {formatCentral(data.runs[0].started_at)}
                  </span>
                  <QualityMatrixKey />
                </div>
              </>
            )}
          </Block>

          <RunsTable runs={data.runs} now={now} />
          <Definitions data={data} />
        </>
      )}
    </Page>
  )
}

function LimitRail({ limit, onChange }: { limit: QualityLimit; onChange: (size: QualityLimit) => void }) {
  return <Rail label="How many runs" options={QUALITY_LIMITS.map((size) => ({ key: size, label: String(size) }))} value={limit} onChange={onChange} />
}

function LatestTiles({ data, now }: { data: QualityOverview; now: number }) {
  const latest: QualityRun | undefined = data.runs[0]
  const summary = summarizeLatest(data.checks)
  return (
    <Tiles
      tiles={[
        {
          label: "Latest run",
          value: latest ? relativeTime(latest.started_at, now) : "never",
          tone: !latest ? "quiet" : latest.status === "success" ? "up" : latest.status === "running" ? "live" : "down",
          note: latest ? `${latest.status} · ${latest.triggered_by ?? "unknown"}` : "",
        },
        { label: "Passing", value: latest ? `${summary.passed}/${summary.total}` : "—", note: "checks in the latest run" },
        {
          label: "Critical failing",
          value: latest ? String(summary.criticalFailing) : "—",
          tone: summary.criticalFailing > 0 ? "down" : "quiet",
          note: "each one alerts",
        },
        {
          label: "Warnings failing",
          value: latest ? String(summary.warningFailing) : "—",
          tone: summary.warningFailing > 0 ? "warn" : "quiet",
          note: "timing checks fail on off-days",
        },
      ]}
    />
  )
}

function RunsTable({ runs, now }: { runs: QualityRun[]; now: number }) {
  if (runs.length === 0) return null
  return (
    <Block title="Runs" count={runs.length} note="newest first · Central time · open one for every check's outcome">
      <div className={s.bleed}>
        <table className={s.table} style={{ minWidth: "40rem" }}>
          <thead>
            <tr>
              <th scope="col">Started</th>
              <th scope="col">Status</th>
              <th scope="col" className={s.num}>Passed</th>
              <th scope="col" className={s.num}>Failed</th>
              <th scope="col" className={s.num}>Duration</th>
              <th scope="col">Triggered by</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((entry) => (
              <tr key={entry.run_id}>
                <td className={`${s.mono} ${s.nowrap}`}>
                  <Link to={`/quality/runs/${entry.run_id}`} className={s.nameLink}>
                    {formatCentralLong(entry.started_at)}
                  </Link>
                  <span className={s.under}>{relativeTime(entry.started_at, now)}</span>
                </td>
                <td>
                  <StatusBadge status={entry.status} />
                </td>
                <td className={s.num}>
                  {entry.passed_checks}/{entry.total_checks}
                </td>
                <td className={`${s.num} ${entry.failed_checks > 0 ? s.bad : ""}`}>{entry.failed_checks}</td>
                <td className={s.num}>{formatDuration(entry.duration_seconds)}</td>
                <td className={s.muted}>{entry.triggered_by ?? "unknown"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Block>
  )
}

/** The catalogue: every check, what it guards, and the SQL that decides it. */
function Definitions({ data }: { data: QualityOverview }) {
  return (
    <Block title="What each check asserts" count={data.checks.length} note="each is one query that counts offending rows · zero passes">
      {groupChecks(data.checks).map((group) => (
        <section key={group.key} className={q.groupBlock} aria-label={`${group.label} check definitions`}>
          <h3 className={q.groupHead}>{group.label}</h3>
          {group.description && <p className={q.groupText}>{group.description}.</p>}
          <ul className={q.defList}>
            {group.checks.map((check) => (
              <li key={check.name}>
                <details>
                  <summary className={q.defSummary}>
                    <ChevronRight size={12} className={q.chevron} aria-hidden />
                    <span className={q.defName}>{check.name}</span>
                    <Tag tone={check.severity === "critical" ? "down" : undefined}>{check.severity}</Tag>
                    <span className={`${s.mono} ${s.muted}`}>{check.table}</span>
                  </summary>
                  <div className={q.defBody}>
                    <CheckDefinition definition={check} />
                  </div>
                </details>
              </li>
            ))}
          </ul>
        </section>
      ))}
    </Block>
  )
}
