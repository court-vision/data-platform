import { useParams, useSearchParams } from "react-router"

import { Block, Head, Lost, Notice, Page, Rail, RefreshNote, SheetSkeleton, Tag, Tiles, type Tone } from "@/components/blocks"
import { DurationChart } from "@/components/DurationChart"
import { PipelineChecks } from "@/components/PipelineChecks"
import { RunPipelineButton } from "@/components/RunPipelineButton"
import { usePageContext } from "@/components/shell/pageContext"
import { StateBadge, StatusBadge } from "@/components/StateBadge"
import { useNow } from "@/hooks/useNow"
import { RUNS_REFETCH_MS, usePipelineRuns } from "@/hooks/usePipelineRuns"
import { ApiError } from "@/lib/api"
import { CATEGORIES } from "@/lib/pipelines"
import { formatRate, LIMITS, parseLimit, type Limit, type PipelineInfo, type PipelineRun, type RunsSummary } from "@/lib/runs"
import { formatCentral, formatCentralLong, formatDuration, relativeTime } from "@/lib/time"
import s from "@/components/sheet.module.css"

export function PipelineDetail() {
  const { name = "" } = useParams()
  usePageContext(name)
  const [params, setParams] = useSearchParams()
  const limit = parseLimit(params.get("limit"))
  const now = useNow()
  const query = usePipelineRuns(name, limit)
  const data = query.data
  const limitRail = <LimitRail limit={limit} onChange={(size) => setParams(size === 50 ? {} : { limit: String(size) })} />

  if (query.error instanceof ApiError && query.error.status === 404) {
    return <Lost title={`No pipeline called ${name}`} back={{ to: "/", label: "Back to the overview" }} />
  }

  return (
    <Page>
      <Head
        back={{ to: "/", label: "Overview" }}
        title={data ? data.pipeline.display_name : name}
        chips={
          data && (
            <>
              <Tag>{categoryLabel(data.pipeline.category)}</Tag>
              {data.pipeline.is_running && <StatusBadge status="running" />}
            </>
          )
        }
        meta={<span>{name}</span>}
        sub={data?.pipeline.description}
        actions={
          <>
            {data && <RunPipelineButton pipeline={data.pipeline} />}
            <RefreshNote updatedAt={query.dataUpdatedAt} fetching={query.isFetching} now={now} intervalMs={RUNS_REFETCH_MS} />
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
        <>
          {/* This limit's fetch failed with nothing to stand in for it. Another
              limit may still be cached, and these are the way back to it. */}
          {query.error && <Block title="Runs" right={limitRail} />}
          <SheetSkeleton label="Loading pipeline" />
        </>
      ) : (
        <>
          <SummaryTiles summary={data.summary} now={now} />
          <Facts pipeline={data.pipeline} />

          <Block title="Duration" note={`the last ${data.runs.length} runs, oldest on the left`} right={limitRail}>
            <DurationChart runs={data.runs} now={now} />
          </Block>

          <PipelineChecks pipeline={data.pipeline.name} />

          <RunsTable runs={data.runs} now={now} />
        </>
      )}
    </Page>
  )
}

function LimitRail({ limit, onChange }: { limit: Limit; onChange: (size: Limit) => void }) {
  return <Rail label="How many runs" options={LIMITS.map((size) => ({ key: size, label: String(size) }))} value={limit} onChange={onChange} />
}

function categoryLabel(key: string): string {
  return CATEGORIES.find((category) => category.key === key)?.label ?? key
}

/** What the registry says: where it writes, what fires it, what gates it. */
function Facts({ pipeline }: { pipeline: PipelineInfo }) {
  const gates: string[] = []
  if (pipeline.espn_gated) gates.push("waits for ESPN's scoring period to advance")
  if (pipeline.earliest_run_time_cst) gates.push(`not before ${pipeline.earliest_run_time_cst} CT`)
  // The API sends the window the gate uses, default resolved, for pre-game pipelines only.
  if (pipeline.pre_game_window_minutes != null) {
    gates.push(`${pipeline.pre_game_window_minutes} min before first tip-off`)
  }
  if (pipeline.allow_concurrent) gates.push("may run concurrently")

  const rows: Array<[string, string]> = [
    ["Writes", pipeline.target_table],
    ["Trigger", `POST ${pipeline.trigger_endpoint}${pipeline.accepts_date ? " · ?date=YYYY-MM-DD to backfill" : ""}`],
    ["Cron job", pipeline.cron_job ?? "none (manual)"],
    ["Depends on", pipeline.depends_on.length > 0 ? pipeline.depends_on.join(", ") : "—"],
    ["Gates", gates.length > 0 ? gates.join(" · ") : "—"],
  ]

  return (
    <Block title="Registry" note="what the pipeline registry says about it">
      <dl className={s.facts}>
        {rows.map(([label, value]) => (
          <div key={label} className={s.fact}>
            <dt className={s.factKey}>{label}</dt>
            <dd className={s.factVal}>{value}</dd>
          </div>
        ))}
      </dl>
    </Block>
  )
}

function SummaryTiles({ summary, now }: { summary: RunsSummary; now: number }) {
  const rate = summary.success_rate
  const rateTone: Tone = rate == null ? "quiet" : rate >= 0.9 ? "up" : rate >= 0.5 ? "warn" : "down"
  return (
    <Tiles
      tiles={[
        { label: "Runs", value: String(summary.total), note: summary.oldest_started_at ? `since ${formatCentral(summary.oldest_started_at)}` : "" },
        { label: "Success rate", value: formatRate(rate), tone: rateTone, note: `${summary.failed} failed${summary.stuck > 0 ? ` · ${summary.stuck} stuck` : ""}` },
        { label: "Median duration", value: formatDuration(summary.median_duration_seconds), note: summary.max_duration_seconds != null ? `max ${formatDuration(summary.max_duration_seconds)}` : "" },
        {
          label: "Last success",
          value: summary.last_success_at ? relativeTime(summary.last_success_at, now) : "never",
          tone: summary.last_success_at ? undefined : "down",
          note: summary.last_success_at ? formatCentral(summary.last_success_at) : "",
        },
      ]}
    />
  )
}

function RunsTable({ runs, now }: { runs: PipelineRun[]; now: number }) {
  return (
    <Block title="Runs" count={runs.length} note="newest first · Central time">
      {runs.length === 0 ? (
        <p className={s.empty}>No runs recorded.</p>
      ) : (
        <div className={s.bleed}>
          <table className={s.table} style={{ minWidth: "44rem" }}>
            <thead>
              <tr>
                <th scope="col">Started</th>
                <th scope="col">Status</th>
                <th scope="col" className={s.num}>Duration</th>
                <th scope="col" className={s.num}>Records</th>
                <th scope="col">Error</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((run) => (
                <tr key={run.id}>
                  <td className={`${s.mono} ${s.nowrap} ${s.top}`}>
                    <span className={s.strong}>{formatCentralLong(run.started_at)}</span>
                    <span className={s.under}>{relativeTime(run.started_at, now)}</span>
                  </td>
                  {/* Stuck is the Overview's word and chip for the same state (lib/pipelines.ts). */}
                  <td className={s.top}>{run.status === "stuck" ? <StateBadge state="stuck" /> : <StatusBadge status={run.status} />}</td>
                  <td className={`${s.num} ${s.top}`}>{run.status === "running" ? "…" : formatDuration(run.duration_seconds)}</td>
                  <td className={`${s.num} ${s.top}`}>{run.status === "success" ? run.records_processed.toLocaleString() : "—"}</td>
                  <td className={`${s.top} ${s.errorCell}`}>
                    <ErrorCell message={run.error_message} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Block>
  )
}

/** Short errors inline; long ones fold, with the full text one click away. */
function ErrorCell({ message }: { message: string | null }) {
  if (!message) return <span className={s.muted}>—</span>
  if (message.length <= 120) return <span className={s.bad}>{message}</span>
  return (
    <details className={s.fold}>
      <summary className={s.bad}>
        {message.slice(0, 120)}… <span className={s.foldHint}>(show all)</span>
      </summary>
      <pre className={`${s.pre} ${s.preWrap} ${s.errorFull}`}>
        {message}
      </pre>
    </details>
  )
}
