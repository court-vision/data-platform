import { ArrowLeft, TriangleAlert } from "lucide-react"
import { Link, useParams, useSearchParams } from "react-router"

import { DurationChart } from "@/components/DurationChart"
import { RefreshNote } from "@/components/RefreshNote"
import { RunPipelineButton } from "@/components/RunPipelineButton"
import { StateBadge, StatusBadge } from "@/components/StateBadge"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"
import { useNow } from "@/hooks/useNow"
import { RUNS_REFETCH_MS, usePipelineRuns } from "@/hooks/usePipelineRuns"
import { ApiError } from "@/lib/api"
import { CATEGORIES } from "@/lib/pipelines"
import { formatRate, LIMITS, parseLimit, type Limit, type PipelineInfo, type PipelineRun, type RunsSummary } from "@/lib/runs"
import { formatCentral, formatCentralLong, formatDuration, relativeTime } from "@/lib/time"
import { cn } from "@/lib/utils"

export function PipelineDetail() {
  const { name = "" } = useParams()
  const [params, setParams] = useSearchParams()
  const limit = parseLimit(params.get("limit"))
  const now = useNow()
  const query = usePipelineRuns(name, limit)
  const data = query.data
  const limitButtons = (
    <LimitButtons limit={limit} onChange={(size) => setParams(size === 50 ? {} : { limit: String(size) })} />
  )

  if (query.error instanceof ApiError && query.error.status === 404) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-2 p-8 text-center">
        <p className="font-mono text-sm text-muted-foreground">404</p>
        <h1 className="font-display text-xl font-bold">No pipeline called {name}</h1>
        <Link to="/" className="text-sm text-primary underline-offset-4 hover:underline">Back to the overview</Link>
      </div>
    )
  }

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-6 p-4 md:p-8">
      <Link to="/" className="flex w-fit items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground">
        <ArrowLeft className="size-4" aria-hidden /> Overview
      </Link>

      <header className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          {data ? (
            <>
              <div className="flex flex-wrap items-center gap-2">
                <h1 className="font-display text-2xl font-bold tracking-tight">{data.pipeline.display_name}</h1>
                <Badge variant="outline">{categoryLabel(data.pipeline.category)}</Badge>
                {data.pipeline.is_running && <StatusBadge status="running" />}
              </div>
              <p className="font-mono text-xs text-muted-foreground">{data.pipeline.name}</p>
              <p className="mt-2 max-w-prose text-sm text-muted-foreground">{data.pipeline.description}</p>
            </>
          ) : (
            <Skeleton className="h-16 w-80 rounded" />
          )}
        </div>
        <div className="flex flex-col items-end gap-2">
          {data && <RunPipelineButton pipeline={data.pipeline} />}
          <RefreshNote updatedAt={query.dataUpdatedAt} fetching={query.isFetching} now={now} intervalMs={RUNS_REFETCH_MS} />
        </div>
      </header>

      {query.error && (
        <div role="alert" className="flex items-start gap-2 rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          <TriangleAlert className="mt-0.5 size-4 shrink-0" aria-hidden />
          <span>Could not refresh: {query.error.message}{data && " Showing the last good data."}</span>
        </div>
      )}

      {!data ? (
        <>
          {/* This limit's fetch failed with nothing to stand in for it. Another
              limit may still be cached, and these are the way back to it. */}
          {query.error && <div className="flex justify-end">{limitButtons}</div>}
          <LoadingState />
        </>
      ) : (
        <>
          <Facts pipeline={data.pipeline} />
          <SummaryTiles summary={data.summary} now={now} />

          <Card>
            <CardHeader className="flex-row flex-wrap items-start justify-between gap-3 space-y-0 pb-3">
              <div className="space-y-1.5">
                <CardTitle className="text-base">Duration</CardTitle>
                <CardDescription>The last {data.runs.length} runs, oldest on the left.</CardDescription>
              </div>
              {limitButtons}
            </CardHeader>
            <CardContent>
              <DurationChart runs={data.runs} now={now} />
            </CardContent>
          </Card>

          <RunsTable runs={data.runs} now={now} />
        </>
      )}
    </div>
  )
}

function LimitButtons({ limit, onChange }: { limit: Limit; onChange: (size: Limit) => void }) {
  return (
    <div className="flex gap-1" role="group" aria-label="How many runs">
      {LIMITS.map((size) => (
        <Button
          key={size}
          size="sm"
          variant={size === limit ? "secondary" : "ghost"}
          className="h-7 px-2 font-mono text-xs"
          onClick={() => onChange(size)}
          aria-pressed={size === limit}
        >
          {size}
        </Button>
      ))}
    </div>
  )
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
    <Card variant="panel">
      <CardContent className="pt-4">
        <dl className="grid gap-x-6 gap-y-2 text-sm sm:grid-cols-2 lg:grid-cols-3">
          {rows.map(([label, value]) => (
            <div key={label} className="min-w-0">
              <dt className="text-xs uppercase tracking-wider text-muted-foreground">{label}</dt>
              <dd className="break-words font-mono text-xs">{value}</dd>
            </div>
          ))}
        </dl>
      </CardContent>
    </Card>
  )
}

function SummaryTiles({ summary, now }: { summary: RunsSummary; now: number }) {
  const rate = summary.success_rate
  const tiles = [
    { label: "Runs", value: String(summary.total), tone: "text-foreground", note: summary.oldest_started_at ? `since ${formatCentral(summary.oldest_started_at)}` : "" },
    { label: "Success rate", value: formatRate(rate), tone: rate == null ? "text-muted-foreground" : rate >= 0.9 ? "text-status-win" : rate >= 0.5 ? "text-status-projected" : "text-status-loss", note: `${summary.failed} failed${summary.stuck > 0 ? ` · ${summary.stuck} stuck` : ""}` },
    { label: "Median duration", value: formatDuration(summary.median_duration_seconds), tone: "text-foreground", note: summary.max_duration_seconds != null ? `max ${formatDuration(summary.max_duration_seconds)}` : "" },
    { label: "Last success", value: summary.last_success_at ? relativeTime(summary.last_success_at, now) : "never", tone: summary.last_success_at ? "text-foreground" : "text-status-loss", note: summary.last_success_at ? formatCentral(summary.last_success_at) : "" },
  ]
  return (
    <dl className="grid grid-cols-2 gap-3 md:grid-cols-4">
      {tiles.map((tile) => (
        <Card key={tile.label} variant="panel" className="px-4 py-3">
          <dt className="text-xs uppercase tracking-wider text-muted-foreground">{tile.label}</dt>
          <dd className={cn("font-mono text-2xl font-bold", tile.tone)}>{tile.value}</dd>
          {tile.note && <dd className="font-mono text-[11px] text-muted-foreground">{tile.note}</dd>}
        </Card>
      ))}
    </dl>
  )
}

function RunsTable({ runs, now }: { runs: PipelineRun[]; now: number }) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-base">Runs</CardTitle>
        <CardDescription>Newest first. Times in Central.</CardDescription>
      </CardHeader>
      <CardContent className="overflow-x-auto px-0 pb-2">
        {runs.length === 0 ? (
          <p className="py-4 text-center text-sm text-muted-foreground">No runs recorded.</p>
        ) : (
          <table className="w-full min-w-[44rem] text-sm">
            <thead>
              <tr className="border-b text-left text-xs uppercase tracking-wider text-muted-foreground">
                <th scope="col" className="px-6 py-2 font-medium">Started</th>
                <th scope="col" className="px-3 py-2 font-medium">Status</th>
                <th scope="col" className="px-3 py-2 text-right font-medium">Duration</th>
                <th scope="col" className="px-3 py-2 text-right font-medium">Records</th>
                <th scope="col" className="px-6 py-2 font-medium">Error</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((run) => (
                <tr key={run.id} className="border-b border-border/50 align-top last:border-0">
                  <td className="whitespace-nowrap px-6 py-2.5 font-mono text-xs">
                    {formatCentralLong(run.started_at)}
                    <span className="block text-muted-foreground">{relativeTime(run.started_at, now)}</span>
                  </td>
                  {/* Stuck is the Overview's word and badge for the same state (lib/pipelines.ts). */}
                  <td className="px-3 py-2.5">
                    {run.status === "stuck" ? <StateBadge state="stuck" /> : <StatusBadge status={run.status} />}
                  </td>
                  <td className="px-3 py-2.5 text-right font-mono text-xs tabular-nums">
                    {run.status === "running" ? "…" : formatDuration(run.duration_seconds)}
                  </td>
                  <td className="px-3 py-2.5 text-right font-mono text-xs tabular-nums">
                    {run.status === "success" ? run.records_processed.toLocaleString() : "—"}
                  </td>
                  <td className="max-w-md px-6 py-2.5 text-xs">
                    <ErrorCell message={run.error_message} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </CardContent>
    </Card>
  )
}

/** Short errors inline; long ones fold, with the full text one click away. */
function ErrorCell({ message }: { message: string | null }) {
  if (!message) return <span className="text-muted-foreground">—</span>
  if (message.length <= 120) return <span className="break-words text-status-loss">{message}</span>
  return (
    <details className="group">
      <summary className="cursor-pointer text-status-loss">
        {message.slice(0, 120)}… <span className="text-muted-foreground group-open:hidden">(show all)</span>
      </summary>
      <pre className="mt-1 max-h-64 overflow-auto whitespace-pre-wrap break-words rounded bg-muted/60 p-2 font-mono text-[11px] text-muted-foreground">
        {message}
      </pre>
    </details>
  )
}

function LoadingState() {
  return (
    <div className="flex flex-col gap-6" aria-busy="true" aria-label="Loading pipeline">
      <Skeleton className="h-24 rounded-xl" />
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        {Array.from({ length: 4 }, (_, i) => <Skeleton key={i} className="h-[4.5rem] rounded-xl" />)}
      </div>
      <Skeleton className="h-52 rounded-xl" />
      <Skeleton className="h-64 rounded-xl" />
    </div>
  )
}
