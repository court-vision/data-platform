import { FlaskConical, Loader2, TriangleAlert } from "lucide-react"
import { Link, useSearchParams } from "react-router"

import { CheckDefinition } from "@/components/CheckDefinition"
import { QualityMatrix, QualityMatrixKey } from "@/components/QualityMatrix"
import { RefreshNote } from "@/components/RefreshNote"
import { StatusBadge } from "@/components/StateBadge"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"
import { useNow } from "@/hooks/useNow"
import { QUALITY_REFETCH_MS, useQualityOverview } from "@/hooks/useQuality"
import { useRunQualityChecks } from "@/hooks/useRunQualityChecks"
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
import { cn } from "@/lib/utils"

export function Quality() {
  const [params, setParams] = useSearchParams()
  const limit = parseQualityLimit(params.get("limit"))
  const now = useNow()
  const query = useQualityOverview(limit)
  const run = useRunQualityChecks()
  const data = query.data

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-6 p-4 md:p-8">
      <header className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="font-display text-2xl font-bold tracking-tight">Quality</h1>
          <p className="text-sm text-muted-foreground">
            What every check asserts, and how each has fared in the newest runs.
          </p>
        </div>
        <div className="flex flex-col items-end gap-2">
          <Button variant="outline" size="sm" disabled={run.isPending} onClick={() => run.mutate()} className="gap-1.5">
            {run.isPending ? <Loader2 className="size-3.5 animate-spin" aria-hidden /> : <FlaskConical className="size-3.5" aria-hidden />}
            {run.isPending ? "Running checks" : "Run checks"}
          </Button>
          <RefreshNote updatedAt={query.dataUpdatedAt} fetching={query.isFetching} now={now} intervalMs={QUALITY_REFETCH_MS} />
        </div>
      </header>

      {query.error && (
        <div role="alert" className="flex items-start gap-2 rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          <TriangleAlert className="mt-0.5 size-4 shrink-0" aria-hidden />
          <span>Could not refresh: {query.error.message}{data && " Showing the last good data."}</span>
        </div>
      )}

      {!data ? (
        <LoadingState />
      ) : (
        <>
          <LatestTiles data={data} now={now} />

          <Card>
            <CardHeader className="flex-row flex-wrap items-start justify-between gap-3 space-y-0 pb-3">
              <div className="space-y-1.5">
                <CardTitle className="text-base">Checks by run</CardTitle>
                <CardDescription>
                  {data.runs.length === 0
                    ? "No runs yet."
                    : `The last ${data.runs.length} ${data.runs.length === 1 ? "run" : "runs"}, oldest on the left. A cell opens that run.`}
                </CardDescription>
              </div>
              <LimitButtons limit={limit} onChange={(size) => setParams(size === 20 ? {} : { limit: String(size) })} />
            </CardHeader>
            <CardContent className="flex flex-col gap-4">
              {data.runs.length === 0 ? (
                <p className="py-4 text-center text-sm text-muted-foreground">
                  No quality runs yet. Run the checks to start a history.
                </p>
              ) : (
                <>
                  <QualityMatrix checks={data.checks} runs={data.runs} limit={data.limit} />
                  <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2">
                    <p className="font-mono text-xs text-muted-foreground">
                      {formatCentral(data.runs[data.runs.length - 1].started_at)} → {formatCentral(data.runs[0].started_at)}
                    </p>
                    <QualityMatrixKey />
                  </div>
                </>
              )}
            </CardContent>
          </Card>

          <RunsTable runs={data.runs} now={now} />
          <Definitions data={data} />
        </>
      )}
    </div>
  )
}

function LimitButtons({ limit, onChange }: { limit: QualityLimit; onChange: (size: QualityLimit) => void }) {
  return (
    <div className="flex gap-1" role="group" aria-label="How many runs">
      {QUALITY_LIMITS.map((size) => (
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

function LatestTiles({ data, now }: { data: QualityOverview; now: number }) {
  const latest: QualityRun | undefined = data.runs[0]
  const summary = summarizeLatest(data.checks)
  const tiles = [
    {
      label: "Latest run",
      value: latest ? relativeTime(latest.started_at, now) : "never",
      tone: !latest ? "text-muted-foreground" : latest.status === "success" ? "text-status-win" : latest.status === "running" ? "text-signal-live" : "text-status-loss",
      note: latest ? `${latest.status} · ${latest.triggered_by ?? "unknown"}` : "",
    },
    { label: "Passing", value: latest ? `${summary.passed}/${summary.total}` : "—", tone: "text-foreground", note: "checks in the latest run" },
    {
      label: "Critical failing",
      value: latest ? String(summary.criticalFailing) : "—",
      tone: summary.criticalFailing > 0 ? "text-status-loss" : "text-muted-foreground",
      note: "each one alerts",
    },
    {
      label: "Warnings failing",
      value: latest ? String(summary.warningFailing) : "—",
      tone: summary.warningFailing > 0 ? "text-status-projected" : "text-muted-foreground",
      note: "timing checks fail on off-days",
    },
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

function RunsTable({ runs, now }: { runs: QualityRun[]; now: number }) {
  if (runs.length === 0) return null
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-base">Runs</CardTitle>
        <CardDescription>Newest first. Times in Central. Open one for every check's outcome.</CardDescription>
      </CardHeader>
      <CardContent className="overflow-x-auto px-0 pb-2">
        <table className="w-full min-w-[40rem] text-sm">
          <thead>
            <tr className="border-b text-left text-xs uppercase tracking-wider text-muted-foreground">
              <th scope="col" className="px-6 py-2 font-medium">Started</th>
              <th scope="col" className="px-3 py-2 font-medium">Status</th>
              <th scope="col" className="px-3 py-2 text-right font-medium">Passed</th>
              <th scope="col" className="px-3 py-2 text-right font-medium">Failed</th>
              <th scope="col" className="px-3 py-2 text-right font-medium">Duration</th>
              <th scope="col" className="px-6 py-2 font-medium">Triggered by</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((entry) => (
              <tr key={entry.run_id} className="border-b border-border/50 last:border-0">
                <td className="whitespace-nowrap px-6 py-2.5 font-mono text-xs">
                  <Link to={`/quality/runs/${entry.run_id}`} className="hover:text-primary hover:underline underline-offset-4">
                    {formatCentralLong(entry.started_at)}
                  </Link>
                  <span className="block text-muted-foreground">{relativeTime(entry.started_at, now)}</span>
                </td>
                <td className="px-3 py-2.5"><StatusBadge status={entry.status} /></td>
                <td className="px-3 py-2.5 text-right font-mono text-xs tabular-nums">{entry.passed_checks}/{entry.total_checks}</td>
                <td className={cn("px-3 py-2.5 text-right font-mono text-xs tabular-nums", entry.failed_checks > 0 && "text-status-loss")}>
                  {entry.failed_checks}
                </td>
                <td className="px-3 py-2.5 text-right font-mono text-xs tabular-nums">{formatDuration(entry.duration_seconds)}</td>
                <td className="px-6 py-2.5 text-xs text-muted-foreground">{entry.triggered_by ?? "unknown"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </CardContent>
    </Card>
  )
}

/** The catalogue: every check, what it guards, and the SQL that decides it. */
function Definitions({ data }: { data: QualityOverview }) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-baseline gap-2 text-base">
          What each check asserts
          <span className="font-mono text-xs font-normal text-muted-foreground">{data.checks.length}</span>
        </CardTitle>
        <CardDescription>Each is one query that counts offending rows. Zero passes.</CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-5">
        {groupChecks(data.checks).map((group) => (
          <section key={group.key} aria-label={`${group.label} check definitions`}>
            <h3 className="text-xs uppercase tracking-wider text-muted-foreground">{group.label}</h3>
            {group.description && <p className="mb-1 text-xs text-muted-foreground">{group.description}.</p>}
            <ul className="divide-y divide-border/50">
              {group.checks.map((check) => (
                <li key={check.name}>
                  <details className="group py-2">
                    <summary className="flex cursor-pointer flex-wrap items-center gap-x-2 gap-y-1 text-xs">
                      <span className="break-all font-mono font-medium">{check.name}</span>
                      <Badge variant={check.severity === "critical" ? "loss" : "neutral"}>{check.severity}</Badge>
                      <span className="font-mono text-muted-foreground">{check.table}</span>
                    </summary>
                    <div className="mt-2 pl-4">
                      <CheckDefinition definition={check} />
                    </div>
                  </details>
                </li>
              ))}
            </ul>
          </section>
        ))}
      </CardContent>
    </Card>
  )
}

function LoadingState() {
  return (
    <div className="flex flex-col gap-6" aria-busy="true" aria-label="Loading quality">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        {Array.from({ length: 4 }, (_, i) => <Skeleton key={i} className="h-[4.5rem] rounded-xl" />)}
      </div>
      <Skeleton className="h-96 rounded-xl" />
      <Skeleton className="h-64 rounded-xl" />
    </div>
  )
}
