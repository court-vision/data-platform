import { ArrowLeft, ChevronLeft, ChevronRight, TriangleAlert } from "lucide-react"
import { Link, useParams } from "react-router"

import { CheckDefinition } from "@/components/CheckDefinition"
import { StatusBadge } from "@/components/StateBadge"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"
import { useNow } from "@/hooks/useNow"
import { useQualityRun } from "@/hooks/useQuality"
import { ApiError } from "@/lib/api"
import { countOutcomes, extraDetails, sampleCell, sampleRows, type QualityOutcome, type QualityRunDetail, type Sample } from "@/lib/quality"
import { formatCentralLong, formatDuration, relativeTime } from "@/lib/time"
import { cn } from "@/lib/utils"

const ROW = "grid grid-cols-[minmax(0,1fr)_5.5rem_5rem_4.5rem_4.5rem] items-center gap-3"

export function QualityRun() {
  const { runId = "" } = useParams()
  const now = useNow()
  const query = useQualityRun(runId)
  const data = query.data

  if (query.error instanceof ApiError && query.error.status === 404) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-2 p-8 text-center">
        <p className="font-mono text-sm text-muted-foreground">404</p>
        <h1 className="font-display text-xl font-bold">No quality run with that id</h1>
        <Link to="/quality" className="text-sm text-primary underline-offset-4 hover:underline">Back to quality</Link>
      </div>
    )
  }

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-6 p-4 md:p-8">
      <Link to="/quality" className="flex w-fit items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground">
        <ArrowLeft className="size-4" aria-hidden /> Quality
      </Link>

      {query.error && (
        <div role="alert" className="flex items-start gap-2 rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          <TriangleAlert className="mt-0.5 size-4 shrink-0" aria-hidden />
          <span>Could not load: {query.error.message}{data && " Showing the last good data."}</span>
        </div>
      )}

      {!data ? (
        <div className="flex flex-col gap-6" aria-busy="true" aria-label="Loading quality run">
          <Skeleton className="h-16 w-96 max-w-full rounded" />
          <Skeleton className="h-96 rounded-xl" />
        </div>
      ) : (
        <>
          <Header data={data} now={now} />
          <Tiles outcomes={data.checks} />
          <Outcomes outcomes={data.checks} />
        </>
      )}
    </div>
  )
}

function Header({ data, now }: { data: QualityRunDetail; now: number }) {
  const { run } = data
  return (
    <header className="flex flex-wrap items-start justify-between gap-4">
      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="font-display text-2xl font-bold tracking-tight">Quality run</h1>
          <StatusBadge status={run.status} />
        </div>
        <p className="mt-1 font-mono text-xs text-muted-foreground">
          {formatCentralLong(run.started_at)}
          <span className="mx-1.5 text-border">·</span>
          {relativeTime(run.started_at, now)}
          <span className="mx-1.5 text-border">·</span>
          {run.status === "running" ? "still running" : `took ${formatDuration(run.duration_seconds)}`}
          <span className="mx-1.5 text-border">·</span>
          triggered by {run.triggered_by ?? "unknown"}
        </p>
        <p className="break-all font-mono text-[11px] text-muted-foreground/70">{run.run_id}</p>
        {run.error_message && <p className="mt-2 text-sm text-status-loss">The run itself failed: {run.error_message}</p>}
      </div>
      <nav className="flex items-center gap-1 text-sm" aria-label="Neighbouring runs">
        <Step to={data.older_run_id} label="Older">
          <ChevronLeft className="size-4" aria-hidden /> Older
        </Step>
        <Step to={data.newer_run_id} label="Newer">
          Newer <ChevronRight className="size-4" aria-hidden />
        </Step>
      </nav>
    </header>
  )
}

function Step({ to, label, children }: { to: string | null; label: string; children: React.ReactNode }) {
  const className = "flex items-center gap-1 rounded-md px-2 py-1"
  if (!to) {
    return (
      <span className={cn(className, "text-muted-foreground/40")} aria-disabled="true" aria-label={`${label} run: none`}>
        {children}
      </span>
    )
  }
  return (
    <Link to={`/quality/runs/${to}`} className={cn(className, "text-muted-foreground hover:bg-accent hover:text-foreground")}>
      {children}
    </Link>
  )
}

function Tiles({ outcomes }: { outcomes: QualityOutcome[] }) {
  const counts = countOutcomes(outcomes)
  const tiles = [
    { label: "Checks", value: counts.total, tone: "text-foreground" },
    { label: "Passed", value: counts.passed, tone: "text-status-win" },
    { label: "Critical failing", value: counts.criticalFailing, tone: counts.criticalFailing > 0 ? "text-status-loss" : "text-muted-foreground" },
    { label: "Warnings failing", value: counts.warningFailing, tone: counts.warningFailing > 0 ? "text-status-projected" : "text-muted-foreground" },
  ]
  return (
    <dl className="grid grid-cols-2 gap-3 md:grid-cols-4">
      {tiles.map((tile) => (
        <Card key={tile.label} variant="panel" className="px-4 py-3">
          <dt className="text-xs uppercase tracking-wider text-muted-foreground">{tile.label}</dt>
          <dd className={cn("font-mono text-3xl font-bold tabular-nums", tile.tone)}>{tile.value}</dd>
        </Card>
      ))}
    </dl>
  )
}

/** Every check in the run, what needs a look first. A failing row starts open. */
function Outcomes({ outcomes }: { outcomes: QualityOutcome[] }) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-base">Checks</CardTitle>
        <CardDescription>Failures first. Open a check for what it asserts and the query behind it.</CardDescription>
      </CardHeader>
      <CardContent className="overflow-x-auto px-0 pb-2">
        {outcomes.length === 0 ? (
          <p className="py-4 text-center text-sm text-muted-foreground">This run recorded no checks.</p>
        ) : (
          <div className="min-w-[44rem]">
            <div className={cn(ROW, "border-b px-6 py-2 text-xs uppercase tracking-wider text-muted-foreground")} aria-hidden>
              <span>Check</span>
              <span>Status</span>
              <span>Severity</span>
              <span className="text-right">Failures</span>
              <span className="text-right">Took</span>
            </div>
            <ul>
              {outcomes.map((outcome) => (
                <Outcome key={outcome.check_name} outcome={outcome} />
              ))}
            </ul>
          </div>
        )}
      </CardContent>
    </Card>
  )
}

function Outcome({ outcome }: { outcome: QualityOutcome }) {
  const failing = outcome.status !== "passed"
  const extra = extraDetails(outcome)
  const sample = sampleRows(outcome)
  return (
    <li className="border-b border-border/50 last:border-0" data-check={outcome.check_name} data-status={outcome.status}>
      <details open={failing} className="group">
        <summary className={cn(ROW, "cursor-pointer px-6 py-2.5 text-sm hover:bg-accent/40")}>
          <span className="min-w-0 break-all font-mono text-xs font-medium">{outcome.check_name}</span>
          <span><StatusBadge status={outcome.status} /></span>
          <span><Badge variant={outcome.severity === "critical" ? "outline" : "neutral"}>{outcome.severity}</Badge></span>
          <span className={cn("text-right font-mono text-xs tabular-nums", failing && "text-status-loss")}>
            {failing ? outcome.failures.toLocaleString() : "—"}
          </span>
          <span className="text-right font-mono text-xs tabular-nums text-muted-foreground">
            {outcome.duration_ms == null ? "—" : formatDuration(outcome.duration_ms / 1000)}
          </span>
        </summary>
        <div className="flex flex-col gap-3 px-6 pb-4 pl-10">
          {outcome.message && <p className="text-sm text-status-loss">{outcome.message}</p>}
          {sample && <OffendingRows sample={sample} failures={outcome.failures} />}
          {extra && (
            <dl className="rounded bg-muted/60 p-3 font-mono text-[11px] text-muted-foreground">
              {Object.entries(extra).map(([key, value]) => (
                <div key={key} className="flex gap-2">
                  <dt className="shrink-0">{key}</dt>
                  {/* A string is shown as it is: through JSON its quotes would come out escaped. */}
                  <dd className="whitespace-pre-wrap break-words">{typeof value === "string" ? value : JSON.stringify(value)}</dd>
                </div>
              ))}
            </dl>
          )}
          {outcome.definition ? (
            <CheckDefinition definition={outcome.definition} />
          ) : (
            <p className="text-xs text-muted-foreground">
              This check is no longer defined in the code, so only its recorded result remains.
            </p>
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
    <figure className="flex flex-col gap-1">
      <figcaption className="text-xs text-muted-foreground">
        {shown < failures ? `The first ${shown} of ${failures.toLocaleString()} offending rows` : shown === 1 ? "The offending row" : `All ${shown} offending rows`}
      </figcaption>
      <div className="overflow-x-auto rounded border border-border/60">
        <table className="w-full text-left font-mono text-[11px]">
          <thead className="bg-muted/60 text-muted-foreground">
            <tr>
              {sample.columns.map((column) => (
                <th key={column} scope="col" className="whitespace-nowrap px-2.5 py-1.5 font-normal">{column}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sample.rows.map((row, i) => (
              <tr key={i} className="border-t border-border/50">
                {sample.columns.map((column) => (
                  <td key={column} className="whitespace-nowrap px-2.5 py-1.5 tabular-nums">{sampleCell(row[column])}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </figure>
  )
}
