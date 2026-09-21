import { TriangleAlert } from "lucide-react"

import { StateBadge } from "@/components/StateBadge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"
import { STATUS_REFETCH_MS, useDashboardStatus, type PipelineHealth } from "@/hooks/useDashboardStatus"
import { useNow } from "@/hooks/useNow"
import { groupByCategory, pipelineState, summarize, type CategoryGroup } from "@/lib/pipelines"
import { formatCentral, formatDuration, relativeTime } from "@/lib/time"
import { cn } from "@/lib/utils"

export function Overview() {
  const status = useDashboardStatus()
  const now = useNow()
  const pipelines = status.data?.pipelines ?? []

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-6 p-4 md:p-8">
      <header className="flex flex-wrap items-end justify-between gap-2">
        <div>
          <h1 className="font-display text-2xl font-bold tracking-tight">Overview</h1>
          <p className="text-sm text-muted-foreground">Every registered pipeline and how its last run went.</p>
        </div>
        <RefreshNote updatedAt={status.dataUpdatedAt} fetching={status.isFetching} now={now} />
      </header>

      {status.error && (
        <div
          role="alert"
          className="flex items-start gap-2 rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-sm text-destructive"
        >
          <TriangleAlert className="mt-0.5 size-4 shrink-0" aria-hidden />
          <span>
            Could not refresh: {status.error.message}
            {status.data && " Showing the last good data."}
          </span>
        </div>
      )}

      {status.isPending ? (
        <LoadingState />
      ) : (
        <>
          <SummaryTiles pipelines={pipelines} />
          {groupByCategory(pipelines).map((group) => (
            <CategorySection key={group.label} group={group} now={now} />
          ))}
        </>
      )}
    </div>
  )
}

function RefreshNote({ updatedAt, fetching, now }: { updatedAt: number; fetching: boolean; now: number }) {
  if (!updatedAt) return null
  const next = Math.max(0, Math.ceil((updatedAt + STATUS_REFETCH_MS - now) / 1000))
  return (
    <p className="font-mono text-xs text-muted-foreground" aria-live="off">
      updated {relativeTime(new Date(updatedAt).toISOString(), now)}
      <span className="mx-1.5 text-border">·</span>
      {fetching ? "refreshing…" : `next in ${next}s`}
    </p>
  )
}

function SummaryTiles({ pipelines }: { pipelines: PipelineHealth[] }) {
  const summary = summarize(pipelines)
  const tiles = [
    { label: "Pipelines", value: summary.total, tone: "text-foreground" },
    { label: "Healthy", value: summary.healthy, tone: "text-status-win" },
    { label: "Failing", value: summary.failing, tone: summary.failing > 0 ? "text-status-loss" : "text-muted-foreground" },
    { label: "Running", value: summary.running, tone: summary.running > 0 ? "text-signal-live" : "text-muted-foreground" },
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

function CategorySection({ group, now }: { group: CategoryGroup; now: number }) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-baseline gap-2 text-base">
          {group.label}
          <span className="font-mono text-xs font-normal text-muted-foreground">{group.pipelines.length}</span>
        </CardTitle>
        <CardDescription>{group.description}</CardDescription>
      </CardHeader>
      <CardContent className="overflow-x-auto px-0 pb-2">
        {/* Fixed widths: every category's columns line up with the others'. */}
        <table className="w-full min-w-[36rem] table-fixed text-sm">
          <colgroup>
            <col className="w-[38%]" />
            <col className="w-[20%]" />
            <col className="w-[16%]" />
            <col className="w-[13%]" />
            <col className="w-[13%]" />
          </colgroup>
          <thead>
            <tr className="border-b text-left text-xs uppercase tracking-wider text-muted-foreground">
              <th scope="col" className="px-6 py-2 font-medium">Pipeline</th>
              <th scope="col" className="px-3 py-2 font-medium">State</th>
              <th scope="col" className="px-3 py-2 font-medium">Last run</th>
              <th scope="col" className="px-3 py-2 text-right font-medium">Duration</th>
              <th scope="col" className="px-6 py-2 text-right font-medium">Records</th>
            </tr>
          </thead>
          <tbody>
            {group.pipelines.map((pipeline) => (
              <tr key={pipeline.name} className="border-b border-border/50 last:border-0">
                <th scope="row" className="px-6 py-2.5 text-left font-medium">
                  {pipeline.display_name}
                  <span className="block font-mono text-xs font-normal text-muted-foreground">{pipeline.name}</span>
                </th>
                <td className="px-3 py-2.5">
                  <StateBadge state={pipelineState(pipeline)} />
                  {pipeline.error_streak > 1 && (
                    <span className="ml-2 font-mono text-xs text-status-loss">×{pipeline.error_streak}</span>
                  )}
                </td>
                <td className="px-3 py-2.5 font-mono text-xs" title={formatCentral(pipeline.last_run_at)}>
                  {relativeTime(pipeline.last_run_at, now)}
                </td>
                <td className="px-3 py-2.5 text-right font-mono text-xs tabular-nums">
                  {formatDuration(pipeline.last_duration_seconds)}
                </td>
                <td className="px-6 py-2.5 text-right font-mono text-xs tabular-nums">
                  {pipeline.last_records_processed?.toLocaleString() ?? "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </CardContent>
    </Card>
  )
}

function LoadingState() {
  return (
    <div className="flex flex-col gap-6" aria-busy="true" aria-label="Loading pipeline status">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        {Array.from({ length: 4 }, (_, i) => (
          <Skeleton key={i} className="h-[4.5rem] rounded-xl" />
        ))}
      </div>
      <Skeleton className="h-64 rounded-xl" />
    </div>
  )
}
