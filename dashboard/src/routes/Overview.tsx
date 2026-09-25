import { TriangleAlert } from "lucide-react"

import { JobsTable } from "@/components/JobsTable"
import { PipelineSection } from "@/components/PipelineSection"
import { QualityPanel } from "@/components/QualityPanel"
import { RefreshNote } from "@/components/RefreshNote"
import { SchedulerTimeline } from "@/components/SchedulerTimeline"
import { ServiceCards } from "@/components/ServiceCards"
import { Card } from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"
import { STATUS_REFETCH_MS, useDashboardStatus, type PipelineHealth } from "@/hooks/useDashboardStatus"
import { useNow } from "@/hooks/useNow"
import { groupByCategory, summarize } from "@/lib/pipelines"
import { cn } from "@/lib/utils"

export function Overview() {
  const status = useDashboardStatus()
  const now = useNow()
  const data = status.data

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-6 p-4 md:p-8">
      <header className="flex flex-wrap items-end justify-between gap-2">
        <div>
          <h1 className="font-display text-2xl font-bold tracking-tight">Overview</h1>
          <p className="text-sm text-muted-foreground">Every pipeline, the scheduler, data quality and what is deployed.</p>
        </div>
        <RefreshNote updatedAt={status.dataUpdatedAt} fetching={status.isFetching} now={now} intervalMs={STATUS_REFETCH_MS} />
      </header>

      {status.error && (
        <div
          role="alert"
          className="flex items-start gap-2 rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-sm text-destructive"
        >
          <TriangleAlert className="mt-0.5 size-4 shrink-0" aria-hidden />
          <span>
            Could not refresh: {status.error.message}
            {data && " Showing the last good data."}
          </span>
        </div>
      )}

      {!data ? (
        <LoadingState />
      ) : (
        <>
          <SummaryTiles pipelines={data.pipelines} />
          <ServiceCards />
          {groupByCategory(data.pipelines).map((group) => (
            <PipelineSection key={group.label} group={group} now={now} />
          ))}
          <SchedulerTimeline runs={data.cron_job_runs} now={now} />
          <QualityPanel
            quality_latest={data.quality_latest}
            recent_quality_runs={data.recent_quality_runs}
            quality_failed_checks={data.quality_failed_checks}
            now={now}
          />
          <JobsTable jobs={data.recent_jobs} now={now} />
        </>
      )}
    </div>
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

function LoadingState() {
  return (
    <div className="flex flex-col gap-6" aria-busy="true" aria-label="Loading pipeline status">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        {Array.from({ length: 4 }, (_, i) => (
          <Skeleton key={i} className="h-[4.5rem] rounded-xl" />
        ))}
      </div>
      <Skeleton className="h-64 rounded-xl" />
      <Skeleton className="h-40 rounded-xl" />
    </div>
  )
}
