import { useSearchParams } from "react-router"

import { Head, Notice, Page, RefreshNote, SheetSkeleton, Tiles } from "@/components/blocks"
import { JobsTable } from "@/components/JobsTable"
import { PipelineSection } from "@/components/PipelineSection"
import { QualityPanel } from "@/components/QualityPanel"
import { SchedulerTimeline } from "@/components/SchedulerTimeline"
import { ServiceCards } from "@/components/ServiceCards"
import { usePageContext } from "@/components/shell/pageContext"
import { STATUS_REFETCH_MS, useDashboardStatus, type PipelineHealth } from "@/hooks/useDashboardStatus"
import { useNow } from "@/hooks/useNow"
import { schedulerWindow, useSchedulerRuns } from "@/hooks/useSchedulerRuns"
import { groupByCategory, summarize } from "@/lib/pipelines"
import { DEFAULT_RANGE, parseRange } from "@/lib/timeline"

export function Overview() {
  usePageContext("Overview")
  const status = useDashboardStatus()
  const now = useNow()
  const data = status.data
  // The scheduler's range lives in the URL. Six hours rides on the status
  // payload; a longer look is its own, slower query.
  const [params, setParams] = useSearchParams()
  const range = parseRange(params.get("scheduler"))
  const longer = useSchedulerRuns(range.hours)
  const defaultRange = range.key === DEFAULT_RANGE.key

  return (
    <Page>
      <Head
        title="Overview"
        sub="Every pipeline, the scheduler, data quality and what is deployed."
        actions={<RefreshNote updatedAt={status.dataUpdatedAt} fetching={status.isFetching} now={now} intervalMs={STATUS_REFETCH_MS} />}
      />

      {status.error && (
        <Notice>
          Could not refresh: {status.error.message}
          {data && " Showing the last good data."}
        </Notice>
      )}

      {!data ? (
        <>
          {/* The services read no database: in an outage they are what can still answer. */}
          {status.error && <ServiceCards />}
          <SheetSkeleton label="Loading pipeline status" />
        </>
      ) : (
        <>
          <SummaryTiles pipelines={data.pipelines} />
          {groupByCategory(data.pipelines).map((group) => (
            <PipelineSection key={group.label} group={group} now={now} />
          ))}
          <SchedulerTimeline
            now={now}
            range={range}
            onRangeChange={(next) => setParams(next.key === DEFAULT_RANGE.key ? {} : { scheduler: next.key }, { replace: true })}
            {...(defaultRange ? { runs: data.cron_job_runs } : schedulerWindow(range, longer))}
          />
          <QualityPanel
            quality_latest={data.quality_latest}
            recent_quality_runs={data.recent_quality_runs}
            quality_failed_checks={data.quality_failed_checks}
            now={now}
          />
          <ServiceCards />
          <JobsTable jobs={data.recent_jobs} now={now} />
        </>
      )}
    </Page>
  )
}

function SummaryTiles({ pipelines }: { pipelines: PipelineHealth[] }) {
  const summary = summarize(pipelines)
  return (
    <Tiles
      tiles={[
        { label: "Pipelines", value: summary.total },
        { label: "Healthy", value: summary.healthy, tone: "up" },
        { label: "Failing", value: summary.failing, tone: summary.failing > 0 ? "down" : "quiet" },
        { label: "Running", value: summary.running, tone: summary.running > 0 ? "live" : "quiet" },
      ]}
    />
  )
}
