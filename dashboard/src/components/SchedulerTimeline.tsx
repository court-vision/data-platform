import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { formatCentralLong, formatClockCentral, formatDayClockCentral, formatDuration } from "@/lib/time"
import {
  axisTicks,
  buildLanes,
  clusterRuns,
  DEFAULT_RANGE,
  rangeMs,
  RANGES,
  runTone,
  SLOTS,
  type Cluster,
  type CronRun,
  type RunTone,
  type TimelineRange,
} from "@/lib/timeline"
import { cn } from "@/lib/utils"

const TONE_CLASS: Record<RunTone, string> = {
  success: "bg-status-win",
  retried: "bg-status-projected",
  failure: "bg-status-loss",
}

const TONE_TEXT: Record<RunTone, string> = {
  success: "text-status-win",
  retried: "text-status-projected",
  failure: "text-status-loss",
}

/** How many of a group's troubled runs its popover lists before it says "and N more". */
const LISTED = 5

interface Props {
  runs: CronRun[]
  now: number
  range?: TimelineRange
  onRangeChange?: (range: TimelineRange) => void
  /** A longer range was asked for and has not answered yet. */
  loading?: boolean
  /** The window held more runs than the reply carries: the oldest are missing. */
  truncated?: boolean
  error?: string | null
}

/** cron-runner activity over the chosen window, one lane per job. */
export function SchedulerTimeline({ runs, now, range = DEFAULT_RANGE, onRangeChange, loading = false, truncated = false, error = null }: Props) {
  const windowMs = rangeMs(range)
  const lanes = buildLanes(runs, now, windowMs)
  const ticks = axisTicks(now, windowMs, range.tickMs)
  const shown = lanes.reduce((count, lane) => count + lane.runs.length, 0)

  return (
    <Card>
      <CardHeader className="flex-row flex-wrap items-start justify-between gap-3 space-y-0 pb-3">
        <div className="space-y-1.5">
          <CardTitle className="flex items-baseline gap-2 text-base">
            Scheduler
            <span className="font-mono text-xs font-normal text-muted-foreground">{loading ? "…" : shown.toLocaleString()}</span>
          </CardTitle>
          <CardDescription>
            cron-runner job runs · last {range.words} · Central time · click a mark for details
            {truncated && " · the oldest runs in this window are not shown"}
          </CardDescription>
        </div>
        {onRangeChange && (
          <div className="flex gap-1" role="group" aria-label="How far back">
            {RANGES.map((option) => (
              <Button
                key={option.key}
                size="sm"
                variant={option.key === range.key ? "secondary" : "ghost"}
                className="h-7 px-2 font-mono text-xs"
                aria-pressed={option.key === range.key}
                onClick={() => onRangeChange(option)}
              >
                {option.key}
              </Button>
            ))}
          </div>
        )}
      </CardHeader>
      <CardContent className="overflow-x-auto">
        {error && <p role="alert" className="mb-2 text-xs text-status-loss">Could not load this range: {error}</p>}
        <div className="min-w-[40rem]" aria-busy={loading}>
          <div className="grid grid-cols-[8rem_1fr] items-end">
            <div />
            <div className="relative h-4 font-mono text-[10px] text-muted-foreground">
              {ticks.map((tick, index) => (
                <span
                  key={tick}
                  className={cn(
                    "absolute whitespace-nowrap",
                    // The end labels sit inside the lane; centred, a day-and-hour label runs past its edge.
                    index === 0 ? "translate-x-0" : index === ticks.length - 1 ? "-translate-x-full" : "-translate-x-1/2",
                    index % 2 === 1 && "hidden lg:inline",
                  )}
                  style={{ left: `${(index / (ticks.length - 1)) * 100}%` }}
                >
                  {range.dayTicks ? formatDayClockCentral(tick) : formatClockCentral(tick)}
                </span>
              ))}
            </div>
          </div>

          <ol className="mt-2 flex flex-col gap-1">
            {lanes.map((lane) => (
              <li key={lane.job} className="grid grid-cols-[8rem_1fr] items-center gap-2">
                <span className="truncate font-mono text-xs text-muted-foreground">{lane.job}</span>
                <div className="relative h-7 rounded-sm bg-muted/40">
                  <div className="absolute inset-y-0 left-0 right-0 my-auto h-px bg-border" aria-hidden />
                  {clusterRuns(lane.runs, now, windowMs).map((cluster) =>
                    cluster.runs.length === 1 ? (
                      <RunMarker key={cluster.runs[0].id} run={cluster.runs[0]} left={cluster.position} />
                    ) : (
                      <ClusterMarker key={cluster.slot} job={lane.job} cluster={cluster} />
                    ),
                  )}
                  {lane.runs.length === 0 && (
                    <span className="absolute inset-0 flex items-center justify-center text-[10px] text-muted-foreground/60">
                      {loading ? "loading…" : "no runs in window"}
                    </span>
                  )}
                </div>
              </li>
            ))}
          </ol>

          <p className="mt-3 flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px] text-muted-foreground">
            <span className="flex items-center gap-1.5"><span className="size-2.5 rounded-full bg-status-win" aria-hidden /> succeeded</span>
            <span className="flex items-center gap-1.5"><span className="size-2.5 rounded-full bg-status-projected" aria-hidden /> succeeded after a retry</span>
            <span className="flex items-center gap-1.5"><span className="size-2.5 rounded-full bg-status-loss" aria-hidden /> failed</span>
            <span className="flex items-center gap-1.5"><span className="h-2.5 w-4 rounded-sm bg-muted-foreground/50" aria-hidden /> several runs, in the worst colour among them</span>
          </p>
        </div>
      </CardContent>
    </Card>
  )
}

function RunMarker({ run, left }: { run: CronRun; left: number }) {
  const tone = runTone(run)

  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          type="button"
          aria-label={`${run.job_name} ${run.result} at ${formatCentralLong(run.triggered_at)}`}
          className={cn(
            "absolute top-1/2 size-3 -translate-x-1/2 -translate-y-1/2 rounded-full ring-2 ring-card transition-transform hover:scale-125 focus-visible:scale-125 focus-visible:outline-none",
            TONE_CLASS[tone],
          )}
          style={{ left: `${left}%` }}
        />
      </PopoverTrigger>
      <PopoverContent align="center" className="w-80 p-3 text-xs">
        <dl className="grid grid-cols-[6rem_1fr] gap-y-1">
          <Row label="Job" value={run.job_name} />
          <Row label="Time" value={formatCentralLong(run.triggered_at)} />
          <Row label="Result" value={resultWords(run)} className={TONE_TEXT[tone]} />
          <Row label="Duration" value={formatDuration(run.duration_seconds)} />
          <Row label="HTTP" value={run.http_status?.toString() ?? "—"} />
          {run.error_message && <Row label="Error" value={run.error_message} className="text-status-loss" />}
        </dl>
        {run.response_snippet && (
          <pre className="mt-2 max-h-32 overflow-auto rounded bg-muted/60 p-2 font-mono text-[10px] leading-snug text-muted-foreground">
            {run.response_snippet}
          </pre>
        )}
      </PopoverContent>
    </Popover>
  )
}

function resultWords(run: CronRun): string {
  return run.result + (run.attempts > 1 ? ` after ${run.attempts} attempts` : "")
}

/** What a group holds, in words: the mark's colour is never the only way to know. */
export function clusterSummary(cluster: Cluster): string {
  const failed = cluster.runs.filter((run) => runTone(run) === "failure").length
  const retried = cluster.runs.filter((run) => runTone(run) === "retried").length
  const parts = [`${cluster.runs.length} runs`]
  if (failed > 0) parts.push(`${failed} failed`)
  if (retried > 0) parts.push(`${retried} retried`)
  if (failed === 0 && retried === 0) parts.push("all succeeded")
  return parts.join(", ")
}

function ClusterMarker({ job, cluster }: { job: string; cluster: Cluster }) {
  const first = cluster.runs[0]
  const last = cluster.runs[cluster.runs.length - 1]
  const troubled = cluster.runs.filter((run) => runTone(run) !== "success")
  const summary = clusterSummary(cluster)

  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          type="button"
          aria-label={`${job}: ${summary}, from ${formatCentralLong(first.triggered_at)} to ${formatCentralLong(last.triggered_at)}`}
          data-runs={cluster.runs.length}
          data-tone={cluster.tone}
          className={cn(
            "absolute top-1/2 h-3 min-w-2 -translate-x-1/2 -translate-y-1/2 rounded-sm ring-1 ring-card transition-transform hover:scale-y-150 focus-visible:scale-y-150 focus-visible:outline-none",
            TONE_CLASS[cluster.tone],
          )}
          style={{ left: `${cluster.position}%`, width: `${100 / SLOTS}%` }}
        />
      </PopoverTrigger>
      <PopoverContent align="center" className="w-80 p-3 text-xs">
        <dl className="grid grid-cols-[6rem_1fr] gap-y-1">
          <Row label="Job" value={job} />
          <Row label="Runs" value={summary} className={TONE_TEXT[cluster.tone]} />
          <Row label="From" value={formatCentralLong(first.triggered_at)} />
          <Row label="To" value={formatCentralLong(last.triggered_at)} />
        </dl>
        {troubled.length > 0 && (
          <ul className="mt-2 flex flex-col gap-1 border-t border-border/60 pt-2">
            {troubled.slice(0, LISTED).map((run) => (
              <li key={run.id} className="font-mono text-[11px]">
                <span className="text-muted-foreground">{formatCentralLong(run.triggered_at)}</span>{" "}
                <span className={TONE_TEXT[runTone(run)]}>{resultWords(run)}</span>
                {run.error_message && <span className="block break-words text-status-loss">{run.error_message}</span>}
              </li>
            ))}
            {troubled.length > LISTED && <li className="text-muted-foreground">and {troubled.length - LISTED} more</li>}
          </ul>
        )}
      </PopoverContent>
    </Popover>
  )
}

function Row({ label, value, className }: { label: string; value: string; className?: string }) {
  return (
    <>
      <dt className="text-muted-foreground">{label}</dt>
      <dd className={cn("break-words font-mono", className)}>{value}</dd>
    </>
  )
}
