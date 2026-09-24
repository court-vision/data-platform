import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { formatCentralLong, formatClockCentral, formatDuration } from "@/lib/time"
import { axisTicks, buildLanes, markerPosition, runTone, type CronRun, type RunTone } from "@/lib/timeline"
import { cn } from "@/lib/utils"

const TONE_CLASS: Record<RunTone, string> = {
  success: "bg-status-win",
  retried: "bg-status-projected",
  failure: "bg-status-loss",
}

/** The last six hours of cron-runner activity, one lane per job. */
export function SchedulerTimeline({ runs, now }: { runs: CronRun[]; now: number }) {
  const lanes = buildLanes(runs, now)
  const ticks = axisTicks(now)
  const shown = lanes.reduce((count, lane) => count + lane.runs.length, 0)

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-baseline gap-2 text-base">
          Scheduler
          <span className="font-mono text-xs font-normal text-muted-foreground">{shown}</span>
        </CardTitle>
        <CardDescription>cron-runner job runs · last 6 hours · Central time · click a run for details</CardDescription>
      </CardHeader>
      <CardContent className="overflow-x-auto">
        <div className="min-w-[40rem]">
          <div className="grid grid-cols-[8rem_1fr] items-end">
            <div />
            <div className="relative h-4 font-mono text-[10px] text-muted-foreground">
              {ticks.map((tick, index) => (
                <span
                  key={tick}
                  className={cn(
                    "absolute -translate-x-1/2 whitespace-nowrap",
                    index % 2 === 1 && "hidden lg:inline",
                  )}
                  style={{ left: `${(index / (ticks.length - 1)) * 100}%` }}
                >
                  {formatClockCentral(tick)}
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
                  {lane.runs.map((run) => (
                    <RunMarker key={run.id} run={run} now={now} />
                  ))}
                  {lane.runs.length === 0 && (
                    <span className="absolute inset-0 flex items-center justify-center text-[10px] text-muted-foreground/60">
                      no runs in window
                    </span>
                  )}
                </div>
              </li>
            ))}
          </ol>
        </div>
      </CardContent>
    </Card>
  )
}

function RunMarker({ run, now }: { run: CronRun; now: number }) {
  const left = markerPosition(run.triggered_at, now)
  if (left === null) return null
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
          <Row
            label="Result"
            value={run.result + (run.attempts > 1 ? ` after ${run.attempts} attempts` : "")}
            className={tone === "failure" ? "text-status-loss" : tone === "retried" ? "text-status-projected" : "text-status-win"}
          />
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

function Row({ label, value, className }: { label: string; value: string; className?: string }) {
  return (
    <>
      <dt className="text-muted-foreground">{label}</dt>
      <dd className={cn("break-words font-mono", className)}>{value}</dd>
    </>
  )
}
