import { StatusBadge } from "@/components/StateBadge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import type { Schemas } from "@/lib/api"
import { formatCentral, formatDuration, relativeTime } from "@/lib/time"
import { cn } from "@/lib/utils"

type Job = Schemas["PipelineJobInfo"]

/** Background batch jobs (POST /pipelines/all), from the in-memory job manager. */
export function JobsTable({ jobs, now }: { jobs: Job[]; now: number }) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-baseline gap-2 text-base">
          Recent jobs
          <span className="font-mono text-xs font-normal text-muted-foreground">{jobs.length}</span>
        </CardTitle>
        <CardDescription>Batch runs of every pipeline. Held in memory, so a restart clears them.</CardDescription>
      </CardHeader>
      <CardContent className="overflow-x-auto px-0 pb-2">
        {jobs.length === 0 ? (
          <p className="py-4 text-center text-sm text-muted-foreground">No batch jobs since the last restart.</p>
        ) : (
          <table className="w-full min-w-[40rem] text-sm">
            <thead>
              <tr className="border-b text-left text-xs uppercase tracking-wider text-muted-foreground">
                <th scope="col" className="px-6 py-2 font-medium">Job</th>
                <th scope="col" className="px-3 py-2 font-medium">Status</th>
                <th scope="col" className="px-3 py-2 font-medium">Created</th>
                <th scope="col" className="px-3 py-2 text-right font-medium">Duration</th>
                <th scope="col" className="px-3 py-2 font-medium">Progress</th>
                <th scope="col" className="px-6 py-2 font-medium">Current</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => {
                const pct = job.pipelines_total > 0 ? Math.round((job.pipelines_completed / job.pipelines_total) * 100) : 0
                const bad = job.pipelines_failed > 0
                return (
                  <tr key={job.job_id} className="border-b border-border/50 last:border-0">
                    <td className="px-6 py-2.5 font-mono text-xs" title={job.job_id}>{job.job_id.slice(0, 8)}</td>
                    <td className="px-3 py-2.5"><StatusBadge status={job.status} /></td>
                    <td className="px-3 py-2.5 font-mono text-xs" title={formatCentral(job.created_at)}>{relativeTime(job.created_at, now)}</td>
                    <td className="px-3 py-2.5 text-right font-mono text-xs tabular-nums">
                      {job.duration_seconds != null ? formatDuration(job.duration_seconds) : job.status === "running" ? "…" : "—"}
                    </td>
                    <td className="px-3 py-2.5">
                      <div className="flex items-center gap-2">
                        <div className="h-1.5 w-24 overflow-hidden rounded-full bg-muted" role="progressbar" aria-valuenow={pct} aria-valuemin={0} aria-valuemax={100}>
                          <div className={cn("h-full rounded-full", bad ? "bg-status-loss" : "bg-status-win")} style={{ width: `${pct}%` }} />
                        </div>
                        <span className="font-mono text-xs tabular-nums">
                          {job.pipelines_completed}/{job.pipelines_total}
                          {bad && <span className="text-status-loss"> · {job.pipelines_failed} failed</span>}
                        </span>
                      </div>
                    </td>
                    <td className="px-6 py-2.5 font-mono text-xs">
                      {job.current_pipeline ? <span className="text-status-projected">{job.current_pipeline}</span> : <span className="text-muted-foreground">—</span>}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}
      </CardContent>
    </Card>
  )
}
