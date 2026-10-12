import { Block } from "@/components/blocks"
import { StatusBadge } from "@/components/StateBadge"
import type { Schemas } from "@/lib/api"
import { formatCentral, formatDuration, relativeTime } from "@/lib/time"
import s from "./sheet.module.css"
import j from "./overview.module.css"

type Job = Schemas["PipelineJobInfo"]

/** Background batch jobs (POST /pipelines/all), from the in-memory job manager. */
export function JobsTable({ jobs, now }: { jobs: Job[]; now: number }) {
  return (
    <Block
      title="Recent jobs"
      count={jobs.length}
      text="Batch runs of every pipeline. Held in memory, so a restart clears them."
    >
      {jobs.length === 0 ? (
        <p className={s.empty}>No batch jobs since the last restart.</p>
      ) : (
        <div className={s.bleed}>
          <table className={s.table} style={{ minWidth: "40rem" }}>
            <thead>
              <tr>
                <th scope="col">Job</th>
                <th scope="col">Status</th>
                <th scope="col">Created</th>
                <th scope="col" className={s.num}>Duration</th>
                <th scope="col">Progress</th>
                <th scope="col">Current</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => {
                const pct = job.pipelines_total > 0 ? Math.round((job.pipelines_completed / job.pipelines_total) * 100) : 0
                const bad = job.pipelines_failed > 0
                return (
                  <tr key={job.job_id}>
                    <td className={`${s.mono} ${s.strong}`} title={job.job_id}>
                      {job.job_id.slice(0, 8)}
                    </td>
                    <td>
                      <StatusBadge status={job.status} />
                    </td>
                    <td className={s.mono} title={formatCentral(job.created_at)}>
                      {relativeTime(job.created_at, now)}
                    </td>
                    <td className={s.num}>
                      {job.duration_seconds != null ? formatDuration(job.duration_seconds) : job.status === "running" ? "…" : "—"}
                    </td>
                    <td>
                      <span className={j.progress}>
                        <span className={j.progressTrack} role="progressbar" aria-valuenow={pct} aria-valuemin={0} aria-valuemax={100}>
                          <span className={j.progressFill} data-tone={bad ? "down" : "up"} style={{ width: `${pct}%` }} />
                        </span>
                        <span className={s.mono}>
                          {job.pipelines_completed}/{job.pipelines_total}
                          {bad && <span className={s.bad}> · {job.pipelines_failed} failed</span>}
                        </span>
                      </span>
                    </td>
                    <td className={s.mono}>
                      {job.current_pipeline ? <span className={s.warn}>{job.current_pipeline}</span> : <span className={s.muted}>—</span>}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </Block>
  )
}
