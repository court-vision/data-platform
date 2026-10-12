import { Link } from "react-router"

import { Block } from "@/components/blocks"
import { RunPipelineButton } from "@/components/RunPipelineButton"
import { StateBadge } from "@/components/StateBadge"
import { pipelineState, type CategoryGroup } from "@/lib/pipelines"
import { formatCentral, formatDuration, relativeTime } from "@/lib/time"
import s from "./sheet.module.css"

export function PipelineSection({ group, now }: { group: CategoryGroup; now: number }) {
  return (
    <Block title={group.label} count={group.pipelines.length} text={group.description}>
      <div className={s.bleed}>
        {/* Fixed widths: every category's columns line up with the others'. */}
        <table className={s.table} style={{ minWidth: "46rem", tableLayout: "fixed" }}>
          <colgroup>
            <col style={{ width: "27%" }} />
            <col style={{ width: "14%" }} />
            <col style={{ width: "13%" }} />
            <col style={{ width: "13%" }} />
            <col style={{ width: "10%" }} />
            <col style={{ width: "10%" }} />
            <col style={{ width: "13%" }} />
          </colgroup>
          <thead>
            <tr>
              <th scope="col">Pipeline</th>
              <th scope="col">State</th>
              <th scope="col">Last run</th>
              <th scope="col">Last success</th>
              <th scope="col" className={s.num}>Duration</th>
              <th scope="col" className={s.num}>Records</th>
              <th scope="col" className={s.num}>
                <span className={s.srOnly}>Run</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {group.pipelines.map((pipeline) => (
              <tr key={pipeline.name}>
                <th scope="row">
                  <Link to={`/pipelines/${pipeline.name}`} className={s.nameLink}>
                    {pipeline.display_name}
                  </Link>
                  <span className={s.under}>{pipeline.name}</span>
                </th>
                <td>
                  <span className={s.chips}>
                    <StateBadge state={pipelineState(pipeline)} />
                    {pipeline.error_streak > 1 && (
                      <span className={`${s.mono} ${s.bad}`} title={`${pipeline.error_streak} consecutive failures`}>
                        ×{pipeline.error_streak}
                      </span>
                    )}
                  </span>
                </td>
                <td className={s.mono} title={formatCentral(pipeline.last_run_at)}>
                  {relativeTime(pipeline.last_run_at, now)}
                </td>
                <td className={`${s.mono} ${pipeline.last_success_at ? "" : s.muted}`} title={formatCentral(pipeline.last_success_at)}>
                  {pipeline.last_success_at ? relativeTime(pipeline.last_success_at, now) : "never"}
                </td>
                <td className={s.num}>{formatDuration(pipeline.last_duration_seconds)}</td>
                <td className={s.num}>{pipeline.last_records_processed?.toLocaleString() ?? "—"}</td>
                <td className={s.num}>
                  <RunPipelineButton pipeline={pipeline} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Block>
  )
}
