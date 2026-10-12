import { Link } from "react-router"

import { Block } from "@/components/blocks"
import { RunChecksButton } from "@/components/RunChecksButton"
import { StatusBadge } from "@/components/StateBadge"
import type { DashboardStatus } from "@/hooks/useDashboardStatus"
import { formatCentral, formatDuration, relativeTime } from "@/lib/time"
import dk from "@/desk/desk.module.css"
import s from "./sheet.module.css"
import o from "./overview.module.css"

type Props = Pick<DashboardStatus, "quality_latest" | "recent_quality_runs" | "quality_failed_checks"> & {
  now: number
}

export function QualityPanel({ quality_latest: latest, recent_quality_runs: recent, quality_failed_checks: failed, now }: Props) {
  return (
    <Block
      title="Data quality"
      count={recent.length}
      right={<RunChecksButton />}
      text={
        <>
          Assertions over the nba.* tables. Runs take a minute.{" "}
          <Link to="/quality" className={s.textLink}>
            Every check and its history
          </Link>
        </>
      }
    >
      {!latest ? (
        <p className={s.empty}>No quality runs yet.</p>
      ) : (
        <>
          <section aria-label="Latest run" className={o.panelPart}>
            <div className={o.subhead}>
              <Link to={`/quality/runs/${latest.run_id}`} className={`${dk.label} ${o.subheadLink}`}>
                Latest run
              </Link>
              <StatusBadge status={latest.status} />
            </div>
            <dl className={s.facts}>
              <Fact label="Started" value={relativeTime(latest.started_at, now)} title={formatCentral(latest.started_at)} />
              <Fact label="Duration" value={formatDuration(latest.duration_seconds)} />
              <Fact label="Checks" value={`${latest.passed_checks}/${latest.total_checks} passed`} />
              <Fact label="Failures" value={String(latest.failed_checks)} tone={latest.failed_checks > 0 ? "down" : undefined} />
              <Fact label="Triggered by" value={latest.triggered_by ?? "unknown"} />
            </dl>
            {latest.error_message && <p className={`${o.rowNote} ${s.bad}`}>{latest.error_message}</p>}
          </section>

          <section aria-label="Failed checks" className={o.panelPart}>
            <div className={o.subhead}>
              <span className={dk.label}>Failed checks</span>
            </div>
            {failed.length === 0 ? (
              <p className={s.empty}>None in the latest run.</p>
            ) : (
              <ul className={o.rows}>
                {failed.map((check) => (
                  <li key={check.check_name}>
                    <div className={o.rowLine}>
                      <span className={o.checkName}>{check.check_name}</span>
                      <StatusBadge status={check.status} />
                      <span className={s.muted}>
                        {check.severity} · {check.failures.toLocaleString()} failures
                      </span>
                    </div>
                    {check.message && <p className={o.rowNote}>{check.message}</p>}
                  </li>
                ))}
              </ul>
            )}
          </section>

          {recent.length > 1 && (
            <section aria-label="Recent runs" className={o.panelPart}>
              <div className={o.subhead}>
                <span className={dk.label}>Recent runs</span>
              </div>
              <ol className={o.rows}>
                {recent.map((entry) => (
                  <li key={entry.run_id} className={o.rowLine}>
                    <StatusBadge status={entry.status} />
                    <Link to={`/quality/runs/${entry.run_id}`} className={`${s.nameLink} ${s.mono}`} title={formatCentral(entry.started_at)}>
                      {relativeTime(entry.started_at, now)}
                    </Link>
                    <span className={`${s.mono} ${s.muted}`}>
                      {entry.passed_checks}/{entry.total_checks} passed · {formatDuration(entry.duration_seconds)}
                    </span>
                    <span className={`${s.muted} ${o.pushRight}`}>{entry.triggered_by}</span>
                  </li>
                ))}
              </ol>
            </section>
          )}
        </>
      )}
    </Block>
  )
}

function Fact({ label, value, title, tone }: { label: string; value: string; title?: string; tone?: "down" }) {
  return (
    <div className={s.fact}>
      <dt className={s.factKey}>{label}</dt>
      <dd className={s.factVal} title={title} data-tone={tone}>
        {value}
      </dd>
    </div>
  )
}
