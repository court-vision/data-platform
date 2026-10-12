import { Block, Pop, Rail } from "@/components/blocks"
import { formatCentralLong, formatClockCentral, formatDayClockCentral, formatDuration } from "@/lib/time"
import {
  axisTicks,
  buildLanes,
  clusterBuckets,
  clusterRuns,
  DEFAULT_RANGE,
  parseRange,
  rangeMs,
  RANGES,
  runTone,
  type Cluster,
  type Counted,
  type CronRun,
  type TimelineRange,
} from "@/lib/timeline"
import s from "./sheet.module.css"
import o from "./overview.module.css"

/**
 * How many of a group's troubled runs its popover lists before it says "and N
 * more". A counted column comes with that many (SCHEDULER_LISTED in
 * api/v1/dashboard.py).
 */
const LISTED = 5

interface Props {
  runs: CronRun[]
  now: number
  range?: TimelineRange
  onRangeChange?: (range: TimelineRange) => void
  /** The window's runs counted per job and column: `runs` are then only the ones a mark can open. */
  counted?: Counted | null
  /** A longer range was asked for and has not answered yet. */
  loading?: boolean
  /** The window held more runs than the reply carries: the oldest are missing. */
  truncated?: boolean
  error?: string | null
}

/** cron-runner activity over the chosen window, one lane per job. */
export function SchedulerTimeline({ runs, now, range = DEFAULT_RANGE, onRangeChange, counted = null, loading = false, truncated = false, error = null }: Props) {
  const windowMs = rangeMs(range)
  const lanes = buildLanes(runs, now, windowMs, counted)
  const ticks = axisTicks(now, windowMs, range.tickMs)
  const shown = lanes.reduce((count, lane) => count + lane.count, 0)

  return (
    <Block
      title="Scheduler"
      count={loading ? "…" : shown.toLocaleString()}
      note={`cron-runner job runs · last ${range.words} · Central time · click a mark for details${truncated ? " · the oldest runs in this window are not shown" : ""}`}
      right={
        onRangeChange && (
          <Rail
            label="How far back"
            options={RANGES.map((option) => ({ key: option.key, label: option.key }))}
            value={range.key}
            onChange={(key) => onRangeChange(parseRange(key))}
          />
        )
      }
    >
      {error && (
        <p role="alert" className={`${s.empty} ${s.bad}`}>
          Could not load this range: {error}
        </p>
      )}
      <div className={o.scroller}>
        <div className={o.timeline} aria-busy={loading}>
          <div className={o.axisRow}>
            <div />
            <div className={o.axis}>
              {ticks.map((tick, index) => (
                <span
                  key={tick}
                  className={o.tick}
                  data-edge={index === 0 ? "start" : index === ticks.length - 1 ? "end" : undefined}
                  data-minor={(ticks.length - 1 - index) % 2 === 1 ? "true" : undefined}
                  style={{ left: `${(index / (ticks.length - 1)) * 100}%` }}
                >
                  {range.dayTicks ? formatDayClockCentral(tick) : formatClockCentral(tick)}
                </span>
              ))}
            </div>
          </div>

          <ol className={o.lanes}>
            {lanes.map((lane) => (
              <li key={lane.job} className={o.lane}>
                <span className={o.laneName} title={lane.job}>
                  {lane.job}
                </span>
                <div className={o.laneTrack}>
                  {(lane.counted ? clusterBuckets(lane.counted, lane.runs, now, windowMs) : clusterRuns(lane.runs, now, windowMs)).map((cluster) =>
                    cluster.count === 1 && cluster.runs.length === 1 ? (
                      <RunMarker key={cluster.runs[0].id} run={cluster.runs[0]} left={cluster.position} />
                    ) : (
                      <ClusterMarker key={cluster.slot} job={lane.job} cluster={cluster} />
                    ),
                  )}
                  {lane.count === 0 && <span className={o.laneEmpty}>{loading ? "loading…" : "no runs in window"}</span>}
                </div>
              </li>
            ))}
          </ol>
        </div>
      </div>

      {/* Outside the scroller: on a phone the key wraps rather than sliding away with the lanes. */}
      <ul className={`${s.legend} ${o.legendWrap}`} aria-label="Key">
        <li>
          <span className={`${s.swatch} ${s.swatchRound}`} data-tone="success" aria-hidden /> succeeded
        </li>
        <li>
          <span className={`${s.swatch} ${s.swatchRound}`} data-tone="retried" aria-hidden /> succeeded after a retry
        </li>
        <li>
          <span className={`${s.swatch} ${s.swatchRound}`} data-tone="failure" aria-hidden /> failed
        </li>
        <li>
          <span className={`${s.swatch} ${o.legendBar}`} aria-hidden /> several runs, in the worst colour among them
        </li>
      </ul>
    </Block>
  )
}

function RunMarker({ run, left }: { run: CronRun; left: number }) {
  const tone = runTone(run)

  return (
    <Pop
      align="center"
      trigger={
        <button
          type="button"
          aria-label={`${run.job_name} ${run.result} at ${formatCentralLong(run.triggered_at)}`}
          className={o.mark}
          data-tone={tone}
          style={{ left: `${left}%` }}
        />
      }
    >
      <dl className={s.dl}>
        <Row label="Job" value={run.job_name} />
        <Row label="Time" value={formatCentralLong(run.triggered_at)} />
        <Row label="Result" value={resultWords(run)} tone={tone} />
        <Row label="Duration" value={formatDuration(run.duration_seconds)} />
        <Row label="HTTP" value={run.http_status?.toString() ?? "—"} />
        {run.error_message && <Row label="Error" value={run.error_message} tone="failure" />}
      </dl>
      {run.response_snippet && <pre className={`${s.pre} ${o.snippet}`}>{run.response_snippet}</pre>}
    </Pop>
  )
}

function resultWords(run: CronRun): string {
  return run.result + (run.attempts > 1 ? ` after ${run.attempts} attempts` : "")
}

/** What a group holds, in words: the mark's colour is never the only way to know. */
export function clusterSummary(cluster: Cluster): string {
  const { failed, retried } = cluster
  const parts = [cluster.count === 1 ? "1 run" : `${cluster.count} runs`]
  if (failed > 0) parts.push(`${failed} failed`)
  if (retried > 0) parts.push(`${retried} retried`)
  if (failed === 0 && retried === 0) parts.push("all succeeded")
  return parts.join(", ")
}

function ClusterMarker({ job, cluster }: { job: string; cluster: Cluster }) {
  return (
    <Pop
      align="center"
      trigger={
        <button
          type="button"
          aria-label={`${job}: ${clusterSummary(cluster)}, from ${formatCentralLong(cluster.from)} to ${formatCentralLong(cluster.to)}`}
          data-runs={cluster.count}
          data-tone={cluster.tone}
          className={`${o.mark} ${o.cluster}`}
          style={{ left: `${cluster.position}%`, width: `${cluster.width}%` }}
        />
      }
    >
      <ClusterDetail job={job} cluster={cluster} />
    </Pop>
  )
}

/**
 * What a group's popover says: what it holds, its newest run as a lone dot
 * would show it (a poll that did nothing says why in its response body), and
 * the troubled runs it came with.
 */
export function ClusterDetail({ job, cluster }: { job: string; cluster: Cluster }) {
  const listed = cluster.runs.filter((run) => runTone(run) !== "success").slice(0, LISTED)
  const unlisted = cluster.failed + cluster.retried - listed.length
  const latest = cluster.runs.at(-1)

  return (
    <>
      <dl className={s.dl}>
        <Row label="Job" value={job} />
        <Row label="Runs" value={clusterSummary(cluster)} tone={cluster.tone} />
        <Row label="From" value={formatCentralLong(cluster.from)} />
        <Row label="To" value={formatCentralLong(cluster.to)} />
      </dl>
      {latest && (
        <div className={s.popRule}>
          <dl className={s.dl}>
            <Row label="Latest run" value={formatCentralLong(latest.triggered_at)} />
            <Row label="Result" value={resultWords(latest)} tone={runTone(latest)} />
            <Row label="Duration" value={formatDuration(latest.duration_seconds)} />
            <Row label="HTTP" value={latest.http_status?.toString() ?? "—"} />
          </dl>
          {latest.response_snippet && <pre className={`${s.pre} ${o.snippet}`}>{latest.response_snippet}</pre>}
        </div>
      )}
      {(listed.length > 0 || unlisted > 0) && (
        <ul className={`${o.runList} ${s.popRule}`}>
          {listed.map((run) => (
            <li key={run.id}>
              <span className={s.muted}>{formatCentralLong(run.triggered_at)}</span>{" "}
              <span data-tone={runTone(run)} className={s.toned}>
                {resultWords(run)}
              </span>{" "}
              <span className={s.muted}>
                · {formatDuration(run.duration_seconds)} · HTTP {run.http_status ?? "—"}
              </span>
              {run.error_message && <span className={o.runError}>{run.error_message}</span>}
            </li>
          ))}
          {unlisted > 0 && <li className={s.muted}>and {unlisted} more</li>}
        </ul>
      )}
    </>
  )
}

function Row({ label, value, tone }: { label: string; value: string; tone?: string }) {
  return (
    <>
      <dt>{label}</dt>
      <dd data-tone={tone}>{value}</dd>
    </>
  )
}
