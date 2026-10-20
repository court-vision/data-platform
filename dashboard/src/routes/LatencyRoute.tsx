import { useSearchParams } from "react-router"

import { AnatomyBar, AnatomyKey } from "@/components/Anatomy"
import { Block, Choice, Head, Lost, Notice, Page, RefreshNote, Sep, SheetSkeleton, Tag } from "@/components/blocks"
import { AnatomyColumns, PercentileChart } from "@/components/LatencyCharts"
import { usePageContext } from "@/components/shell/pageContext"
import { useNow } from "@/hooks/useNow"
import { LATENCY_REFETCH_MS, useRouteLatency } from "@/hooks/useLatency"
import {
  bucketLabel,
  bucketRows,
  formatCalls,
  formatMs,
  formatShare,
  groupsUsed,
  parseWindow,
  WINDOWS,
  type LatencyWindow,
  type RouteLatency,
  type SlowRequest,
} from "@/lib/latency"
import { formatCentral, formatCentralLong, relativeTime } from "@/lib/time"
import { StatsTiles } from "@/routes/Latency"
import s from "@/components/sheet.module.css"
import l from "@/components/latency.module.css"

/** One backend route: its times over the window, where they went, and its slowest requests. */
export function LatencyRoute() {
  const [params, setParams] = useSearchParams()
  const route = params.get("route") ?? ""
  const method = (params.get("method") ?? "GET").toUpperCase()
  const window = parseWindow(params.get("window"))
  const query = useRouteLatency(method, route, window)
  const now = useNow()
  const data = query.data
  usePageContext(route || "Not found")

  if (!route) {
    return <Lost title="No route given" back={{ to: "/latency", label: "Back to latency" }} />
  }

  const back = { to: window === "24h" ? "/latency" : `/latency?window=${window}`, label: "Latency" }
  const windowChoice = (
    <Choice
      label="Over"
      options={WINDOWS}
      value={window}
      onChange={(value: LatencyWindow) =>
        setParams(
          (current) => {
            const next = new URLSearchParams(current)
            if (value === "24h") next.delete("window")
            else next.set("window", value)
            return next
          },
          { replace: true },
        )
      }
    />
  )

  return (
    <Page>
      <Head
        back={back}
        title={route}
        chips={<Tag>{method}</Tag>}
        meta={
          data && (
            <>
              <span>{data.service}</span>
              <Sep />
              <span>since {formatCentral(data.since)}</span>
            </>
          )
        }
        actions={<RefreshNote updatedAt={query.dataUpdatedAt} fetching={query.isFetching} now={now} intervalMs={LATENCY_REFETCH_MS} />}
      />

      {query.error && (
        <Notice>
          Could not load this route: {query.error.message}
          {data && " Showing the last good data."}
        </Notice>
      )}

      {!data ? (
        query.error ? null : <SheetSkeleton label="Loading the route" />
      ) : !data.available || !data.route ? (
        <Block title="Response time" right={windowChoice}>
          <p className={s.empty}>
            {data.available ? `No ${method} requests to this route in the last ${data.window}.` : "No request timings yet."}
          </p>
        </Block>
      ) : (
        <>
          <StatsTiles stats={data.route} note={`in the last ${data.window}`} />

          <Block title="Response time" note="p50 and p95 per slice · Central time" right={windowChoice}>
            <PercentileChart series={data.series} binSeconds={data.bin_seconds} />
          </Block>

          <Block title="Where the time went" note="the average request of each slice">
            <AnatomyColumns series={data.series} binSeconds={data.bin_seconds} />
            <AnatomyKey groups={groupsUsed(data.series.map((bin) => bin.parts))} />
          </Block>

          <BucketsTable route={data.route} window={data.window} />

          <SlowestTable slowest={data.slowest} now={now} />
        </>
      )}
    </Page>
  )
}

/** The route's average request, a row per bucket: the chart's numbers in text, finer than its colours. */
function BucketsTable({ route, window }: { route: RouteLatency; window: LatencyWindow }) {
  const rows = bucketRows(route.parts, route.calls)
  return (
    <Block title="Average request" count={formatMs(route.mean_ms)} note={`over the last ${window}`}>
      <div className={s.bleed}>
        <table className={`${s.table} ${l.viz}`}>
          <thead>
            <tr>
              <th scope="col">Where</th>
              <th scope="col" className={s.num}>Time</th>
              <th scope="col" className={s.num}>Share</th>
              <th scope="col" className={s.num}>Calls per request</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.bucket}>
                <th scope="row">
                  <span className={l.routeName}>
                    <span className={l.swatch} data-group={row.group.key} aria-hidden />
                    <span title={row.group.note}>{bucketLabel(row.bucket)}</span>
                  </span>
                </th>
                <td className={s.num}>{formatMs(row.ms)}</td>
                <td className={s.num}>{formatShare(row.share)}</td>
                <td className={s.num}>{formatCalls(row.calls)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Block>
  )
}

function SlowestTable({ slowest, now }: { slowest: SlowRequest[]; now: number }) {
  const scale = Math.max(0, ...slowest.map((request) => request.duration_ms))
  return (
    <Block
      title="Slowest requests"
      count={slowest.length}
      note="slowest first · Central time"
      text="A trace id means Railway traced the request: search for it on the api service's Traces page for the full timeline."
    >
      {slowest.length === 0 ? (
        <p className={s.empty}>No requests.</p>
      ) : (
        <div className={s.bleed}>
          <table className={s.table} style={{ minWidth: "48rem" }}>
            <thead>
              <tr>
                <th scope="col">Started</th>
                <th scope="col" className={s.num}>Duration</th>
                <th scope="col" className={s.num}>Status</th>
                <th scope="col">Where the time went</th>
                <th scope="col">Trace</th>
              </tr>
            </thead>
            <tbody>
              {slowest.map((request, index) => (
                <tr key={`${request.started_at}-${index}`}>
                  <td className={`${s.mono} ${s.nowrap}`}>
                    {formatCentralLong(request.started_at)}
                    <span className={s.under}>{relativeTime(request.started_at, now)}</span>
                  </td>
                  <td className={s.num}>{formatMs(request.duration_ms)}</td>
                  <td className={`${s.num} ${request.status_code >= 500 ? l.bad : ""}`}>{request.status_code}</td>
                  <td className={l.anatomyCell}>
                    <AnatomyBar parts={request.parts} scale={scale} />
                  </td>
                  <td className={l.trace}>
                    {request.trace_id ?? "—"}
                    {request.version && <span className={s.under}>{request.version.slice(0, 7)}</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Block>
  )
}
