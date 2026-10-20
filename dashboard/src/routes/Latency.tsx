import { Link, useSearchParams } from "react-router"

import { AnatomyBar, AnatomyKey } from "@/components/Anatomy"
import { Block, Choice, Head, Notice, Page, RefreshNote, Sep, SheetSkeleton, Tiles, type Tone } from "@/components/blocks"
import { usePageContext } from "@/components/shell/pageContext"
import { useNow } from "@/hooks/useNow"
import { LATENCY_REFETCH_MS, useLatency } from "@/hooks/useLatency"
import {
  externalShare,
  formatMs,
  formatShare,
  groupsUsed,
  parseSort,
  parseWindow,
  routeHref,
  SORTS,
  sortRoutes,
  WINDOWS,
  type LatencyOverview,
  type LatencyStats,
  type LatencyWindow,
  type Sort,
} from "@/lib/latency"
import { formatCentral } from "@/lib/time"
import s from "@/components/sheet.module.css"
import l from "@/components/latency.module.css"

/**
 * Every backend route's response times, and where its time goes: ESPN, the
 * NBA, Postgres, our own code. The window and the order live in the URL; the
 * text names the window of the data on screen, which lags the URL while
 * another window loads.
 */
export function Latency() {
  usePageContext("Latency")
  const [params, setParams] = useSearchParams()
  const window = parseWindow(params.get("window"))
  const sort = parseSort(params.get("sort"))
  const query = useLatency(window)
  const now = useNow()
  const data = query.data

  function update(name: string, value: string, fallback: string) {
    setParams(
      (current) => {
        const next = new URLSearchParams(current)
        if (value === fallback) next.delete(name)
        else next.set(name, value)
        return next
      },
      { replace: true },
    )
  }

  return (
    <Page>
      <Head
        title="Latency"
        sub="Every backend request, timed and split into where its time went: ESPN, Yahoo, the NBA, Claude, Postgres, our own services, waiting for a free slot, and our own code."
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
          Could not load the timings: {query.error.message}
          {data && " Showing the last good data."}
        </Notice>
      )}

      {!data ? (
        query.error ? null : <SheetSkeleton label="Loading latency" />
      ) : !data.available ? (
        <Block title="Routes" right={<WindowChoice window={window} onChange={(value) => update("window", value, "24h")} />}>
          <p className={s.empty}>No request timings yet. The backend starts writing them once its request timing is deployed.</p>
        </Block>
      ) : (
        <>
          <TotalsTiles totals={data.totals} window={data.window} />
          <RoutesBlock data={data} window={window} sort={sort} onWindow={(value) => update("window", value, "24h")} onSort={(value) => update("sort", value, "time")} />
        </>
      )}
    </Page>
  )
}

function WindowChoice({ window, onChange }: { window: LatencyWindow; onChange: (value: LatencyWindow) => void }) {
  return <Choice label="Over" options={WINDOWS} value={window} onChange={onChange} />
}

export function errorTone(stats: LatencyStats): Tone | undefined {
  if (stats.requests === 0 || stats.errors === 0) return undefined
  return stats.errors / stats.requests >= 0.01 ? "down" : "warn"
}

export function StatsTiles({ stats, note }: { stats: LatencyStats; note: string }) {
  return (
    <Tiles
      tiles={[
        { label: "Requests", value: stats.requests.toLocaleString(), note },
        { label: "p50", value: formatMs(stats.p50_ms), note: `mean ${formatMs(stats.mean_ms)}` },
        { label: "p95", value: formatMs(stats.p95_ms), note: `p99 ${formatMs(stats.p99_ms)} · max ${formatMs(stats.max_ms)}` },
        {
          label: "5xx",
          value: stats.errors.toLocaleString(),
          tone: errorTone(stats),
          note: stats.requests > 0 ? `${formatShare(stats.errors / stats.requests)} of requests` : "",
        },
      ]}
    />
  )
}

function TotalsTiles({ totals, window }: { totals: LatencyStats; window: LatencyWindow }) {
  return <StatsTiles stats={totals} note={`in the last ${window}`} />
}

function RoutesBlock({
  data,
  window,
  sort,
  onWindow,
  onSort,
}: {
  data: LatencyOverview
  window: LatencyWindow
  sort: Sort
  onWindow: (value: LatencyWindow) => void
  onSort: (value: Sort) => void
}) {
  const routes = sortRoutes(data.routes, sort)
  return (
    <Block
      title="Routes"
      count={routes.length}
      right={<WindowChoice window={window} onChange={onWindow} />}
      text="The bar splits the route's average request into where its time went, as shares of it: the times are in the columns before it. Outside our code is the share spent waiting on ESPN, Yahoo, the NBA, Claude, Postgres, Redis, Clerk or our other services."
    >
      <div className={l.controls}>
        <Choice label="Order by" options={SORTS} value={sort} onChange={onSort} />
        <AnatomyKey groups={groupsUsed(routes.map((route) => route.parts))} />
      </div>
      {routes.length === 0 ? (
        <p className={s.empty}>No requests in the last {data.window}.</p>
      ) : (
        <div className={s.bleed}>
          <table className={s.table} style={{ minWidth: "60rem" }}>
            <thead>
              <tr>
                <th scope="col">Route</th>
                <th scope="col" className={s.num}>Requests</th>
                <th scope="col" className={s.num}>p50</th>
                <th scope="col" className={s.num}>p95</th>
                <th scope="col" className={s.num}>p99</th>
                <th scope="col" className={s.num}>5xx</th>
                <th scope="col">Where the time goes</th>
                <th scope="col" className={s.num}>Outside our code</th>
              </tr>
            </thead>
            <tbody>
              {routes.map((route) => (
                <tr key={`${route.method} ${route.route}`}>
                  <th scope="row" className={l.routeCell}>
                    <span className={l.routeName}>
                      <span className={l.method}>{route.method}</span>
                      <Link to={routeHref(route, window)} className={`${s.nameLink} ${l.path}`}>
                        {route.route}
                      </Link>
                    </span>
                  </th>
                  <td className={s.num}>{route.requests.toLocaleString()}</td>
                  <td className={s.num}>{formatMs(route.p50_ms)}</td>
                  <td className={s.num}>{formatMs(route.p95_ms)}</td>
                  <td className={s.num}>{formatMs(route.p99_ms)}</td>
                  <td className={`${s.num} ${route.errors > 0 ? l.bad : ""}`}>{route.errors > 0 ? route.errors.toLocaleString() : "—"}</td>
                  <td className={l.anatomyCell}>
                    <AnatomyBar parts={route.parts} label="Average request" />
                  </td>
                  <td className={s.num}>{formatShare(externalShare(route.parts))}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Block>
  )
}
