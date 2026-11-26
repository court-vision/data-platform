import { Link, useSearchParams } from "react-router"

import { AnatomyBar, AnatomyKey } from "@/components/Anatomy"
import { Block, Choice, Head, Notice, Page, RefreshNote, Sep, SheetSkeleton, SortHead, Tiles, type Tone } from "@/components/blocks"
import { usePageContext } from "@/components/shell/pageContext"
import { useNow } from "@/hooks/useNow"
import { LATENCY_REFETCH_MS, useLatency } from "@/hooks/useLatency"
import {
  defaultDirection,
  externalShare,
  formatMs,
  formatShare,
  formatTotal,
  groupsUsed,
  parseDirection,
  parseSort,
  parseWindow,
  routeHref,
  routeTag,
  routeTagTitle,
  sortRoutes,
  totalTime,
  WINDOWS,
  type Direction,
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
 * NBA, Postgres, our own code. The window and the order (the column and its
 * direction, set from the table's heads) live in the URL; the text names the
 * window of the data on screen, which lags the URL while another window loads.
 */
export function Latency() {
  usePageContext("Latency")
  const [params, setParams] = useSearchParams()
  const window = parseWindow(params.get("window"))
  const sort = parseSort(params.get("sort"))
  const direction = parseDirection(params.get("dir"), sort)
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

  /** A head's click: its column, biggest first (routes A to Z); the column in use turns round. */
  function orderBy(column: Sort) {
    const nextDirection: Direction =
      column === sort ? (direction === "asc" ? "desc" : "asc") : defaultDirection(column)
    setParams(
      (current) => {
        const next = new URLSearchParams(current)
        if (column === "time") next.delete("sort")
        else next.set("sort", column)
        if (nextDirection === defaultDirection(column)) next.delete("dir")
        else next.set("dir", nextDirection)
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
          <RoutesBlock
            data={data}
            window={window}
            sort={sort}
            direction={direction}
            onWindow={(value) => update("window", value, "24h")}
            onSort={orderBy}
          />
        </>
      )}
    </Page>
  )
}

function WindowChoice({ window, onChange }: { window: LatencyWindow; onChange: (value: LatencyWindow) => void }) {
  return <Choice label="Over" options={WINDOWS} value={window} onChange={onChange} />
}

/** A route's tag (AI, ESPN, NBA, CV, Other) as a chip in its anatomy colour; the hover text says what it waits on. */
export function RouteTagChip({ parts }: { parts: Record<string, number> }) {
  const tag = routeTag(parts)
  return (
    <span className={`${l.viz} ${l.tag}`} data-group={tag.group} title={routeTagTitle(parts)}>
      {tag.label}
    </span>
  )
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
  direction,
  onWindow,
  onSort,
}: {
  data: LatencyOverview
  window: LatencyWindow
  sort: Sort
  direction: Direction
  onWindow: (value: LatencyWindow) => void
  onSort: (column: Sort) => void
}) {
  const routes = sortRoutes(data.routes, sort, direction)
  const head = (column: Sort, label: string, num = true, title?: string) => (
    <SortHead
      column={column}
      label={label}
      sort={sort}
      direction={direction}
      firstDirection={defaultDirection(column)}
      onSort={onSort}
      num={num}
      title={title}
    />
  )
  return (
    <Block
      title="Routes"
      count={routes.length}
      right={<WindowChoice window={window} onChange={onWindow} />}
      text="Press a column's head to order by it, and again to turn it round. A route's tag is what it calls: AI (Claude), ESPN, NBA, Other (Yahoo or another outside service), or CV when it only touches Postgres, Redis and our own services. Total time is every request's time added up: what the route cost the service over the window. The bar splits the route's average request into where its time went, as shares of it: point at it for each part's time and share. Outside our code is the share spent waiting on ESPN, Yahoo, the NBA, Claude, Postgres, Redis, Clerk or our other services."
    >
      <div className={l.controls}>
        <AnatomyKey groups={groupsUsed(routes.map((route) => route.parts))} />
      </div>
      {routes.length === 0 ? (
        <p className={s.empty}>No requests in the last {data.window}.</p>
      ) : (
        <div className={s.bleed}>
          <table className={s.table} style={{ minWidth: "70rem" }}>
            <thead>
              <tr>
                {head("tag", "Tag", false, "What the route calls: AI, then ESPN, then NBA, then Other; CV when it only touches our own things")}
                {head("route", "Route", false)}
                {head("requests", "Requests")}
                {head("time", "Total time", true, "Every request's time added up: requests × the average request")}
                {head("p50", "p50")}
                {head("p95", "p95")}
                {head("p99", "p99")}
                {head("errors", "5xx")}
                <th scope="col">Where the time goes</th>
                {head("outside", "Outside our code")}
              </tr>
            </thead>
            <tbody>
              {routes.map((route) => (
                <tr key={`${route.method} ${route.route}`}>
                  <td className={l.tagCell}>
                    <RouteTagChip parts={route.parts} />
                  </td>
                  <th scope="row" className={l.routeCell}>
                    <span className={l.routeName}>
                      <span className={l.method}>{route.method}</span>
                      <Link to={routeHref(route, window)} className={`${s.nameLink} ${l.path}`}>
                        {route.route}
                      </Link>
                    </span>
                  </th>
                  <td className={s.num}>{route.requests.toLocaleString()}</td>
                  <td className={s.num}>{formatTotal(totalTime(route))}</td>
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
