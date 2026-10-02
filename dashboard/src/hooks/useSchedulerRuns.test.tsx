import { describe, expect, test } from "bun:test"
import { QueryClient, QueryObserver } from "@tanstack/react-query"
import { renderToStaticMarkup } from "react-dom/server"

import { SchedulerTimeline } from "@/components/SchedulerTimeline"
import { schedulerRunsQuery, schedulerWindow, type SchedulerRuns } from "@/hooks/useSchedulerRuns"
import { ApiError } from "@/lib/api"
import { parseRange, rangeMs, type CronRun, type RunBucket } from "@/lib/timeline"

const now = Date.parse("2026-09-24T18:00:00Z")

function run(overrides: Partial<CronRun> = {}): CronRun {
  return {
    id: "r1",
    job_name: "post-game",
    triggered_at: "2026-09-24T09:00:00",
    completed_at: "2026-09-24T09:00:02",
    duration_ms: 2000,
    duration_seconds: 2,
    result: "success",
    http_status: 200,
    attempts: 1,
    error_message: null,
    response_snippet: null,
    ...overrides,
  }
}

/** A day's answer: every run, as rows. */
const DAY: SchedulerRuns = { hours: 24, runs: [run()], buckets: null, bucket_seconds: null, truncated: true, fetched_at: "2026-09-24T18:00:00Z" }

/** A week's answer: counted columns, and the rows a mark can open. */
const WEEK_COLUMN_MS = rangeMs(parseRange("7d")) / 96
const COLUMN: RunBucket = {
  job_name: "live-stats",
  start: new Date(Math.floor(Date.parse("2026-09-23T20:00:00Z") / WEEK_COLUMN_MS) * WEEK_COLUMN_MS).toISOString().slice(0, 19),
  runs: 210,
  failed: 0,
  retried: 0,
  first_triggered_at: "2026-09-23T19:00:00",
  last_triggered_at: "2026-09-23T20:00:00",
}
const WEEK: SchedulerRuns = {
  hours: 168,
  runs: [run({ id: "newest", job_name: "live-stats", triggered_at: "2026-09-23T20:00:00" })],
  buckets: [COLUMN],
  bucket_seconds: WEEK_COLUMN_MS / 1000,
  truncated: false,
  fetched_at: "2026-09-24T18:00:00Z",
}

/** The query as the Overview holds it just after the range button is pressed: the next range not yet answered. */
function pressed(from: SchedulerRuns, toHours: number) {
  const client = new QueryClient()
  client.setQueryData(schedulerRunsQuery(from.hours).queryKey, from)
  const observer = new QueryObserver(client, { ...schedulerRunsQuery(from.hours), enabled: false })
  observer.setOptions({ ...schedulerRunsQuery(toHours), enabled: false })
  return observer.getCurrentResult()
}

describe("schedulerWindow", () => {
  test("widening the range draws none of the narrower answer, and says it is loading", () => {
    const query = pressed(DAY, 168)
    expect([query.isPlaceholderData, query.data?.hours]).toEqual([true, 24])

    const shown = schedulerWindow(parseRange("7d"), query)
    // The day's runs on the week's axis read as six days in which nothing ran;
    // and what was cut from the day says nothing about the week.
    expect(shown).toEqual({ runs: [], counted: null, loading: true, truncated: false, error: null })

    const html = renderToStaticMarkup(<SchedulerTimeline now={now} range={parseRange("7d")} {...shown} />)
    expect(html).toContain('aria-busy="true"')
    expect(html).toContain(">…</span>") // the count is not known yet
    expect(html.match(/loading…/g)).toHaveLength(3)
    expect(html).not.toContain("post-game success at")
    expect(html).not.toContain("the oldest runs in this window are not shown")
  })

  test("narrowing draws the wider answer, which covers the window, until its own lands", () => {
    const query = pressed(WEEK, 72)
    expect(query.isPlaceholderData).toBe(true)

    const shown = schedulerWindow(parseRange("3d"), query)
    expect(shown.counted).toEqual({ buckets: [COLUMN], bucketMs: WEEK_COLUMN_MS })
    expect(shown.runs).toBe(WEEK.runs)
    expect(shown.loading).toBe(true)

    const html = renderToStaticMarkup(<SchedulerTimeline now={now} range={parseRange("3d")} {...shown} />)
    expect(html).toMatch(/data-runs="210" data-tone="success"/)
    expect(html).toContain('aria-busy="true"')
  })

  test("the range's own answer is drawn as it came", () => {
    const client = new QueryClient()
    client.setQueryData(schedulerRunsQuery(24).queryKey, DAY)
    const query = new QueryObserver(client, { ...schedulerRunsQuery(24), enabled: false }).getCurrentResult()
    expect(schedulerWindow(parseRange("24h"), query)).toEqual({ runs: DAY.runs, counted: null, loading: false, truncated: true, error: null })
  })

  test("a range that has not answered is loading, and one that failed says why instead", () => {
    expect(schedulerWindow(parseRange("3d"), { data: undefined, error: null, isPlaceholderData: false }).loading).toBe(true)
    const failed = schedulerWindow(parseRange("3d"), { data: undefined, error: new ApiError(502, "Bad Gateway"), isPlaceholderData: false })
    expect([failed.loading, failed.error]).toEqual([false, "Bad Gateway"])
  })
})

describe("schedulerRunsQuery", () => {
  const retry = schedulerRunsQuery(72).retry as (failures: number, error: Error) => boolean

  test("gives up after two failures, like the page's other queries", () => {
    // The default is three retries with backoff: seven seconds of "loading…" before the error shows.
    const down = new ApiError(502, "Bad Gateway")
    expect([retry(0, down), retry(1, down), retry(2, down)]).toEqual([true, true, false])
  })

  test("does not retry a rejected token", () => {
    expect(retry(0, new ApiError(401, "Unauthorized"))).toBe(false)
  })
})
