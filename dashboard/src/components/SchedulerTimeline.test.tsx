import { describe, expect, test } from "bun:test"
import { renderToStaticMarkup } from "react-dom/server"

import { ClusterDetail, clusterSummary, SchedulerTimeline } from "@/components/SchedulerTimeline"
import { clusterBuckets, clusterRuns, parseRange, rangeMs, type CronRun, type RunBucket } from "@/lib/timeline"

const now = Date.parse("2026-09-24T18:00:00Z")

function run(overrides: Partial<CronRun> = {}): CronRun {
  return {
    id: "r1",
    job_name: "live-stats",
    triggered_at: "2026-09-24T15:00:00",
    completed_at: "2026-09-24T15:00:02",
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

const POLLS = [0, 1, 2].map((m) => run({ id: `m${m}`, triggered_at: `2026-09-24T17:0${m}:00`, result: m === 1 ? "failure" : "success" }))

describe("SchedulerTimeline", () => {
  test("polls a minute apart are one mark that says what it holds", () => {
    const html = renderToStaticMarkup(<SchedulerTimeline runs={POLLS} now={now} />)
    expect(html).toMatch(/aria-label="live-stats: 3 runs, 1 failed, from Sep 24, 12:00:00 PM CT to Sep 24, 12:02:00 PM CT"/)
    expect(html).toMatch(/data-runs="3" data-tone="failure"/)
    expect(html).toContain("last 6 hours")
  })

  test("a lone run is still its own dot", () => {
    const html = renderToStaticMarkup(<SchedulerTimeline runs={[run({ job_name: "post-game" })]} now={now} />)
    expect(html).toContain('aria-label="post-game success at Sep 24, 10:00:00 AM CT"')
    expect(html).not.toContain("data-runs=")
  })

  test("the range buttons mark the one in use, and a longer range reaches further back", () => {
    const yesterday = run({ id: "y", job_name: "post-game", triggered_at: "2026-09-23T20:00:00" })
    const short = renderToStaticMarkup(<SchedulerTimeline runs={[yesterday]} now={now} onRangeChange={() => {}} />)
    expect(short).toMatch(/aria-pressed="true"[^>]*>6h</)
    expect(short).not.toContain("post-game success at")
    const long = renderToStaticMarkup(<SchedulerTimeline runs={[yesterday]} now={now} range={parseRange("3d")} onRangeChange={() => {}} />)
    expect(long).toMatch(/aria-pressed="true"[^>]*>3d</)
    expect(long).toContain("last 3 days")
    expect(long).toContain("post-game success at Sep 23, 3:00:00 PM")
    expect(long).toContain(">Thu 1 PM<") // day-and-hour ticks, ending at now
  })

  test("without a handler there is nothing to press", () => {
    expect(renderToStaticMarkup(<SchedulerTimeline runs={[]} now={now} />)).not.toContain("aria-pressed")
  })

  test("a range still loading, cut short, or failed says so", () => {
    const loading = renderToStaticMarkup(<SchedulerTimeline runs={[]} now={now} range={parseRange("7d")} loading />)
    expect(loading).toContain("loading…")
    expect(loading).not.toContain("no runs in window")
    const cut = renderToStaticMarkup(<SchedulerTimeline runs={POLLS} now={now} range={parseRange("7d")} truncated />)
    expect(cut).toContain("the oldest runs in this window are not shown")
    const failed = renderToStaticMarkup(<SchedulerTimeline runs={[]} now={now} range={parseRange("24h")} error="HTTP 500" />)
    expect(failed).toContain("Could not load this range: HTTP 500")
  })
})

describe("a window counted on the server", () => {
  const week = parseRange("7d")
  const bucketMs = rangeMs(week) / 96
  const column = Math.floor(Date.parse("2026-09-22T03:00:00Z") / bucketMs) * bucketMs
  const polls: RunBucket = {
    job_name: "live-stats",
    start: new Date(column).toISOString().slice(0, 19),
    runs: 210,
    failed: 8,
    retried: 0,
    first_triggered_at: "2026-09-22T02:15:00",
    last_triggered_at: "2026-09-22T03:59:30",
  }
  const bracket: RunBucket = { ...polls, job_name: "playoffs", runs: 1, failed: 0, first_triggered_at: "2026-09-22T03:00:00", last_triggered_at: "2026-09-22T03:00:00" }
  const carried = [
    run({ id: "latest", triggered_at: "2026-09-22T03:59:30", duration_seconds: 0.4 }),
    ...[3, 4, 5, 6, 7].map((i) =>
      run({ id: `f${i}`, triggered_at: `2026-09-22T03:0${i}:00`, result: "failure", http_status: 503, attempts: 3, duration_seconds: 31, error_message: "endpoint returned status 503" }),
    ),
    run({ id: "bracket", job_name: "playoffs", triggered_at: "2026-09-22T03:00:00" }),
  ]
  const counted = { buckets: [polls, bracket], bucketMs }

  test("a column is one mark holding every run counted, not only the ones carried", () => {
    const html = renderToStaticMarkup(<SchedulerTimeline runs={carried} now={now} range={week} counted={counted} />)
    expect(html).toMatch(/data-runs="210" data-tone="failure"/)
    expect(html).toContain('aria-label="live-stats: 210 runs, 8 failed, from Sep 21, 9:15:00 PM CT to Sep 21, 10:59:30 PM CT"')
    // The card counts what the window holds: 210 polls and one bracket refresh.
    expect(html).toContain(">211</span>")
    // Only the two lanes that fired nothing say so.
    expect(html.match(/no runs in window/g)).toHaveLength(2)
  })

  test("a run alone in its column is still its own dot", () => {
    const html = renderToStaticMarkup(<SchedulerTimeline runs={carried} now={now} range={week} counted={counted} />)
    expect(html).toContain('aria-label="playoffs success at Sep 21, 10:00:00 PM CT"')
  })

  test("its popover lists the failures carried and counts the rest", () => {
    const [mark] = clusterBuckets({ buckets: [polls], bucketMs }, carried.filter((r) => r.job_name === "live-stats"), now, rangeMs(week))
    const html = renderToStaticMarkup(<ClusterDetail job="live-stats" cluster={mark} />)
    expect(html.match(/failure after 3 attempts/g)).toHaveLength(5)
    expect(html).toContain("and 3 more")
  })
})

test("clusterSummary counts what went wrong, or says nothing did", () => {
  const [mixed] = clusterRuns([...POLLS, run({ id: "m3", triggered_at: "2026-09-24T17:03:00", attempts: 2 })], now)
  expect(clusterSummary(mixed)).toBe("4 runs, 1 failed, 1 retried")
  const [clean] = clusterRuns([POLLS[0], POLLS[2]], now)
  expect(clusterSummary(clean)).toBe("2 runs, all succeeded")
})
