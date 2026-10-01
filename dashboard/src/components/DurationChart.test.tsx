import { describe, expect, test } from "bun:test"
import { renderToStaticMarkup } from "react-dom/server"

import { DurationChart } from "@/components/DurationChart"
import type { PipelineRun } from "@/lib/runs"

// No DOM here: the chart is rendered to markup and read as text. What needs a
// browser (focus, the arrow keys) is in lib/runs.test.ts as far as it is pure.

const now = Date.parse("2026-03-05T12:00:30Z")

function run(overrides: Partial<PipelineRun> = {}): PipelineRun {
  return {
    id: "r",
    started_at: "2026-03-05T12:00:00",
    completed_at: "2026-03-05T12:00:12",
    status: "success",
    duration_seconds: 12,
    records_processed: 240,
    error_message: null,
    ...overrides,
  }
}

/** Newest first, as the API sends them. */
function runs(count: number): PipelineRun[] {
  return Array.from({ length: count }, (_, i) => run({ id: `r${i}` }))
}

function render(list: PipelineRun[]): string {
  return renderToStaticMarkup(<DurationChart runs={list} now={now} />)
}

/** The markup of one run's bar group. */
function bar(html: string, id: string): string {
  const match = html.match(new RegExp(`<g data-run="${id}".*?</g>`))
  if (!match) throw new Error(`no bar for run ${id}`)
  return match[0]
}

describe("DurationChart", () => {
  test("is one tab stop however many runs it draws, on the newest run", () => {
    const html = render(runs(200))
    expect(html.match(/tabindex="0"/g)).toHaveLength(1)
    expect(html.match(/<g data-run=/g)).toHaveLength(200)
    expect(bar(html, "r0")).toContain('tabindex="0"')
    expect(bar(html, "r199")).toContain('tabindex="-1"')
  })

  test("names the chart as a group and each bar as an image of its run", () => {
    const html = render(runs(3))
    expect(html).toContain('role="group"')
    expect(html).toContain('aria-label="Duration of the last 3 runs, oldest first. Arrow keys move between runs."')
    expect(bar(html, "r1")).toContain('role="img"')
    // A <title> on the svg is a native tooltip over the whole chart, on top of the caption.
    expect(html).not.toContain("<title")
  })

  test("the tick labels are HTML at their own size, not text scaled with the svg", () => {
    const html = render([run({ duration_seconds: 570 })])
    expect(html).not.toContain("<text")
    const axis = html.slice(0, html.indexOf("<svg"))
    expect(axis).toContain("text-[10px]")
    expect([...axis.matchAll(/<span[^>]*>([^<]+)<\/span>/g)].map((match) => match[1])).toEqual(["0s", "5m", "10m"])
  })

  test("a run that failed in 50ms still shows its colour above the baseline", () => {
    const html = render([run({ id: "fast", status: "failed", duration_seconds: 0.05 }), run({ id: "slow", duration_seconds: 60 })])
    const fast = bar(html, "fast")
    expect(fast).toContain("fill-status-loss")
    expect(fast).toMatch(/ d="M[\d.]+,120 V120 Q[\d.]+,118 /)
  })

  test("a stuck run is a hollow red marker with its own key, not a growing running bar", () => {
    const stuck = run({ id: "hung", status: "stuck", started_at: "2026-03-02T12:00:00", completed_at: null, duration_seconds: null })
    const html = render([run({ id: "ok" }), stuck])
    const hung = bar(html, "hung")
    expect(hung).toContain("fill-status-loss stroke-status-loss")
    expect(hung).toContain("stroke-dasharray")
    expect(hung).toContain("stuck · never finished")
    expect(html).toContain(">stuck</span>")
    // The other bar is still drawn to its own scale: 12s of a 20s ceiling.
    expect(bar(html, "ok")).toMatch(/Q[\d.]+,48 /)
  })

  test("the key names stuck only when there is such a run", () => {
    expect(render(runs(3))).not.toContain("stuck")
  })

  test("a failed run past the axis is broken at the top and says so", () => {
    const swept = run({ id: "swept", status: "failed", duration_seconds: 10_800, error_message: "Interrupted by service restart" })
    const html = render([swept, run({ id: "ok", duration_seconds: 30 })])
    expect(bar(html, "swept")).toContain("fill-card")
    expect(bar(html, "swept")).toContain("180m 0s (off the scale)")
    expect(bar(html, "ok")).not.toContain("fill-card")
  })

  test("no runs is a sentence, not an empty plot", () => {
    expect(render([])).toContain("No runs recorded.")
  })
})
