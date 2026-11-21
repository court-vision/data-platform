import { describe, expect, test } from "bun:test"
import { renderToStaticMarkup } from "react-dom/server"

import { AnatomyBar, AnatomyTipBody } from "@/components/Anatomy"

// No DOM here: the bar and its tooltip's body render to markup and are read as text.

describe("AnatomyBar", () => {
  test("says its numbers in its label, and leaves the browser's own tooltip out", () => {
    const html = renderToStaticMarkup(<AnatomyBar parts={{ espn: 200, app: 40 }} label="Average request" />)
    expect(html).toContain('aria-label="Average request: 240 ms · ESPN 200 ms · Our code 40 ms"')
    expect(html).not.toContain("title=")
    // Closed until pointed at: no segment lit, no tooltip.
    expect(html).not.toContain("data-active")
    expect(html).not.toContain("data-hovering")
  })
})

describe("AnatomyTipBody", () => {
  test("names each part's group with its time and share, lights the one pointed at, and says what it is", () => {
    const html = renderToStaticMarkup(
      <AnatomyTipBody parts={{ espn: 200, postgres: 20, app: 35, wait: 5, clerk: 3, redis: 1 }} total={264} label="Average request" active="postgres" />,
    )
    expect(html).toContain(">Average request</span>")
    expect(html).toContain(">264 ms</span>")
    const groups = html.split('class="tipGroup"').slice(1)
    expect(groups.map((group) => group.match(/data-group="([a-z]+)"/)?.[1])).toEqual(["espn", "postgres", "app", "wait", "other"])
    expect(groups[0]).toContain(">ESPN</span><span class=\"tipNum\">200 ms</span><span class=\"tipNum\">76%</span>")
    expect(groups[1].startsWith(' data-active="true"')).toBe(true)
    expect(html.match(/data-active="true"/g)).toHaveLength(1)
    // Other folds Clerk and Redis: each gets its own line under it.
    expect(groups[4]).toContain(">Clerk</span><span class=\"tipNum\">3.0 ms</span><span class=\"tipNum\">1%</span>")
    expect(groups[4]).toContain(">Redis</span><span class=\"tipNum\">1.0 ms</span><span class=\"tipNum\">&lt;1%</span>")
    expect(html).toContain("Postgres:</span> queries, from checkout to the last row read")
  })
})
