import { describe, expect, test } from "bun:test"
import { renderToStaticMarkup } from "react-dom/server"
import { MemoryRouter } from "react-router"

import { DeskBar } from "@/components/shell/DeskBar"
import { PAGES, pageFor } from "@/components/shell/nav"

function render(path: string, context: string | null = null): string {
  return renderToStaticMarkup(
    <MemoryRouter initialEntries={[path]}>
      <DeskBar context={context} onRefresh={() => {}} />
    </MemoryRouter>,
  )
}

/** The tab marked as the current page, by its label. */
function current(html: string): string | undefined {
  return html.match(/aria-current="page"[^>]*>(?:<svg.*?<\/svg>)?<span class="tabLabel">([^<]+)</)?.[1]
}

describe("DeskBar", () => {
  test("every page is a tab, its word beside an icon that stands in for it on a phone", () => {
    const html = render("/")
    for (const { to, label, key } of PAGES) {
      expect(html).toContain(`href="${to}"`)
      expect(html).toContain(`title="${label} (${key})"`)
      expect(html).toContain(`<span class="tabLabel">${label}</span>`)
    }
    expect(html.match(/class="[^"]*tabIcon/g)).toHaveLength(PAGES.length)
  })

  test("a pipeline stays under the Overview and a quality run under Quality", () => {
    expect(current(render("/"))).toBe("Overview")
    expect(current(render("/pipelines/post_game_stats"))).toBe("Overview")
    expect(current(render("/data"))).toBe("Data")
    expect(current(render("/quality/runs/abc"))).toBe("Quality")
    expect(current(render("/projections?open=2"))).toBe("Projections")
    expect(current(render("/nowhere"))).toBeUndefined()
  })

  test("names what the page is showing after the console's own name", () => {
    expect(render("/pipelines/post_game_stats", "post_game_stats")).toContain("Data platform · post_game_stats")
    expect(render("/")).toContain(">Data platform</span>")
  })
})

describe("pageFor", () => {
  test("a prefix is a page only at a segment boundary", () => {
    expect(pageFor("/quality")?.label).toBe("Quality")
    expect(pageFor("/qualityx")).toBeNull()
    expect(pageFor("/data/")?.label).toBe("Data")
  })
})
