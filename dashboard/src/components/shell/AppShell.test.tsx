import { describe, expect, test } from "bun:test"
import { renderToStaticMarkup } from "react-dom/server"
import { MemoryRouter } from "react-router"

import { AppShell } from "@/components/shell/AppShell"
import { NAV_ITEMS } from "@/components/shell/nav"

describe("AppShell", () => {
  const html = renderToStaticMarkup(
    <MemoryRouter>
      <AppShell />
    </MemoryRouter>,
  )

  test("a page's nav label takes no room below md, and is still the link's name", () => {
    // The phone header is one row that cannot wrap or scroll (html and body
    // are overflow: hidden), so every visible label is width the Forget-token
    // button loses: two of them put it off a 375 px screen.
    for (const { label } of NAV_ITEMS) {
      expect(html).toContain(`<span class="sr-only md:not-sr-only">${label}</span>`)
    }
  })
})
