import { useQueryClient } from "@tanstack/react-query"
import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react"
import { Outlet, useLocation, useNavigate } from "react-router"

import { DeskBar, DeskStatus } from "@/components/shell/DeskBar"
import { PAGES } from "@/components/shell/nav"
import { PageContextSetter } from "@/components/shell/pageContext"
import { toggleDeskTheme } from "@/desk/useDeskTheme"
import { useServices } from "@/hooks/useServices"
import { formatUptime } from "@/lib/time"
import dk from "@/desk/desk.module.css"
import s from "./shell.module.css"

const KEYS: Array<[string, string]> = [
  ["1–4", "page"],
  ["R", "refresh"],
  ["T", "theme"],
]

/** The bar, the page (the only scroller), and the status line. */
export function AppShell() {
  const [context, setContext] = useState<string | null>(null)
  const queryClient = useQueryClient()
  // A fetch already in flight is left to finish, not restarted: each Projections
  // fetch rebuilds the projection on the server, so a press must not cancel one.
  const refresh = useCallback(() => void queryClient.refetchQueries({ type: "active" }, { cancelRefetch: false }), [queryClient])
  useShellKeys(refresh)

  // A new page opens at its top, and the keyboard starts there too, not on the
  // tab or link that opened it. A change of query (a range, a filter) moves neither.
  const { pathname } = useLocation()
  const sheet = useRef<HTMLElement>(null)
  const firstPage = useRef(true)
  // Before paint, so the new page never shows a frame at the old one's offset.
  useLayoutEffect(() => {
    sheet.current?.scrollTo({ top: 0 })
    if (firstPage.current) {
      firstPage.current = false
      return
    }
    sheet.current?.focus({ preventScroll: true })
  }, [pathname])

  return (
    <PageContextSetter.Provider value={setContext}>
      <DeskBar context={context} onRefresh={refresh} />
      <main ref={sheet} className={s.sheet} tabIndex={-1}>
        <Outlet />
      </main>
      <DeskStatus keys={KEYS}>
        <ServiceLine />
      </DeskStatus>
    </PageContextSetter.Provider>
  )
}

/**
 * 1–4 open the pages, R refreshes what is on screen, T swaps the theme for its
 * pair; none while typing. The page keys also wait outside a form, a dialog or
 * a menu: a button keeps focus after a click, and "1.5" typed after "Add a
 * multiplier" must not leave the unsaved adjustment for the Overview.
 */
function useShellKeys(refresh: () => void) {
  const navigate = useNavigate()
  const { pathname } = useLocation()
  useEffect(() => {
    const element = (target: EventTarget | null) => (target instanceof HTMLElement ? target : null)
    const typing = (el: HTMLElement | null) => !!el && (el.isContentEditable || ["INPUT", "TEXTAREA", "SELECT"].includes(el.tagName))
    const working = (el: HTMLElement | null) =>
      !!el && el.closest('form, [role="dialog"], [role="menu"], [data-radix-popper-content-wrapper]') !== null
    const onKey = (event: KeyboardEvent) => {
      const el = element(event.target)
      if (event.metaKey || event.ctrlKey || event.altKey || event.repeat || typing(el)) return
      const page = PAGES.find((p) => p.key === event.key)
      if (page) {
        // The page already open keeps its query (a search, a range, an open row).
        if (!working(el) && pathname !== page.to) navigate(page.to)
      } else if (event.key === "r" || event.key === "R") refresh()
      else if (event.key === "t" || event.key === "T") toggleDeskTheme()
    }
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
  }, [navigate, pathname, refresh])
}

/** What each service is running, on every page: a deploy shows here first. */
function ServiceLine() {
  const services = useServices()
  if (!services.data) return null
  return (
    <span className={s.services}>
      {services.data.services.map((service) => {
        const state = !service.configured ? "unknown" : service.ok ? "ok" : "down"
        return (
          <span key={service.key} className={s.service} title={service.error ?? undefined}>
            <span className={s.dot} data-state={state} />
            {service.name.toLowerCase()} {service.version ?? "—"}
            {service.environment ? <span className={dk.sub}> · {service.environment}</span> : null}
            {service.uptime_s != null ? <span className={s.uptime}> · up {formatUptime(service.uptime_s)}</span> : null}
          </span>
        )
      })}
    </span>
  )
}
