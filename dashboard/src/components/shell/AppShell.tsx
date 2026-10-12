import { useQueryClient } from "@tanstack/react-query"
import { useCallback, useEffect, useRef, useState } from "react"
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
  const refresh = useCallback(() => void queryClient.refetchQueries({ type: "active" }), [queryClient])
  useShellKeys(refresh)

  // A new page opens at its top, and the keyboard starts there too, not on the
  // tab or link that opened it. A change of query (a range, a filter) moves neither.
  const { pathname } = useLocation()
  const sheet = useRef<HTMLElement>(null)
  const firstPage = useRef(true)
  useEffect(() => {
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

/** 1–4 open the pages, R refreshes what is on screen, T swaps the theme for its pair. */
function useShellKeys(refresh: () => void) {
  const navigate = useNavigate()
  useEffect(() => {
    const typing = (target: EventTarget | null) => {
      const el = target as HTMLElement | null
      return !!el && (el.isContentEditable || ["INPUT", "TEXTAREA", "SELECT"].includes(el.tagName))
    }
    const onKey = (event: KeyboardEvent) => {
      if (event.metaKey || event.ctrlKey || event.altKey || event.repeat || typing(event.target)) return
      const page = PAGES.find((p) => p.key === event.key)
      if (page) navigate(page.to)
      else if (event.key === "r" || event.key === "R") refresh()
      else if (event.key === "t" || event.key === "T") toggleDeskTheme()
    }
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
  }, [navigate, refresh])
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
