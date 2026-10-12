import { ArrowUpRight, LogOut, RotateCw } from "lucide-react"
import { Link, useLocation } from "react-router"

import { PAGES, pageFor } from "@/components/shell/nav"
import { ThemePicker } from "@/desk/ThemePicker"
import { clearToken } from "@/lib/token"
import dk from "@/desk/desk.module.css"
import s from "./shell.module.css"

/** The app this console runs the data for: the mark's way out. */
export const APP_URL = "https://www.courtvision.dev"

/**
 * The top of every page, as on the frontend's desks: the mark, the pages as
 * tabs, what this page is showing, then the bar's controls. Everything after
 * the tabs scrolls sideways when it does not fit, so no control is squeezed off
 * a phone.
 */
export function DeskBar({ context, onRefresh }: { context: string | null; onRefresh: () => void }) {
  const { pathname } = useLocation()
  const current = pageFor(pathname)
  return (
    <header className={dk.bar}>
      {/* The frontend's wordmark: the full word from md, its initials on phones. */}
      <Link to="/" className={dk.mark} title="Data platform overview" aria-label="Court Vision data platform">
        <span className={dk.markCompact} aria-hidden>
          C<span className={dk.markAccent}>V</span>
        </span>
        <span className={dk.markText} aria-hidden>
          COURT<span className={dk.markAccent}>VISION</span>
        </span>
      </Link>
      <nav className={dk.tabs} aria-label="Pages">
        {PAGES.map(({ to, label, icon: Icon, key }) => (
          <Link
            key={to}
            to={to}
            className={`${dk.tab} ${s.tab}`}
            aria-current={current?.to === to ? "page" : undefined}
            title={`${label} (${key})`}
          >
            <Icon size={14} className={s.tabIcon} aria-hidden />
            <span className={s.tabLabel}>{label}</span>
          </Link>
        ))}
      </nav>
      <div className={dk.barScroll}>
        <span className={dk.divider} />
        <span className={`${dk.label} ${s.context}`}>
          Data platform{context ? ` · ${context}` : ""}
        </span>
        <span className={dk.spacer} />
        <button type="button" className={dk.iconBtn} onClick={onRefresh} aria-label="Refresh" title="Refresh (R)">
          <RotateCw size={14} />
        </button>
        <ThemePicker />
        <a href={APP_URL} className={`${dk.iconBtn} ${dk.iconLink}`} aria-label="Court Vision" title="Court Vision, the app">
          <ArrowUpRight size={14} />
        </a>
        <button
          type="button"
          className={dk.iconBtn}
          onClick={clearToken}
          aria-label="Forget token"
          title="Forget the pipeline token in this browser"
        >
          <LogOut size={14} />
        </button>
      </div>
    </header>
  )
}

/** The bottom line of every page: the keys that work here, then what is deployed. */
export function DeskStatus({ keys, children }: { keys: Array<[string, string]>; children?: React.ReactNode }) {
  return (
    <footer className={dk.status}>
      <span className={`${dk.keys} ${s.keys}`}>
        {keys.map(([k, label]) => (
          <span key={`${k}-${label}`}>
            <span className={dk.kbd}>{k}</span> {label}
          </span>
        ))}
      </span>
      <span className={dk.spacer} />
      {children}
    </footer>
  )
}
