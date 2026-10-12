import { Activity, Database, FlaskConical, SlidersHorizontal, type LucideIcon } from "lucide-react"

export interface Page {
  to: string
  label: string
  /** Shown instead of the label on a phone, where four words do not fit the bar. */
  icon: LucideIcon
  /** The number key that opens it. */
  key: string
}

/** One tab per page. Adding a page is a route in App.tsx plus a line here. */
export const PAGES: readonly Page[] = [
  { to: "/", label: "Overview", icon: Activity, key: "1" },
  { to: "/data", label: "Data", icon: Database, key: "2" },
  { to: "/quality", label: "Quality", icon: FlaskConical, key: "3" },
  { to: "/projections", label: "Projections", icon: SlidersHorizontal, key: "4" },
]

/**
 * The tab a path belongs to. A pipeline is opened from the Overview's rows and
 * a quality run from the Quality page, so each stays lit under its own tab.
 */
export function pageFor(pathname: string): Page | null {
  if (pathname === "/" || pathname.startsWith("/pipelines/")) return PAGES[0]
  return PAGES.find((page) => page.to !== "/" && (pathname === page.to || pathname.startsWith(`${page.to}/`))) ?? null
}
