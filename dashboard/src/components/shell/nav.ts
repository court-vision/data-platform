import { Activity, type LucideIcon } from "lucide-react"

export interface NavItem {
  to: string
  label: string
  icon: LucideIcon
  /** Match `to` exactly (the index route) rather than as a prefix. */
  end?: boolean
}

/** One entry per page. Adding a page is a route in App.tsx plus a line here. */
export const NAV_ITEMS: readonly NavItem[] = [
  { to: "/", label: "Overview", icon: Activity, end: true },
]
