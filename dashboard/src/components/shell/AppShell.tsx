import { LogOut } from "lucide-react"
import { NavLink, Outlet } from "react-router"

import { NAV_ITEMS } from "@/components/shell/nav"
import { Wordmark } from "@/components/shell/Wordmark"
import { Button } from "@/components/ui/button"
import { clearToken } from "@/lib/token"
import { cn } from "@/lib/utils"

export function AppShell() {
  return (
    <div className="flex h-full flex-col md:flex-row">
      <aside className="flex shrink-0 items-center gap-4 border-b bg-card/60 px-4 py-3 md:w-56 md:flex-col md:items-stretch md:gap-6 md:border-b-0 md:border-r md:px-3 md:py-5">
        <div className="md:px-2">
          <Wordmark />
        </div>

        <nav aria-label="Pages" className="flex flex-1 gap-1 md:flex-col">
          {NAV_ITEMS.map(({ to, label, icon: Icon, end }) => (
            <NavLink
              key={to}
              to={to}
              end={end}
              className={({ isActive }) =>
                cn(
                  "flex items-center gap-2.5 rounded-md px-2.5 py-2 text-sm text-muted-foreground transition-colors hover:bg-accent hover:text-foreground",
                  isActive && "bg-accent font-semibold text-primary hover:text-primary",
                )
              }
            >
              <Icon className="size-4" aria-hidden />
              {label}
            </NavLink>
          ))}
        </nav>

        <Button
          variant="ghost"
          size="sm"
          onClick={clearToken}
          className="justify-start gap-2.5 px-2.5 text-muted-foreground"
        >
          <LogOut className="size-4" aria-hidden />
          <span className="hidden md:inline">Forget token</span>
        </Button>
      </aside>

      <main className="page-enter min-w-0 flex-1 overflow-y-auto">
        <Outlet />
      </main>
    </div>
  )
}
