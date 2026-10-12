import { AlertTriangle, CheckCircle2, Info, Loader2, XCircle } from "lucide-react"
import { createContext, useContext, useEffect, useState } from "react"
import { Toaster } from "sonner"

import { themeScheme } from "./themes"
import { useDeskTheme } from "./useDeskTheme"
import dk from "./desk.module.css"

const PortalContext = createContext<HTMLElement | null>(null)

/** Where popovers portal to: inside the frame, so its tokens apply. */
export function useDeskPortal(): HTMLElement | null {
  return useContext(PortalContext)
}

/**
 * The one element every page renders inside, as in the frontend: tokens,
 * theme and the console's own toasts.
 */
export function DeskFrame({ children }: { children: React.ReactNode }) {
  const theme = useDeskTheme()
  const [el, setEl] = useState<HTMLDivElement | null>(null)

  // The browser's own chrome (a phone's address bar) takes the theme's ground.
  useEffect(() => {
    if (!el) return
    const ground = getComputedStyle(el).getPropertyValue("--bg").trim()
    if (ground) document.querySelector('meta[name="theme-color"]')?.setAttribute("content", ground)
  }, [el, theme])

  return (
    <div ref={setEl} className={`${dk.root} ${dk.tokens}`} data-theme={theme}>
      <PortalContext.Provider value={el}>{children}</PortalContext.Provider>
      <Toaster
        theme={themeScheme(theme)}
        position="bottom-right"
        offset={40}
        gap={8}
        icons={{
          success: <CheckCircle2 size={14} />,
          error: <XCircle size={14} />,
          warning: <AlertTriangle size={14} />,
          info: <Info size={14} />,
          loading: <Loader2 size={14} className={dk.spin} />,
        }}
        toastOptions={{
          unstyled: true,
          classNames: {
            toast: dk.toast,
            title: dk.toastTitle,
            description: dk.toastDescription,
            icon: dk.toastIcon,
            actionButton: dk.toastAction,
            cancelButton: dk.toastAction,
          },
        }}
      />
    </div>
  )
}
