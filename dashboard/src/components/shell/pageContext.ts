import { createContext, useContext, useEffect } from "react"

/** The shell's setter for what the bar names after "Data platform ·". */
export const PageContextSetter = createContext<(context: string | null) => void>(() => {})

/**
 * Name what this page is showing: in the bar beside the tabs, and as the
 * browser tab's title. Cleared when the page goes.
 */
export function usePageContext(context: string | null) {
  const set = useContext(PageContextSetter)
  useEffect(() => {
    set(context)
    if (typeof document !== "undefined") document.title = context ? `${context} · Data Platform` : "Court Vision · Data Platform"
  }, [set, context])
  useEffect(() => () => set(null), [set])
}
