import { useSyncExternalStore } from "react"

import { DEFAULT_DARK, isDeskTheme, themePair, themeScheme, type DeskScheme, type DeskThemeId } from "./themes"

// The frontend's key. This is another origin, so the two never share a choice:
// a theme is remembered per browser, per site.
const THEME_KEY = "cv.desk.theme"

// The choice when storage refuses it (site data blocked): until reload.
let memoryTheme: DeskThemeId | null = null

const listeners = new Set<() => void>()

function read(): DeskThemeId {
  try {
    const stored = localStorage.getItem(THEME_KEY)
    if (isDeskTheme(stored)) return stored
  } catch {
    // storage refused: fall through to memory
  }
  return memoryTheme ?? DEFAULT_DARK
}

function emit() {
  listeners.forEach((listener) => listener())
}

export function setDeskTheme(id: DeskThemeId) {
  memoryTheme = id
  try {
    localStorage.setItem(THEME_KEY, id)
  } catch {
    // kept in memory
  }
  emit()
}

/** Light to dark and back, within the theme's pair. */
export function toggleDeskTheme() {
  setDeskTheme(themePair(read()))
}

function subscribe(listener: () => void) {
  listeners.add(listener)
  // A pick in another tab repaints this one.
  window.addEventListener("storage", listener)
  return () => {
    listeners.delete(listener)
    window.removeEventListener("storage", listener)
  }
}

/** One theme for every page, remembered per browser. Midnight until storage says otherwise. */
export function useDeskTheme(): DeskThemeId {
  return useSyncExternalStore(subscribe, read, () => DEFAULT_DARK)
}

export function useDeskScheme(): DeskScheme {
  return themeScheme(useDeskTheme())
}
