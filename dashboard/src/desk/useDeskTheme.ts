import { useSyncExternalStore } from "react"

import { DEFAULT_DARK, isDeskTheme, themePair, themeScheme, type DeskScheme, type DeskThemeId } from "./themes"

// The frontend's key. This is another origin, so the two never share a choice:
// a theme is remembered per browser, per site.
const THEME_KEY = "cv.desk.theme"

// The choice when storage refuses it (site data blocked, quota): until reload.
// Only set then, so a pick in another tab still repaints this one.
let memoryTheme: DeskThemeId | null = null

const listeners = new Set<() => void>()

function read(): DeskThemeId {
  if (memoryTheme) return memoryTheme
  try {
    const stored = localStorage.getItem(THEME_KEY)
    if (isDeskTheme(stored)) return stored
  } catch {
    // storage refused: the default
  }
  return DEFAULT_DARK
}

function emit() {
  listeners.forEach((listener) => listener())
}

export function setDeskTheme(id: DeskThemeId) {
  try {
    localStorage.setItem(THEME_KEY, id)
    memoryTheme = null
  } catch {
    // Storage refused the write (it may still read an older pick): memory wins until reload.
    memoryTheme = id
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
