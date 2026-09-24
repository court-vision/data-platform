import { useSyncExternalStore } from "react"

// Same key the Jinja dashboard used, on the same origin: a token saved there
// carries over, so the rewrite does not ask for it again.
const TOKEN_KEY = "cv_pipeline_token"

// Dev only: `VITE_DEV_TOKEN=... bun run dev` starts signed in, so a fresh
// browser profile does not need the token pasted. `import.meta.env.DEV` is a
// build-time constant, so none of this reaches a production bundle.
let devToken: string | null = import.meta.env.DEV ? (import.meta.env.VITE_DEV_TOKEN ?? null) : null

// Where the token lives when storage refuses it (site data blocked): until
// reload. Only set then, so a sign-out in another tab still signs this one out.
let memoryToken: string | null = null

const listeners = new Set<() => void>()

function read(): string | null {
  try {
    return localStorage.getItem(TOKEN_KEY) ?? memoryToken ?? devToken
  } catch {
    return memoryToken ?? devToken
  }
}

function emit() {
  listeners.forEach((listener) => listener())
}

export function getToken(): string | null {
  return read()
}

export function setToken(token: string) {
  try {
    localStorage.setItem(TOKEN_KEY, token)
    memoryToken = null
  } catch {
    memoryToken = token
  }
  emit()
}

export function clearToken() {
  devToken = null // "Forget token" must work in dev too, until the next reload
  memoryToken = null
  try {
    localStorage.removeItem(TOKEN_KEY)
  } catch {
    // nothing to clear
  }
  emit()
}

function subscribe(listener: () => void) {
  listeners.add(listener)
  // Another tab signing out signs this one out too.
  window.addEventListener("storage", listener)
  return () => {
    listeners.delete(listener)
    window.removeEventListener("storage", listener)
  }
}

export function useToken(): string | null {
  return useSyncExternalStore(subscribe, read, () => null)
}
