import type { components } from "@/types/generated/api"
import { clearToken, getToken } from "@/lib/token"

export type Schemas = components["schemas"]

export class ApiError extends Error {
  readonly status: number
  readonly code: string | null

  constructor(status: number, message: string, code: string | null = null) {
    super(message)
    this.name = "ApiError"
    this.status = status
    this.code = code
  }

  get unauthorized() {
    return this.status === 401 || this.status === 403
  }
}

/** Pull `message` / `error_code` out of the API's error envelope, if it is one. */
async function readError(res: Response): Promise<ApiError> {
  let message = `${res.status} ${res.statusText}`.trim()
  let code: string | null = null
  try {
    const body = await res.json()
    if (typeof body?.message === "string") message = body.message
    else if (typeof body?.detail === "string") message = body.detail
    if (typeof body?.error_code === "string") code = body.error_code
  } catch {
    // not JSON: keep the status line
  }
  return new ApiError(res.status, message, code)
}

/**
 * Fetch a same-origin API path with the pipeline token attached.
 *
 * A rejected token is cleared here, so every caller falls back to the token
 * gate without handling 401 itself.
 */
export async function apiFetch<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = getToken()
  const res = await fetch(path, {
    ...init,
    headers: {
      Accept: "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...init.headers,
    },
  })

  if (!res.ok) {
    const error = await readError(res)
    if (error.unauthorized) clearToken()
    throw error
  }
  return (await res.json()) as T
}

/** Check a token before saving it: true when the API accepts it. */
export async function tokenIsValid(token: string): Promise<boolean> {
  const res = await fetch("/v1/dashboard/status", {
    headers: { Accept: "application/json", Authorization: `Bearer ${token}` },
  })
  if (res.status === 401 || res.status === 403) return false
  if (!res.ok) throw await readError(res)
  return true
}
