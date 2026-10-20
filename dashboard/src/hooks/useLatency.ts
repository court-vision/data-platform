import { queryOptions, useQuery } from "@tanstack/react-query"

import { apiFetch, type Schemas } from "@/lib/api"
import type { LatencyWindow } from "@/lib/latency"
import { useToken } from "@/lib/token"

/** The backend writes its timings every 10 s; a minute is fresh enough for a page read by eye. */
export const LATENCY_REFETCH_MS = 60_000

const KEY = ["dashboard", "latency"] as const
const BASE = "/v1/dashboard/latency"

const giveUpOn = (error: unknown, failures: number) =>
  !(error instanceof Error && "unauthorized" in error && error.unauthorized) && failures < 2

export function latencyQuery(window: LatencyWindow) {
  return queryOptions({
    queryKey: [...KEY, window] as const,
    queryFn: async () => (await apiFetch<Schemas["LatencyResponse"]>(`${BASE}?window=${window}`)).data,
    refetchInterval: LATENCY_REFETCH_MS,
    // Another window's routes stay on screen while this one loads, so the
    // window buttons (and the focus on them) do not drop out under a skeleton.
    placeholderData: (previous) => previous,
    retry: (failures, error) => giveUpOn(error, failures),
  })
}

/** Every backend route over the window, the costliest first. */
export function useLatency(window: LatencyWindow) {
  const token = useToken()
  return useQuery({ ...latencyQuery(window), enabled: token !== null })
}

export function routeLatencyQuery(method: string, route: string, window: LatencyWindow) {
  return queryOptions({
    queryKey: [...KEY, "route", method, route, window] as const,
    queryFn: async () => {
      const params = new URLSearchParams({ method, route, window })
      return (await apiFetch<Schemas["RouteLatencyResponse"]>(`${BASE}/route?${params.toString()}`)).data
    },
    refetchInterval: LATENCY_REFETCH_MS,
    // Only the same route's other window stands in while a window loads.
    placeholderData: (previous, previousQuery) =>
      previousQuery?.queryKey[3] === method && previousQuery?.queryKey[4] === route ? previous : undefined,
    retry: (failures, error) => giveUpOn(error, failures),
  })
}

/** One route: its times over the window and its slowest requests. */
export function useRouteLatency(method: string, route: string, window: LatencyWindow) {
  const token = useToken()
  return useQuery({ ...routeLatencyQuery(method, route, window), enabled: token !== null && route !== "" })
}
