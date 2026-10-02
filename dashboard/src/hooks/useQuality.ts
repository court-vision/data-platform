import { queryOptions, useQuery } from "@tanstack/react-query"

import { apiFetch, type Schemas } from "@/lib/api"
import { useToken } from "@/lib/token"

export const QUALITY_REFETCH_MS = 60_000

/** Every quality query: what a finished run of the checks invalidates. */
export const QUALITY_KEY = ["dashboard", "quality"] as const

const notWorthRetrying = (error: unknown) =>
  error instanceof Error && "status" in error && (error.status === 404 || (error as { unauthorized?: boolean }).unauthorized)

export function qualityOverviewQuery(limit: number) {
  return queryOptions({
    queryKey: [...QUALITY_KEY, "overview", limit] as const,
    queryFn: async () =>
      (await apiFetch<Schemas["QualityOverviewResponse"]>(`/v1/dashboard/quality?limit=${limit}`)).data,
    refetchInterval: QUALITY_REFETCH_MS,
    // A new limit is a new key; the window on screen stays while it loads.
    placeholderData: (previous) => previous,
    retry: (failures, error) => !notWorthRetrying(error) && failures < 2,
  })
}

export function qualityRunQuery(runId: string) {
  return queryOptions({
    queryKey: [...QUALITY_KEY, "run", runId] as const,
    queryFn: async () =>
      (await apiFetch<Schemas["QualityRunDetailResponse"]>(`/v1/dashboard/quality/runs/${encodeURIComponent(runId)}`)).data,
    // A finished run never changes; one still running is worth watching, and
    // either may gain a newer neighbour.
    refetchInterval: (query) => (query.state.data?.run.status === "running" ? 15_000 : QUALITY_REFETCH_MS),
    retry: (failures, error) => !notWorthRetrying(error) && failures < 2,
  })
}

/** Every check's definition and its result in the newest runs. */
export function useQualityOverview(limit: number) {
  const token = useToken()
  return useQuery({ ...qualityOverviewQuery(limit), enabled: token !== null })
}

/** One run: every check's outcome. */
export function useQualityRun(runId: string) {
  const token = useToken()
  return useQuery({ ...qualityRunQuery(runId), enabled: token !== null })
}
