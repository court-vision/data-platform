import { queryOptions, useQuery } from "@tanstack/react-query"

import { apiFetch, type Schemas } from "@/lib/api"
import { useToken } from "@/lib/token"

export const RUNS_REFETCH_MS = 30_000

/** Every pipeline's runs, at any limit: what a finished run invalidates. */
export const RUNS_KEY = ["dashboard", "runs"] as const

export function pipelineRunsQuery(name: string, limit: number) {
  return queryOptions({
    queryKey: [...RUNS_KEY, name, limit] as const,
    queryFn: async () =>
      (
        await apiFetch<Schemas["PipelineRunsResponse"]>(
          `/v1/dashboard/pipelines/${encodeURIComponent(name)}/runs?limit=${limit}`,
        )
      ).data,
    refetchInterval: RUNS_REFETCH_MS,
    // Changing the limit is a new key. The window already on screen stays
    // there while the next one loads: a skeleton would take the limit buttons,
    // and the keyboard focus on them, away with it. Another pipeline's runs
    // are never a stand-in.
    placeholderData: (previous, previousQuery) => (previousQuery?.queryKey[2] === name ? previous : undefined),
    // A 404 is "no such pipeline": retrying will not change it.
    retry: (failures, error) =>
      !(error instanceof Error && "status" in error && (error.status === 404 || (error as { unauthorized?: boolean }).unauthorized)) &&
      failures < 2,
  })
}

/** One pipeline's config and newest runs. */
export function usePipelineRuns(name: string, limit: number) {
  const token = useToken()
  return useQuery({ ...pipelineRunsQuery(name, limit), enabled: token !== null })
}
