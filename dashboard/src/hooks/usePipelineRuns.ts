import { useQuery } from "@tanstack/react-query"

import { apiFetch, type Schemas } from "@/lib/api"
import { useToken } from "@/lib/token"

export const RUNS_REFETCH_MS = 30_000

/** One pipeline's config and newest runs. */
export function usePipelineRuns(name: string, limit: number) {
  const token = useToken()
  return useQuery({
    queryKey: ["dashboard", "runs", name, limit],
    queryFn: async () =>
      (
        await apiFetch<Schemas["PipelineRunsResponse"]>(
          `/v1/dashboard/pipelines/${encodeURIComponent(name)}/runs?limit=${limit}`,
        )
      ).data,
    enabled: token !== null,
    refetchInterval: RUNS_REFETCH_MS,
    // A 404 is "no such pipeline": retrying will not change it.
    retry: (failures, error) =>
      !(error instanceof Error && "status" in error && (error.status === 404 || (error as { unauthorized?: boolean }).unauthorized)) &&
      failures < 2,
  })
}
