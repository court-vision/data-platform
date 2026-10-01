import { useQuery } from "@tanstack/react-query"

import { apiFetch, type Schemas } from "@/lib/api"
import { useToken } from "@/lib/token"

export const FRESHNESS_REFETCH_MS = 60_000

/** What date each pipeline's table runs through. Its own route, its own poll. */
export function useFreshness() {
  const token = useToken()
  return useQuery({
    queryKey: ["dashboard", "freshness"],
    queryFn: async () => (await apiFetch<Schemas["FreshnessResponse"]>("/v1/dashboard/freshness")).data,
    enabled: token !== null,
    refetchInterval: FRESHNESS_REFETCH_MS,
    retry: (failures, error) =>
      !(error instanceof Error && "unauthorized" in error && error.unauthorized) && failures < 2,
  })
}
