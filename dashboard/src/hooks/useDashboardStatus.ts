import { useQuery } from "@tanstack/react-query"

import { apiFetch, type Schemas } from "@/lib/api"
import { useToken } from "@/lib/token"

export const STATUS_REFETCH_MS = 30_000

export type DashboardStatus = Schemas["DashboardStatusData"]
export type PipelineHealth = Schemas["PipelineHealthEntry"]

export function useDashboardStatus() {
  const token = useToken()
  return useQuery({
    queryKey: ["dashboard", "status"],
    queryFn: async () =>
      (await apiFetch<Schemas["DashboardStatusResponse"]>("/v1/dashboard/status")).data,
    enabled: token !== null,
    refetchInterval: STATUS_REFETCH_MS,
    // A failed poll keeps the last good payload on screen (the page shows a
    // banner); retrying a rejected token would only repeat the 401.
    retry: (failures, error) =>
      !(error instanceof Error && "unauthorized" in error && error.unauthorized) && failures < 2,
  })
}
