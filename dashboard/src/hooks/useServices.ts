import { useQuery } from "@tanstack/react-query"

import { apiFetch, type Schemas } from "@/lib/api"
import { useToken } from "@/lib/token"

export type ServiceInfo = Schemas["ServiceInfo"]

/** What each service is running. Changes on deploy, so a slower poll than status. */
export function useServices() {
  const token = useToken()
  return useQuery({
    queryKey: ["dashboard", "services"],
    queryFn: async () => (await apiFetch<Schemas["ServicesResponse"]>("/v1/dashboard/services")).data,
    enabled: token !== null,
    refetchInterval: 60_000,
  })
}
