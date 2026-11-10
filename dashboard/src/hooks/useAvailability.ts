import { queryOptions, useQuery } from "@tanstack/react-query"

import { apiFetch, type Schemas } from "@/lib/api"
import type { Days } from "@/lib/availability"
import { useToken } from "@/lib/token"

/** The availability pipeline ticks every 2 minutes; the scorecard can change no faster. */
export const AVAILABILITY_REFETCH_MS = 120_000

const KEY = ["dashboard", "availability"] as const

const giveUpOn = (error: unknown, failures: number) =>
  !(error instanceof Error && "unauthorized" in error && error.unauthorized) && failures < 2

export function availabilityQuery(days: Days) {
  return queryOptions({
    queryKey: [...KEY, days] as const,
    queryFn: async () =>
      (await apiFetch<Schemas["AvailabilityResponse"]>(`/v1/dashboard/availability?days=${days}`)).data,
    refetchInterval: AVAILABILITY_REFETCH_MS,
    // Another window's scorecard stays on screen while this one loads.
    placeholderData: (previous) => previous,
    retry: (failures, error) => giveUpOn(error, failures),
  })
}

/** The availability pipeline's verdicts against ESPN fantasy over the last `days` game dates. */
export function useAvailability(days: Days) {
  const token = useToken()
  return useQuery({ ...availabilityQuery(days), enabled: token !== null })
}
