import { useQuery } from "@tanstack/react-query"

import { apiFetch, type Schemas } from "@/lib/api"
import { DEFAULT_RANGE } from "@/lib/timeline"
import { useToken } from "@/lib/token"

/** A day is watched like the page's other panels; several days are a heavier reply and change less. */
export function schedulerRefetchMs(hours: number): number {
  return hours <= 24 ? 60_000 : 5 * 60_000
}

export function schedulerRunsQuery(hours: number) {
  return {
    queryKey: ["dashboard", "scheduler", hours] as const,
    queryFn: async () =>
      (await apiFetch<Schemas["SchedulerRunsResponse"]>(`/v1/dashboard/scheduler?hours=${hours}`)).data,
    refetchInterval: schedulerRefetchMs(hours),
  }
}

/**
 * Cron runs over a window longer than the status payload's six hours. Idle at
 * the default range: those runs are already on the page.
 */
export function useSchedulerRuns(hours: number) {
  const token = useToken()
  return useQuery({
    ...schedulerRunsQuery(hours),
    enabled: token !== null && hours !== DEFAULT_RANGE.hours,
    // A new range is a new key; the window on screen stays while it loads.
    placeholderData: (previous) => previous,
  })
}
