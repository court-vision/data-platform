import { queryOptions, useQuery } from "@tanstack/react-query"

import { apiFetch, type Schemas } from "@/lib/api"
import { DEFAULT_RANGE, type Counted, type CronRun, type TimelineRange } from "@/lib/timeline"
import { useToken } from "@/lib/token"

export type SchedulerRuns = Schemas["SchedulerRunsData"]

/** A day is watched like the page's other panels; several days are a heavier reply and change less. */
export function schedulerRefetchMs(hours: number): number {
  return hours <= 24 ? 60_000 : 5 * 60_000
}

export function schedulerRunsQuery(hours: number) {
  return queryOptions({
    queryKey: ["dashboard", "scheduler", hours] as const,
    queryFn: async () =>
      (await apiFetch<Schemas["SchedulerRunsResponse"]>(`/v1/dashboard/scheduler?hours=${hours}`)).data,
    refetchInterval: schedulerRefetchMs(hours),
    // A new range is a new key. The last range's answer stays in hand while
    // the next one loads; `schedulerWindow` says whether it may be drawn.
    placeholderData: (previous) => previous,
    // Retrying a rejected token would only repeat the 401.
    retry: (failures, error) =>
      !(error instanceof Error && "unauthorized" in error && error.unauthorized) && failures < 2,
  })
}

/**
 * Cron runs over a window longer than the status payload's six hours. Idle at
 * the default range: those runs are already on the page.
 */
export function useSchedulerRuns(hours: number) {
  const token = useToken()
  return useQuery({ ...schedulerRunsQuery(hours), enabled: token !== null && hours !== DEFAULT_RANGE.hours })
}

/** What the timeline is handed for a range past the default. */
export interface SchedulerWindow {
  runs: CronRun[]
  counted: Counted | null
  loading: boolean
  truncated: boolean
  error: string | null
}

/**
 * What the timeline draws for a range past the default, from the query as it
 * stands.
 *
 * While a new range loads, the query still holds the last range's answer. A
 * wider one covers the window asked for and is drawn until the range's own
 * lands. A narrower one is not: the axis is already the new range's, so its
 * runs would fill the right-hand end and leave the rest reading as days in
 * which nothing ran. Either way the timeline is told it is loading, and
 * `truncated` waits for the range's own answer.
 */
export function schedulerWindow(
  range: TimelineRange,
  query: { data: SchedulerRuns | undefined; error: Error | null; isPlaceholderData: boolean },
): SchedulerWindow {
  const waiting = query.isPlaceholderData
  const data = waiting && (query.data?.hours ?? 0) < range.hours ? undefined : query.data
  return {
    runs: data?.runs ?? [],
    counted: data?.buckets && data.bucket_seconds ? { buckets: data.buckets, bucketMs: data.bucket_seconds * 1000 } : null,
    loading: !query.error && (waiting || !query.data),
    truncated: !waiting && (data?.truncated ?? false),
    error: query.error?.message ?? null,
  }
}
