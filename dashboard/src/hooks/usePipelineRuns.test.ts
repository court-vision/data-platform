import { describe, expect, test } from "bun:test"
import { QueryClient, QueryObserver } from "@tanstack/react-query"

import { pipelineRunsQuery } from "@/hooks/usePipelineRuns"
import { refreshAfterRun } from "@/hooks/useTriggerPipeline"
import type { Schemas } from "@/lib/api"

type RunsData = Schemas["PipelineRunsData"]

/** Enough of a payload to tell two apart. */
function payload(name: string, limit: number): RunsData {
  return { pipeline: { name }, runs: [], limit } as unknown as RunsData
}

describe("pipelineRunsQuery", () => {
  test("the window on screen stays there while another limit loads", () => {
    const client = new QueryClient()
    const fifty = payload("player_game_stats", 50)
    client.setQueryData(pipelineRunsQuery("player_game_stats", 50).queryKey, fifty)
    const observer = new QueryObserver(client, { ...pipelineRunsQuery("player_game_stats", 50), enabled: false })
    expect(observer.getCurrentResult().data).toBe(fifty)

    observer.setOptions({ ...pipelineRunsQuery("player_game_stats", 100), enabled: false })

    const result = observer.getCurrentResult()
    expect(result.data).toBe(fifty) // undefined before: the page fell back to skeletons
    expect(result.isPlaceholderData).toBe(true)
  })

  test("another pipeline's runs never stand in", () => {
    const client = new QueryClient()
    client.setQueryData(pipelineRunsQuery("player_game_stats", 50).queryKey, payload("player_game_stats", 50))
    const observer = new QueryObserver(client, { ...pipelineRunsQuery("player_game_stats", 50), enabled: false })

    observer.setOptions({ ...pipelineRunsQuery("espn_injury_status", 50), enabled: false })

    expect(observer.getCurrentResult().data).toBeUndefined()
  })
})

describe("refreshAfterRun", () => {
  /** Watch a key the way a mounted page does, already loaded; count its refetches. */
  function watch(client: QueryClient, queryKey: readonly unknown[]) {
    const seen = { fetches: 0 }
    client.setQueryData(queryKey, "before the run")
    const observer = new QueryObserver(client, {
      queryKey,
      queryFn: async () => {
        seen.fetches += 1
        return "after the run"
      },
      staleTime: Infinity,
    })
    return { seen, stop: observer.subscribe(() => {}) }
  }

  test("refetches the pipeline's own page, at whatever limit, and the Overview", async () => {
    const client = new QueryClient()
    const page = watch(client, pipelineRunsQuery("player_game_stats", 100).queryKey)
    const overview = watch(client, ["dashboard", "status"])
    const services = watch(client, ["dashboard", "services"])

    await refreshAfterRun(client)

    // The page used to stay on the pre-run state until its next poll, 30s on.
    expect(page.seen.fetches).toBe(1)
    expect(overview.seen.fetches).toBe(1)
    expect(services.seen.fetches).toBe(0) // a run does not change what is deployed
    for (const { stop } of [page, overview, services]) stop()
  })
})
