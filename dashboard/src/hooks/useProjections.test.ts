import { describe, expect, test } from "bun:test"
import { QueryClient, QueryObserver } from "@tanstack/react-query"

import { analystDigestQuery } from "@/hooks/useProjections"
import type { Schemas } from "@/lib/api"

type Digest = Schemas["AnalystDigest"]

describe("analystDigestQuery", () => {
  test("each morning is its own entry, and no day is the newest", () => {
    expect([...analystDigestQuery(null).queryKey]).toEqual(["dashboard", "projections", "analyst", "newest"])
    expect([...analystDigestQuery("2026-11-19").queryKey]).toEqual(["dashboard", "projections", "analyst", "2026-11-19"])
  })

  test("the morning is asked for again when the view reopens, however soon", async () => {
    const client = new QueryClient()
    const seen = { fetches: 0 }
    const options = {
      ...analystDigestQuery(null),
      queryFn: async () => {
        seen.fetches += 1
        return { reads: [] } as unknown as Digest
      },
    }
    // The view open on a morning that has just been loaded
    client.setQueryData(options.queryKey, { reads: [] } as unknown as Digest)
    const observer = new QueryObserver(client, { ...options, enabled: false })
    const stop = observer.subscribe(() => {})
    expect(seen.fetches).toBe(0)

    // Back from the players' view within the same minute: a run's later reads must not be missed
    observer.setOptions({ ...options, enabled: true })
    await new Promise((done) => setTimeout(done, 0))
    expect(seen.fetches).toBe(1)
    stop()
  })
})
