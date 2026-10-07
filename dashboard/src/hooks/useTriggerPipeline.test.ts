import { afterEach, expect, test } from "bun:test"

import { triggerPipeline } from "@/hooks/useTriggerPipeline"
import type { Runnable } from "@/lib/pipelines"

// No network: fetch is replaced by one that records what Run posts and answers
// the way a trigger route does.

const realFetch = globalThis.fetch

afterEach(() => {
  globalThis.fetch = realFetch
})

function recordPosts(): string[] {
  const posts: string[] = []
  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    posts.push(`${init?.method} ${String(input)}`)
    return new Response(JSON.stringify({ status: "success", message: "ok", data: null }), {
      headers: { "Content-Type": "application/json" },
    })
  }) as typeof fetch
  return posts
}

function runnable(overrides: Partial<Runnable> = {}): Runnable {
  return {
    display_name: "Player Game Stats",
    is_running: false,
    trigger_endpoint: "/v1/internal/pipelines/daily-player-stats",
    accepts_date: true,
    force_on_run: false,
    ...overrides,
  }
}

test("Run on scheduled pickups forces the tick, which its route skips when nothing is due", async () => {
  const posts = recordPosts()
  const pickups = runnable({
    display_name: "Scheduled Pickups",
    trigger_endpoint: "/v1/internal/pipelines/scheduled-pickups",
    accepts_date: false,
    force_on_run: true,
  })

  await triggerPipeline({ pipeline: pickups })

  expect(posts).toEqual(["POST /v1/internal/pipelines/scheduled-pickups?force=true"])
})

test("Run on any other pipeline posts its route, with the backfill date when one is given", async () => {
  const posts = recordPosts()

  await triggerPipeline({ pipeline: runnable() })
  await triggerPipeline({ pipeline: runnable(), date: "2026-03-04" })

  expect(posts).toEqual([
    "POST /v1/internal/pipelines/daily-player-stats",
    "POST /v1/internal/pipelines/daily-player-stats?date=2026-03-04",
  ])
})
