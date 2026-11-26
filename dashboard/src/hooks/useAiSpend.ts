import { queryOptions, useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { toast } from "sonner"

import { apiFetch, type Schemas } from "@/lib/api"
import type { CapLineKey, SpendWindow } from "@/lib/aiSpend"
import { useToken } from "@/lib/token"

/** Spend lands a turn at a time; a minute is fresh enough for a page read by eye. */
export const AI_SPEND_REFETCH_MS = 60_000

export const AI_SPEND_KEY = ["dashboard", "ai-spend"] as const
const BASE = "/v1/dashboard/ai-spend"

const giveUpOn = (error: unknown, failures: number) =>
  !(error instanceof Error && "unauthorized" in error && error.unauthorized) && failures < 2

export function aiSpendQuery(window: SpendWindow) {
  return queryOptions({
    queryKey: [...AI_SPEND_KEY, window] as const,
    queryFn: async () => (await apiFetch<Schemas["AiSpendResponse"]>(`${BASE}?days=${window}`)).data,
    refetchInterval: AI_SPEND_REFETCH_MS,
    // The last window stays on screen while another loads
    placeholderData: (previous) => previous,
    retry: (failures, error) => giveUpOn(error, failures),
  })
}

/** This month against each cap, and the window's spend by day, feature, model and team. */
export function useAiSpend(window: SpendWindow) {
  const token = useToken()
  return useQuery({ ...aiSpendQuery(window), enabled: token !== null })
}

/** The backend keeps Anthropic's answer five minutes, and Anthropic's own data lags hours. */
export const AI_BILL_REFETCH_MS = 300_000

export function aiBillQuery() {
  return queryOptions({
    // Apart from the report's key: a cap saved there has nothing to change here
    queryKey: ["dashboard", "ai-bill"] as const,
    queryFn: async () => (await apiFetch<Schemas["AiBillResponse"]>(`${BASE}/bill`)).data,
    refetchInterval: AI_BILL_REFETCH_MS,
    retry: (failures, error) => giveUpOn(error, failures),
  })
}

/** What Anthropic bills, last month and this one, beside the ledger. */
export function useAiBill() {
  const token = useToken()
  return useQuery({ ...aiBillQuery(), enabled: token !== null })
}

export interface CapChange {
  line: CapLineKey
  userId: number | null
  /** Dollars as typed ("25", "12.50"), or null to go back to the default. */
  usd: string | null
  author: string
}

/** Set or clear one cap; the page re-reads the report after. */
export function useSetCap() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async (change: CapChange) =>
      await apiFetch<Schemas["AiCapResponse"]>(`${BASE}/caps`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ line: change.line, user_id: change.userId, monthly_usd: change.usd, author: change.author }),
      }),
    onSuccess: (response) => toast.success("Cap saved", { description: response.message }),
    onError: (error) => toast.error("Cap not saved", { description: error.message }),
    onSettled: () => queryClient.invalidateQueries({ queryKey: AI_SPEND_KEY }),
  })
}
