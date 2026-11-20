import { queryOptions, useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { toast } from "sonner"

import { apiFetch, type Schemas } from "@/lib/api"
import type { AdjustmentChange, AdjustmentSave, NoteChange, NoteSave } from "@/lib/projections"
import { useToken } from "@/lib/token"

export const PROJECTIONS_KEY = ["dashboard", "projections"] as const
const BASE = "/v1/dashboard/projections"
/** The pipeline a save re-runs, and the page's Republish button runs by hand. */
const REPUBLISH = "/v1/internal/pipelines/cv-projection?force=true"

const giveUpOn = (error: unknown, failures: number) =>
  !(error instanceof Error && "unauthorized" in error && error.unauthorized) && failures < 2

export function projectionsQuery() {
  return queryOptions({
    queryKey: PROJECTIONS_KEY,
    queryFn: async () => (await apiFetch<Schemas["ProjectionsResponse"]>(BASE)).data,
    // Every read rebuilds the projection and asks the backend to value it: it
    // is fetched when the page opens, after a write, and when asked, not on a
    // timer or every time the window regains focus.
    staleTime: Infinity,
    refetchOnWindowFocus: false,
    retry: (failures, error) => giveUpOn(error, failures),
  })
}

/**
 * Every projected player: the four lines, the ranks, the live adjustment.
 * Off while the page shows the analyst's morning, which needs none of it.
 */
export function useProjections(enabled = true) {
  const token = useToken()
  return useQuery({ ...projectionsQuery(), enabled: enabled && token !== null })
}

export function adjustmentHistoryQuery(playerId: number) {
  return queryOptions({
    queryKey: [...PROJECTIONS_KEY, "history", playerId] as const,
    queryFn: async () =>
      (await apiFetch<Schemas["AdjustmentHistoryResponse"]>(`${BASE}/${playerId}/adjustments`)).data,
    staleTime: Infinity,
    refetchOnWindowFocus: false,
    retry: (failures, error) => giveUpOn(error, failures),
  })
}

/** Every version of one player's adjustment. Asked for when his row is opened. */
export function useAdjustmentHistory(playerId: number, enabled: boolean) {
  const token = useToken()
  return useQuery({ ...adjustmentHistoryQuery(playerId), enabled: enabled && token !== null })
}

/** One player's line and ranks under an edit. Writes nothing. */
export function usePreviewAdjustment() {
  return useMutation({
    mutationFn: async ({ playerId, change }: { playerId: number; change: AdjustmentChange | null }) =>
      (
        await apiFetch<Schemas["PreviewResponse"]>(`${BASE}/preview`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ player_id: playerId, adjustment: change }),
        })
      ).data,
    onError: (error) => toast.error("Preview failed", { description: error.message }),
  })
}

function reportWrite(name: string, response: Schemas["AdjustmentSavedResponse"]) {
  if (response.data.published) toast.success(name, { description: response.message })
  // Saved, but the board is still on the old snapshot: not a success to wave through.
  else toast.warning(name, { description: `${response.message}. Use Republish to try again.` })
}

/** Save a new version of a player's adjustment; the API republishes the projection. */
export function useSaveAdjustment() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ playerId, body }: { playerId: number; name: string; body: AdjustmentSave }) =>
      apiFetch<Schemas["AdjustmentSavedResponse"]>(`${BASE}/${playerId}/adjustment`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }),
    onSuccess: (response, { name }) => reportWrite(name, response),
    onError: (error, { name }) => toast.error(`${name}: not saved`, { description: error.message }),
    onSettled: () => queryClient.invalidateQueries({ queryKey: PROJECTIONS_KEY }),
  })
}

/** Withdraw a player's live adjustment; the API republishes the projection. */
export function useRetireAdjustment() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ playerId }: { playerId: number; name: string }) =>
      apiFetch<Schemas["AdjustmentSavedResponse"]>(`${BASE}/${playerId}/adjustment`, { method: "DELETE" }),
    onSuccess: (response, { name }) => reportWrite(name, response),
    onError: (error, { name }) => toast.error(`${name}: not retired`, { description: error.message }),
    onSettled: () => queryClient.invalidateQueries({ queryKey: PROJECTIONS_KEY }),
  })
}

// ---- notes in season -----------------------------------------------------------------------

export function playerNewsQuery(playerId: number) {
  return queryOptions({
    queryKey: [...PROJECTIONS_KEY, "news", playerId] as const,
    queryFn: async () => (await apiFetch<Schemas["PlayerNewsResponse"]>(`${BASE}/${playerId}/news`)).data,
    // The feed moves every few minutes; a form left open should not cite from an old list
    staleTime: 60_000,
    refetchOnWindowFocus: false,
    retry: (failures, error) => giveUpOn(error, failures),
  })
}

/** A player's recent stored news: what a season note can cite. Asked for when the note form is open. */
export function usePlayerNews(playerId: number, enabled: boolean) {
  const token = useToken()
  return useQuery({ ...playerNewsQuery(playerId), enabled: enabled && token !== null })
}

/** One player's live projection under a season note. Writes nothing. */
export function usePreviewNote() {
  return useMutation({
    mutationFn: async ({ playerId, note, reviewDate }: { playerId: number; note: NoteChange | null; reviewDate: string | null }) =>
      (
        await apiFetch<Schemas["NotePreviewResponse"]>(`${BASE}/note/preview`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ player_id: playerId, note, review_date: reviewDate }),
        })
      ).data,
    onError: (error) => toast.error("Preview failed", { description: error.message }),
  })
}

function reportNote(name: string, response: Schemas["NoteSavedResponse"]) {
  if (response.data.rebuilt) toast.success(name, { description: response.message })
  // Kept, and read by the next morning's run: said plainly, not an error.
  else toast.warning(name, { description: `${response.message}.` })
}

/** Save a new version of a player's season note; the API rebuilds the day's live projection. */
export function useSaveNote() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ playerId, body }: { playerId: number; name: string; body: NoteSave }) =>
      apiFetch<Schemas["NoteSavedResponse"]>(`${BASE}/${playerId}/note`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }),
    onSuccess: (response, { name }) => reportNote(name, response),
    onError: (error, { name }) => toast.error(`${name}: note not saved`, { description: error.message }),
    onSettled: () => queryClient.invalidateQueries({ queryKey: PROJECTIONS_KEY }),
  })
}

/** Withdraw a player's live season note; the API rebuilds the day's live projection. */
export function useRetireNote() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ playerId }: { playerId: number; name: string }) =>
      apiFetch<Schemas["NoteSavedResponse"]>(`${BASE}/${playerId}/note`, { method: "DELETE" }),
    onSuccess: (response, { name }) => reportNote(name, response),
    onError: (error, { name }) => toast.error(`${name}: note not retired`, { description: error.message }),
    onSettled: () => queryClient.invalidateQueries({ queryKey: PROJECTIONS_KEY }),
  })
}

// ---- the analyst, in shadow ------------------------------------------------------------------

/** `day` null is the newest morning with reads. */
export function analystDigestQuery(day: string | null) {
  return queryOptions({
    queryKey: [...PROJECTIONS_KEY, "analyst", day ?? "newest"] as const,
    queryFn: async () =>
      (await apiFetch<Schemas["AnalystDigestResponse"]>(`${BASE}/analyst${day ? `?day=${encodeURIComponent(day)}` : ""}`)).data,
    // A cheap read, and a run's reads land over a few minutes: it is asked
    // again every time the view opens, so coming back to it never shows a
    // morning's first few reads as the whole of it. Not on a timer, and not
    // every time the window regains focus.
    staleTime: 0,
    refetchOnWindowFocus: false,
    retry: (failures, error) => giveUpOn(error, failures),
  })
}

/** A morning of the analyst: its reads, the notes they left, what those moved and the month's spend. */
export function useAnalystDigest(day: string | null, enabled: boolean) {
  const token = useToken()
  return useQuery({ ...analystDigestQuery(day), enabled: enabled && token !== null })
}

/** Run cv-projection by hand: for a snapshot a failed republish left behind. */
export function useRepublish() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: () => apiFetch<Schemas["PipelineResponse"]>(REPUBLISH, { method: "POST" }),
    onSuccess: (response) => {
      if (response.status === "success") toast.success("Projection republished", { description: response.message })
      else toast.warning("Projection not republished", { description: response.message })
    },
    onError: (error) => toast.error("Republish failed", { description: error.message }),
    onSettled: () =>
      Promise.all([
        queryClient.invalidateQueries({ queryKey: PROJECTIONS_KEY }),
        queryClient.invalidateQueries({ queryKey: ["dashboard", "status"] }),
      ]),
  })
}
