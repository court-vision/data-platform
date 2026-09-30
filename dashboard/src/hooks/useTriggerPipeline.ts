import { useMutation, useQueryClient } from "@tanstack/react-query"
import { toast } from "sonner"

import { apiFetch, type Schemas } from "@/lib/api"
import type { Runnable } from "@/lib/pipelines"

interface TriggerArgs {
  pipeline: Runnable
  /** YYYY-MM-DD backfill date; only for pipelines whose route accepts one. */
  date?: string
}

/**
 * Run one pipeline. The trigger routes are synchronous — the request returns
 * when the pipeline has finished — so `isPending` is "running", and the toast
 * reports the outcome rather than the start. One instance per row, so rows
 * run independently.
 */
export function useTriggerPipeline() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ pipeline, date }: TriggerArgs) => {
      const url = date
        ? `${pipeline.trigger_endpoint}?date=${encodeURIComponent(date)}`
        : pipeline.trigger_endpoint
      return apiFetch<Schemas["PipelineResponse"]>(url, { method: "POST" })
    },
    onSuccess: (response, { pipeline, date }) => {
      const label = date ? `${pipeline.display_name} for ${date}` : pipeline.display_name
      if (response.status === "success") toast.success(label, { description: response.message })
      else toast.warning(label, { description: response.message })
    },
    onError: (error, { pipeline }) => {
      toast.error(`${pipeline.display_name} failed`, { description: error.message })
    },
    onSettled: () => queryClient.invalidateQueries({ queryKey: ["dashboard", "status"] }),
  })
}
