import { useMutation, useQueryClient } from "@tanstack/react-query"
import { toast } from "sonner"

import { apiFetch, type Schemas } from "@/lib/api"

/** Run every data-quality check now. Synchronous like the triggers. */
export function useRunQualityChecks() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: () =>
      apiFetch<Schemas["DataQualityRunResponse"]>("/v1/internal/quality/run?triggered_by=dashboard", {
        method: "POST",
      }),
    onSuccess: ({ data }) => {
      const summary = `${data.passed_checks}/${data.total_checks} passed`
      if (data.failed_checks > 0) toast.warning(`Data checks: ${data.failed_checks} failed`, { description: summary })
      else toast.success("Data checks passed", { description: summary })
    },
    onError: (error) => toast.error("Data checks failed to run", { description: error.message }),
    onSettled: () => queryClient.invalidateQueries({ queryKey: ["dashboard", "status"] }),
  })
}
