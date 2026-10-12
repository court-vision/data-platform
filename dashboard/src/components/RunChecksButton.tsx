import { FlaskConical, Loader2 } from "lucide-react"

import { useRunQualityChecks } from "@/hooks/useRunQualityChecks"
import dk from "@/desk/desk.module.css"

/** Run every data-quality check now. A run takes about a minute; its toast says how it went. */
export function RunChecksButton() {
  const run = useRunQualityChecks()
  return (
    <button type="button" className={`${dk.btn} ${dk.btnSmall}`} disabled={run.isPending} onClick={() => run.mutate()}>
      {run.isPending ? <Loader2 size={12} className={dk.spin} aria-hidden /> : <FlaskConical size={12} aria-hidden />}
      {run.isPending ? "Running checks" : "Run checks"}
    </button>
  )
}
