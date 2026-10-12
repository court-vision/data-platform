import { Loader2, Play } from "lucide-react"
import { useState, type FormEvent } from "react"

import { Pop } from "@/components/blocks"
import { useTriggerPipeline } from "@/hooks/useTriggerPipeline"
import type { Runnable } from "@/lib/pipelines"
import dk from "@/desk/desk.module.css"
import s from "./sheet.module.css"

/** Run one pipeline: a popover to confirm, with a date box where a backfill can go. */
export function RunPipelineButton({ pipeline }: { pipeline: Runnable }) {
  const [open, setOpen] = useState(false)
  const [date, setDate] = useState("")
  const trigger = useTriggerPipeline()

  const running = trigger.isPending || pipeline.is_running
  const disabled = running || !pipeline.trigger_endpoint

  function submit(event: FormEvent) {
    event.preventDefault()
    setOpen(false)
    trigger.mutate({ pipeline, date: pipeline.accepts_date && date ? date : undefined })
  }

  return (
    <Pop
      open={open}
      onOpenChange={setOpen}
      trigger={
        <button type="button" className={`${dk.btn} ${dk.btnSmall}`} disabled={disabled} aria-label={`Run ${pipeline.display_name}`}>
          {trigger.isPending ? <Loader2 size={12} className={dk.spin} aria-hidden /> : <Play size={12} aria-hidden />}
          {running ? "Running" : "Run"}
        </button>
      }
    >
      <form onSubmit={submit} className={s.popForm}>
        <div>
          <p className={s.popTitle}>Run {pipeline.display_name}</p>
          <p className={s.popText}>
            {pipeline.accepts_date ? "Leave the date blank for today's NBA date." : "Runs for now; this pipeline takes no date."}
          </p>
        </div>
        {pipeline.accepts_date && (
          <input
            type="date"
            aria-label="Date override"
            value={date}
            onChange={(event) => setDate(event.target.value)}
            className={`${dk.input} ${s.input}`}
          />
        )}
        <button type="submit" className={`${dk.btn} ${dk.btnPrimary}`}>
          <Play size={12} aria-hidden /> Run now
        </button>
      </form>
    </Pop>
  )
}
