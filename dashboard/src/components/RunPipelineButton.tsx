import { Loader2, Play } from "lucide-react"
import { useState, type FormEvent } from "react"

import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import type { PipelineHealth } from "@/hooks/useDashboardStatus"
import { useTriggerPipeline } from "@/hooks/useTriggerPipeline"

/** Run one pipeline: a popover to confirm, with a date box where a backfill can go. */
export function RunPipelineButton({ pipeline }: { pipeline: PipelineHealth }) {
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
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          size="sm"
          disabled={disabled}
          aria-label={`Run ${pipeline.display_name}`}
          className="h-7 gap-1.5 px-2 font-mono text-xs"
        >
          {trigger.isPending ? (
            <Loader2 className="size-3 animate-spin" aria-hidden />
          ) : (
            <Play className="size-3" aria-hidden />
          )}
          {running ? "Running" : "Run"}
        </Button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-64">
        <form onSubmit={submit} className="flex flex-col gap-3">
          <div>
            <p className="text-sm font-medium">Run {pipeline.display_name}</p>
            <p className="mt-0.5 text-xs text-muted-foreground">
              {pipeline.accepts_date
                ? "Leave the date blank for today's NBA date."
                : "Runs for now; this pipeline takes no date."}
            </p>
          </div>
          {pipeline.accepts_date && (
            <Input
              type="date"
              aria-label="Date override"
              value={date}
              onChange={(event) => setDate(event.target.value)}
              className="h-8 font-mono text-xs"
            />
          )}
          <Button type="submit" size="sm">
            Run now
          </Button>
        </form>
      </PopoverContent>
    </Popover>
  )
}
