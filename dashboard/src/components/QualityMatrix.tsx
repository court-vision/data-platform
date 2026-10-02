import { useLayoutEffect, useRef } from "react"
import { Link } from "react-router"

import { failedThroughout, failingStreak, groupChecks, resultLabel, type CheckResult, type QualityCheckRow, type QualityRun } from "@/lib/quality"
import { formatCentral } from "@/lib/time"
import { cn } from "@/lib/utils"

/**
 * Checks down, runs across (oldest on the left, so time reads left to right).
 * Status is the colour and also the shape: a pass is a soft square, a failure
 * a solid one, a check that could not run a hollow one, a gap a dot. Every
 * cell carries its meaning as text, and the runs table below is the same data
 * for a keyboard or a screen reader, so the cells are not tab stops.
 */
export function QualityMatrix({ checks, runs, limit }: { checks: QualityCheckRow[]; runs: QualityRun[]; limit: number }) {
  // The API sends runs (and each check's results) newest first.
  const columns = runs.map((run, index) => ({ run, index })).reverse()
  // As many runs as were asked for: there are likely older ones this page does
  // not have, so a streak that fills the window is a floor ("20+ runs").
  const windowFull = runs.length >= limit
  // Wide enough for the longest check name beside its streak, and it stays put
  // while the runs scroll under it — so on a narrow screen it is capped at
  // under half the viewport, or it would cover every cell.
  const template = { gridTemplateColumns: `minmax(8rem, min(27rem, 45vw)) repeat(${columns.length}, 0.875rem)` }

  // With more runs than fit, open on the newest: they are on the right.
  const scroller = useRef<HTMLDivElement>(null)
  useLayoutEffect(() => {
    const element = scroller.current
    if (element) element.scrollLeft = element.scrollWidth
  }, [columns.length])

  return (
    <div ref={scroller} className="overflow-x-auto">
      <div className="flex w-max min-w-full flex-col gap-3">
        {groupChecks(checks).map((group) => (
          <section key={group.key} aria-label={`${group.label} checks`}>
            {/* Sticky like the labels: the matrix opens scrolled to the newest runs. */}
            <h3 className="sticky left-0 mb-1 w-fit text-xs uppercase tracking-wider text-muted-foreground" title={group.description}>
              {group.label}
              <span className="ml-1.5 font-mono normal-case tracking-normal">{group.checks.length}</span>
            </h3>
            <ul className="flex flex-col gap-[2px]">
              {group.checks.map((check) => {
                const streak = failingStreak(check.results)
                const orMore = windowFull && failedThroughout(check.results)
                return (
                  <li key={check.name} className="grid items-center gap-x-[2px]" style={template}>
                    <span className="sticky left-0 z-10 flex min-w-0 items-baseline gap-2 bg-card pr-3">
                      <span className="truncate font-mono text-xs" title={check.name}>
                        {check.name}
                      </span>
                      {streak > 0 && (
                        <span
                          className={cn(
                            "shrink-0 font-mono text-[10px]",
                            check.severity === "critical" ? "text-status-loss" : "text-status-projected",
                          )}
                          title={orMore ? "Not passed in any run shown; older runs are not on this page" : undefined}
                        >
                          {check.severity === "critical" ? "critical" : "warning"} · {streak}
                          {orMore ? "+ runs" : streak === 1 ? " run" : " runs"}
                        </span>
                      )}
                    </span>
                    {columns.map(({ run, index }) => (
                      <Cell key={run.run_id} check={check.name} run={run} result={check.results[index] ?? null} />
                    ))}
                  </li>
                )
              })}
            </ul>
          </section>
        ))}
      </div>
    </div>
  )
}

const CELL: Record<string, string> = {
  passed: "bg-status-win/45 hover:bg-status-win",
  failed: "bg-status-loss hover:brightness-125",
  error: "border-2 border-status-loss hover:bg-status-loss/40",
}

function Cell({ check, run, result }: { check: string; run: QualityRun; result: CheckResult }) {
  const label = `${check} · ${formatCentral(run.started_at)} · ${resultLabel(result)}`
  return (
    <Link
      to={`/quality/runs/${run.run_id}`}
      tabIndex={-1}
      title={label}
      aria-label={label}
      data-result={result ?? "none"}
      className="flex size-3.5 items-center justify-center"
    >
      {result === null ? (
        <span className="size-1 rounded-full bg-muted-foreground/40" />
      ) : (
        <span className={cn("size-3.5 rounded-[3px]", CELL[result] ?? "bg-muted-foreground")} />
      )}
    </Link>
  )
}

/** Names each mark. Status never rides on colour alone. */
export function QualityMatrixKey() {
  return (
    <ul className="flex flex-wrap items-center gap-x-4 gap-y-1 font-mono text-xs text-muted-foreground" aria-label="Key">
      <li className="flex items-center gap-1.5">
        <span className="size-3 rounded-[3px] bg-status-win/45" aria-hidden /> passed
      </li>
      <li className="flex items-center gap-1.5">
        <span className="size-3 rounded-[3px] bg-status-loss" aria-hidden /> failed
      </li>
      <li className="flex items-center gap-1.5">
        <span className="size-3 rounded-[3px] border-2 border-status-loss" aria-hidden /> could not run
      </li>
      <li className="flex items-center gap-1.5">
        <span className="flex size-3 items-center justify-center" aria-hidden>
          <span className="size-1 rounded-full bg-muted-foreground/40" />
        </span>
        not in that run
      </li>
    </ul>
  )
}
