import { useLayoutEffect, useRef } from "react"
import { Link } from "react-router"

import { failedThroughout, failingStreak, groupChecks, resultLabel, type CheckResult, type QualityCheckRow, type QualityRun } from "@/lib/quality"
import { formatCentral } from "@/lib/time"
import s from "./sheet.module.css"
import q from "./quality.module.css"

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
  const template = { gridTemplateColumns: `minmax(8rem, min(27rem, 45vw)) repeat(${columns.length}, 14px)` }

  // With more runs than fit, open on the newest: they are on the right.
  const scroller = useRef<HTMLDivElement>(null)
  useLayoutEffect(() => {
    const element = scroller.current
    if (element) element.scrollLeft = element.scrollWidth
  }, [columns.length])

  return (
    <div ref={scroller} className={q.matrixScroller}>
      <div className={q.matrix}>
        {groupChecks(checks).map((group) => (
          <section key={group.key} aria-label={`${group.label} checks`}>
            {/* Sticky like the labels: the matrix opens scrolled to the newest runs. */}
            <h3 className={q.groupHead} title={group.description}>
              {group.label}
              <span className={q.groupCount}>{group.checks.length}</span>
            </h3>
            <ul className={q.matrixRows}>
              {group.checks.map((check) => {
                const streak = failingStreak(check.results)
                const orMore = windowFull && failedThroughout(check.results)
                return (
                  <li key={check.name} className={q.matrixRow} style={template}>
                    {/* Under md the column is too narrow for both on one line: the name wraps
                        in full and the streak sits beneath it, so neither is cut to fit the other. */}
                    <span className={q.matrixLabel}>
                      <span className={q.matrixName} title={check.name}>
                        {check.name}
                      </span>
                      {streak > 0 && (
                        <span
                          className={q.streak}
                          data-tone={check.severity === "critical" ? "down" : "warn"}
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

function Cell({ check, run, result }: { check: string; run: QualityRun; result: CheckResult }) {
  const label = `${check} · ${formatCentral(run.started_at)} · ${resultLabel(result)}`
  return (
    <Link to={`/quality/runs/${run.run_id}`} tabIndex={-1} title={label} aria-label={label} data-result={result ?? "none"} className={q.cell}>
      <span className={q.cellMark} />
    </Link>
  )
}

/** Names each mark. Status never rides on colour alone. */
export function QualityMatrixKey() {
  return (
    <ul className={s.legend} aria-label="Key">
      <li data-result="passed" className={q.keyItem}>
        <span className={q.cellMark} aria-hidden /> passed
      </li>
      <li data-result="failed" className={q.keyItem}>
        <span className={q.cellMark} aria-hidden /> failed
      </li>
      <li data-result="error" className={q.keyItem}>
        <span className={q.cellMark} aria-hidden /> could not run
      </li>
      <li data-result="none" className={q.keyItem}>
        <span className={q.cellMark} aria-hidden /> not in that run
      </li>
    </ul>
  )
}
