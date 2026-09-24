import { FlaskConical, Loader2 } from "lucide-react"

import { StatusBadge } from "@/components/StateBadge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import type { DashboardStatus } from "@/hooks/useDashboardStatus"
import { useRunQualityChecks } from "@/hooks/useRunQualityChecks"
import { formatCentral, formatDuration, relativeTime } from "@/lib/time"
import { cn } from "@/lib/utils"

type Props = Pick<DashboardStatus, "quality_latest" | "recent_quality_runs" | "quality_failed_checks"> & {
  now: number
}

export function QualityPanel({ quality_latest: latest, recent_quality_runs: recent, quality_failed_checks: failed, now }: Props) {
  const run = useRunQualityChecks()

  return (
    <Card>
      <CardHeader className="flex-row items-start justify-between gap-4 space-y-0 pb-3">
        <div className="space-y-1.5">
          <CardTitle className="flex items-baseline gap-2 text-base">
            Data quality
            <span className="font-mono text-xs font-normal text-muted-foreground">{recent.length}</span>
          </CardTitle>
          <CardDescription>Assertions over the nba.* tables. Runs take a minute.</CardDescription>
        </div>
        <Button variant="outline" size="sm" disabled={run.isPending} onClick={() => run.mutate()} className="gap-1.5">
          {run.isPending ? <Loader2 className="size-3.5 animate-spin" aria-hidden /> : <FlaskConical className="size-3.5" aria-hidden />}
          {run.isPending ? "Running checks" : "Run checks"}
        </Button>
      </CardHeader>
      <CardContent>
        {!latest ? (
          <p className="py-4 text-center text-sm text-muted-foreground">No quality runs yet.</p>
        ) : (
          <div className="grid gap-4 md:grid-cols-2">
            <section aria-label="Latest run" className="rounded-md border p-3">
              <div className="mb-2 flex items-center justify-between">
                <span className="text-xs uppercase tracking-wider text-muted-foreground">Latest run</span>
                <StatusBadge status={latest.status} />
              </div>
              <dl className="grid grid-cols-2 gap-x-3 gap-y-1.5 text-xs">
                <Field label="Started" value={relativeTime(latest.started_at, now)} title={formatCentral(latest.started_at)} />
                <Field label="Duration" value={formatDuration(latest.duration_seconds)} />
                <Field label="Checks" value={`${latest.passed_checks}/${latest.total_checks} passed`} />
                <Field
                  label="Failures"
                  value={String(latest.failed_checks)}
                  className={latest.failed_checks > 0 ? "text-status-loss" : undefined}
                />
                <Field label="Triggered by" value={latest.triggered_by ?? "unknown"} />
              </dl>
              {latest.error_message && <p className="mt-2 text-xs text-status-loss">{latest.error_message}</p>}
            </section>

            <section aria-label="Failed checks" className="rounded-md border p-3">
              <span className="text-xs uppercase tracking-wider text-muted-foreground">Failed checks</span>
              {failed.length === 0 ? (
                <p className="mt-2 text-xs text-muted-foreground">None in the latest run.</p>
              ) : (
                <ul className="mt-2 flex flex-col gap-2">
                  {failed.map((check) => (
                    <li key={check.check_name} className="text-xs">
                      <div className="flex items-center gap-2">
                        <span className="font-mono font-medium">{check.check_name}</span>
                        <StatusBadge status={check.status} />
                        <span className="text-muted-foreground">
                          {check.severity} · {check.failures.toLocaleString()} failures
                        </span>
                      </div>
                      {check.message && <p className="mt-0.5 text-muted-foreground">{check.message}</p>}
                    </li>
                  ))}
                </ul>
              )}
            </section>

            {recent.length > 1 && (
              <ol className="md:col-span-2 flex flex-col divide-y divide-border/50 text-xs" aria-label="Recent runs">
                {recent.map((entry) => (
                  <li key={entry.run_id} className="flex items-center gap-3 py-1.5">
                    <StatusBadge status={entry.status} />
                    <span className="font-mono" title={formatCentral(entry.started_at)}>{relativeTime(entry.started_at, now)}</span>
                    <span className="font-mono text-muted-foreground">
                      {entry.passed_checks}/{entry.total_checks} passed · {formatDuration(entry.duration_seconds)}
                    </span>
                    <span className="ml-auto text-muted-foreground">{entry.triggered_by}</span>
                  </li>
                ))}
              </ol>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

function Field({ label, value, title, className }: { label: string; value: string; title?: string; className?: string }) {
  return (
    <div>
      <dt className="text-muted-foreground">{label}</dt>
      <dd className={cn("font-mono", className)} title={title}>{value}</dd>
    </div>
  )
}
