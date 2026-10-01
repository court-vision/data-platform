import { RunPipelineButton } from "@/components/RunPipelineButton"
import { StateBadge } from "@/components/StateBadge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { pipelineState, type CategoryGroup } from "@/lib/pipelines"
import { formatCentral, formatDuration, relativeTime } from "@/lib/time"

export function PipelineSection({ group, now }: { group: CategoryGroup; now: number }) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-baseline gap-2 text-base">
          {group.label}
          <span className="font-mono text-xs font-normal text-muted-foreground">{group.pipelines.length}</span>
        </CardTitle>
        <CardDescription>{group.description}</CardDescription>
      </CardHeader>
      <CardContent className="overflow-x-auto px-0 pb-2">
        {/* Fixed widths: every category's columns line up with the others'. */}
        <table className="w-full min-w-[44rem] table-fixed text-sm">
          <colgroup>
            <col className="w-[26%]" />
            <col className="w-[14%]" />
            <col className="w-[13%]" />
            <col className="w-[13%]" />
            <col className="w-[10%]" />
            <col className="w-[10%]" />
            <col className="w-[14%]" />
          </colgroup>
          <thead>
            <tr className="border-b text-left text-xs uppercase tracking-wider text-muted-foreground">
              <th scope="col" className="px-6 py-2 font-medium">Pipeline</th>
              <th scope="col" className="px-3 py-2 font-medium">State</th>
              <th scope="col" className="px-3 py-2 font-medium">Last run</th>
              <th scope="col" className="px-3 py-2 font-medium">Last success</th>
              <th scope="col" className="px-3 py-2 text-right font-medium">Duration</th>
              <th scope="col" className="px-3 py-2 text-right font-medium">Records</th>
              <th scope="col" className="px-6 py-2 text-right font-medium">
                <span className="sr-only">Run</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {group.pipelines.map((pipeline) => (
              <tr key={pipeline.name} className="border-b border-border/50 last:border-0">
                <th scope="row" className="px-6 py-2.5 text-left font-medium">
                  {pipeline.display_name}
                  <span className="block font-mono text-xs font-normal text-muted-foreground">{pipeline.name}</span>
                </th>
                <td className="px-3 py-2.5">
                  <StateBadge state={pipelineState(pipeline)} />
                  {pipeline.error_streak > 1 && (
                    <span
                      className="ml-2 font-mono text-xs text-status-loss"
                      title={`${pipeline.error_streak} consecutive failures`}
                    >
                      ×{pipeline.error_streak}
                    </span>
                  )}
                </td>
                <td className="px-3 py-2.5 font-mono text-xs" title={formatCentral(pipeline.last_run_at)}>
                  {relativeTime(pipeline.last_run_at, now)}
                </td>
                <td className="px-3 py-2.5 font-mono text-xs" title={formatCentral(pipeline.last_success_at)}>
                  {pipeline.last_success_at ? relativeTime(pipeline.last_success_at, now) : "never"}
                </td>
                <td className="px-3 py-2.5 text-right font-mono text-xs tabular-nums">
                  {formatDuration(pipeline.last_duration_seconds)}
                </td>
                <td className="px-3 py-2.5 text-right font-mono text-xs tabular-nums">
                  {pipeline.last_records_processed?.toLocaleString() ?? "—"}
                </td>
                <td className="whitespace-nowrap px-6 py-2.5 text-right">
                  <RunPipelineButton pipeline={pipeline} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </CardContent>
    </Card>
  )
}
