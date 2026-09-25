import { FreshnessBadge } from "@/components/StateBadge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { daysBehind, splitTable, type TableFreshness } from "@/lib/freshness"
import { formatCentral, formatDay, relativeTime } from "@/lib/time"

/** Every table a pipeline owns: what date it runs through, when it was last written, the verdict. */
export function FreshnessTable({ tables, now }: { tables: TableFreshness[]; now: number }) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-baseline gap-2 text-base">
          Tables
          <span className="font-mono text-xs font-normal text-muted-foreground">{tables.length}</span>
        </CardTitle>
        <CardDescription>
          Nightly tables are judged in season against the last settled game date. Live and scheduled
          writers, and pipelines that only write when there is something to say, are shown but not judged.
        </CardDescription>
      </CardHeader>
      <CardContent className="overflow-x-auto px-0 pb-2">
        <table className="w-full min-w-[52rem] table-fixed text-sm">
          <colgroup>
            <col className="w-[24%]" />
            <col className="w-[20%]" />
            <col className="w-[16%]" />
            <col className="w-[14%]" />
            <col className="w-[10%]" />
            <col className="w-[16%]" />
          </colgroup>
          <thead>
            <tr className="border-b text-left text-xs uppercase tracking-wider text-muted-foreground">
              <th scope="col" className="px-6 py-2 font-medium">Table</th>
              <th scope="col" className="px-3 py-2 font-medium">Written by</th>
              <th scope="col" className="px-3 py-2 font-medium">Runs through</th>
              <th scope="col" className="px-3 py-2 font-medium">Last write</th>
              <th scope="col" className="px-3 py-2 text-right font-medium">Rows</th>
              <th scope="col" className="px-6 py-2 font-medium">State</th>
            </tr>
          </thead>
          <tbody>
            {tables.map((table) => (
              <FreshnessRow key={table.table} table={table} now={now} />
            ))}
          </tbody>
        </table>
      </CardContent>
    </Card>
  )
}

function FreshnessRow({ table, now }: { table: TableFreshness; now: number }) {
  const { schema, name } = splitTable(table.table)
  const behind = daysBehind(table)
  return (
    <tr className="border-b border-border/50 last:border-0">
      <th scope="row" className="px-6 py-2.5 text-left font-mono text-xs font-medium">
        <span className="text-muted-foreground">{schema}.</span>
        {name}
      </th>
      <td className="px-3 py-2.5 text-xs">
        {table.pipelines.map((pipeline) => (
          <span key={pipeline.name} className="block truncate" title={pipeline.name}>
            {pipeline.display_name}
          </span>
        ))}
      </td>
      <td className="px-3 py-2.5 font-mono text-xs" title={table.date_column ? `max(${table.date_column})` : "no business date"}>
        {table.date_column ? formatDay(table.latest_date) : <span className="text-muted-foreground">—</span>}
        {behind != null && (
          <span className="block text-status-loss">
            expected {formatDay(table.expected_date)} · {behind}d behind
          </span>
        )}
      </td>
      <td
        className="px-3 py-2.5 font-mono text-xs"
        title={`max(${table.write_column ?? "?"}) · ${formatCentral(table.latest_written_at)}`}
      >
        {relativeTime(table.latest_written_at, now)}
      </td>
      <td className="px-3 py-2.5 text-right font-mono text-xs tabular-nums" title="Planner estimate, not a count">
        {table.rows_estimate?.toLocaleString() ?? "—"}
      </td>
      <td className="px-6 py-2.5">
        <FreshnessBadge state={table.state} />
        {table.error && (
          <span className="mt-1 block truncate text-xs text-status-loss" title={table.error}>
            {table.error}
          </span>
        )}
      </td>
    </tr>
  )
}
