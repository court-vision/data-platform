import { Block } from "@/components/blocks"
import { FreshnessBadge } from "@/components/StateBadge"
import { daysBehind, splitTable, type TableFreshness } from "@/lib/freshness"
import { formatCentral, formatDay, relativeTime } from "@/lib/time"
import s from "./sheet.module.css"

/** Every table a pipeline owns: what date it runs through, when it was last written, the verdict. */
export function FreshnessTable({ tables, now }: { tables: TableFreshness[]; now: number }) {
  return (
    <Block
      title="Tables"
      count={tables.length}
      text="Nightly tables are judged against the schedule: post-game tables through the last settled night, pre-game tables through the last day whose first tip-off has passed. Live and scheduled writers, and pipelines that only write when there is something to say, are shown but not judged. Two tables keep their own dates: nba.games is the schedule, so it runs through its last game day with a result, and daily_matchup_scores is dated by its run, the morning after the game night."
    >
      <div className={s.bleed}>
        <table className={s.table} style={{ minWidth: "52rem", tableLayout: "fixed" }}>
          <colgroup>
            <col style={{ width: "26%" }} />
            <col style={{ width: "15%" }} />
            <col style={{ width: "20%" }} />
            <col style={{ width: "16%" }} />
            <col style={{ width: "12%" }} />
            <col style={{ width: "11%" }} />
          </colgroup>
          <thead>
            <tr>
              <th scope="col">Table</th>
              {/* The verdict sits beside the name: the table is wider than a
                  phone, and a last column is scrolled out of sight there. */}
              <th scope="col">State</th>
              <th scope="col">Written by</th>
              <th scope="col">Runs through</th>
              <th scope="col">Last write</th>
              <th scope="col" className={s.num}>Rows</th>
            </tr>
          </thead>
          <tbody>
            {tables.map((table) => (
              <FreshnessRow key={table.table} table={table} now={now} />
            ))}
          </tbody>
        </table>
      </div>
    </Block>
  )
}

function FreshnessRow({ table, now }: { table: TableFreshness; now: number }) {
  const { schema, name } = splitTable(table.table)
  const behind = daysBehind(table)
  return (
    <tr>
      <th scope="row" className={`${s.mono} ${s.top}`}>
        <span className={s.muted}>{schema}.</span>
        {/* A name too long for the column wraps here, not over the verdict. */}
        <wbr />
        {name}
      </th>
      <td className={s.top}>
        <FreshnessBadge state={table.state} />
        {table.error && (
          <span className={`${s.under} ${s.bad} ${s.clip}`} title={table.error}>
            {table.error}
          </span>
        )}
      </td>
      <td className={s.top}>
        {table.pipelines.map((pipeline) => (
          <span key={pipeline.name} title={pipeline.name} className={s.clip}>
            {pipeline.display_name}
          </span>
        ))}
      </td>
      <td className={`${s.mono} ${s.top}`} title={table.date_column ? `max(${table.date_column})` : "no business date"}>
        {table.date_column ? <span className={s.strong}>{formatDay(table.latest_date)}</span> : <span className={s.muted}>—</span>}
        {behind != null && (
          <span className={`${s.under} ${s.bad}`}>
            expected {formatDay(table.expected_date)} · {behind}d behind
          </span>
        )}
      </td>
      <td className={`${s.mono} ${s.top}`} title={`max(${table.write_column ?? "?"}) · ${formatCentral(table.latest_written_at)}`}>
        {relativeTime(table.latest_written_at, now)}
      </td>
      <td className={`${s.num} ${s.top}`} title="Planner estimate, not a count">
        {table.rows_estimate?.toLocaleString() ?? "—"}
      </td>
    </tr>
  )
}
