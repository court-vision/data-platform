import { ChevronDown, ChevronRight } from "lucide-react"
import { Fragment } from "react"

import { ProjectionDetail } from "@/components/ProjectionDetail"
import { Badge } from "@/components/ui/badge"
import {
  describeChange,
  espnRankOf,
  formatStat,
  gapIsWide,
  kindLabel,
  rankGap,
  rankOf,
  STAT_COLUMNS,
  type Format,
  type ProjectionRow,
} from "@/lib/projections"
import { cn } from "@/lib/utils"

// Rank, ESPN, gap, player, adjustment, then the stat columns.
const COLUMNS = 5 + STAT_COLUMNS.length

/**
 * Every projected player's published line, with Court Vision's rank beside
 * ESPN's. A row opens onto the lines it was built from and the adjustment on
 * top of them; one row is open at a time.
 */
export function ProjectionsTable({
  rows,
  format,
  openId,
  onToggle,
  kinds,
  espnWeight,
}: {
  rows: ProjectionRow[]
  format: Format
  openId: number | null
  onToggle: (playerId: number) => void
  kinds: string[]
  espnWeight: number
}) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[72rem] text-sm">
        <thead>
          <tr className="border-b text-left text-xs uppercase tracking-wider text-muted-foreground">
            <th scope="col" className="py-2 pl-4 pr-2 text-right font-medium" title="Court Vision's rank in the standard league">CV</th>
            <th scope="col" className="px-2 py-2 text-right font-medium" title="ESPN's published draft rank for the same format">ESPN</th>
            <th scope="col" className="px-2 py-2 text-right font-medium" title="ESPN's rank minus Court Vision's: positive means Court Vision is higher on him">Gap</th>
            <th scope="col" className="px-3 py-2 font-medium">Player</th>
            <th scope="col" className="px-3 py-2 font-medium">Adjustment</th>
            {STAT_COLUMNS.map((column) => (
              <th key={column.key} scope="col" title={column.title} className="px-2 py-2 text-right font-medium last:pr-4">
                {column.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const open = row.player_id === openId
            return (
              <Fragment key={row.player_id}>
                <ProjectionRowView row={row} format={format} open={open} onToggle={onToggle} />
                {open && (
                  <tr className="border-b">
                    <td colSpan={COLUMNS} className="p-0">
                      <ProjectionDetail
                        // A save replaces the live row: start the form again from what was saved.
                        key={row.adjustment?.id ?? "none"}
                        row={row}
                        kinds={kinds}
                        espnWeight={espnWeight}
                      />
                    </td>
                  </tr>
                )}
              </Fragment>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function ProjectionRowView({
  row,
  format,
  open,
  onToggle,
}: {
  row: ProjectionRow
  format: Format
  open: boolean
  onToggle: (playerId: number) => void
}) {
  const gap = rankGap(row, format)
  const Chevron = open ? ChevronDown : ChevronRight
  const context = [row.team, row.position, row.age != null ? `${Math.floor(row.age)}y` : null].filter(Boolean).join(" · ")
  return (
    <tr className={cn("border-b border-border/50", open && "bg-muted/30")}>
      <td className="py-2 pl-4 pr-2 text-right font-mono text-xs font-semibold tabular-nums">{rankOf(row, format) ?? "—"}</td>
      <td className="px-2 py-2 text-right font-mono text-xs tabular-nums text-muted-foreground">{espnRankOf(row, format) ?? "—"}</td>
      <td className={cn("px-2 py-2 text-right font-mono text-xs tabular-nums", gapIsWide(row, format) ? "font-semibold text-status-projected" : "text-muted-foreground")}>
        {gap == null ? "—" : gap > 0 ? `+${gap}` : gap < 0 ? `−${-gap}` : "0"}
      </td>
      <th scope="row" className="px-3 py-2 text-left font-normal">
        <button
          type="button"
          onClick={() => onToggle(row.player_id)}
          aria-expanded={open}
          className="flex items-center gap-1.5 whitespace-nowrap rounded text-left focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
        >
          <Chevron className="size-3.5 shrink-0 text-muted-foreground" aria-hidden />
          <span className="font-medium">{row.name}</span>
          {context && <span className="font-mono text-xs text-muted-foreground">{context}</span>}
          {!row.statistical && <Badge variant="neutral" title="No NBA minutes: ESPN's line is his line">No history</Badge>}
        </button>
      </th>
      <td className="px-3 py-2 text-xs">
        {row.adjustment ? (
          <span title={row.adjustment.note} className="flex items-center gap-1.5 whitespace-nowrap">
            <Badge variant="projected">{kindLabel(row.adjustment.kind)}</Badge>
            <span className="font-mono text-muted-foreground">{describeChange(row.adjustment)}</span>
          </span>
        ) : (
          <span className="text-muted-foreground">—</span>
        )}
      </td>
      {STAT_COLUMNS.map((column) => (
        <td key={column.key} className="px-2 py-2 text-right font-mono text-xs tabular-nums last:pr-4">
          {formatStat(column, row.final)}
        </td>
      ))}
    </tr>
  )
}
