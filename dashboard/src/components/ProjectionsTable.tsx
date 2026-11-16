import { ChevronRight } from "lucide-react"
import { Fragment } from "react"

import { Chip, Tag } from "@/components/blocks"
import { ProjectionDetail, type Editing } from "@/components/ProjectionDetail"
import {
  describeChange,
  describeNote,
  espnRankOf,
  formatStat,
  gapIsWide,
  kindLabel,
  rankGap,
  rankOf,
  standingLabel,
  STAT_COLUMNS,
  teammatesOf,
  type AdjustmentEntry,
  type Format,
  type ProjectionRow,
} from "@/lib/projections"
import s from "./sheet.module.css"
import p from "./projections.module.css"

// Rank, ESPN, gap, player, adjustment, then the stat columns.
const COLUMNS = 5 + STAT_COLUMNS.length

/**
 * Every projected player's published line, with Court Vision's rank beside
 * ESPN's. A row opens onto the lines it was built from, the adjustment on
 * top of them and his season note; one row is open at a time.
 */
export function ProjectionsTable({
  rows,
  players,
  format,
  openId,
  onToggle,
  editing,
}: {
  /** The rows shown, filtered and ordered. */
  rows: ProjectionRow[]
  /** Everybody, whatever the filter: a note can wait on a teammate the view has filtered out. */
  players: ProjectionRow[]
  format: Format
  openId: number | null
  onToggle: (playerId: number) => void
  editing: Omit<Editing, "season"> & { season: Omit<Editing["season"], "teammates"> }
}) {
  return (
    <div className={s.bleed}>
      <table className={`${s.table} ${p.table}`}>
        <thead>
          <tr>
            <th scope="col" className={s.num} title="Court Vision's rank in the standard league">CV</th>
            <th scope="col" className={s.num} title="ESPN's published draft rank for the same format">ESPN</th>
            <th scope="col" className={s.num} title="ESPN's rank minus Court Vision's: positive means Court Vision is higher on him">Gap</th>
            <th scope="col">Player</th>
            <th scope="col" title="His season note, read by the live projection, and his preseason adjustment">Note and adjustment</th>
            {STAT_COLUMNS.map((column) => (
              <th key={column.key} scope="col" title={column.title} className={s.num}>
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
                  <tr className={p.detailRow}>
                    <td colSpan={COLUMNS}>
                      <ProjectionDetail
                        // A save replaces the live row: start the form again from what was saved.
                        key={`${row.adjustment?.id ?? "none"}:${row.note?.id ?? "none"}`}
                        row={row}
                        editing={{ ...editing, season: { ...editing.season, teammates: teammatesOf(row, players) } }}
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
  const context = [row.team, row.position, row.age != null ? `${Math.floor(row.age)}y` : null].filter(Boolean).join(" · ")
  return (
    <tr className={p.row} data-open={open ? "true" : undefined}>
      <td className={`${s.num} ${s.strong}`}>{rankOf(row, format) ?? "—"}</td>
      <td className={`${s.num} ${s.muted}`}>{espnRankOf(row, format) ?? "—"}</td>
      <td className={`${s.num} ${gapIsWide(row, format) ? p.wideGap : s.muted}`}>
        {gap == null ? "—" : gap > 0 ? `+${gap}` : gap < 0 ? `−${-gap}` : "0"}
      </td>
      <th scope="row">
        <button type="button" onClick={() => onToggle(row.player_id)} aria-expanded={open} className={p.toggle}>
          <ChevronRight size={13} className={p.chevron} aria-hidden />
          <span className={p.name}>{row.name}</span>
          {context && <span className={`${s.mono} ${s.muted}`}>{context}</span>}
          {!row.statistical && <Tag title="No NBA minutes: ESPN's line is his line">No history</Tag>}
        </button>
      </th>
      <td>
        {row.adjustment || row.note ? (
          <span className={p.judgments}>
            {row.note && <NoteSummary note={row.note} />}
            {row.adjustment && (
              <span title={row.adjustment.note} className={p.adjustment}>
                <Chip tone="warn">{kindLabel(row.adjustment.kind)}</Chip>
                <span className={`${s.mono} ${s.muted}`}>{describeChange(row.adjustment)}</span>
              </span>
            )}
          </span>
        ) : (
          <span className={s.muted}>—</span>
        )}
      </td>
      {STAT_COLUMNS.map((column) => (
        <td key={column.key} className={s.num}>
          {formatStat(column, row.final)}
        </td>
      ))}
    </tr>
  )
}

/** His live season note in a line: whether it is still applied, and what it states. */
function NoteSummary({ note }: { note: AdjustmentEntry }) {
  const standing = standingLabel(note.standing)
  return (
    <span title={note.note} className={p.adjustment}>
      <Chip tone={standing?.tone ?? "up"} title={standing?.detail || undefined}>
        {standing && standing.label !== "in force" ? `Note: ${standing.label}` : "Note"}
      </Chip>
      <span className={`${s.mono} ${s.muted}`}>{describeNote(note)}</span>
    </span>
  )
}
