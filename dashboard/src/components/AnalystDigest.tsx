import { Link } from "react-router"

import { Block, Chip, Choice, Sep, Tag, Tiles, type Tone } from "@/components/blocks"
import { Evidence } from "@/components/ProjectionNote"
import {
  analystTiles,
  dayLabel,
  describeCounts,
  describeSide,
  describeSpend,
  formatSide,
  movedBoardsDay,
  outcomeOf,
  pointsMove,
  READ_FILTERS,
  ruleLabel,
  SIDE_COLUMNS,
  sideDiffers,
  sidesDiffer,
  usd,
  visibleReads,
  type AnalystDigest,
  type DigestMove,
  type DigestRead,
  type DigestSide,
  type OutcomeTone,
  type ReadFilter,
} from "@/lib/analyst"
import { describeNote, kindLabel, readableReason, standingLabel, type AdjustmentEntry } from "@/lib/projections"
import { formatCentral, formatDay } from "@/lib/time"
import dk from "@/desk/desk.module.css"
import s from "./sheet.module.css"
import p from "./projections.module.css"
import a from "./analyst.module.css"

const TONES: Record<OutcomeTone | "plain", Tone | undefined> = {
  plain: undefined,
  up: "up",
  warn: "warn",
  down: "down",
  quiet: "quiet",
}

/** Where the analyst's own runs are listed, and where one is started by hand. */
const RUNS = "/pipelines/analyst"

/**
 * A morning of the analyst, in shadow: every player it read with why the
 * screen picked him, what the read came to, the note it left and what that
 * rests on, and him on the live projection beside the second one, which has
 * the analyst's notes applied and which nothing else reads. Then the widest
 * moves between the two points boards, and the month's spend.
 */
export function AnalystDigestView({
  digest,
  show,
  onShow,
  onDay,
  playerLink,
}: {
  digest: AnalystDigest
  show: ReadFilter
  onShow: (value: ReadFilter) => void
  /** Pick another morning; null is the newest. */
  onDay: (day: string | null) => void
  /** The address of a player's row among the players, opened on his season note. */
  playerLink: (playerId: number) => string
}) {
  const spend = describeSpend(digest.spend)
  const reads = visibleReads(digest.reads, show)
  const boardsDay = movedBoardsDay(digest)

  return (
    <>
      <p className={a.status}>
        <span>{digest.in_shadow ? "in shadow: its notes reach only the second projection and boards" : "live"}</span>
        {digest.spend.model && (
          <>
            <Sep />
            <span>
              {digest.spend.model}
              {digest.spend.prompt_version ? `, prompt ${digest.spend.prompt_version}` : ""}
            </span>
          </>
        )}
        <Sep />
        <span className={spend.tone === "quiet" ? undefined : spend.tone === "down" ? s.bad : s.warn}>{spend.text}</span>
        <Sep />
        <Link to={RUNS} className={s.textLink}>
          its runs
        </Link>
      </p>

      {digest.as_of == null ? (
        <Block title="Reads" count={0}>
          <p className={s.empty}>
            The analyst has not read anybody yet. It runs each morning once the day's projection and boards are stored, and a run can be started by
            hand from its runs.
          </p>
        </Block>
      ) : (
        <>
          <Tiles tiles={analystTiles(digest).map((tile) => ({ ...tile, tone: TONES[tile.tone] }))} />

          <Block
            title="Reads"
            count={reads.length === digest.reads.length ? reads.length : `${reads.length} of ${digest.reads.length}`}
            note={describeCounts(digest.counts)}
            right={<DayPicker digest={digest} onDay={onDay} />}
            text="Everybody the screen picked that morning: why, what the read came to, the note it left with what it rests on, and him on the live projection beside the second one. Nothing else reads the analyst's notes. To act on a read, write a note of your own from his row: a note written by hand is the one applied wherever a player has both."
          >
            <div className={p.controls}>
              <Choice label="Show" options={READ_FILTERS} value={show} onChange={onShow} />
            </div>
            {reads.length === 0 ? (
              <p className={s.empty}>{digest.reads.length === 0 ? "Nobody was read that morning." : "No read of that kind that morning."}</p>
            ) : (
              <ol className={a.reads}>
                {reads.map((read) => (
                  <ReadItem key={read.id} read={read} link={playerLink(read.player_id)} />
                ))}
              </ol>
            )}
          </Block>

          <Block
            title="Moved on the second board"
            count={digest.moved.length}
            note={boardsDay ? `the two boards of ${formatDay(boardsDay)}` : "no second boards stored for that morning yet"}
            text="The widest moves between the two standard points boards that morning: the live one, and the one priced on the projection with the analyst's notes applied. A player moves for his own note or for a teammate's."
          >
            {digest.moved.length === 0 ? (
              <p className={s.empty}>
                {boardsDay ? "Nobody's place differs between the two boards." : "The second boards of that morning are not stored yet, so there is nothing to compare."}
              </p>
            ) : (
              <MovedTable moved={digest.moved} playerLink={playerLink} />
            )}
          </Block>
        </>
      )}
    </>
  )
}

/** The mornings with reads, newest first. One morning needs no picker. */
function DayPicker({ digest, onDay }: { digest: AnalystDigest; onDay: (day: string | null) => void }) {
  if (digest.as_of == null || digest.days.length < 2) return null
  // A morning asked for by its address may be older than the mornings listed
  const days = digest.days.includes(digest.as_of) ? digest.days : [digest.as_of, ...digest.days]
  return (
    <label className={a.day}>
      <span className={dk.label}>Morning</span>
      <select
        className={`${dk.input} ${a.daySelect}`}
        value={digest.as_of}
        onChange={(event) => onDay(event.target.value === digest.days[0] ? null : event.target.value)}
      >
        {days.map((day) => (
          <option key={day} value={day}>
            {dayLabel(day, digest.days)}
          </option>
        ))}
      </select>
    </label>
  )
}

function ReadItem({ read, link }: { read: DigestRead; link: string }) {
  const outcome = outcomeOf(read.outcome)
  return (
    <li className={a.read} data-outcome={read.outcome}>
      <div className={a.readHead}>
        <Link to={link} className={`${p.name} ${a.player}`} title="Open his row among the players, on his season note">
          {read.name}
        </Link>
        {read.team && <Tag>{read.team}</Tag>}
        <Chip tone={TONES[outcome.tone] ?? "quiet"} title={outcome.meaning}>
          {outcome.label}
        </Chip>
        {read.owner_note && <Tag title="A note written by hand stands for him. It is the one applied.">your note stands</Tag>}
        <span className={`${s.mono} ${s.muted} ${a.readMeta}`}>
          {usd(read.cost_usd)} · {formatCentral(read.read_at)}
        </span>
      </div>

      {read.reasons.length > 0 && (
        <ul className={a.reasons} aria-label={`Why ${read.name} was picked`}>
          {read.reasons.map((reason, index) => (
            <li key={index}>
              <Tag>{ruleLabel(reason.rule)}</Tag>
              <span>{readableReason(reason.text)}</span>
            </li>
          ))}
        </ul>
      )}

      {read.summary && <p className={a.summary}>{read.summary}</p>}

      {read.note && <NoteLeft note={read.note} live={read.note_live} said={read.summary} />}

      {read.disagrees && (
        <p className={a.disagrees}>
          <Tag tone="warn">against your note</Tag>
          <span>{read.disagrees}</span>
        </p>
      )}

      {(read.live || read.shadow) && <Sides name={read.name} live={read.live} shadow={read.shadow} />}
    </li>
  )
}

/** The note a read left, as stored: what it states, how sure, when to read it again and what it rests on. */
function NoteLeft({ note, live, said }: { note: AdjustmentEntry; live: boolean; said: string | null | undefined }) {
  const standing = live ? standingLabel(note.standing) : null
  const terms = [
    note.confidence,
    note.review_date && note.standing?.state !== "review_due" ? `read again by ${formatDay(note.review_date)}` : null,
  ].filter(Boolean)
  return (
    <div className={a.note}>
      <div className={p.versionLine}>
        <Chip tone={live ? "up" : "quiet"} title={live ? "The analyst's live note for him" : "A later read replaced or withdrew it"}>
          {live ? "its note" : "replaced since"}
        </Chip>
        <span>{kindLabel(note.kind)}</span>
        <span className={s.sep}>·</span>
        <span>{describeNote(note)}</span>
      </div>
      {(standing || terms.length > 0) && (
        <div className={`${p.versionLine} ${p.versionTerms}`}>
          {standing && <Chip tone={standing.tone}>{standing.label}</Chip>}
          {standing?.detail && <span className={s.muted}>{standing.detail}</span>}
          {terms.length > 0 && <span className={s.muted}>{terms.join(" · ")}</span>}
        </div>
      )}
      {/* A read that wrote a note gives its reason as the read's summary: said once */}
      {note.note && note.note !== said && <p className={p.versionNote}>{note.note}</p>}
      {note.evidence.length > 0 && (
        <ul className={p.evidence} aria-label="What it rests on">
          {note.evidence.map((item, index) => (
            <li key={item.id ?? index}>
              <Evidence item={item} />
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

/**
 * One player on the two projections and their boards. A number the analyst's
 * notes changed is marked; a player they left where he was is one line, so
 * the reads that moved somebody stand out.
 */
function Sides({ name, live, shadow }: { name: string; live: DigestSide | null | undefined; shadow: DigestSide | null | undefined }) {
  if (live && shadow && !sidesDiffer(live, shadow)) {
    return <p className={p.hint}>The same with its notes applied: {describeSide(live)}.</p>
  }
  return (
    <div className={p.miniWrap}>
      <table className={`${p.mini} ${a.sides}`} aria-label={`${name} on the two projections`}>
        <thead>
          <tr>
            <th scope="col" />
            {SIDE_COLUMNS.map((column) => (
              <th key={column.key} scope="col" title={column.title}>
                {column.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          <tr>
            <th scope="row">Live</th>
            {SIDE_COLUMNS.map((column) => (
              <td key={column.key}>{formatSide(column, live)}</td>
            ))}
          </tr>
          <tr>
            <th scope="row">With its notes</th>
            {SIDE_COLUMNS.map((column) => (
              <td key={column.key} data-moved={sideDiffers(column, live, shadow) || undefined}>
                {formatSide(column, shadow)}
              </td>
            ))}
          </tr>
        </tbody>
      </table>
    </div>
  )
}

function MovedTable({ moved, playerLink }: { moved: DigestMove[]; playerLink: (playerId: number) => string }) {
  // Rank on each board and the move, then the three lines' numbers on each side
  const lines = SIDE_COLUMNS.filter((column) => !column.rank)
  return (
    <div className={s.bleed}>
      <table className={`${s.table} ${a.moved}`}>
        <thead>
          <tr>
            <th scope="col">Player</th>
            <th scope="col" className={s.num} title="His place on the live points board">
              Live
            </th>
            <th scope="col" className={s.num} title="His place on the points board with the analyst's notes applied">
              Second
            </th>
            <th scope="col" className={s.num} title="Places gained (+) or lost (−) with the analyst's notes applied">
              Move
            </th>
            {lines.map((column) => (
              <th key={column.key} scope="col" className={s.num} title={`${column.title}: live, then with the analyst's notes`}>
                {column.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {moved.map((move) => {
            const places = pointsMove(move)
            return (
              <tr key={move.player_id}>
                <th scope="row">
                  <Link to={playerLink(move.player_id)} className={`${p.name} ${a.player}`}>
                    {move.name}
                  </Link>{" "}
                  {move.team && <Tag>{move.team}</Tag>}
                </th>
                <td className={s.num}>{formatSide(SIDE_COLUMNS[0], move.live)}</td>
                <td className={s.num}>{formatSide(SIDE_COLUMNS[0], move.shadow)}</td>
                <td className={`${s.num} ${places.startsWith("+") ? s.good : places ? s.bad : ""}`}>{places || "—"}</td>
                {lines.map((column) => (
                  <td key={column.key} className={s.num}>
                    {sideDiffers(column, move.live, move.shadow)
                      ? `${formatSide(column, move.live)} → ${formatSide(column, move.shadow)}`
                      : formatSide(column, move.live)}
                  </td>
                ))}
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
