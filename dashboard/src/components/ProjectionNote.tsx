import { Loader2 } from "lucide-react"
import { useId, useMemo, useState, type FormEvent, type ReactNode } from "react"

import { Chip, Pop, Tag } from "@/components/blocks"
import { usePlayerNews, usePreviewNote, useRetireNote, useSaveNote } from "@/hooks/useProjections"
import { readAuthor, rememberAuthor } from "@/lib/author"
import {
  describeStretch,
  formatStat,
  kindLabel,
  LIVE_COLUMNS,
  noteFormFrom,
  noteKey,
  noteSaveBlocker,
  noteStatesSomething,
  parseNote,
  standingLabel,
  statDiffers,
  withOutForSeason,
  type AdjustmentKind,
  type EvidenceItem,
  type NoteForm,
  type NotePreview,
  type NoteSave,
  type ProjectionRow,
} from "@/lib/projections"
import { formatCentral, formatDay } from "@/lib/time"
import dk from "@/desk/desk.module.css"
import s from "./sheet.module.css"
import p from "./projections.module.css"

const FIELD = `${dk.input} ${p.field}`

/** What the note form needs from the page beyond the player's own row. */
export interface SeasonContext {
  /** The live projection's latest snapshot; null before the season's first. */
  liveAsOf: string | null
  confidences: string[]
  /** How long a note runs before it must be read again, when the form does not say. */
  reviewDays: number
  /** Everyone else on his team: who his minutes can wait on. */
  teammates: ProjectionRow[]
}

/**
 * A player's season note: a fact with an end, read by the live projection
 * alone. A return date (with its range) or a season written off; minutes and
 * a share of the offense that hold through a date or until a teammate plays
 * again; what it rests on, how sure it is, and the day it is read again.
 *
 * Like an adjustment it is tried on before it can be saved. The preview is
 * his live line and the games he is expected to play, not a rank: the board
 * works that out on its next read.
 */
export function NoteEditor({ row, kinds, season }: { row: ProjectionRow; kinds: string[]; season: SeasonContext }) {
  const id = useId()
  const [form, setForm] = useState<NoteForm>(() => noteFormFrom(row.note))
  const [author, setAuthor] = useState(readAuthor)
  const [previewed, setPreviewed] = useState<{ key: string; data: NotePreview } | null>(null)
  const [confirmRetire, setConfirmRetire] = useState(false)
  const news = usePlayerNews(row.player_id, true)
  const preview = usePreviewNote()
  const save = useSaveNote()
  const retire = useRetireNote()

  const parsed = useMemo(() => parseNote(form), [form])
  const key = noteKey(parsed)
  const valid = Object.keys(parsed.errors).length === 0 && noteStatesSomething(parsed.change)
  const fresh = previewed?.key === key ? previewed.data : null
  const blocker = noteSaveBlocker(form, parsed, previewed?.key ?? null, news.isPending)
  const busy = save.isPending || retire.isPending
  const standing = standingLabel(row.note?.standing)

  // Only what is still stored can be cited: a save keeps each item as it reads now.
  const items = news.data?.items ?? []
  const citable = new Set(items.flatMap((item) => (item.id != null ? [item.id] : [])))
  const cited = form.evidence.filter((newsId) => citable.has(newsId))
  const lost = news.data ? form.evidence.length - cited.length : 0

  const set = <K extends keyof NoteForm>(field: K, value: NoteForm[K]) => setForm((current) => ({ ...current, [field]: value }))
  const cite = (newsId: number, on: boolean) =>
    set("evidence", on ? [...form.evidence.filter((other) => other !== newsId), newsId] : form.evidence.filter((other) => other !== newsId))

  function runPreview() {
    preview.mutate(
      { playerId: row.player_id, note: parsed.change, reviewDate: parsed.review_date },
      { onSuccess: (data) => setPreviewed({ key, data }) },
    )
  }

  function submit(event: FormEvent) {
    event.preventDefault()
    if (blocker) return
    rememberAuthor(author)
    save.mutate({
      playerId: row.player_id,
      name: row.name,
      body: {
        ...parsed.change,
        kind: form.kind as AdjustmentKind,
        note: form.note.trim(),
        source_url: form.source_url.trim() || null,
        author: author.trim() || "dashboard",
        confidence: (form.confidence || null) as NoteSave["confidence"],
        review_date: parsed.review_date,
        evidence_news_ids: cited,
      },
    })
  }

  return (
    <form onSubmit={submit} className={p.editor} aria-label={`Season note for ${row.name}`}>
      <div className={p.editorHead}>
        <h3 className={dk.label}>{row.note ? "Edit the season note" : "Add a season note"}</h3>
        <p className={p.hint}>
          A fact with an end, read by the live projection alone.
          {!season.liveAsOf && " None has been built this season: a note saved now is read by the first."}
        </p>
      </div>

      {row.note && standing && (
        <p className={p.standing} data-state={row.note.standing?.state}>
          <Chip tone={standing.tone}>{standing.label}</Chip>
          <span>
            {standing.detail && `${standing.detail}. `}
            {row.note.standing?.state === "review_due" && "It is not applied until it is saved again."}
            {row.note.standing?.state === "ended" && "Retire it, or save what is true now."}
            {row.note.standing?.state === "in_force" && row.note.review_date && `Read again by ${formatDay(row.note.review_date)}.`}
          </span>
        </p>
      )}

      <div className={p.fields}>
        <Field id={`${id}-kind`} label="Kind">
          <select id={`${id}-kind`} value={form.kind} onChange={(event) => set("kind", event.target.value)} className={FIELD}>
            {kinds.map((kind) => (
              <option key={kind} value={kind}>
                {kindLabel(kind)}
              </option>
            ))}
          </select>
        </Field>
        <Field id={`${id}-return`} label="Back on" error={parsed.errors.return_date}>
          <input id={`${id}-return`} type="date" value={form.return_date} disabled={form.out_for_season}
            onChange={(event) => set("return_date", event.target.value)} aria-invalid={!!parsed.errors.return_date} className={FIELD} />
        </Field>
        <Field id={`${id}-earliest`} label="Earliest" error={parsed.errors.return_earliest}>
          <input id={`${id}-earliest`} type="date" value={form.return_earliest} disabled={form.out_for_season}
            onChange={(event) => set("return_earliest", event.target.value)} aria-invalid={!!parsed.errors.return_earliest} className={FIELD} />
        </Field>
        <Field id={`${id}-latest`} label="Latest" error={parsed.errors.return_latest}>
          <input id={`${id}-latest`} type="date" value={form.return_latest} disabled={form.out_for_season}
            onChange={(event) => set("return_latest", event.target.value)} aria-invalid={!!parsed.errors.return_latest} className={FIELD} />
        </Field>
      </div>
      <label className={p.check}>
        <input type="checkbox" checked={form.out_for_season} onChange={(event) => setForm((current) => withOutForSeason(current, event.target.checked))} />
        Out for the season
      </label>

      <div className={p.fields}>
        <Field id={`${id}-minutes`} label="Minutes" error={parsed.errors.minutes}>
          <input id={`${id}-minutes`} inputMode="decimal" placeholder={row.live?.min != null ? row.live.min.toFixed(1) : "the model's"}
            value={form.minutes} onChange={(event) => set("minutes", event.target.value)} aria-invalid={!!parsed.errors.minutes} className={FIELD} />
        </Field>
        <Field id={`${id}-usage`} label="Usage ×" error={parsed.errors.usage}>
          <input id={`${id}-usage`} inputMode="decimal" placeholder="1.0" value={form.usage}
            onChange={(event) => set("usage", event.target.value)} aria-invalid={!!parsed.errors.usage} className={FIELD} />
        </Field>
        <Field id={`${id}-until`} label="Holds through" error={parsed.errors.until_date}>
          <input id={`${id}-until`} type="date" value={form.until_date} onChange={(event) => set("until_date", event.target.value)}
            aria-invalid={!!parsed.errors.until_date} className={FIELD} />
        </Field>
        <Field id={`${id}-teammate`} label="Or until he plays" error={parsed.errors.depends_on}>
          <select id={`${id}-teammate`} value={form.depends_on} onChange={(event) => set("depends_on", event.target.value)}
            aria-invalid={!!parsed.errors.depends_on} className={FIELD}>
            <option value="">nobody</option>
            {/* A teammate since traded, or one this page does not project, is still the one the note names */}
            {row.note?.depends_on_player_id != null && !season.teammates.some((mate) => mate.player_id === row.note?.depends_on_player_id) && (
              <option value={String(row.note.depends_on_player_id)}>
                {row.note.depends_on_name ?? `player ${row.note.depends_on_player_id}`}
              </option>
            )}
            {season.teammates.map((mate) => (
              <option key={mate.player_id} value={String(mate.player_id)}>
                {mate.name}
              </option>
            ))}
          </select>
        </Field>
      </div>

      <div className={p.fields}>
        <Field id={`${id}-confidence`} label="Confidence">
          <select id={`${id}-confidence`} value={form.confidence} onChange={(event) => set("confidence", event.target.value)} className={FIELD}>
            <option value="">not said</option>
            {season.confidences.map((level) => (
              <option key={level} value={level}>
                {level}
              </option>
            ))}
          </select>
        </Field>
        <Field id={`${id}-review`} label="Read again by" error={parsed.errors.review_date}>
          <input id={`${id}-review`} type="date" value={form.review_date} onChange={(event) => set("review_date", event.target.value)}
            aria-invalid={!!parsed.errors.review_date} className={FIELD} />
        </Field>
        <p className={`${p.hint} ${p.fieldNote}`}>
          Blank is {season.reviewDays} days from the save. Past that day the note is no longer applied.
        </p>
      </div>

      <Field id={`${id}-note`} label="Why">
        <textarea id={`${id}-note`} rows={2} value={form.note} onChange={(event) => set("note", event.target.value)}
          placeholder="The fact being recorded: what the report says, and what it does not"
          className={`${dk.input} ${p.textarea}`} />
      </Field>

      <fieldset className={p.rates}>
        <legend className={p.fieldLabel}>What it rests on</legend>
        {news.isPending ? (
          <p className={p.hint}>Loading his news…</p>
        ) : news.error ? (
          <p className={`${p.hint} ${s.bad}`}>His news could not be loaded ({news.error.message}): a note saved now cites nothing.</p>
        ) : items.length === 0 ? (
          <p className={p.hint}>No stored news about him in the last three weeks.</p>
        ) : (
          <ul className={p.evidence}>
            {items.map((item) => (
              <li key={item.id ?? item.headline}>
                <label className={p.check}>
                  <input type="checkbox" checked={item.id != null && form.evidence.includes(item.id)} disabled={item.id == null}
                    onChange={(event) => item.id != null && cite(item.id, event.target.checked)} />
                  <Evidence item={item} />
                </label>
              </li>
            ))}
          </ul>
        )}
        {lost > 0 && (
          <p className={p.hint}>
            {lost === 1 ? "1 item this note cited is" : `${lost} items this note cited are`} no longer in the list and will not be carried
            to a new version. The old version keeps them.
          </p>
        )}
      </fieldset>

      <div className={p.sourceRow}>
        <Field id={`${id}-source`} label="Source">
          <input id={`${id}-source`} type="url" placeholder="https://…" value={form.source_url} onChange={(event) => set("source_url", event.target.value)} className={FIELD} />
        </Field>
        <Field id={`${id}-author`} label="Saved by">
          <input id={`${id}-author`} value={author} maxLength={64} onChange={(event) => setAuthor(event.target.value)} className={FIELD} />
        </Field>
      </div>

      {fresh && <NotePreviewPanel preview={fresh} />}

      <div className={p.actions}>
        <button type="button" className={`${dk.btn} ${dk.btnSmall}`} disabled={!valid || preview.isPending || busy} onClick={runPreview}>
          {preview.isPending && <Loader2 size={12} className={dk.spin} aria-hidden />}
          Preview
        </button>
        <button type="submit" className={`${dk.btn} ${dk.btnSmall} ${dk.btnPrimary}`} disabled={blocker !== null || busy}>
          {save.isPending && <Loader2 size={12} className={dk.spin} aria-hidden />}
          {save.isPending ? "Saving and rebuilding" : "Save and rebuild"}
        </button>
        {row.note && (
          <span className={p.retire}>
            <Pop
              open={confirmRetire}
              onOpenChange={setConfirmRetire}
              trigger={
                <button type="button" className={`${dk.btn} ${dk.btnSmall} ${dk.btnDanger}`} disabled={busy}>
                  {retire.isPending ? "Retiring" : "Retire"}
                </button>
              }
            >
              <p className={s.popTitle}>Retire this note?</p>
              <p className={s.popText}>
                The live projection goes back to the box scores and the availability feed for him. The version is kept, marked retired.
              </p>
              <button
                type="button"
                className={`${dk.btn} ${dk.btnDanger} ${p.confirm}`}
                onClick={() => {
                  setConfirmRetire(false)
                  retire.mutate({ playerId: row.player_id, name: row.name })
                }}
              >
                Retire and rebuild
              </button>
            </Pop>
          </span>
        )}
        {blocker && <p className={`${p.hint} ${p.blocker}`}>{blocker}.</p>}
      </div>
    </form>
  )
}

function Field({ id, label, error, children }: { id: string; label: string; error?: string; children: ReactNode }) {
  return (
    <div className={p.fieldBox}>
      <label htmlFor={id} className={p.fieldLabel}>
        {label}
      </label>
      {children}
      {error && <p className={`${p.hint} ${s.bad}`}>{error}</p>}
    </div>
  )
}

/** One stored item a note can rest on, or did: what it is, what it says, where and when. */
export function Evidence({ item }: { item: EvidenceItem }) {
  const when = item.published_at ? formatCentral(item.published_at) : null
  return (
    <span className={p.evidenceItem}>
      {item.kind && <Tag>{item.kind}</Tag>}
      {item.url ? (
        <a href={item.url} target="_blank" rel="noreferrer" className={s.textLink}>
          {item.headline ?? item.url}
        </a>
      ) : (
        <span>{item.headline ?? "an item with no headline"}</span>
      )}
      <span className={`${s.mono} ${s.muted}`}>{[item.source, when].filter(Boolean).join(", ")}</span>
    </span>
  )
}

/** His live line and the games ahead of him as they stand, and as this note would leave them. */
function NotePreviewPanel({ preview }: { preview: NotePreview }) {
  const { before, after, standing } = preview
  const applies = standing == null || standing.state === "in_force"
  return (
    <div className={p.preview} role="status">
      <p className={p.previewHead}>
        <span className={p.previewTitle}>Preview</span>
        <span>the live projection of {formatDay(preview.as_of)}</span>
        <span>
          {formatStat(LIVE_COLUMNS[0], before.line)} → <span className={s.strong}>{formatStat(LIVE_COLUMNS[0], after.line)}</span> games from
          here
        </span>
      </p>
      {!applies && (
        <p className={`${p.hint} ${s.warn}`}>
          As written it would not be applied: {standing.reasons.join("; ") || "nothing of it is in force"}.
        </p>
      )}
      <div className={p.miniWrap}>
        <table className={p.mini}>
          <thead>
            <tr>
              <th scope="col">
                <span className={s.srOnly}>Line</span>
              </th>
              {LIVE_COLUMNS.map((column) => (
                <th key={column.key} scope="col" title={column.title}>
                  {column.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {([["Now", before.line, undefined], ["After", after.line, before.line]] as const).map(([label, line, against]) => (
              <tr key={label}>
                <th scope="row">{label}</th>
                {LIVE_COLUMNS.map((column) => (
                  <td key={column.key} data-moved={against && statDiffers(column, line, against) ? "true" : undefined}>
                    {formatStat(column, line)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <ol className={p.stretches} aria-label="His team's games from here, after the note">
        {after.stretches.map((stretch) => (
          <li key={stretch.first}>{describeStretch(stretch)}</li>
        ))}
      </ol>
    </div>
  )
}
