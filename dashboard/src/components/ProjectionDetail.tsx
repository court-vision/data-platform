import { Loader2, Plus, X } from "lucide-react"
import { useId, useMemo, useState, type FormEvent, type ReactNode } from "react"

import { Chip, Pop, Rail, Tag, type Tone } from "@/components/blocks"
import { Evidence, NoteEditor, type SeasonContext } from "@/components/ProjectionNote"
import {
  useAdjustmentHistory,
  usePreviewAdjustment,
  useRetireAdjustment,
  useSaveAdjustment,
} from "@/hooks/useProjections"
import { readAuthor, rememberAuthor } from "@/lib/author"
import {
  changeKey,
  changesSomething,
  describeEntry,
  EDIT_MODES,
  formFrom,
  formatStat,
  kindLabel,
  LIVE_COLUMNS,
  parseChange,
  rankMove,
  RATE_KEYS,
  saveBlocker,
  standingLabel,
  STAT_COLUMNS,
  statDiffers,
  type AdjustmentEntry,
  type AdjustmentForm,
  type AdjustmentKind,
  type EditMode,
  type ProjectionLine,
  type ProjectionPreview,
  type ProjectionRow,
} from "@/lib/projections"
import { formatCentral, formatDay } from "@/lib/time"
import dk from "@/desk/desk.module.css"
import s from "./sheet.module.css"
import p from "./projections.module.css"

const FIELD = `${dk.input} ${p.field}`

/** What an opened row needs from the page: how to edit, and what a season note is written against. */
export interface Editing {
  kinds: string[]
  espnWeight: number
  season: SeasonContext
  /** Which of the two forms is open: the preseason adjustment or the season note. */
  mode: EditMode
  onMode: (mode: EditMode) => void
}

/**
 * One player, opened: the four lines his projection passes through, the live
 * line the board is priced on in season, and the two things a person can lay
 * over them. A preseason adjustment turns the blend into the final line; a
 * season note is a fact with an end, read by the live projection. Either is
 * previewed before it can be saved.
 *
 * The parent keys this by the ids of the live adjustment and the live note,
 * so a save (which replaces that row) starts the form again from what was
 * saved.
 */
export function ProjectionDetail({ row, editing }: { row: ProjectionRow; editing: Editing }) {
  const { kinds, espnWeight, season, mode, onMode } = editing
  return (
    <div className={p.detail}>
      <div className={p.detailSide}>
        <LinesTable row={row} espnWeight={espnWeight} liveAsOf={season.liveAsOf} />
        <History playerId={row.player_id} />
      </div>
      <div className={p.editing}>
        <Rail label="Edit" options={EDIT_MODES} value={mode} onChange={onMode} />
        {mode === "note" ? <NoteEditor row={row} kinds={kinds} season={season} /> : <AdjustmentEditor row={row} kinds={kinds} />}
      </div>
    </div>
  )
}

// ---- the four lines --------------------------------------------------------------------------

function LinesTable({ row, espnWeight, liveAsOf }: { row: ProjectionRow; espnWeight: number; liveAsOf: string | null }) {
  const weight = row.espn_weight ?? espnWeight
  const lines: { label: string; hint: string; line: ProjectionLine | null; against?: ProjectionLine | null; live?: boolean }[] = [
    {
      label: "Statistical",
      hint: row.statistical
        ? `History alone: ${row.seasons.map((year) => `${year}-${String((year + 1) % 100).padStart(2, "0")}`).join(", ")}, aged and regressed`
        : "No NBA minutes to project from",
      line: row.statistical,
    },
    { label: "ESPN", hint: row.espn ? "ESPN's published projection" : "ESPN projects nobody here", line: row.espn },
    {
      label: "Blend",
      hint: `The two combined: ESPN's share is ${Math.round(weight * 100)}%`,
      line: row.blended,
    },
    {
      label: "Final",
      hint: row.adjustment ? "The blend with the adjustment applied: what is published" : "No adjustment: the blend is what is published",
      line: row.final,
      against: row.blended,
    },
  ]
  // In season the board is priced on this one. Its games are the ones left, not a season's.
  if (liveAsOf) {
    lines.push({
      label: "Live, from here",
      hint: row.live
        ? `The live projection of ${formatDay(liveAsOf)}: his line in a game he plays, over the games he is expected to play from here`
        : `Not in the live projection of ${formatDay(liveAsOf)}`,
      line: row.live,
      live: true,
    })
  }
  return (
    <div className={p.miniWrap}>
      <table className={p.mini}>
        <thead>
          <tr>
            <th scope="col">Line</th>
            {STAT_COLUMNS.map((column) => (
              <th key={column.key} scope="col" title={column.title}>
                {column.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {lines.map(({ label, hint, line, against, live }) => (
            <tr key={label} className={live ? p.liveLine : undefined}>
              <th scope="row" title={hint} className={label === "Final" ? p.final : undefined}>
                {label}
              </th>
              {(live ? LIVE_COLUMNS : STAT_COLUMNS).map((column) => (
                <td
                  key={column.key}
                  // Where the adjustment moved the blend.
                  data-moved={against !== undefined && statDiffers(column, line, against) ? "true" : undefined}
                  className={!line ? s.muted : undefined}
                  title={live ? column.title : undefined}
                >
                  {formatStat(column, line)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ---- every version ---------------------------------------------------------------------------

const STATE_TONE: Record<string, Tone> = { live: "up", superseded: "quiet", retired: "down" }

function History({ playerId }: { playerId: number }) {
  const history = useAdjustmentHistory(playerId, true)
  const versions = history.data?.versions ?? []
  if (history.isPending) return <p className={`${s.mono} ${s.muted}`}>Loading versions…</p>
  if (history.error) return <p className={`${s.mono} ${s.bad}`}>Versions unavailable: {history.error.message}</p>
  if (versions.length === 0) return <p className={`${s.mono} ${s.muted}`}>No adjustment or note has been recorded for him this season.</p>
  return (
    <div>
      <h3 className={`${dk.label} ${p.sectionLabel}`}>Versions</h3>
      <ol className={p.versions}>
        {versions.map((version) => (
          <VersionItem key={version.id} version={version} />
        ))}
      </ol>
    </div>
  )
}

function VersionItem({ version }: { version: AdjustmentEntry }) {
  const season = version.scope === "season"
  // Only the live note has a standing: whether it is still applied this morning.
  const standing = standingLabel(version.standing)
  const flagged = standing != null && standing.label !== "in force"
  const terms = [
    version.confidence,
    // Past its review date the standing says so itself
    version.review_date && version.standing?.state !== "review_due" ? `read again by ${formatDay(version.review_date)}` : null,
  ].filter(Boolean)
  return (
    <li>
      <div className={p.versionLine}>
        <Chip tone={STATE_TONE[version.state] ?? "quiet"}>{version.state}</Chip>
        <Tag>{season ? "Season note" : "Preseason"}</Tag>
        <span>{kindLabel(version.kind)}</span>
        <span className={s.sep}>·</span>
        <span>{describeEntry(version)}</span>
        <span className={s.sep}>·</span>
        <span className={s.muted}>
          {version.author}, {formatCentral(version.created_at)}
        </span>
      </div>
      {season && (terms.length > 0 || flagged) && (
        <div className={`${p.versionLine} ${p.versionTerms}`}>
          {flagged && <Chip tone={standing.tone}>{standing.label}</Chip>}
          {flagged && standing.detail && <span className={s.muted}>{standing.detail}</span>}
          {terms.length > 0 && <span className={s.muted}>{terms.join(" · ")}</span>}
        </div>
      )}
      <p className={p.versionNote}>
        {version.note}
        {version.source_url && (
          <>
            {" "}
            <a href={version.source_url} target="_blank" rel="noreferrer" className={s.textLink}>
              source
            </a>
          </>
        )}
      </p>
      {version.evidence.length > 0 && (
        <ul className={p.evidence}>
          {version.evidence.map((item, index) => (
            <li key={item.id ?? index}>
              <Evidence item={item} />
            </li>
          ))}
        </ul>
      )}
    </li>
  )
}

// ---- the editor ------------------------------------------------------------------------------

function AdjustmentEditor({ row, kinds }: { row: ProjectionRow; kinds: string[] }) {
  const id = useId()
  const [form, setForm] = useState<AdjustmentForm>(() => formFrom(row.adjustment))
  const [author, setAuthor] = useState(readAuthor)
  const [previewed, setPreviewed] = useState<{ key: string; data: ProjectionPreview } | null>(null)
  const [confirmRetire, setConfirmRetire] = useState(false)
  const preview = usePreviewAdjustment()
  const save = useSaveAdjustment()
  const retire = useRetireAdjustment()

  const parsed = useMemo(() => parseChange(form), [form])
  const key = changeKey(parsed.change)
  const valid = Object.keys(parsed.errors).length === 0 && changesSomething(parsed.change)
  const fresh = previewed?.key === key ? previewed.data : null
  const blocker = saveBlocker(form, parsed, previewed?.key ?? null)
  const busy = save.isPending || retire.isPending

  const set = <K extends keyof AdjustmentForm>(field: K, value: AdjustmentForm[K]) => setForm((current) => ({ ...current, [field]: value }))
  const setRate = (index: number, patch: Partial<AdjustmentForm["rates"][number]>) =>
    set("rates", form.rates.map((rate, i) => (i === index ? { ...rate, ...patch } : rate)))

  function runPreview() {
    preview.mutate(
      { playerId: row.player_id, change: parsed.change },
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
      },
    })
  }

  return (
    <form onSubmit={submit} className={p.editor} aria-label={`Adjustment for ${row.name}`}>
      <div className={p.editorHead}>
        <h3 className={dk.label}>{row.adjustment ? "Edit the adjustment" : "Add an adjustment"}</h3>
        <p className={p.hint}>Minutes and games are targets, not changes. Blank leaves the blend alone.</p>
      </div>

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
        <Field id={`${id}-minutes`} label="Minutes" error={parsed.errors.minutes}>
          <input id={`${id}-minutes`} inputMode="decimal" placeholder={formatStat(STAT_COLUMNS[1], row.blended)} value={form.minutes}
            onChange={(event) => set("minutes", event.target.value)} aria-invalid={!!parsed.errors.minutes} className={FIELD} />
        </Field>
        <Field id={`${id}-games`} label="Games" error={parsed.errors.games}>
          <input id={`${id}-games`} inputMode="numeric" placeholder={formatStat(STAT_COLUMNS[0], row.blended)} value={form.games}
            onChange={(event) => set("games", event.target.value)} aria-invalid={!!parsed.errors.games} className={FIELD} />
        </Field>
        <Field id={`${id}-return`} label="Back on" error={parsed.errors.return_date}>
          <input id={`${id}-return`} type="date" value={form.return_date} onChange={(event) => set("return_date", event.target.value)}
            aria-invalid={!!parsed.errors.return_date} className={FIELD} />
        </Field>
        <Field id={`${id}-usage`} label="Usage ×" error={parsed.errors.usage}>
          <input id={`${id}-usage`} inputMode="decimal" placeholder="1.0" value={form.usage} onChange={(event) => set("usage", event.target.value)}
            aria-invalid={!!parsed.errors.usage} className={FIELD} />
        </Field>
      </div>

      <fieldset className={p.rates}>
        <legend className={p.fieldLabel}>Single-stat multipliers</legend>
        {form.rates.map((rate, index) => (
          <div key={index} className={p.rate}>
            <select aria-label={`Stat ${index + 1}`} value={rate.key} onChange={(event) => setRate(index, { key: event.target.value })} className={FIELD}>
              <option value="">stat…</option>
              {RATE_KEYS.map((stat) => (
                <option key={stat} value={stat}>
                  {stat}
                </option>
              ))}
            </select>
            <span className={`${s.mono} ${s.muted}`}>×</span>
            <input aria-label={`Multiplier ${index + 1}`} inputMode="decimal" placeholder="1.0" value={rate.multiplier}
              onChange={(event) => setRate(index, { multiplier: event.target.value })} className={`${FIELD} ${p.multiplier}`} />
            <button type="button" className={dk.iconBtn} aria-label={`Remove multiplier ${index + 1}`}
              onClick={() => set("rates", form.rates.filter((_, i) => i !== index))}>
              <X size={13} aria-hidden />
            </button>
          </div>
        ))}
        {parsed.errors.rates && <p className={`${p.hint} ${s.bad}`}>{parsed.errors.rates}</p>}
        <button type="button" className={`${dk.ghost} ${p.addRate}`} onClick={() => set("rates", [...form.rates, { key: "", multiplier: "" }])}>
          <Plus size={13} aria-hidden /> Add a multiplier
        </button>
      </fieldset>

      <Field id={`${id}-note`} label="Why">
        <textarea id={`${id}-note`} rows={2} value={form.note} onChange={(event) => set("note", event.target.value)}
          placeholder="The judgment being recorded: what changed, and how sure it is"
          className={`${dk.input} ${p.textarea}`} />
      </Field>
      <div className={p.sourceRow}>
        <Field id={`${id}-source`} label="Source">
          <input id={`${id}-source`} type="url" placeholder="https://…" value={form.source_url} onChange={(event) => set("source_url", event.target.value)} className={FIELD} />
        </Field>
        <Field id={`${id}-author`} label="Saved by">
          <input id={`${id}-author`} value={author} maxLength={64} onChange={(event) => setAuthor(event.target.value)} className={FIELD} />
        </Field>
      </div>

      {fresh && <PreviewPanel preview={fresh} />}

      <div className={p.actions}>
        <button type="button" className={`${dk.btn} ${dk.btnSmall}`} disabled={!valid || preview.isPending || busy} onClick={runPreview}>
          {preview.isPending && <Loader2 size={12} className={dk.spin} aria-hidden />}
          Preview
        </button>
        <button type="submit" className={`${dk.btn} ${dk.btnSmall} ${dk.btnPrimary}`} disabled={blocker !== null || busy}>
          {save.isPending && <Loader2 size={12} className={dk.spin} aria-hidden />}
          {save.isPending ? "Saving and republishing" : "Save and republish"}
        </button>
        {row.adjustment && (
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
              <p className={s.popTitle}>Retire this adjustment?</p>
              <p className={s.popText}>His published line goes back to the blend. The version is kept, marked retired.</p>
              <button
                type="button"
                className={`${dk.btn} ${dk.btnDanger} ${p.confirm}`}
                onClick={() => {
                  setConfirmRetire(false)
                  retire.mutate({ playerId: row.player_id, name: row.name })
                }}
              >
                Retire and republish
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

/** The line and the ranks as they stand, and as these numbers would leave them. */
function PreviewPanel({ preview }: { preview: ProjectionPreview }) {
  const { before, after } = preview
  const rank = (label: string, was: number | null, will: number | null) => {
    const move = rankMove(was, will)
    return (
      <span>
        {label} #{was ?? "—"} → <span className={s.strong}>#{will ?? "—"}</span>
        {move && <span className={move.startsWith("+") ? s.good : s.bad}> {move}</span>}
      </span>
    )
  }
  return (
    <div className={p.preview} role="status">
      <p className={p.previewHead}>
        <span className={p.previewTitle}>Preview</span>
        {preview.ranks_available ? (
          <>
            {rank("points", before.ranks.points, after.ranks.points)}
            {rank("9-cat", before.ranks.categories, after.ranks.categories)}
          </>
        ) : (
          <span>ranks unavailable ({preview.ranks_reason ?? "the backend did not answer"})</span>
        )}
      </p>
      <div className={p.miniWrap}>
        <table className={p.mini}>
          <thead>
            <tr>
              <th scope="col">
                <span className={s.srOnly}>Line</span>
              </th>
              {STAT_COLUMNS.map((column) => (
                <th key={column.key} scope="col">
                  {column.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {([["Now", before.final, undefined], ["After", after.final, before.final]] as const).map(([label, line, against]) => (
              <tr key={label}>
                <th scope="row">{label}</th>
                {STAT_COLUMNS.map((column) => (
                  <td key={column.key} data-moved={against && statDiffers(column, line, against) ? "true" : undefined}>
                    {formatStat(column, line)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
