import { Loader2, Plus, X } from "lucide-react"
import { useId, useMemo, useState, type FormEvent, type ReactNode } from "react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
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
  describeChange,
  formFrom,
  formatStat,
  kindLabel,
  parseChange,
  rankMove,
  RATE_KEYS,
  saveBlocker,
  STAT_COLUMNS,
  statDiffers,
  type AdjustmentEntry,
  type AdjustmentForm,
  type AdjustmentKind,
  type ProjectionLine,
  type ProjectionPreview,
  type ProjectionRow,
} from "@/lib/projections"
import { formatCentral } from "@/lib/time"
import { cn } from "@/lib/utils"

const FIELD = "h-8 font-mono text-xs"
const SELECT =
  "h-8 rounded-md border border-input bg-transparent px-2 font-mono text-xs shadow-sm focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"

/**
 * One player, opened: the four lines his projection passes through, and the
 * adjustment that turns the blend into the final line. An edit is previewed
 * before it can be saved, so the rank move is seen before it is made.
 *
 * The parent keys this by the live adjustment's id, so a save (which replaces
 * that row) starts the form again from what was saved.
 */
export function ProjectionDetail({ row, kinds, espnWeight }: { row: ProjectionRow; kinds: string[]; espnWeight: number }) {
  return (
    <div className="grid gap-5 bg-muted/20 px-4 py-4 md:px-6 xl:grid-cols-[minmax(0,1.15fr)_minmax(0,1fr)]">
      <div className="flex min-w-0 flex-col gap-4">
        <LinesTable row={row} espnWeight={espnWeight} />
        <History playerId={row.player_id} />
      </div>
      <AdjustmentEditor row={row} kinds={kinds} />
    </div>
  )
}

// ---- the four lines --------------------------------------------------------------------------

function LinesTable({ row, espnWeight }: { row: ProjectionRow; espnWeight: number }) {
  const weight = row.espn_weight ?? espnWeight
  const lines: { label: string; hint: string; line: ProjectionLine | null; against?: ProjectionLine | null }[] = [
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
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[38rem] text-xs">
        <thead>
          <tr className="border-b text-left uppercase tracking-wider text-muted-foreground">
            <th scope="col" className="py-1.5 pr-3 font-medium">Line</th>
            {STAT_COLUMNS.map((column) => (
              <th key={column.key} scope="col" title={column.title} className="px-2 py-1.5 text-right font-medium">
                {column.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="font-mono tabular-nums">
          {lines.map(({ label, hint, line, against }) => (
            <tr key={label} className="border-b border-border/50 last:border-0">
              <th scope="row" title={hint} className={cn("py-1.5 pr-3 text-left font-sans font-medium", label === "Final" && "text-primary")}>
                {label}
              </th>
              {STAT_COLUMNS.map((column) => (
                <td
                  key={column.key}
                  className={cn(
                    "px-2 py-1.5 text-right",
                    !line && "text-muted-foreground",
                    // Where the adjustment moved the blend.
                    against !== undefined && statDiffers(column, line, against) && "font-semibold text-status-projected",
                  )}
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

const STATE_VARIANT = { live: "win", superseded: "neutral", retired: "loss" } as const

function History({ playerId }: { playerId: number }) {
  const history = useAdjustmentHistory(playerId, true)
  const versions = history.data?.versions ?? []
  if (history.isPending) return <p className="font-mono text-xs text-muted-foreground">Loading versions…</p>
  if (history.error) return <p className="font-mono text-xs text-status-loss">Versions unavailable: {history.error.message}</p>
  if (versions.length === 0) return <p className="font-mono text-xs text-muted-foreground">No adjustment has been recorded for him this season.</p>
  return (
    <div>
      <h3 className="mb-1.5 text-xs font-medium uppercase tracking-wider text-muted-foreground">Versions</h3>
      <ol className="flex flex-col gap-2">
        {versions.map((version) => (
          <VersionItem key={version.id} version={version} />
        ))}
      </ol>
    </div>
  )
}

function VersionItem({ version }: { version: AdjustmentEntry }) {
  const variant = STATE_VARIANT[version.state as keyof typeof STATE_VARIANT] ?? "neutral"
  return (
    <li className="text-xs">
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1 font-mono">
        <Badge variant={variant}>{version.state}</Badge>
        <span>{kindLabel(version.kind)}</span>
        <span className="text-border">·</span>
        <span>{describeChange(version)}</span>
        <span className="text-border">·</span>
        <span className="text-muted-foreground">
          {version.author}, {formatCentral(version.created_at)}
        </span>
      </div>
      <p className="mt-0.5 text-muted-foreground">
        {version.note}
        {version.source_url && (
          <>
            {" "}
            <a href={version.source_url} target="_blank" rel="noreferrer" className="text-primary underline-offset-4 hover:underline">
              source
            </a>
          </>
        )}
      </p>
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
    <form onSubmit={submit} className="flex min-w-0 flex-col gap-3" aria-label={`Adjustment for ${row.name}`}>
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h3 className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
          {row.adjustment ? "Edit the adjustment" : "Add an adjustment"}
        </h3>
        <p className="text-xs text-muted-foreground">Minutes and games are targets, not changes. Blank leaves the blend alone.</p>
      </div>

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
        <Field id={`${id}-kind`} label="Kind">
          <select id={`${id}-kind`} value={form.kind} onChange={(event) => set("kind", event.target.value)} className={cn(SELECT, "w-full")}>
            {kinds.map((kind) => (
              <option key={kind} value={kind}>{kindLabel(kind)}</option>
            ))}
          </select>
        </Field>
        <Field id={`${id}-minutes`} label="Minutes" error={parsed.errors.minutes}>
          <Input id={`${id}-minutes`} inputMode="decimal" placeholder={formatStat(STAT_COLUMNS[1], row.blended)} value={form.minutes}
            onChange={(event) => set("minutes", event.target.value)} aria-invalid={!!parsed.errors.minutes} className={FIELD} />
        </Field>
        <Field id={`${id}-games`} label="Games" error={parsed.errors.games}>
          <Input id={`${id}-games`} inputMode="numeric" placeholder={formatStat(STAT_COLUMNS[0], row.blended)} value={form.games}
            onChange={(event) => set("games", event.target.value)} aria-invalid={!!parsed.errors.games} className={FIELD} />
        </Field>
        <Field id={`${id}-return`} label="Back on" error={parsed.errors.return_date}>
          <Input id={`${id}-return`} type="date" value={form.return_date} onChange={(event) => set("return_date", event.target.value)}
            aria-invalid={!!parsed.errors.return_date} className={FIELD} />
        </Field>
        <Field id={`${id}-usage`} label="Usage ×" error={parsed.errors.usage}>
          <Input id={`${id}-usage`} inputMode="decimal" placeholder="1.0" value={form.usage} onChange={(event) => set("usage", event.target.value)}
            aria-invalid={!!parsed.errors.usage} className={FIELD} />
        </Field>
      </div>

      <fieldset className="flex flex-col gap-1.5">
        <legend className="mb-1 text-xs text-muted-foreground">Single-stat multipliers</legend>
        {form.rates.map((rate, index) => (
          <div key={index} className="flex items-center gap-2">
            <select aria-label={`Stat ${index + 1}`} value={rate.key} onChange={(event) => setRate(index, { key: event.target.value })} className={SELECT}>
              <option value="">stat…</option>
              {RATE_KEYS.map((stat) => (
                <option key={stat} value={stat}>{stat}</option>
              ))}
            </select>
            <span className="font-mono text-xs text-muted-foreground">×</span>
            <Input aria-label={`Multiplier ${index + 1}`} inputMode="decimal" placeholder="1.0" value={rate.multiplier}
              onChange={(event) => setRate(index, { multiplier: event.target.value })} className={cn(FIELD, "w-20")} />
            <Button type="button" variant="ghost" size="sm" aria-label={`Remove multiplier ${index + 1}`} className="h-7 px-1.5"
              onClick={() => set("rates", form.rates.filter((_, i) => i !== index))}>
              <X className="size-3.5" aria-hidden />
            </Button>
          </div>
        ))}
        {parsed.errors.rates && <p className="text-xs text-status-loss">{parsed.errors.rates}</p>}
        <Button type="button" variant="ghost" size="sm" className="h-7 w-fit gap-1 px-2 text-xs text-muted-foreground"
          onClick={() => set("rates", [...form.rates, { key: "", multiplier: "" }])}>
          <Plus className="size-3.5" aria-hidden /> Add a multiplier
        </Button>
      </fieldset>

      <Field id={`${id}-note`} label="Why">
        <textarea id={`${id}-note`} rows={2} value={form.note} onChange={(event) => set("note", event.target.value)}
          placeholder="The judgment being recorded: what changed, and how sure it is"
          className="w-full rounded-md border border-input bg-transparent px-3 py-1.5 text-xs shadow-sm placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring" />
      </Field>
      <div className="grid gap-3 sm:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
        <Field id={`${id}-source`} label="Source">
          <Input id={`${id}-source`} type="url" placeholder="https://…" value={form.source_url} onChange={(event) => set("source_url", event.target.value)} className={FIELD} />
        </Field>
        <Field id={`${id}-author`} label="Saved by">
          <Input id={`${id}-author`} value={author} maxLength={64} onChange={(event) => setAuthor(event.target.value)} className={FIELD} />
        </Field>
      </div>

      {fresh && <PreviewPanel preview={fresh} />}

      <div className="flex flex-wrap items-center gap-2">
        <Button type="button" variant="outline" size="sm" disabled={!valid || preview.isPending || busy} onClick={runPreview} className="gap-1.5">
          {preview.isPending && <Loader2 className="size-3.5 animate-spin" aria-hidden />}
          Preview
        </Button>
        <Button type="submit" size="sm" disabled={blocker !== null || busy} className="gap-1.5">
          {save.isPending && <Loader2 className="size-3.5 animate-spin" aria-hidden />}
          {save.isPending ? "Saving and republishing" : "Save and republish"}
        </Button>
        {row.adjustment && (
          <Popover open={confirmRetire} onOpenChange={setConfirmRetire}>
            <PopoverTrigger asChild>
              <Button type="button" variant="ghost" size="sm" disabled={busy} className="ml-auto text-status-loss hover:text-status-loss">
                {retire.isPending ? "Retiring" : "Retire"}
              </Button>
            </PopoverTrigger>
            <PopoverContent align="end" className="w-64">
              <p className="text-sm font-medium">Retire this adjustment?</p>
              <p className="mt-0.5 text-xs text-muted-foreground">
                His published line goes back to the blend. The version is kept, marked retired.
              </p>
              <Button type="button" variant="destructive" size="sm" className="mt-3 w-full"
                onClick={() => {
                  setConfirmRetire(false)
                  retire.mutate({ playerId: row.player_id, name: row.name })
                }}>
                Retire and republish
              </Button>
            </PopoverContent>
          </Popover>
        )}
        {blocker && <p className="basis-full text-xs text-muted-foreground">{blocker}.</p>}
      </div>
    </form>
  )
}

function Field({ id, label, error, children }: { id: string; label: string; error?: string; children: ReactNode }) {
  return (
    <div className="flex min-w-0 flex-col gap-1">
      <label htmlFor={id} className="text-xs text-muted-foreground">{label}</label>
      {children}
      {error && <p className="text-xs text-status-loss">{error}</p>}
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
        {label} #{was ?? "—"} → <span className="font-semibold text-foreground">#{will ?? "—"}</span>
        {move && <span className={cn("ml-1", move.startsWith("+") ? "text-status-win" : "text-status-loss")}>{move}</span>}
      </span>
    )
  }
  return (
    <div className="rounded-md border border-primary/30 bg-primary/5 px-3 py-2" role="status">
      <p className="mb-1.5 flex flex-wrap gap-x-4 gap-y-1 font-mono text-xs text-muted-foreground">
        <span className="font-sans font-medium text-foreground">Preview</span>
        {preview.ranks_available ? (
          <>
            {rank("points", before.ranks.points, after.ranks.points)}
            {rank("9-cat", before.ranks.categories, after.ranks.categories)}
          </>
        ) : (
          <span>ranks unavailable ({preview.ranks_reason ?? "the backend did not answer"})</span>
        )}
      </p>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[34rem] font-mono text-xs tabular-nums">
          <thead>
            <tr className="text-muted-foreground">
              <th scope="col" className="pr-3 text-left font-sans font-medium"><span className="sr-only">Line</span></th>
              {STAT_COLUMNS.map((column) => (
                <th key={column.key} scope="col" className="px-1.5 text-right font-medium">{column.label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {([["Now", before.final, undefined], ["After", after.final, before.final]] as const).map(([label, line, against]) => (
              <tr key={label}>
                <th scope="row" className="pr-3 text-left font-sans font-medium">{label}</th>
                {STAT_COLUMNS.map((column) => (
                  <td key={column.key} className={cn("px-1.5 text-right", against && statDiffers(column, line, against) && "font-semibold text-status-projected")}>
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
