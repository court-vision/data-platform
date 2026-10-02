import { Loader2, RefreshCw, TriangleAlert, Upload } from "lucide-react"
import { useSearchParams } from "react-router"

import { ProjectionsTable } from "@/components/ProjectionsTable"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Skeleton } from "@/components/ui/skeleton"
import { useNow } from "@/hooks/useNow"
import { useProjections, useRepublish } from "@/hooks/useProjections"
import {
  describePublished,
  describeRanks,
  FILTERS,
  FORMATS,
  SORTS,
  summaryTiles,
  visibleRows,
  type Filter,
  type Format,
  type ProjectionsData,
  type Sort,
  type TileTone,
} from "@/lib/projections"
import { formatDay, relativeTime } from "@/lib/time"
import { cn } from "@/lib/utils"

function oneOf<T extends string>(options: readonly { key: T }[], value: string | null, fallback: T): T {
  return options.some((option) => option.key === value) ? (value as T) : fallback
}

/**
 * Court Vision's projection, the lines it is built from, and the adjustments
 * on top. The view lives in the URL — format, filter, sort, search and the
 * open player — so a link lands on the row being discussed.
 */
export function Projections() {
  const [params, setParams] = useSearchParams()
  const projections = useProjections()
  const republish = useRepublish()
  const now = useNow()
  const data = projections.data

  const format = oneOf(FORMATS, params.get("format"), "points")
  const filter = oneOf(FILTERS, params.get("filter"), "all")
  const sort = oneOf(SORTS, params.get("sort"), "cv")
  const query = params.get("q") ?? ""
  const openId = Number(params.get("open")) || null

  /** Change one part of the view; a default is left out of the URL. */
  function update(name: string, value: string | null, fallback: string) {
    setParams(
      (current) => {
        const next = new URLSearchParams(current)
        if (!value || value === fallback) next.delete(name)
        else next.set(name, value)
        return next
      },
      { replace: true },
    )
  }

  const rows = data ? visibleRows(data.players, { query, filter, sort, format }) : []

  return (
    <div className="mx-auto flex max-w-[92rem] flex-col gap-6 p-4 md:p-8">
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="font-display text-2xl font-bold tracking-tight">Projections</h1>
          <p className="max-w-prose text-sm text-muted-foreground">
            Court Vision's projection for {data?.season ?? "the season"}: the lines it is built from, and the adjustments on top.
            Saving an adjustment republishes what the draft board reads.
          </p>
        </div>
        <div className="flex items-center gap-2">
          {projections.dataUpdatedAt > 0 && (
            <p className="font-mono text-xs text-muted-foreground">
              computed {relativeTime(new Date(projections.dataUpdatedAt).toISOString(), now)}
            </p>
          )}
          <Button variant="outline" size="sm" className="gap-1.5" disabled={projections.isFetching} onClick={() => projections.refetch()}>
            <RefreshCw className={cn("size-3.5", projections.isFetching && "animate-spin")} aria-hidden />
            Refresh
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="gap-1.5"
            disabled={republish.isPending}
            title="Run cv-projection now: write today's snapshot from the lines on this page"
            onClick={() => republish.mutate()}
          >
            {republish.isPending ? <Loader2 className="size-3.5 animate-spin" aria-hidden /> : <Upload className="size-3.5" aria-hidden />}
            Republish
          </Button>
        </div>
      </header>

      {projections.error && (
        <div role="alert" className="flex items-start gap-2 rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          <TriangleAlert className="mt-0.5 size-4 shrink-0" aria-hidden />
          <span>
            Could not load the projections: {projections.error.message}
            {data && " Showing the last good data."}
          </span>
        </div>
      )}

      {!data ? (
        projections.error ? null : <LoadingState />
      ) : (
        <>
          <SummaryTiles data={data} format={format} />
          <StatusLines data={data} />

          <Card>
            <CardHeader className="gap-3 pb-3">
              <CardTitle className="flex items-baseline gap-2 text-base">
                Players
                <span className="font-mono text-xs font-normal text-muted-foreground">
                  {rows.length === data.players.length ? rows.length : `${rows.length} of ${data.players.length}`}
                </span>
              </CardTitle>
              <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
                <Input
                  type="search"
                  aria-label="Search players"
                  placeholder="Name, or a team code"
                  value={query}
                  onChange={(event) => update("q", event.target.value, "")}
                  className="h-8 w-56 text-xs"
                />
                <Choice label="Rank in" options={FORMATS} value={format} onChange={(value: Format) => update("format", value, "points")} />
                <Choice label="Show" options={FILTERS} value={filter} onChange={(value: Filter) => update("filter", value, "all")} />
                <Choice label="Order by" options={SORTS} value={sort} onChange={(value: Sort) => update("sort", value, "cv")} />
              </div>
            </CardHeader>
            <CardContent className="px-0 pb-2">
              {rows.length === 0 ? (
                <p className="px-6 py-8 text-center text-sm text-muted-foreground">Nobody matches.</p>
              ) : (
                <ProjectionsTable
                  rows={rows}
                  format={format}
                  openId={openId}
                  onToggle={(playerId) => update("open", playerId === openId ? null : String(playerId), "")}
                  kinds={data.kinds}
                  espnWeight={data.espn_weight}
                />
              )}
            </CardContent>
          </Card>
        </>
      )}
    </div>
  )
}

/** A row of mutually exclusive buttons: a radio group that reads as one. */
function Choice<T extends string>({
  label,
  options,
  value,
  onChange,
}: {
  label: string
  options: readonly { key: T; label: string }[]
  value: T
  onChange: (value: T) => void
}) {
  return (
    <div role="group" aria-label={label} className="flex items-center gap-1">
      <span className="mr-1 text-xs text-muted-foreground">{label}</span>
      {options.map((option) => (
        <Button
          key={option.key}
          type="button"
          size="sm"
          variant={option.key === value ? "secondary" : "ghost"}
          aria-pressed={option.key === value}
          onClick={() => onChange(option.key)}
          className="h-7 px-2 font-mono text-xs"
        >
          {option.label}
        </Button>
      ))}
    </div>
  )
}

const TILE_TONES: Record<TileTone, string> = {
  plain: "text-foreground",
  good: "text-status-win",
  warn: "text-status-projected",
  quiet: "text-muted-foreground",
}

function SummaryTiles({ data, format }: { data: ProjectionsData; format: Format }) {
  return (
    <dl className="grid grid-cols-2 gap-3 md:grid-cols-4">
      {summaryTiles(data, format).map((tile) => (
        <Card key={tile.label} variant="panel" className="px-4 py-3">
          <dt className="text-xs uppercase tracking-wider text-muted-foreground">{tile.label}</dt>
          <dd className={cn("font-mono text-3xl font-bold tabular-nums", TILE_TONES[tile.tone])}>{tile.value}</dd>
        </Card>
      ))}
    </dl>
  )
}

/** What the page was computed from, whether the board is reading it, and what the ranks mean. */
function StatusLines({ data }: { data: ProjectionsData }) {
  return (
    <div className="flex flex-col gap-1 font-mono text-xs text-muted-foreground">
      <p>
        model {data.coefficients_version}
        <span className="mx-1.5 text-border">·</span>
        ESPN's line of {formatDay(data.espn_as_of)}, blended at {Math.round(data.espn_weight * 100)}%
        <span className="mx-1.5 text-border">·</span>
        <span className={cn(data.unpublished > 0 && "text-status-projected")}>{describePublished(data)}</span>
      </p>
      <p className={cn(!data.ranks_available && "text-status-projected")}>{describeRanks(data)}</p>
    </div>
  )
}

function LoadingState() {
  return (
    <div className="flex flex-col gap-6" aria-busy="true" aria-label="Loading projections">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        {Array.from({ length: 4 }, (_, i) => (
          <Skeleton key={i} className="h-[4.5rem] rounded-xl" />
        ))}
      </div>
      <Skeleton className="h-8 w-96 rounded" />
      <Skeleton className="h-[32rem] rounded-xl" />
    </div>
  )
}
