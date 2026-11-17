import { Loader2, RefreshCw, Upload } from "lucide-react"
import { useSearchParams } from "react-router"

import { Block, Choice, Head, Notice, Page, Sep, SheetSkeleton, Tiles, type Tone } from "@/components/blocks"
import { ProjectionsTable } from "@/components/ProjectionsTable"
import { usePageContext } from "@/components/shell/pageContext"
import { useNow } from "@/hooks/useNow"
import { useProjections, useRepublish } from "@/hooks/useProjections"
import {
  defaultEditMode,
  describeLive,
  describePublished,
  describeRanks,
  EDIT_MODES,
  FILTERS,
  FORMATS,
  SORTS,
  summaryTiles,
  visibleRows,
  type EditMode,
  type Filter,
  type Format,
  type ProjectionsData,
  type Sort,
  type TileTone,
} from "@/lib/projections"
import { formatDay, relativeTime } from "@/lib/time"
import dk from "@/desk/desk.module.css"
import s from "@/components/sheet.module.css"
import p from "@/components/projections.module.css"

function oneOf<T extends string>(options: readonly { key: T }[], value: string | null, fallback: T): T {
  return options.some((option) => option.key === value) ? (value as T) : fallback
}

/**
 * Court Vision's projection, the lines it is built from, and what a person
 * lays over them: the preseason adjustments, and in season the dated notes
 * the live projection reads. The view lives in the URL — format, filter,
 * sort, search, the open player and which of his two forms is open — so a
 * link lands on the row being discussed.
 */
export function Projections() {
  usePageContext("Projections")
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
  // In season a row opens on its note; before the first live projection, on its adjustment
  const firstMode: EditMode = data ? defaultEditMode(data) : "adjustment"
  const mode = oneOf(EDIT_MODES, params.get("edit"), firstMode)

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
    <Page>
      <Head
        title="Projections"
        sub={`Court Vision's projection for ${data?.season ?? "the season"}: the lines it is built from, the adjustments on top, and the season's notes. Saving an adjustment republishes the preseason projection; saving a note rebuilds the day's live projection, which the board is priced on in season.`}
        meta={data && <StatusLines data={data} />}
        actions={
          <>
            <span className={s.headButtons}>
              <button type="button" className={`${dk.btn} ${dk.btnSmall}`} disabled={projections.isFetching} onClick={() => projections.refetch()}>
                <RefreshCw size={12} className={projections.isFetching ? dk.spin : undefined} aria-hidden />
                Refresh
              </button>
              <button
                type="button"
                className={`${dk.btn} ${dk.btnSmall}`}
                disabled={republish.isPending}
                title="Run cv-projection now: write today's snapshot from the lines on this page"
                onClick={() => republish.mutate()}
              >
                {republish.isPending ? <Loader2 size={12} className={dk.spin} aria-hidden /> : <Upload size={12} aria-hidden />}
                Republish
              </button>
            </span>
            {projections.dataUpdatedAt > 0 && (
              <p className={s.refresh}>computed {relativeTime(new Date(projections.dataUpdatedAt).toISOString(), now)}</p>
            )}
          </>
        }
      />

      {projections.error && (
        <Notice>
          Could not load the projections: {projections.error.message}
          {data && " Showing the last good data."}
        </Notice>
      )}

      {!data ? (
        projections.error ? null : <SheetSkeleton label="Loading projections" rows={12} />
      ) : (
        <>
          <Tiles tiles={summaryTiles(data, format).map((tile) => ({ ...tile, tone: TILE_TONES[tile.tone] }))} />

          <Block
            title="Players"
            count={rows.length === data.players.length ? rows.length : `${rows.length} of ${data.players.length}`}
            right={
              <input
                type="search"
                aria-label="Search players"
                placeholder="Name, or a team code"
                value={query}
                onChange={(event) => update("q", event.target.value, "")}
                className={`${dk.input} ${p.search}`}
              />
            }
          >
            <div className={p.controls}>
              <Choice label="Rank in" options={FORMATS} value={format} onChange={(value: Format) => update("format", value, "points")} />
              <Choice label="Show" options={FILTERS} value={filter} onChange={(value: Filter) => update("filter", value, "all")} />
              <Choice label="Order by" options={SORTS} value={sort} onChange={(value: Sort) => update("sort", value, "cv")} />
            </div>
            {rows.length === 0 ? (
              <p className={s.empty}>Nobody matches.</p>
            ) : (
              <ProjectionsTable
                rows={rows}
                players={data.players}
                format={format}
                openId={openId}
                onToggle={(playerId) => update("open", playerId === openId ? null : String(playerId), "")}
                editing={{
                  kinds: data.kinds,
                  espnWeight: data.espn_weight,
                  season: { liveAsOf: data.live_as_of, confidences: data.confidences, reviewDays: data.review_days },
                  mode,
                  onMode: (value) => update("edit", value, firstMode),
                }}
              />
            )}
          </Block>
        </>
      )}
    </Page>
  )
}

const TILE_TONES: Record<TileTone, Tone | undefined> = {
  plain: undefined,
  good: "up",
  warn: "warn",
  quiet: "quiet",
}

/** What the page was computed from, whether the board is reading it, and what the ranks mean. */
function StatusLines({ data }: { data: ProjectionsData }) {
  return (
    <>
      <span>model {data.coefficients_version}</span>
      <Sep />
      <span>
        ESPN's line of {formatDay(data.espn_as_of)}, blended at {Math.round(data.espn_weight * 100)}%
      </span>
      <Sep />
      <span className={data.unpublished > 0 ? s.warn : undefined}>{describePublished(data)}</span>
      <Sep />
      <span>{describeLive(data)}</span>
      <span className={`${p.ranksLine} ${data.ranks_available ? "" : s.warn}`}>{describeRanks(data)}</span>
    </>
  )
}
