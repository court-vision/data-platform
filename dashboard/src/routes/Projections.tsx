import { Loader2, RefreshCw, Upload } from "lucide-react"
import { useSearchParams } from "react-router"

import { AnalystDigestView } from "@/components/AnalystDigest"
import { Block, Choice, Head, Notice, Page, Rail, Sep, SheetSkeleton, Tiles, type Tone } from "@/components/blocks"
import { ProjectionsTable } from "@/components/ProjectionsTable"
import { usePageContext } from "@/components/shell/pageContext"
import { useNow } from "@/hooks/useNow"
import { useAnalystDigest, useProjections, useRepublish } from "@/hooks/useProjections"
import { READ_FILTERS, VIEWS, type ReadFilter, type View } from "@/lib/analyst"
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

/** A plain `YYYY-MM-DD` from the address, or nothing. */
function dayIn(value: string | null): string | null {
  return value && /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : null
}

/**
 * Court Vision's projection, the lines it is built from, and what a person
 * lays over them: the preseason adjustments, and in season the dated notes
 * the live projection reads. Its second view is the analyst's morning, in
 * shadow: who it read, what it wrote and what that would have moved. The view
 * lives in the URL — which of the two, format, filter, sort, search, the open
 * player and which of his two forms is open, the analyst's morning and which
 * reads are listed — so a link lands on the row being discussed.
 */
export function Projections() {
  usePageContext("Projections")
  const [params, setParams] = useSearchParams()
  const view = oneOf(VIEWS, params.get("view"), "players")
  const onAnalyst = view === "analyst"
  // Each view asks only for its own data: the players' read rebuilds the whole projection
  const projections = useProjections(!onAnalyst)
  const analyst = useAnalystDigest(dayIn(params.get("day")), onAnalyst)
  const republish = useRepublish()
  const now = useNow()
  const data = projections.data
  const shown = onAnalyst ? analyst : projections
  const reads = oneOf(READ_FILTERS, params.get("reads"), "all")

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

  /** A player's row among the players, opened on his season note: where a read is acted on. */
  function playerLink(playerId: number): string {
    const next = new URLSearchParams()
    if (format !== "points") next.set("format", format)
    next.set("open", String(playerId))
    next.set("edit", "note")
    return `?${next}`
  }

  const rows = data ? visibleRows(data.players, { query, filter, sort, format }) : []

  return (
    <Page>
      <Head
        title="Projections"
        chips={<Rail label="View" options={VIEWS} value={view} onChange={(value: View) => update("view", value, "players")} />}
        sub={
          onAnalyst
            ? analyst.data && !analyst.data.in_shadow
              ? `The analyst's morning for ${analyst.data.season}, live. Each morning it reads the stored news about a short list of players and writes dated notes with their sources, never a rank. Its notes are on the projection and boards everybody sees, and the same morning without them is kept beside, so this is where its work is judged.`
              : `The analyst's morning for ${analyst.data?.season ?? "the season"}, in shadow. Each morning it reads the stored news about a short list of players and writes dated notes with their sources, never a rank. Its notes reach only a second projection and second boards that nothing else reads, so this is where its work is judged.`
            : `Court Vision's projection for ${data?.season ?? "the season"}: the lines it is built from, the adjustments on top, and the season's notes. Saving an adjustment republishes the preseason projection; saving a note rebuilds the day's live projection, which the board is priced on in season.`
        }
        meta={!onAnalyst && data && <StatusLines data={data} />}
        actions={
          <>
            <span className={s.headButtons}>
              <button type="button" className={`${dk.btn} ${dk.btnSmall}`} disabled={shown.isFetching} onClick={() => shown.refetch()}>
                <RefreshCw size={12} className={shown.isFetching ? dk.spin : undefined} aria-hidden />
                Refresh
              </button>
              {!onAnalyst && (
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
              )}
            </span>
            {shown.dataUpdatedAt > 0 && (
              <p className={s.refresh}>
                {onAnalyst ? "loaded" : "computed"} {relativeTime(new Date(shown.dataUpdatedAt).toISOString(), now)}
              </p>
            )}
          </>
        }
      />

      {shown.error && (
        <Notice>
          Could not load {onAnalyst ? "the analyst's morning" : "the projections"}: {shown.error.message}
          {shown.data && " Showing the last good data."}
        </Notice>
      )}

      {onAnalyst ? (
        !analyst.data ? (
          analyst.error ? null : <SheetSkeleton label="Loading the analyst's morning" rows={10} />
        ) : (
          <AnalystDigestView
            digest={analyst.data}
            show={reads}
            onShow={(value: ReadFilter) => update("reads", value, "all")}
            onDay={(day) => update("day", day, "")}
            playerLink={playerLink}
          />
        )
      ) : !data ? (
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
