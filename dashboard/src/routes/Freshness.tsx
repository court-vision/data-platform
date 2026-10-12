import { Head, Notice, Page, RefreshNote, Sep, SheetSkeleton, Tiles } from "@/components/blocks"
import { FreshnessTable } from "@/components/FreshnessTable"
import { usePageContext } from "@/components/shell/pageContext"
import { FRESHNESS_REFETCH_MS, useFreshness } from "@/hooks/useFreshness"
import { useNow } from "@/hooks/useNow"
import { describeSeason, sortByUrgency, summarizeFreshness, summaryTiles, type FreshnessData, type TileTone } from "@/lib/freshness"
import { formatDay } from "@/lib/time"
import type { Tone } from "@/components/blocks"

export function Freshness() {
  usePageContext("Data")
  const freshness = useFreshness()
  const now = useNow()
  const data = freshness.data

  return (
    <Page>
      <Head
        title="Data"
        sub="What date each pipeline's table runs through, and when it was last written."
        meta={data && <SeasonLine data={data} />}
        actions={
          <RefreshNote updatedAt={freshness.dataUpdatedAt} fetching={freshness.isFetching} now={now} intervalMs={FRESHNESS_REFETCH_MS} />
        }
      />

      {freshness.error && (
        <Notice>
          Could not refresh: {freshness.error.message}
          {data && " Showing the last good data."}
        </Notice>
      )}

      {!data ? (
        <SheetSkeleton label="Loading table freshness" />
      ) : (
        <>
          <Tiles tiles={summaryTiles(summarizeFreshness(data.tables)).map((tile) => ({ ...tile, tone: TILE_TONES[tile.tone] }))} />
          <FreshnessTable tables={sortByUrgency(data.tables)} now={now} />
        </>
      )}
    </Page>
  )
}

const TILE_TONES: Record<TileTone, Tone | undefined> = {
  plain: undefined,
  good: "up",
  bad: "down",
  warn: "warn",
  quiet: "quiet",
}

/** What the verdicts are judged against. */
function SeasonLine({ data }: { data: FreshnessData }) {
  return (
    <>
      <span>{describeSeason(data)}</span>
      <Sep />
      <span title="The ET calendar date">today {formatDay(data.today)}</span>
    </>
  )
}
