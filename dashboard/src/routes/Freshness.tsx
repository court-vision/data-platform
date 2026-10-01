import { TriangleAlert } from "lucide-react"

import { FreshnessTable } from "@/components/FreshnessTable"
import { RefreshNote } from "@/components/RefreshNote"
import { Card } from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"
import { FRESHNESS_REFETCH_MS, useFreshness } from "@/hooks/useFreshness"
import { useNow } from "@/hooks/useNow"
import { describeSeason, sortByUrgency, summarizeFreshness, summaryTiles, type FreshnessData, type TileTone } from "@/lib/freshness"
import { formatDay } from "@/lib/time"
import { cn } from "@/lib/utils"

export function Freshness() {
  const freshness = useFreshness()
  const now = useNow()
  const data = freshness.data

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-6 p-4 md:p-8">
      <header className="flex flex-wrap items-end justify-between gap-2">
        <div>
          <h1 className="font-display text-2xl font-bold tracking-tight">Data</h1>
          <p className="text-sm text-muted-foreground">
            What date each pipeline's table runs through, and when it was last written.
          </p>
        </div>
        <RefreshNote
          updatedAt={freshness.dataUpdatedAt}
          fetching={freshness.isFetching}
          now={now}
          intervalMs={FRESHNESS_REFETCH_MS}
        />
      </header>

      {freshness.error && (
        <div
          role="alert"
          className="flex items-start gap-2 rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-sm text-destructive"
        >
          <TriangleAlert className="mt-0.5 size-4 shrink-0" aria-hidden />
          <span>
            Could not refresh: {freshness.error.message}
            {data && " Showing the last good data."}
          </span>
        </div>
      )}

      {!data ? (
        <LoadingState />
      ) : (
        <>
          <SummaryTiles data={data} />
          <SeasonLine data={data} />
          <FreshnessTable tables={sortByUrgency(data.tables)} now={now} />
        </>
      )}
    </div>
  )
}

const TILE_TONES: Record<TileTone, string> = {
  plain: "text-foreground",
  good: "text-status-win",
  bad: "text-status-loss",
  warn: "text-status-projected",
  quiet: "text-muted-foreground",
}

function SummaryTiles({ data }: { data: FreshnessData }) {
  const tiles = summaryTiles(summarizeFreshness(data.tables))
  return (
    <dl className="grid grid-cols-2 gap-3 md:grid-cols-4">
      {tiles.map((tile, i) => (
        <Card key={i} variant="panel" className="px-4 py-3">
          <dt className="text-xs uppercase tracking-wider text-muted-foreground">{tile.label}</dt>
          <dd className={cn("font-mono text-3xl font-bold tabular-nums", TILE_TONES[tile.tone])}>{tile.value}</dd>
        </Card>
      ))}
    </dl>
  )
}

/** What the verdicts are judged against. */
function SeasonLine({ data }: { data: FreshnessData }) {
  return (
    <p className="font-mono text-xs text-muted-foreground">
      {describeSeason(data)}
      <span className="mx-1.5 text-border">·</span>
      <span title="The ET calendar date">today {formatDay(data.today)}</span>
    </p>
  )
}

function LoadingState() {
  return (
    <div className="flex flex-col gap-6" aria-busy="true" aria-label="Loading table freshness">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        {Array.from({ length: 4 }, (_, i) => (
          <Skeleton key={i} className="h-[4.5rem] rounded-xl" />
        ))}
      </div>
      <Skeleton className="h-4 w-80 rounded" />
      <Skeleton className="h-96 rounded-xl" />
    </div>
  )
}
