import type { ReactNode } from "react"
import { useSearchParams } from "react-router"

import { Block, Chip, Choice, Head, Notice, Page, RefreshNote, Sep, SheetSkeleton, Tiles, type Tone } from "@/components/blocks"
import { usePageContext } from "@/components/shell/pageContext"
import { AVAILABILITY_REFETCH_MS, useAvailability } from "@/hooks/useAvailability"
import { useNow } from "@/hooks/useNow"
import {
  DAYS,
  formatLead,
  formatPercent,
  OUTCOMES,
  ownFalseOuts,
  parseDays,
  parseShow,
  RACES,
  SHOWS,
  shownCalls,
  sourceLabel,
  type AvailabilityCall,
  type AvailabilityDay,
  type AvailabilitySummary,
  type Days,
  type Outcome,
  type Show,
} from "@/lib/availability"
import { formatCentral, formatClockCentral, formatDay, parseUtc } from "@/lib/time"
import s from "@/components/sheet.module.css"

/**
 * The availability pipeline's dark-week scorecard: for each scratch, whether
 * our verdict or ESPN fantasy said out first, and every player out at his tip
 * who played. The gate for letting the verdicts drive anything is read from
 * the tiles. The window and the rows shown live in the URL.
 */
export function Availability() {
  usePageContext("Availability")
  const [params, setParams] = useSearchParams()
  const days = parseDays(params.get("days"))
  const show = parseShow(params.get("show"))
  const query = useAvailability(days)
  const now = useNow()
  const data = query.data

  function update(name: string, value: string, fallback: string) {
    setParams(
      (current) => {
        const next = new URLSearchParams(current)
        if (value === fallback) next.delete(name)
        else next.set(name, value)
        return next
      },
      { replace: true },
    )
  }

  const daysChoice = <Choice label="Over" options={DAYS} value={days} onChange={(value: Days) => update("days", value, "7")} />
  const covered = data?.dates.filter((day) => day.covered).length ?? 0

  return (
    <Page>
      <Head
        title="Availability"
        sub="Our availability verdicts against ESPN fantasy, while they run dark: for each player who sat, which side said out first, and every player out at his tip who played."
        meta={
          data && (
            <>
              <span>
                {covered} {covered === 1 ? "date" : "dates"} scored
              </span>
              <Sep />
              <span>
                {formatDay(data.since)} to {formatDay(data.until)}
              </span>
            </>
          )
        }
        actions={<RefreshNote updatedAt={query.dataUpdatedAt} fetching={query.isFetching} now={now} intervalMs={AVAILABILITY_REFETCH_MS} />}
      />

      {query.error && (
        <Notice>
          Could not load the scorecard: {query.error.message}
          {data && " Showing the last good data."}
        </Notice>
      )}

      {!data ? (
        query.error ? null : <SheetSkeleton label="Loading the scorecard" />
      ) : !data.available ? (
        <Block title="Outcomes" right={daysChoice}>
          <p className={s.empty}>No availability reports in the last {data.days} days. The pipeline writes them from its first poll.</p>
        </Block>
      ) : (
        <>
          <GateTiles summary={data.summary} />
          <OutcomesBlock summary={data.summary} right={daysChoice} />
          <CallsBlock calls={data.calls} show={show} onShow={(value) => update("show", value, "races")} />
          <DatesBlock dates={data.dates} />
        </>
      )}
    </Page>
  )
}

function GateTiles({ summary }: { summary: AvailabilitySummary }) {
  const first = summary.us_first + summary.us_only
  const both = summary.us_first + summary.same_poll + summary.espn_first
  const own = ownFalseOuts(summary)
  const shareTone: Tone | undefined =
    summary.first_share == null ? undefined : summary.first_share > 0.5 ? "up" : "warn"
  return (
    <Tiles
      tiles={[
        {
          label: "Us first",
          value: formatPercent(summary.first_share, 1),
          tone: shareTone,
          note: `${first} of ${summary.races} ${summary.races === 1 ? "scratch" : "scratches"}, first or only`,
        },
        {
          label: "Median lead",
          value: formatLead(summary.median_lead_minutes),
          note: `over the ${both} both sides called`,
        },
        {
          label: "False outs",
          value: own.toLocaleString(),
          tone: own > 0 ? "down" : summary.races > 0 ? "up" : undefined,
          note:
            summary.false_outs_floor > 0
              ? `ours; ${summary.false_outs_floor} more from ESPN fantasy's floor`
              : "ours; none from ESPN fantasy's floor",
        },
        {
          label: "Gate",
          value: summary.gate_met ? "Met" : "Not met",
          tone: summary.gate_met ? "up" : "warn",
          note: "first on most scratches, no false out of ours",
        },
      ]}
    />
  )
}

const OUTCOME_ROWS: readonly Outcome[] = [...RACES, "false_out", "carried", "unannounced", "pending", "not_in_box"]

function OutcomesBlock({ summary, right }: { summary: AvailabilitySummary; right: ReactNode }) {
  const count = (outcome: Outcome) => (outcome === "false_out" ? summary.false_outs : summary[outcome])
  const firstBy = Object.entries(summary.first_by).sort((a, b) => b[1] - a[1])
  return (
    <Block
      title="Outcomes"
      count={summary.races}
      right={right}
      text={
        <>
          A race is a player who sat and whom at least one side had out at his tip. Each side's time is when its out began
          and stayed out to the tip. ESPN's time is when our fantasy poll saw the flip, and it polls every 15 minutes from 10 AM CT
          on a game day (hourly before), so a lead shorter than that may be the poll rather than ESPN.
          {firstBy.length > 0 && (
            <> Our firsts came from {firstBy.map(([source, n]) => `${sourceLabel(source)} (${n})`).join(", ")}.</>
          )}
        </>
      }
    >
      <div className={s.bleed}>
        <table className={s.table}>
          <thead>
            <tr>
              <th scope="col">Outcome</th>
              <th scope="col" className={s.num}>Players</th>
              <th scope="col" className={s.num}>Of races</th>
              <th scope="col">What it means</th>
            </tr>
          </thead>
          <tbody>
            {OUTCOME_ROWS.map((outcome) => {
              const n = count(outcome)
              const race = RACES.includes(outcome)
              return (
                <tr key={outcome}>
                  <th scope="row">
                    <OutcomeChip outcome={outcome} />
                  </th>
                  <td className={s.num}>{n.toLocaleString()}</td>
                  <td className={s.num}>{race && summary.races > 0 ? formatPercent(n / summary.races) : "—"}</td>
                  <td className={s.muted}>{OUTCOMES[outcome].note}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </Block>
  )
}

function OutcomeChip({ outcome }: { outcome: Outcome }) {
  const { label, note, tone } = OUTCOMES[outcome]
  return (
    <Chip tone={tone ?? "quiet"} title={note}>
      {label}
    </Chip>
  )
}

function clock(iso: string | null | undefined): string {
  const date = parseUtc(iso)
  return date ? formatClockCentral(date.getTime()) : "—"
}

function CallsBlock({ calls, show, onShow }: { calls: AvailabilityCall[]; show: Show; onShow: (value: Show) => void }) {
  const rows = shownCalls(calls, show)
  return (
    <Block
      title="Players"
      count={rows.length}
      right={<Choice label="Show" options={SHOWS} value={show} onChange={onShow} />}
      text="Newest date first, then by tip. Times are Central. Ours is the verdict from every source but ESPN fantasy; at the tip is the full verdict, ESPN fantasy's floor included, replayed from the report log."
    >
      {rows.length === 0 ? (
        <p className={s.empty}>Nobody here yet.</p>
      ) : (
        <div className={s.bleed}>
          <table className={s.table} style={{ minWidth: "62rem" }}>
            <thead>
              <tr>
                <th scope="col">Player</th>
                <th scope="col">Date</th>
                <th scope="col">Tip</th>
                <th scope="col">Outcome</th>
                <th scope="col">Ours</th>
                <th scope="col">ESPN</th>
                <th scope="col" className={s.num}>Lead</th>
                <th scope="col">At the tip</th>
                <th scope="col">Box</th>
                <th scope="col">Why</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((call) => (
                <tr key={`${call.game_date} ${call.player_id}`}>
                  <th scope="row" className={s.nowrap}>
                    {call.name}
                    {call.team && <span className={s.under}>{call.team}</span>}
                  </th>
                  <td className={s.nowrap}>{formatDay(call.game_date)}</td>
                  <td className={`${s.nowrap} ${s.mono}`}>{clock(call.tip)}</td>
                  <td>
                    <OutcomeChip outcome={call.outcome} />
                  </td>
                  <td className={s.nowrap}>
                    <span className={s.mono}>{clock(call.ours_at)}</span>
                    {call.ours_by && <span className={s.under}>{sourceLabel(call.ours_by)}</span>}
                  </td>
                  <td className={s.nowrap}>
                    <span className={s.mono}>{clock(call.espn_at)}</span>
                    {call.espn_at_tip && <span className={s.under}>{call.espn_at_tip} at the tip</span>}
                  </td>
                  <td className={`${s.num} ${leadClass(call.lead_minutes)}`}>{formatLead(call.lead_minutes)}</td>
                  <td className={s.nowrap}>
                    {call.verdict_at_tip ?? "—"}
                    {call.verdict_by && <span className={s.under}>{sourceLabel(call.verdict_by)}</span>}
                  </td>
                  <td className={call.box === "active" && call.outcome === "false_out" ? s.bad : undefined}>{call.box ?? "—"}</td>
                  <td className={s.muted} style={{ maxWidth: "18rem" }}>
                    <span className={s.clip} title={call.detail ?? undefined}>
                      {[call.reason, call.detail].filter(Boolean).join(" · ") || "—"}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Block>
  )
}

function leadClass(minutes: number | null): string {
  if (minutes == null || Math.abs(minutes) < 2) return ""
  return minutes > 0 ? s.good : s.warn
}

function DatesBlock({ dates }: { dates: AvailabilityDay[] }) {
  return (
    <Block
      title="Dates"
      count={dates.length}
      text="A date is scored only when both ESPN sources were first read before its first tip, so the date the pipeline went live on (Nov 2, its games already over) is not."
    >
      <div className={s.bleed}>
        <table className={s.table}>
          <thead>
            <tr>
              <th scope="col">Date</th>
              <th scope="col" className={s.num}>Games</th>
              <th scope="col">First poll</th>
              <th scope="col">First tip</th>
              <th scope="col">Scored</th>
              <th scope="col" className={s.num}>Players</th>
            </tr>
          </thead>
          <tbody>
            {dates.map((day) => (
              <tr key={day.game_date}>
                <th scope="row">{formatDay(day.game_date)}</th>
                <td className={s.num}>{day.games}</td>
                <td className={s.nowrap}>{formatCentral(day.first_poll)}</td>
                <td className={s.nowrap}>{formatCentral(day.first_tip)}</td>
                <td className={day.covered ? undefined : s.muted}>{day.covered ? "yes" : "no"}</td>
                <td className={s.num}>{day.covered ? day.calls : "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Block>
  )
}
