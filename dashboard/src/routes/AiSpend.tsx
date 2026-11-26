import { useId, useRef, useState, type FormEvent } from "react"
import { useSearchParams } from "react-router"

import { Block, Choice, Head, Notice, Page, RefreshNote, Sep, SheetSkeleton, Tag, Tiles, type TileSpec } from "@/components/blocks"
import { SpendChart } from "@/components/SpendChart"
import { usePageContext } from "@/components/shell/pageContext"
import { AI_SPEND_REFETCH_MS, useAiBill, useAiSpend, useSetCap, type CapChange } from "@/hooks/useAiSpend"
import { useNow } from "@/hooks/useNow"
import {
  APP_LINE,
  billDayShort,
  billGap,
  billProblem,
  byDay,
  byFeature,
  byModel,
  byTeam,
  cacheHitRate,
  capInput,
  capShare,
  checkinKindLabel,
  FEATURES,
  formatGap,
  formatShare,
  formatTokens,
  formatUsd,
  LINES,
  monthLabel,
  monthTotal,
  parseCap,
  parseWindow,
  recentBillDays,
  WINDOWS,
  type AiBill,
  type AiSpendReport,
  type BillDay,
  type BillMonth,
  type CapLineKey,
  type SpendWindow,
  type UserLine,
} from "@/lib/aiSpend"
import { DEFAULT_AUTHOR, readAuthor, rememberAuthor } from "@/lib/author"
import { formatCentral } from "@/lib/time"
import dk from "@/desk/desk.module.css"
import s from "@/components/sheet.module.css"
import a from "@/components/aiSpend.module.css"

/**
 * What Dime and the analyst spend, and the monthly caps they run under, which
 * this page can change. The backend owns the ledger and the caps; a cap set here
 * applies from the next turn, check-in or read on. The window lives in the URL.
 */
export function AiSpend() {
  usePageContext("AI spend")
  const [params, setParams] = useSearchParams()
  const window = parseWindow(params.get("days"))
  const query = useAiSpend(window)
  const now = useNow()
  const data = query.data

  function setWindow(value: SpendWindow) {
    setParams(
      (current) => {
        const next = new URLSearchParams(current)
        if (value === "30") next.delete("days")
        else next.set("days", value)
        return next
      },
      { replace: true },
    )
  }

  return (
    <Page>
      <Head
        title="AI spend"
        sub="Every model call Dime and the analyst make, priced as it happens and counted against a monthly cap. Days and months are UTC, the calendar the caps reset on."
        meta={
          data && (
            <>
              <span>month since {data.month_start}</span>
              <Sep />
              <span>as of {formatCentral(data.as_of)}</span>
            </>
          )
        }
        actions={<RefreshNote updatedAt={query.dataUpdatedAt} fetching={query.isFetching} now={now} intervalMs={AI_SPEND_REFETCH_MS} />}
      />

      {query.error && (
        <Notice>
          Could not load the spend: {query.error.message}
          {data && " Showing the last good data."}
        </Notice>
      )}

      {!data ? (
        query.error ? null : <SheetSkeleton label="Loading AI spend" />
      ) : (
        <>
          <MonthTiles data={data} />
          <CapsBlock data={data} />
          <BillBlock />
          <DaysBlock data={data} window={window} onWindow={setWindow} />
          <FeaturesBlock data={data} />
          <ModelsBlock data={data} />
          <TeamsBlock data={data} />
        </>
      )}
    </Page>
  )
}

// ---------------------------------------------------------------- the bill check

const BILL_TEXT =
  "What Anthropic bills the organization (before credits), read with the Admin API key, beside what the ledger priced. The ledger is held to the app's own key when Anthropic names it; other keys (a laptop's evals) count only in the bill's total. Anthropic's numbers land 4 to 24 hours late, so the latest days read short."

/** Anthropic's bill beside the ledger: its own query, so a slow or refused read never holds up the page. */
function BillBlock() {
  const query = useAiBill()
  const bill = query.data
  const problem = bill ? billProblem(bill) : null
  return (
    <Block
      title="Against Anthropic's bill"
      note={bill?.state === "ok" && <BillFreshness bill={bill} />}
      text={BILL_TEXT}
    >
      {query.error ? (
        <p className={s.empty}>Could not ask for the bill: {query.error.message}</p>
      ) : !bill ? (
        <p className={s.empty}>Reading Anthropic's bill…</p>
      ) : problem ? (
        <p className={s.empty}>{problem}</p>
      ) : (
        <>
          <BillTable label="By month" first="Month" rows={bill.months.map((m) => ({ key: m.month, name: monthLabel(m.month), row: m }))} />
          <BillTable
            label="Last 14 days"
            first="Day (UTC)"
            rows={recentBillDays(bill.days).map((d) => ({ key: d.day, name: d.day, row: d, short: billDayShort(d.day, bill.refreshed_at) }))}
          />
          <BillKeys bill={bill} />
        </>
      )}
    </Block>
  )
}

function BillFreshness({ bill }: { bill: AiBill }) {
  return (
    <span>
      {bill.refreshed_at ? `complete to ${formatCentral(bill.refreshed_at)}` : "no data landed yet"} · read {formatCentral(bill.read_at)}
    </span>
  )
}

interface BillLine {
  key: string
  name: string
  row: BillMonth | BillDay
  short?: boolean
}

function BillTable({ label, first, rows }: { label: string; first: string; rows: BillLine[] }) {
  return (
    <div className={s.bleed}>
      <table className={s.table} style={{ minWidth: "40rem" }} aria-label={label}>
        <thead>
          <tr>
            <th scope="col">{first}</th>
            <th scope="col" className={s.num}>Anthropic billed</th>
            <th scope="col" className={s.num}>On the app's key</th>
            <th scope="col" className={s.num}>Our ledger</th>
            <th scope="col" className={s.num}>Ledger less bill</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(({ key, name, row, short }) => {
            const { gap, share } = billGap(row)
            const open = row.open_micros ?? 0
            return (
              <tr key={key}>
                <th scope="row">
                  {name}
                  {short && <span className={a.sub}>may still read short</span>}
                </th>
                <td className={s.num}>{formatUsd(row.billed_micros)}</td>
                <td className={s.num}>{row.ours_micros == null ? "not named" : formatUsd(row.ours_micros)}</td>
                <td className={s.num}>
                  {formatUsd(row.ledger_micros)}
                  {open > 0 && <span className={a.sub}>{formatUsd(open)} open</span>}
                </td>
                <td className={s.num}>
                  {formatGap(gap)}
                  {share != null && Math.abs(gap) >= 5_000 && <span className={a.sub}>{formatShare(Math.abs(share))}</span>}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function BillKeys({ bill }: { bill: AiBill }) {
  if (bill.keys.length === 0) return <p className={s.empty}>Anthropic has billed no key this month yet.</p>
  return (
    <div className={s.bleed}>
      <table className={s.table} style={{ minWidth: "32rem" }} aria-label="This month by API key">
        <thead>
          <tr>
            <th scope="col">API key, this month</th>
            <th scope="col" className={s.num}>Billed</th>
          </tr>
        </thead>
        <tbody>
          {bill.keys.map((key) => (
            <tr key={key.api_key_id ?? "none"}>
              <th scope="row">
                {key.name ?? (key.api_key_id ? key.api_key_id : "No key (the Console)")}
                {key.ours && <> <Tag>this app</Tag></>}
                {key.hint && <span className={a.sub}>{key.hint}</span>}
              </th>
              <td className={s.num}>{formatUsd(key.billed_micros)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ---------------------------------------------------------------- this month

function MonthTiles({ data }: { data: AiSpendReport }) {
  const total = monthTotal(data)
  const whole = capShare(total.spent, total.cap)
  const tiles: TileSpec[] = [
    total.whole
      ? {
          label: "This month",
          value: formatUsd(total.spent),
          tone: whole.tone,
          note: `${formatShare(whole.share)} of ${formatUsd(total.cap)} in all`,
        }
      : { label: "This month", value: formatUsd(total.spent), note: `of ${formatUsd(total.cap)} across every cap` },
    ...data.lines.map((line) => {
      const used = line.spent_micros + line.open_micros
      const { share, tone } = capShare(used, line.cap_micros)
      const open = line.open_micros > 0 ? ` · ${formatUsd(line.open_micros)} open` : ""
      return {
        label: LINES[line.line].label,
        value: formatUsd(used),
        tone,
        note: `${formatShare(share)} of ${formatUsd(line.cap_micros)}${open}`,
      }
    }),
  ]
  return <Tiles tiles={tiles} />
}

function Meter({ spent, cap }: { spent: number; cap: number | null | undefined }) {
  const { share, tone } = capShare(spent, cap)
  if (share == null) return <span className={a.sub}>no cap of their own</span>
  const label = `${formatUsd(spent)} of ${formatUsd(cap)} (${formatShare(share)})`
  return (
    <div className={a.meter} role="img" aria-label={label} title={label}>
      <span className={a.meterFill} data-tone={tone} style={{ width: `${Math.min(share, 1) * 100}%` }} />
    </div>
  )
}

// ---------------------------------------------------------------- the caps

function CapsBlock({ data }: { data: AiSpendReport }) {
  const [author, setAuthor] = useState(readAuthor)
  const authorId = useId()
  const save = useSetCap()

  function change(line: CapLineKey, userId: number | null, usd: string | null) {
    rememberAuthor(author)
    return save.mutateAsync({ line, userId, usd, author: author.trim() || DEFAULT_AUTHOR } satisfies CapChange)
  }

  return (
    <Block
      title="Caps"
      text={
        <>
          A line stops taking new turns once less than one turn's ceiling is left under its cap, or under the whole app's, which every line's
          spend counts against together ({formatUsd(data.turn_max_micros)} a desk turn or check-in, {formatUsd(data.read_max_micros)} an
          analyst read). A cap set here replaces the backend's default from the next turn on;
          Default hands it back. Open spend is a turn running now, or one that died mid-call, counted at its ceiling.
        </>
      }
    >
      <div className={a.author}>
        <label htmlFor={authorId} className={dk.label}>
          Saving as
        </label>
        <input id={authorId} className={`${dk.input} ${a.authorInput}`} value={author} maxLength={80} onChange={(event) => setAuthor(event.target.value)} />
      </div>
      <div className={s.bleed}>
        <table className={s.table} style={{ minWidth: "52rem" }}>
          <thead>
            <tr>
              <th scope="col">Line</th>
              <th scope="col" className={s.num}>This month</th>
              <th scope="col">Of its cap</th>
              <th scope="col">Monthly cap</th>
              <th scope="col">Set by</th>
            </tr>
          </thead>
          <tbody>
            {data.app && (
              <CapRow label={APP_LINE.label} note={APP_LINE.note} cap={data.app} onSave={(usd) => change("app", null, usd)} />
            )}
            {data.lines.map((line) => (
              <CapRow
                key={line.line}
                label={LINES[line.line].label}
                note={LINES[line.line].note}
                cap={line}
                onSave={(usd) => change(line.line, null, usd)}
              />
            ))}
          </tbody>
        </table>
      </div>

      <h3 className={s.blockTitle} style={{ marginTop: 20 }}>
        Per user
        <span className={s.blockCount}>{data.users.length}</span>
      </h3>
      <p className={s.blockText}>
        On the desk, a user's cap is an equal share of the desk's cap unless one is set for them. On check-ins, a user has no cap of their
        own until one is set: their check-ins are then skipped, not failed, once it is used.
      </p>
      {data.users.length === 0 ? (
        <p className={s.empty}>No users on the AI desk's allowlist, and no one has spent this month.</p>
      ) : (
        <div className={s.bleed}>
          <table className={s.table} style={{ minWidth: "60rem" }}>
            <thead>
              <tr>
                <th scope="col">User</th>
                <th scope="col" className={s.num}>Desk this month</th>
                <th scope="col">Desk cap</th>
                <th scope="col" className={s.num}>Check-ins this month</th>
                <th scope="col">Check-in cap</th>
              </tr>
            </thead>
            <tbody>
              {data.users.map((user) => (
                <tr key={user.user_id}>
                  <th scope="row">
                    {user.email}
                    <span className={a.sub}>
                      user {user.user_id} {!user.allowed && <Tag title="Not on AI_AGENT_USER_IDS: Dime is off for them">no access</Tag>}
                    </span>
                  </th>
                  <UserLineCells
                    line={user.desk}
                    label={`${user.email} desk cap`}
                    onSave={(usd) => change("desk", user.user_id, usd)}
                  />
                  <UserLineCells
                    line={user.autopilot}
                    label={`${user.email} check-in cap`}
                    onSave={(usd) => change("autopilot", user.user_id, usd)}
                  />
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Block>
  )
}

/** One cap's row: a line's, or the whole app's. */
function CapRow({
  label,
  note,
  cap,
  onSave,
}: {
  label: string
  note: string
  cap: { spent_micros: number; open_micros: number; cap_micros?: number | null; default_cap_micros?: number | null; cap_set: boolean; updated_by?: string | null; updated_at?: string | null }
  onSave: SaveCap
}) {
  return (
    <tr>
      <th scope="row">
        {label}
        <span className={a.sub}>{note}</span>
      </th>
      <td className={s.num}>
        {formatUsd(cap.spent_micros + cap.open_micros)}
        {cap.open_micros > 0 && <span className={a.sub}>{formatUsd(cap.open_micros)} open</span>}
      </td>
      <td className={a.meterCell}>
        <Meter spent={cap.spent_micros + cap.open_micros} cap={cap.cap_micros} />
      </td>
      <td>
        <CapEditor label={`${label} monthly cap`} cap={cap} onSave={onSave} />
      </td>
      <td>
        <SetBy cap={cap} />
      </td>
    </tr>
  )
}

function UserLineCells({ line, label, onSave }: { line: UserLine; label: string; onSave: SaveCap }) {
  return (
    <>
      <td className={s.num}>
        <UserSpend line={line} />
      </td>
      <td>
        <CapEditor label={label} cap={line} onSave={onSave} />
        <span className={a.sub}>
          {line.cap_set ? (
            <SetBy cap={line} />
          ) : line.default_cap_micros != null ? (
            `share: ${formatUsd(line.default_cap_micros)}`
          ) : (
            "the line's cap only"
          )}
        </span>
      </td>
    </>
  )
}

/** A user's month, and how much of the cap in force on them it has used. */
function UserSpend({ line }: { line: UserLine }) {
  const { share, tone } = capShare(line.spent_micros, line.cap_micros)
  return (
    <>
      {formatUsd(line.spent_micros)}
      {share != null && (
        <span className={a.sub}>
          {tone ? <Tag tone={tone}>{formatShare(share)}</Tag> : formatShare(share)} of {formatUsd(line.cap_micros)}
        </span>
      )}
    </>
  )
}

function SetBy({ cap }: { cap: { cap_set: boolean; updated_by?: string | null; updated_at?: string | null; default_cap_micros?: number | null } }) {
  if (!cap.cap_set) return <span className={a.sub}>default</span>
  return (
    <span className={a.sub}>
      {cap.updated_by ?? "someone"} · {formatCentral(cap.updated_at)}
    </span>
  )
}

/** Saves one cap ("25", or null for the default) and answers with the cap now in force. */
type SaveCap = (usd: string | null) => Promise<{ data: { cap_micros?: number | null } }>

/** The cap in force, editable: Save writes it, Default clears a cap set here. */
export function CapEditor({
  label,
  cap,
  onSave,
}: {
  label: string
  cap: { cap_micros?: number | null; default_cap_micros?: number | null; cap_set: boolean }
  onSave: SaveCap
}) {
  const shown = capInput(cap.cap_micros)
  const [text, setText] = useState(shown)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  // The report value the field last followed, and whether it holds typing of its own
  const [base, setBase] = useState(shown)
  const [draft, setDraft] = useState(false)
  // Counts keystrokes, so a save that lands after more typing leaves that typing be
  const edits = useRef(0)
  const inputId = useId()

  // A fresh report (after a save here or elsewhere, or on the minute) moves a field
  // with nothing typed in it; typing is never overwritten, even typing that happens
  // to match the cap the report had
  if (shown !== base) {
    setBase(shown)
    if (!draft) {
      setText(shown)
      setError(null)
    }
  }

  async function run(usd: string | null) {
    const before = edits.current
    setBusy(true)
    try {
      const answer = await onSave(usd)
      if (edits.current === before) {
        // Nothing typed since: the field shows the cap as the backend now has it
        // ("12.50" comes back "12.5", Default the cap it went back to), and follows
        // the report from here
        setText(capInput(answer.data.cap_micros))
        setError(null)
        setDraft(false)
      }
    } catch {
      // the toast says why; the field keeps what was typed
    } finally {
      setBusy(false)
    }
  }

  function submit(event: FormEvent) {
    event.preventDefault()
    const parsed = parseCap(text)
    if ("error" in parsed) {
      setError(parsed.error)
      return
    }
    setError(null)
    void run(parsed.usd)
  }

  const dirty = text.trim() !== shown
  return (
    <form className={a.capForm} onSubmit={submit} aria-label={label}>
      <label htmlFor={inputId} className={s.srOnly}>
        {label} in dollars
      </label>
      <input
        id={inputId}
        className={`${dk.input} ${a.capInput}`}
        inputMode="decimal"
        placeholder={cap.cap_micros == null ? "none" : undefined}
        value={text}
        aria-invalid={error ? true : undefined}
        onChange={(event) => {
          edits.current += 1
          setText(event.target.value)
          setDraft(true)
        }}
      />
      <button type="submit" className={`${dk.btn} ${dk.btnSmall} ${dirty ? dk.btnPrimary : ""}`} disabled={busy || !dirty}>
        Save
      </button>
      {cap.cap_set && (
        <button
          type="button"
          className={`${dk.btn} ${dk.btnSmall}`}
          disabled={busy}
          title={cap.default_cap_micros != null ? `Back to ${formatUsd(cap.default_cap_micros)}` : "Back to no cap of their own"}
          onClick={() => void run(null)}
        >
          Default
        </button>
      )}
      {error && <span className={a.capError}>{error}</span>}
    </form>
  )
}

// ---------------------------------------------------------------- the window

function DaysBlock({ data, window, onWindow }: { data: AiSpendReport; window: SpendWindow; onWindow: (value: SpendWindow) => void }) {
  return (
    <Block
      title="By day"
      right={<Choice label="Over" options={WINDOWS} value={window} onChange={onWindow} />}
      text="What each UTC day spent, split by what spent it. Point at a day, or move along with the arrow keys, for its numbers."
    >
      <SpendChart days={byDay(data)} />
    </Block>
  )
}

function FeaturesBlock({ data }: { data: AiSpendReport }) {
  const features = byFeature(data)
  const total = features.reduce((sum, feature) => sum + feature.cost, 0)
  return (
    <Block
      title="By feature"
      count={features.length}
      text="Spend is from the ledger the caps count. Turns and tokens are from each call's own record, so a deleted chat or a turn still open has spend and no tokens. Cache is the share of input read from the prompt cache."
    >
      {features.length === 0 ? (
        <p className={s.empty}>Nothing spent in the last {data.days} days.</p>
      ) : (
        <div className={`${s.bleed} ${a.viz}`}>
          <table className={s.table} style={{ minWidth: "52rem" }}>
            <thead>
              <tr>
                <th scope="col">Feature</th>
                <th scope="col" className={s.num}>Spend</th>
                <th scope="col" className={s.num}>Share</th>
                <th scope="col" className={s.num}>Turns</th>
                <th scope="col" className={s.num}>Per turn</th>
                <th scope="col" className={s.num}>Input</th>
                <th scope="col" className={s.num}>Output</th>
                <th scope="col" className={s.num}>Cache</th>
              </tr>
            </thead>
            <tbody>
              {features.map((row) => (
                <tr key={row.feature.key}>
                  <th scope="row" title={row.feature.note}>
                    <span className={a.featureName}>
                      <span className={a.swatch} data-feature={row.feature.key} aria-hidden />
                      {row.feature.label}
                    </span>
                    {row.kinds.length > 0 && (
                      <span className={a.kinds}>
                        {row.kinds.map((kind) => (
                          <span key={kind.kind}>
                            {checkinKindLabel(kind.kind)} {formatUsd(kind.cost)}
                          </span>
                        ))}
                      </span>
                    )}
                  </th>
                  <td className={s.num}>
                    {formatUsd(row.cost)}
                    {row.open > 0 && <span className={a.sub}>{formatUsd(row.open)} open</span>}
                  </td>
                  <td className={s.num}>{total > 0 ? formatShare(row.cost / total) : "—"}</td>
                  <td className={s.num}>{row.records > 0 ? row.records.toLocaleString() : "—"}</td>
                  <td className={s.num}>{row.records > 0 ? formatUsd(row.recordCost / row.records) : "—"}</td>
                  <td className={s.num}>{row.records > 0 ? formatTokens(row.input + row.cacheRead + row.cacheWrite) : "—"}</td>
                  <td className={s.num}>{row.records > 0 ? formatTokens(row.output) : "—"}</td>
                  <td className={s.num}>{formatShare(cacheHitRate(row))}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Block>
  )
}

function ModelsBlock({ data }: { data: AiSpendReport }) {
  const models = byModel(data.usage)
  return (
    <Block title="By model" count={models.length} text="From each call's record. A model other than the one configured is a fallback that served a declined request.">
      {models.length === 0 ? (
        <p className={s.empty}>No calls recorded in the last {data.days} days.</p>
      ) : (
        <div className={s.bleed}>
          <table className={s.table} style={{ minWidth: "44rem" }}>
            <thead>
              <tr>
                <th scope="col">Model</th>
                <th scope="col" className={s.num}>Spend</th>
                <th scope="col" className={s.num}>Turns</th>
                <th scope="col" className={s.num}>Model calls</th>
                <th scope="col" className={s.num}>Input</th>
                <th scope="col" className={s.num}>Output</th>
                <th scope="col" className={s.num}>Cache</th>
              </tr>
            </thead>
            <tbody>
              {models.map((row) => (
                <tr key={row.model}>
                  <th scope="row">{row.model}</th>
                  <td className={s.num}>{formatUsd(row.recordCost)}</td>
                  <td className={s.num}>{row.records.toLocaleString()}</td>
                  <td className={s.num}>{row.modelCalls.toLocaleString()}</td>
                  <td className={s.num}>{formatTokens(row.input + row.cacheRead + row.cacheWrite)}</td>
                  <td className={s.num}>{formatTokens(row.output)}</td>
                  <td className={s.num}>{formatShare(cacheHitRate(row))}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Block>
  )
}

function TeamsBlock({ data }: { data: AiSpendReport }) {
  const teams = byTeam(data)
  const shown = FEATURES.filter((feature) => teams.some((team) => (team.parts[feature.key] ?? 0) > 0))
  return (
    <Block title="By team" count={teams.length} text="Who it was spent for. Spend with no team is a chat pinned to none, or the draft room; the analyst's reads are no one's.">
      {teams.length === 0 ? (
        <p className={s.empty}>Nothing spent in the last {data.days} days.</p>
      ) : (
        <div className={s.bleed}>
          <table className={s.table} style={{ minWidth: `${24 + shown.length * 7}rem` }}>
            <thead>
              <tr>
                <th scope="col">Team</th>
                <th scope="col" className={s.num}>Spend</th>
                {shown.map((feature) => (
                  <th key={feature.key} scope="col" className={s.num}>
                    {feature.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {teams.map((team) => (
                <tr key={team.key}>
                  <th scope="row">
                    {team.team}
                    {team.user && <span className={a.sub}>{team.user}</span>}
                  </th>
                  <td className={s.num}>{formatUsd(team.cost)}</td>
                  {shown.map((feature) => (
                    <td key={feature.key} className={s.num}>
                      {(team.parts[feature.key] ?? 0) > 0 ? formatUsd(team.parts[feature.key]) : "—"}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Block>
  )
}
