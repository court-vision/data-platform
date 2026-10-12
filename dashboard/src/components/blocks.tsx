import * as Popover from "@radix-ui/react-popover"
import { ArrowLeft, TriangleAlert } from "lucide-react"
import type { ReactNode } from "react"
import { Link } from "react-router"

import { useDeskPortal } from "@/desk/DeskFrame"
import { relativeTime } from "@/lib/time"
import dk from "@/desk/desk.module.css"
import s from "./sheet.module.css"

/**
 * The tones a value can take, each a desk colour: `up` good, `down` bad,
 * `warn` needs a look, `live` happening now, `quiet` nothing to say.
 */
export type Tone = "up" | "down" | "warn" | "live" | "quiet"

/** A page: its head, then blocks, ruled off from one another. */
export function Page({ children }: { children: ReactNode }) {
  return <div className={s.page}>{children}</div>
}

/** The page's head: where it was opened from, its name, what it is, and its own actions. */
export function Head({
  back,
  title,
  chips,
  meta,
  sub,
  actions,
}: {
  back?: { to: string; label: string }
  title: ReactNode
  chips?: ReactNode
  meta?: ReactNode
  sub?: ReactNode
  actions?: ReactNode
}) {
  return (
    <header className={s.head}>
      <div className={s.headBody}>
        {back && (
          <Link to={back.to} className={s.back}>
            <ArrowLeft size={11} aria-hidden /> {back.label}
          </Link>
        )}
        <h1 className={s.headTitle}>
          {title}
          {chips}
        </h1>
        {meta && <div className={s.headMeta}>{meta}</div>}
        {sub && <p className={s.headSub}>{sub}</p>}
      </div>
      {actions && <div className={s.headActions}>{actions}</div>}
    </header>
  )
}

/** A mid-dot between facts in a meta line. */
export function Sep() {
  return (
    <span className={s.sep} aria-hidden>
      ·
    </span>
  )
}

/** "updated 12s ago · next in 18s": the poll, made visible. */
export function RefreshNote({
  updatedAt,
  fetching,
  now,
  intervalMs,
}: {
  updatedAt: number
  fetching: boolean
  now: number
  intervalMs: number
}) {
  if (!updatedAt) return null
  const next = Math.max(0, Math.ceil((updatedAt + intervalMs - now) / 1000))
  return (
    <p className={s.refresh} aria-live="off">
      updated {relativeTime(new Date(updatedAt).toISOString(), now)} · {fetching ? "refreshing…" : `next in ${next}s`}
    </p>
  )
}

/** A failed refresh. The last good data stays on screen below it. */
export function Notice({ children }: { children: ReactNode }) {
  return (
    <div role="alert" className={s.notice}>
      <TriangleAlert size={14} aria-hidden />
      <span>{children}</span>
    </div>
  )
}

export interface TileSpec {
  label: string
  value: ReactNode
  tone?: Tone
  note?: string
}

/** The page's numbers at a glance: four tiles, two to a row on a phone. */
export function Tiles({ tiles }: { tiles: TileSpec[] }) {
  return (
    <dl className={s.tiles}>
      {tiles.map((tile) => (
        <div key={tile.label} className={s.tile}>
          <dt className={s.tileLabel}>{tile.label}</dt>
          <dd className={s.tileValue} data-tone={tile.tone}>
            {tile.value}
          </dd>
          {tile.note ? <dd className={s.tileNote}>{tile.note}</dd> : null}
        </div>
      ))}
    </dl>
  )
}

/** A titled section of a page: a mono title and count, a note, its own controls on the right. */
export function Block({
  title,
  count,
  countTone,
  note,
  right,
  text,
  label,
  children,
}: {
  title: string
  count?: ReactNode
  countTone?: Tone
  note?: ReactNode
  right?: ReactNode
  /** A paragraph under the head: what the block shows and how it is judged. */
  text?: ReactNode
  label?: string
  children?: ReactNode
}) {
  return (
    <section className={s.block} aria-label={label ?? title}>
      <div className={s.blockHead}>
        <h2 className={s.blockTitle}>
          {title}
          {count != null && (
            <span className={s.blockCount} data-tone={countTone}>
              {count}
            </span>
          )}
        </h2>
        {note && <span className={s.blockNote}>{note}</span>}
        {right && <div className={s.blockRight}>{right}</div>}
      </div>
      {text && <p className={s.blockText}>{text}</p>}
      {children}
    </section>
  )
}

/** A chip for a result or a state, in its tone. A running one pulses. */
export function Chip({ tone, children, title }: { tone: Tone; children: ReactNode; title?: string }) {
  const toneClass = tone === "up" || tone === "live" ? dk.up : tone === "down" ? dk.down : tone === "warn" ? dk.warnChip : dk.flat
  return (
    <span className={`${dk.chip} ${toneClass} ${tone === "live" ? s.chipLive : ""}`} data-tone={tone} title={title}>
      {tone === "live" && <span className={dk.liveDot} aria-hidden />}
      {children}
    </span>
  )
}

/** What a thing is, not how it did: a group, a severity. A critical one is outlined in red. */
export function Tag({ children, tone, title }: { children: ReactNode; tone?: Tone; title?: string }) {
  return (
    <span className={s.tag} data-tone={tone} title={title}>
      {children}
    </span>
  )
}

/** Mutually exclusive buttons that read as one: a desk rail, each option a pressed or unpressed button. */
export function Rail<T extends string | number>({
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
    <div className={`${dk.rail} ${s.railSmall}`} role="group" aria-label={label}>
      {options.map((option) => {
        const on = option.key === value
        return (
          <button
            key={String(option.key)}
            type="button"
            aria-pressed={on}
            className={`${dk.seg} ${on ? dk.segOn : ""}`}
            onClick={() => onChange(option.key)}
          >
            {on ? <span className={dk.segPill} /> : null}
            <span className={dk.segLabel}>{option.label}</span>
          </button>
        )
      })}
    </div>
  )
}

/** A rail with its name before it ("Rank in", "Show"). */
export function Choice<T extends string>(props: {
  label: string
  options: readonly { key: T; label: string }[]
  value: T
  onChange: (value: T) => void
}) {
  return (
    <span className={s.choice}>
      <span className={dk.label}>{props.label}</span>
      <Rail {...props} />
    </span>
  )
}

/** A popover in the desk's menu style, portalled inside the frame so its tokens apply. */
export function Pop({
  trigger,
  children,
  open,
  onOpenChange,
  align = "end",
}: {
  trigger: ReactNode
  children: ReactNode
  open?: boolean
  onOpenChange?: (open: boolean) => void
  align?: "start" | "center" | "end"
}) {
  const container = useDeskPortal()
  return (
    <Popover.Root open={open} onOpenChange={onOpenChange}>
      <Popover.Trigger asChild>{trigger}</Popover.Trigger>
      <Popover.Portal container={container}>
        <Popover.Content className={`${dk.menu} ${s.pop}`} align={align} sideOffset={6} collisionPadding={8}>
          {children}
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  )
}

/** Loading: the tiles' places, then a few lines. */
export function SheetSkeleton({ label, tiles = true, rows = 8 }: { label: string; tiles?: boolean; rows?: number }) {
  return (
    <div aria-busy="true" aria-label={label}>
      {tiles && (
        <div className={s.skelTiles}>
          {Array.from({ length: 4 }, (_, i) => (
            <span key={i} className={`${s.skelTile} ${dk.skel}`} />
          ))}
        </div>
      )}
      <div className={s.skeleton}>
        {Array.from({ length: rows }, (_, i) => (
          <span key={i} className={dk.skel} style={{ width: `${92 - (i % 4) * 14}%`, opacity: 1 - i * 0.07 }} />
        ))}
      </div>
    </div>
  )
}

/** A page with nothing to show at this address. */
export function Lost({ title, back }: { title: string; back: { to: string; label: string } }) {
  return (
    <div className={`${s.page} ${s.lost}`}>
      <span className={s.lostCode}>404</span>
      <h1 className={s.lostTitle}>{title}</h1>
      <Link to={back.to} className={s.textLink}>
        {back.label}
      </Link>
    </div>
  )
}
