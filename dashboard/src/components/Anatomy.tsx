import { formatMs, segments, type Group } from "@/lib/latency"
import s from "./sheet.module.css"
import l from "./latency.module.css"

/** "ESPN 212 ms · Postgres 18 ms · Our code 40 ms": a bar's numbers in words, for its title and screen readers. */
export function describeParts(parts: Record<string, number>): string {
  const segs = segments(parts)
  return segs.length === 0 ? "no time recorded" : segs.map((p) => `${p.group.label} ${formatMs(p.ms)}`).join(" · ")
}

/**
 * Where one request's time went (or a route's mean request), as segments in
 * the groups' colours. With a `scale`, its length is its total against that,
 * so the bars in one column compare: a longer bar is a slower request.
 * Without one it fills the cell, and only the shares show.
 */
export function AnatomyBar({ parts, scale, label }: { parts: Record<string, number>; scale?: number; label?: string }) {
  const segs = segments(parts)
  const total = segs.reduce((sum, part) => sum + part.ms, 0)
  const width = scale == null ? 100 : scale > 0 ? Math.max(0.5, Math.min(100, (total / scale) * 100)) : 0
  const text = `${label ? `${label}: ` : ""}${formatMs(total)} · ${describeParts(parts)}`
  return (
    <div className={`${l.viz} ${l.anatomy}`} style={{ width: `${width}%` }} role="img" aria-label={text} title={text}>
      {segs.map((part) => (
        <span
          key={part.group.key}
          className={l.segment}
          data-group={part.group.key}
          style={{ width: `calc((100% - ${2 * (segs.length - 1)}px) * ${total > 0 ? part.ms / total : 0})` }}
        />
      ))}
    </div>
  )
}

/** Names each colour a bar uses. */
export function AnatomyKey({ groups }: { groups: readonly Group[] }) {
  if (groups.length === 0) return null
  return (
    <ul className={`${s.legend} ${l.viz}`} aria-label="Key">
      {groups.map((group) => (
        <li key={group.key} title={group.note}>
          <span className={l.swatch} data-group={group.key} aria-hidden />
          {group.label}
        </li>
      ))}
    </ul>
  )
}
