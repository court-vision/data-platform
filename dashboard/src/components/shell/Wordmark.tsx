import { Link } from "react-router"

/** The frontend's mark: the wordmark's own letters, amber from the V. */
export function Wordmark() {
  return (
    <Link
      to="/"
      aria-label="Data Platform home"
      className="flex flex-col gap-1 font-display font-black leading-none tracking-tighter text-foreground"
    >
      <span className="text-lg">
        COURT<span className="text-primary">VISION</span>
      </span>
      <span className="font-mono text-[10px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
        Data Platform
      </span>
    </Link>
  )
}
