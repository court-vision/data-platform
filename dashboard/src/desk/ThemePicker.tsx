import * as Popover from "@radix-ui/react-popover"
import { Check, Palette } from "lucide-react"

import { useDeskPortal } from "./DeskFrame"
import { DESK_THEMES } from "./themes"
import { setDeskTheme, useDeskTheme } from "./useDeskTheme"
import dk from "./desk.module.css"

/**
 * The bar's theme button, as in the frontend: every theme as a small desk drawn
 * in its own tokens, light beside dark, each row one pair. A pick applies at
 * once and the menu stays open, so themes can be clicked through.
 */
export function ThemePicker() {
  const container = useDeskPortal()
  const theme = useDeskTheme()
  return (
    <Popover.Root>
      <Popover.Trigger asChild>
        <button type="button" className={dk.iconBtn} aria-label="Theme" title="Theme (T swaps light and dark)">
          <Palette size={14} />
        </button>
      </Popover.Trigger>
      <Popover.Portal container={container}>
        <Popover.Content className={`${dk.menu} ${dk.themeMenu}`} align="end" sideOffset={6} collisionPadding={8}>
          <div className={dk.menuHead}>
            <span className={dk.menuTitle}>Theme</span>
            <span className={dk.sub}>T swaps a theme with its pair</span>
          </div>
          <div className={dk.menuList}>
            <div className={dk.themeGrid}>
              <div className={`${dk.label} ${dk.menuGroupLabel}`}>Light</div>
              <div className={`${dk.label} ${dk.menuGroupLabel}`}>Dark</div>
              {DESK_THEMES.map((t) => (
                <button
                  key={t.id}
                  type="button"
                  className={dk.themeCard}
                  aria-pressed={t.id === theme}
                  onClick={() => setDeskTheme(t.id)}
                >
                  <ThemeSwatch id={t.id} />
                  <span className={dk.themeName}>
                    {t.label}
                    {t.id === theme ? <Check size={12} /> : null}
                  </span>
                  <span className={dk.themeNote}>{t.note}</span>
                </button>
              ))}
            </div>
          </div>
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  )
}

/** A desk in miniature: bar, a today column, rows with values, a chip; all in the theme's own tokens. */
function ThemeSwatch({ id }: { id: string }) {
  return (
    <span className={`${dk.tokens} ${dk.swatch}`} data-theme={id} aria-hidden>
      <span className={dk.swatchBar}>
        <span className={dk.swatchWord}>
          <span />
          <span className={dk.swatchWordAccent} />
        </span>
        <span className={dk.swatchTab} />
      </span>
      <span className={dk.swatchHead}>
        <span className={dk.swatchText} style={{ width: "34%" }} />
        <span className={dk.swatchToday} />
      </span>
      {(["up", "down", "up"] as const).map((tone, i) => (
        <span key={i} className={dk.swatchRow}>
          <span className={dk.swatchText} style={{ width: `${46 - i * 8}%` }} />
          <span className={dk.swatchToday}>
            <span className={dk.swatchValue} data-tone={tone} />
          </span>
        </span>
      ))}
      <span className={dk.swatchFoot}>
        <span className={dk.swatchChip} />
        <span className={dk.swatchButton} />
      </span>
    </span>
  )
}
