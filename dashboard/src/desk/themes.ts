// Copied from frontend/src/components/desk/themes.ts by `python scripts/check_frontend_theme.py --write`. Do not edit here: change the frontend's, then re-sync.
/**
 * The desk themes, in light/dark pairs: T swaps a theme for its pair. Each id
 * names a token block in desk.module.css (Paper is the base `.tokens` block)
 * and is what storage remembers, so never rename one. `scheme` picks the
 * team-logo variant and the toast theme. The menu lays them out light beside
 * dark, a pair to a row, so keep each light theme just before its pair.
 */
export type DeskScheme = "light" | "dark";

export interface DeskThemeInfo {
  id: string;
  label: string;
  scheme: DeskScheme;
  /** The same family on the other side. */
  pair: string;
  /** One line under the name in the appearance menu. */
  note: string;
}

export const DESK_THEMES = [
  { id: "noon", label: "Noon", scheme: "light", pair: "midnight", note: "Pale sky, navy ink, a cerulean accent" },
  { id: "midnight", label: "Midnight", scheme: "dark", pair: "noon", note: "Deep navy, sky accent, a soft glow" },
  { id: "paper", label: "Paper", scheme: "light", pair: "ember", note: "Warm newsprint, ink and court orange" },
  { id: "ember", label: "Ember", scheme: "dark", pair: "paper", note: "Warm charcoal and amber" },
] as const satisfies readonly DeskThemeInfo[];

export type DeskThemeId = (typeof DESK_THEMES)[number]["id"];

/** The default pair: a first visit opens in Midnight. */
export const DEFAULT_LIGHT: DeskThemeId = "noon";
export const DEFAULT_DARK: DeskThemeId = "midnight";

const BY_ID = new Map<string, DeskThemeInfo>(DESK_THEMES.map((t) => [t.id, t]));

export function isDeskTheme(id: unknown): id is DeskThemeId {
  return typeof id === "string" && BY_ID.has(id);
}

export function themeScheme(id: DeskThemeId): DeskScheme {
  return BY_ID.get(id)?.scheme ?? "light";
}

export function themePair(id: DeskThemeId): DeskThemeId {
  const pair = BY_ID.get(id)?.pair;
  return isDeskTheme(pair) ? pair : themeScheme(id) === "dark" ? DEFAULT_LIGHT : DEFAULT_DARK;
}
