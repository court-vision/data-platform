/**
 * Reading rendered markup in tests (no DOM here). A desk rail's label sits in
 * spans inside its button, after the pill, so "the pressed one says 6h" is
 * the button's aria-pressed, then any tags, then the label.
 */
export function pressed(label: string): RegExp {
  return new RegExp(`aria-pressed="true"[^>]*>(?:<[^>]*>)*${label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}<`)
}
