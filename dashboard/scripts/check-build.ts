/**
 * Post-build assertions on dist/, for failures that are silent in the browser.
 *
 *   bun scripts/check-build.ts
 *
 * 1. Every desk theme reached the CSS. A theme is a token block keyed by
 *    `data-theme` in desk/desk.module.css; one that is listed in themes.ts but
 *    missing from the stylesheet (a stale copy, a block lost in a re-sync)
 *    paints the page in the base theme's colours, without an error. The fonts'
 *    variables are checked the same way: without them every desk face falls
 *    back to the system's.
 * 2. No dev token reached the bundle (src/lib/token.ts). `bun run build` sets
 *    VITE_DEV_TOKEN to a sentinel, so a lost `import.meta.env.DEV` guard puts
 *    that string in the JS. Looking for identifiers could not catch it: the
 *    minifier renames `devToken`, and Vite inlines the value in place of the name.
 */
import { readdirSync, readFileSync } from "node:fs"
import { join } from "node:path"

import { DESK_THEMES } from "../src/desk/themes"

const assets = join(import.meta.dir, "..", "dist", "assets")
const files = readdirSync(assets)
const read = (ext: string) =>
  files.filter((file) => file.endsWith(ext)).map((file) => readFileSync(join(assets, file), "utf8")).join("\n")

const failures: string[] = []
const css = read(".css")
const js = read(".js")

// Paper is the base block (no data-theme); every other theme overrides it.
for (const theme of DESK_THEMES) {
  if (theme.id === "paper") continue
  if (!new RegExp(`\\[data-theme=["']?${theme.id}["']?\\]`).test(css)) {
    failures.push(`theme "${theme.id}" is in themes.ts but has no [data-theme=${theme.id}] block in the built CSS: re-sync desk.module.css`)
  }
}
if (!/--bg:\s*#/.test(css)) failures.push("no `--bg` token in the built CSS: is desk/desk.module.css still imported?")
for (const variable of ["--desk-sans", "--desk-mono", "--font-outfit"]) {
  if (!new RegExp(`${variable}:`).test(css)) failures.push(`\`${variable}\` is never defined: index.css must name the desk's fonts`)
}

if (js.includes("__cv_dev_token_sentinel__")) {
  failures.push("VITE_DEV_TOKEN's value is in the production bundle: src/lib/token.ts must read it only when import.meta.env.DEV")
}

if (failures.length > 0) {
  console.error("build check failed:\n" + failures.map((failure) => `  - ${failure}`).join("\n"))
  process.exit(1)
}
console.log(`build check ok: ${DESK_THEMES.length} desk themes and the desk fonts in the CSS; no dev token in the bundle`)
