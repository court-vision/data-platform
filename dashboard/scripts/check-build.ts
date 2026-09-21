/**
 * Post-build assertions on dist/, for failures that are silent in the browser.
 *
 *   bun scripts/check-build.ts
 *
 * 1. The colour tokens win the cascade. See the note at the top of
 *    src/index.css: if the Tailwind plugin's self-referential
 *    `--background: hsl(var(--background))` lands after the real token, every
 *    colour resolves to nothing and the page renders unthemed, without an error.
 * 2. No dev-only token code reached the bundle (src/lib/token.ts).
 */
import { readdirSync, readFileSync } from "node:fs"
import { join } from "node:path"

const assets = join(import.meta.dir, "..", "dist", "assets")
const files = readdirSync(assets)
const read = (ext: string) =>
  files.filter((file) => file.endsWith(ext)).map((file) => readFileSync(join(assets, file), "utf8")).join("\n")

const failures: string[] = []
const css = read(".css")
const js = read(".js")

const token = css.search(/--background:\s*\d+ \d+% \d+%/)
const selfReference = css.search(/--background:\s*hsl\(var\(--background\)\)/)
if (token === -1) {
  failures.push("no `--background` colour token in the built CSS: is styles/tokens.css still imported?")
} else if (selfReference > token) {
  failures.push(
    "the Tailwind plugin's `--background: hsl(var(--background))` comes after the real token, " +
      "so it wins and the theme is gone. In src/index.css, tokens.css must be imported after tailwindcss/base.",
  )
}

if (/VITE_DEV_TOKEN|devToken/.test(js)) {
  failures.push("dev-only token code is in the production bundle (src/lib/token.ts)")
}

if (failures.length > 0) {
  console.error("build check failed:\n" + failures.map((failure) => `  - ${failure}`).join("\n"))
  process.exit(1)
}
console.log("build check ok: tokens win the cascade; no dev-token code in the bundle")
