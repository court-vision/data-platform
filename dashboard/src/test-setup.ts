/**
 * Preloaded by `bun test` (bunfig.toml). Vite turns `import s from
 * "./x.module.css"` into a map of hashed class names; bun's runtime has no
 * loader for that, so here every CSS module is a map from each class to its
 * own name: `s.row` is "row". Markup in tests then reads the classes as
 * written, and a class that does not exist still renders (as its name).
 */
import { plugin } from "bun"

plugin({
  name: "css-modules",
  setup(build) {
    build.onLoad({ filter: /\.module\.css$/ }, () => ({
      exports: { default: new Proxy({}, { get: (_, key) => (typeof key === "string" ? key : undefined) }) },
      loader: "object",
    }))
  },
})
