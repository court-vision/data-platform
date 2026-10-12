import type { Config } from "tailwindcss"

// Tailwind is here for its preflight only, as in the frontend: the reset the
// desk shell (src/desk/desk.module.css, a copy of the frontend's) is written
// against. Nothing uses a utility class; components style themselves with CSS
// modules in the desk tokens.
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: { extend: {} },
  plugins: [],
} satisfies Config
