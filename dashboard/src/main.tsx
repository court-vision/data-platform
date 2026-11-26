import "@fontsource-variable/geist"
import "@fontsource-variable/geist-mono"
import "@fontsource-variable/outfit"
// The faces a theme can set as its --desk-sans (fonts.ts in the frontend): Space
// Grotesk for Chalk, Barlow Semi Condensed for Blacktop, Fraunces (every axis,
// for its soft one) for Harvest and Hearth. A browser fetches a face's file only
// when a theme draws with it.
import "@fontsource-variable/space-grotesk"
import "@fontsource/barlow-semi-condensed/400.css"
import "@fontsource/barlow-semi-condensed/500.css"
import "@fontsource/barlow-semi-condensed/600.css"
import "@fontsource-variable/fraunces/full.css"
import "@/index.css"

import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { StrictMode } from "react"
import { createRoot } from "react-dom/client"

import { App } from "@/App"

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { staleTime: 10_000, refetchOnWindowFocus: true },
  },
})

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  </StrictMode>,
)
