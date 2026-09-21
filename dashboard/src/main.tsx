import "@fontsource-variable/jetbrains-mono"
import "@fontsource-variable/outfit"
import "@fontsource-variable/plus-jakarta-sans"
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
