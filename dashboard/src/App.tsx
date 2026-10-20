import { createBrowserRouter, RouterProvider } from "react-router"

import { AppShell } from "@/components/shell/AppShell"
import { TokenGate } from "@/components/shell/TokenGate"
import { DeskFrame } from "@/desk/DeskFrame"
import { useToken } from "@/lib/token"
import { Freshness } from "@/routes/Freshness"
import { Latency } from "@/routes/Latency"
import { LatencyRoute } from "@/routes/LatencyRoute"
import { NotFound } from "@/routes/NotFound"
import { Overview } from "@/routes/Overview"
import { PipelineDetail } from "@/routes/PipelineDetail"
import { Projections } from "@/routes/Projections"
import { Quality } from "@/routes/Quality"
import { QualityRun } from "@/routes/QualityRun"

/** The gate lives inside the router, so a deep link survives signing in. */
function Root() {
  const token = useToken()
  return token === null ? <TokenGate /> : <AppShell />
}

// Client-side routes. main_public.py answers any path outside /v1 and /assets
// with index.html, so a hard refresh on a deep link lands back here.
const router = createBrowserRouter([
  {
    element: <Root />,
    children: [
      { index: true, element: <Overview /> },
      { path: "data", element: <Freshness /> },
      { path: "pipelines/:name", element: <PipelineDetail /> },
      { path: "quality", element: <Quality /> },
      { path: "quality/runs/:runId", element: <QualityRun /> },
      { path: "projections", element: <Projections /> },
      { path: "latency", element: <Latency /> },
      { path: "latency/route", element: <LatencyRoute /> },
      { path: "*", element: <NotFound /> },
    ],
  },
])

/** Every page renders inside the desk frame: its tokens, its theme, its toasts. */
export function App() {
  return (
    <DeskFrame>
      <RouterProvider router={router} />
    </DeskFrame>
  )
}
