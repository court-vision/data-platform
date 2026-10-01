import { createBrowserRouter, RouterProvider } from "react-router"

import { AppShell } from "@/components/shell/AppShell"
import { TokenGate } from "@/components/shell/TokenGate"
import { Toaster } from "@/components/ui/sonner"
import { useToken } from "@/lib/token"
import { NotFound } from "@/routes/NotFound"
import { Overview } from "@/routes/Overview"

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
      { path: "*", element: <NotFound /> },
    ],
  },
])

export function App() {
  return (
    <>
      <RouterProvider router={router} />
      <Toaster />
    </>
  )
}
