import path from "node:path"
import react from "@vitejs/plugin-react"
import { defineConfig } from "vite"

// In production FastAPI (main_public.py) serves dist/ and the API from one
// origin. In dev, Vite serves the app and proxies the API to a locally running
// public app, so the browser still sees one origin and no CORS rule is needed.
//   uvicorn main_public:app --port 8002     (8080 is the features service)
const API_TARGET = process.env.DASHBOARD_API_TARGET ?? "http://localhost:8002"

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: { "@": path.resolve(__dirname, "./src") },
  },
  server: {
    port: 5173,
    proxy: { "/v1": API_TARGET, "/health": API_TARGET },
  },
})
