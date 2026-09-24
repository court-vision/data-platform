/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Dev server only: start signed in with this pipeline token. */
  readonly VITE_DEV_TOKEN?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
