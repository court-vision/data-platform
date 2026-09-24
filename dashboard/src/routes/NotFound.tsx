import { Link } from "react-router"

export function NotFound() {
  return (
    <div className="flex h-full flex-col items-center justify-center gap-2 p-8 text-center">
      <p className="font-mono text-sm text-muted-foreground">404</p>
      <h1 className="font-display text-xl font-bold">No such page</h1>
      <Link to="/" className="text-sm text-primary underline-offset-4 hover:underline">
        Back to the overview
      </Link>
    </div>
  )
}
