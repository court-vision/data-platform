import { KeyRound } from "lucide-react"
import { useState, type FormEvent } from "react"

import { Wordmark } from "@/components/shell/Wordmark"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { ApiError, tokenIsValid } from "@/lib/api"
import { setToken } from "@/lib/token"

/** Shown instead of the app until a pipeline token is saved. */
export function TokenGate() {
  const [value, setValue] = useState("")
  const [error, setError] = useState<string | null>(null)
  const [checking, setChecking] = useState(false)

  async function submit(event: FormEvent) {
    event.preventDefault()
    const token = value.trim()
    if (!token) return

    setChecking(true)
    setError(null)
    try {
      if (await tokenIsValid(token)) {
        setToken(token)
      } else {
        setError("That token was rejected. Check PIPELINE_API_TOKEN and try again.")
      }
    } catch (caught) {
      setError(
        caught instanceof ApiError
          ? `Could not check the token: ${caught.message}`
          : "Could not reach the data platform.",
      )
    } finally {
      setChecking(false)
    }
  }

  return (
    <div className="flex h-full items-center justify-center p-4">
      <div className="flex w-full max-w-sm flex-col gap-6">
        <Wordmark />
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              <KeyRound className="size-4 text-primary" aria-hidden />
              Pipeline token
            </CardTitle>
            <CardDescription>
              The same bearer token cron-runner uses. It is kept in this browser&rsquo;s
              local storage and sent only to this origin.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <form onSubmit={submit} className="flex flex-col gap-3">
              <Input
                type="password"
                autoFocus
                autoComplete="off"
                spellCheck={false}
                aria-label="Pipeline token"
                aria-invalid={error !== null}
                placeholder="PIPELINE_API_TOKEN"
                className="font-mono"
                value={value}
                onChange={(event) => setValue(event.target.value)}
              />
              {error && (
                <p role="alert" className="text-sm text-destructive">
                  {error}
                </p>
              )}
              <Button type="submit" disabled={checking || value.trim() === ""}>
                {checking ? "Checking…" : "Continue"}
              </Button>
            </form>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
