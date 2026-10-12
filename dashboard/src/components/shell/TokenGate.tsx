import { KeyRound, Loader2 } from "lucide-react"
import { useState, type FormEvent } from "react"

import { usePageContext } from "@/components/shell/pageContext"
import { ThemePicker } from "@/desk/ThemePicker"
import { ApiError, tokenIsValid } from "@/lib/api"
import { setToken } from "@/lib/token"
import dk from "@/desk/desk.module.css"
import s from "./shell.module.css"

/** Shown instead of the app until a pipeline token is saved. */
export function TokenGate() {
  usePageContext(null)
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
    <div className={s.gate}>
      <div className={s.gateCorner}>
        <ThemePicker />
      </div>
      <form onSubmit={submit} className={s.gateCard} aria-labelledby="gate-title">
        <div className={s.gateMark} aria-hidden>
          <span className={dk.mark}>
            COURT<span className={dk.markAccent}>VISION</span>
          </span>
          <span className={dk.label}>Data platform</span>
        </div>
        <div className={dk.dialogBody}>
          <h1 id="gate-title" className={`${dk.dialogTitle} ${s.gateTitle}`}>
            <KeyRound size={15} aria-hidden /> Pipeline token
          </h1>
          <p className={dk.dialogText}>
            The same bearer token cron-runner uses. It is kept in this browser&rsquo;s local storage and sent only to
            this origin.
          </p>
          <input
            type="password"
            autoFocus
            autoComplete="off"
            spellCheck={false}
            aria-label="Pipeline token"
            aria-invalid={error !== null}
            placeholder="PIPELINE_API_TOKEN"
            className={`${dk.input} ${s.gateInput}`}
            value={value}
            onChange={(event) => setValue(event.target.value)}
          />
          {error && (
            <p role="alert" className={dk.error}>
              {error}
            </p>
          )}
        </div>
        <div className={dk.dialogFoot}>
          <button type="submit" className={`${dk.btn} ${dk.btnPrimary}`} disabled={checking || value.trim() === ""}>
            {checking ? <Loader2 size={13} className={dk.spin} aria-hidden /> : null}
            {checking ? "Checking" : "Continue"}
          </button>
        </div>
      </form>
    </div>
  )
}
