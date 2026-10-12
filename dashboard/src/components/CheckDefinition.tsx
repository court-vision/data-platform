import { Link } from "react-router"

import type { QualityCheckInfo } from "@/lib/quality"
import s from "./sheet.module.css"

/** What a check asserts: the table it guards (and what it is compared against), the pipelines behind it, and the SQL itself. */
export function CheckDefinition({ definition }: { definition: QualityCheckInfo }) {
  return (
    <div className={s.definition}>
      <dl className={s.facts}>
        <div className={s.fact}>
          <dt className={s.factKey}>Guards</dt>
          <dd className={s.factVal}>{definition.table}</dd>
          {definition.against.length > 0 && <dd className={`${s.factVal} ${s.muted}`}>against {definition.against.join(", ")}</dd>}
        </div>
        <div className={s.fact}>
          <dt className={s.factKey}>{definition.group === "timing" ? "Watches" : "Written by"}</dt>
          <dd className={s.factVal}>
            {definition.pipelines.length === 0
              ? "—"
              : definition.pipelines.map((pipeline, i) => (
                  <span key={pipeline}>
                    {i > 0 && ", "}
                    <Link to={`/pipelines/${pipeline}`} className={s.textLink}>
                      {pipeline}
                    </Link>
                  </span>
                ))}
          </dd>
        </div>
        <div className={s.fact}>
          <dt className={s.factKey}>Severity</dt>
          <dd className={s.factVal} data-tone={definition.severity === "critical" ? "down" : undefined}>
            {definition.severity}
            {definition.severity === "critical" ? " · alerts" : " · no alert"}
          </dd>
        </div>
      </dl>
      <p className={s.definitionText}>
        <span className={s.muted}>Fails when </span>
        {definition.failure_message}
      </p>
      <pre className={s.pre}>{definition.sql}</pre>
    </div>
  )
}
