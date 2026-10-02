import { Link } from "react-router"

import type { QualityCheckInfo } from "@/lib/quality"

/** What a check asserts: the table it guards (and what it is compared against), the pipelines behind it, and the SQL itself. */
export function CheckDefinition({ definition }: { definition: QualityCheckInfo }) {
  return (
    <div className="flex flex-col gap-2 text-xs">
      <dl className="grid gap-x-6 gap-y-1 sm:grid-cols-3">
        <div>
          <dt className="text-muted-foreground">Guards</dt>
          <dd className="break-all font-mono">{definition.table}</dd>
          {definition.against.length > 0 && (
            <dd className="break-all font-mono text-muted-foreground">against {definition.against.join(", ")}</dd>
          )}
        </div>
        <div>
          <dt className="text-muted-foreground">{definition.group === "timing" ? "Watches" : "Written by"}</dt>
          <dd className="font-mono">
            {definition.pipelines.length === 0
              ? "—"
              : definition.pipelines.map((pipeline, i) => (
                  <span key={pipeline}>
                    {i > 0 && ", "}
                    <Link to={`/pipelines/${pipeline}`} className="text-primary underline-offset-4 hover:underline">
                      {pipeline}
                    </Link>
                  </span>
                ))}
          </dd>
        </div>
        <div>
          <dt className="text-muted-foreground">Severity</dt>
          <dd className="font-mono">
            {definition.severity}
            {definition.severity === "critical" ? " · alerts" : " · no alert"}
          </dd>
        </div>
      </dl>
      <p>
        <span className="text-muted-foreground">Fails when </span>
        {definition.failure_message}
      </p>
      <pre className="overflow-x-auto rounded bg-muted/60 p-3 font-mono text-[11px] leading-relaxed text-muted-foreground">
        {definition.sql}
      </pre>
    </div>
  )
}
