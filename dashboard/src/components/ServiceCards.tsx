import { Block } from "@/components/blocks"
import { StatusBadge } from "@/components/StateBadge"
import { useServices, type ServiceInfo } from "@/hooks/useServices"
import { formatUptime } from "@/lib/time"
import dk from "@/desk/desk.module.css"
import s from "./sheet.module.css"

/** What each service is running: version, environment, how long it has been up. */
export function ServiceCards() {
  const services = useServices()

  return (
    <Block title="Services" note="what is deployed, read from each service's /health">
      {services.isPending ? (
        <div className={s.facts} aria-busy="true" aria-label="Loading services">
          <span className={`${dk.skel} ${s.skelBox}`} />
          <span className={`${dk.skel} ${s.skelBox}`} />
        </div>
      ) : services.isError ? (
        <p className={s.empty}>Services: {services.error.message}</p>
      ) : (
        <div className={s.facts}>
          {services.data.services.map((service) => (
            <ServiceCard key={service.key} service={service} />
          ))}
        </div>
      )}
    </Block>
  )
}

function ServiceCard({ service }: { service: ServiceInfo }) {
  const status = !service.configured ? "unknown" : service.ok ? "ok" : "degraded"
  const label = !service.configured ? "Not configured" : service.ok ? "Up" : "Degraded"

  return (
    <div className={s.fact}>
      <span className={s.chips}>
        <span className={s.title}>
          {service.name}
        </span>
        <StatusBadge status={status === "ok" ? "success" : status} label={label} />
      </span>
      <p className={`${s.factVal} ${s.muted}`}>
        {service.version ?? "—"}
        {service.environment && <span> · {service.environment}</span>}
        {service.uptime_s != null && <span> · up {formatUptime(service.uptime_s)}</span>}
      </p>
      {service.error && (
        <p className={`${s.factVal} ${s.bad}`} title={service.error}>
          {service.error}
        </p>
      )}
    </div>
  )
}
