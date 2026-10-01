import { StatusBadge } from "@/components/StateBadge"
import { Card } from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"
import { useServices, type ServiceInfo } from "@/hooks/useServices"
import { formatUptime } from "@/lib/time"

/** What each service is running: version, environment, how long it has been up. */
export function ServiceCards() {
  const services = useServices()

  if (services.isPending) {
    return (
      <div className="grid gap-3 sm:grid-cols-2" aria-busy="true" aria-label="Loading services">
        <Skeleton className="h-[4.5rem] rounded-xl" />
        <Skeleton className="h-[4.5rem] rounded-xl" />
      </div>
    )
  }

  if (services.isError) {
    return <p className="text-sm text-muted-foreground">Services: {services.error.message}</p>
  }

  return (
    <div className="grid gap-3 sm:grid-cols-2">
      {services.data.services.map((service) => (
        <ServiceCard key={service.key} service={service} />
      ))}
    </div>
  )
}

function ServiceCard({ service }: { service: ServiceInfo }) {
  const status = !service.configured ? "unknown" : service.ok ? "ok" : "degraded"
  const label = !service.configured ? "Not configured" : service.ok ? "Up" : "Degraded"

  return (
    <Card variant="panel" className="flex items-center justify-between gap-3 px-4 py-3">
      <div className="min-w-0">
        <div className="flex items-center gap-2">
          <span className="font-medium">{service.name}</span>
          <StatusBadge status={status} label={label} />
        </div>
        <p className="mt-1 truncate font-mono text-xs text-muted-foreground">
          {service.version ?? "—"}
          {service.environment && <span> · {service.environment}</span>}
          {service.uptime_s != null && <span> · up {formatUptime(service.uptime_s)}</span>}
        </p>
        {service.error && <p className="mt-1 truncate text-xs text-status-loss" title={service.error}>{service.error}</p>}
      </div>
    </Card>
  )
}
