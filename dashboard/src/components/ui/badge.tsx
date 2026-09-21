import * as React from "react"
import { cva, type VariantProps } from "class-variance-authority"

import { cn } from "@/lib/utils"

const badgeVariants = cva(
  "inline-flex items-center rounded px-1.5 py-0.5 text-[11px] font-semibold tracking-wide uppercase transition-colors focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2",
  {
    variants: {
      variant: {
        default:
          "bg-primary/20 text-primary border border-primary/30",
        secondary:
          "border-transparent bg-secondary text-secondary-foreground hover:bg-secondary/80",
        destructive:
          "border-transparent bg-destructive text-destructive-foreground shadow hover:bg-destructive/80",
        outline: "text-foreground border border-border",
        win: "bg-status-win/15 text-status-win border border-status-win/30",
        loss: "bg-status-loss/15 text-status-loss border border-status-loss/30",
        projected: "bg-status-projected/15 text-status-projected border border-status-projected/30",
        neutral: "bg-muted text-muted-foreground border border-border",
        position: "bg-transparent text-foreground border border-border font-medium uppercase tracking-wider",
        live: "bg-status-win/15 text-status-win border border-status-win/30 animate-beacon",
        hot: "bg-signal-hot/15 text-signal-hot border border-signal-hot/30",
        cold: "bg-signal-cold/15 text-signal-cold border border-signal-cold/30",
        breakout: "bg-status-projected/15 text-status-projected border border-status-projected/30",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  }
)

export interface BadgeProps
  extends React.HTMLAttributes<HTMLDivElement>,
    VariantProps<typeof badgeVariants> {}

function Badge({ className, variant, ...props }: BadgeProps) {
  return (
    <div className={cn(badgeVariants({ variant }), className)} {...props} />
  )
}

export { Badge, badgeVariants }
