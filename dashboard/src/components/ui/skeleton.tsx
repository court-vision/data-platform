import { cn } from "@/lib/utils"

/**
 * A block standing in for content whose size we can't know yet — a player
 * name, a headline, an avatar.
 *
 * Numeric cells should NOT use this: a bar is a guess at the column's width,
 * and the row jumps when the real number replaces it. Use `PendingValue`,
 * which renders inside the real cell at its final width.
 */
function Skeleton({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn("skeleton-shimmer rounded-md", className)}
      {...props}
    />
  )
}

export { Skeleton }
