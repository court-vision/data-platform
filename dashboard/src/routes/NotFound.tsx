import { Lost } from "@/components/blocks"
import { usePageContext } from "@/components/shell/pageContext"

export function NotFound() {
  usePageContext("Not found")
  return <Lost title="No such page" back={{ to: "/", label: "Back to the overview" }} />
}
