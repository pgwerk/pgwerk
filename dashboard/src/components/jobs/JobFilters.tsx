import { ChevronDown, Search } from 'lucide-react'
import { Button } from '@/components/ui/button'
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'

export interface JobFiltersState {
  queues: string[]
  status: string
  search: string
}

interface JobFiltersProps {
  filters: JobFiltersState
  queues: string[]
  onChange: (f: Partial<JobFiltersState>) => void
}

function queueButtonLabel(selected: string[], all: string[]): string {
  if (selected.length === 0 || selected.length === all.length) return 'All queues'
  if (selected.length === 1) return selected[0]
  return `${selected.length} queues`
}

export function JobFilters({ filters, queues, onChange }: JobFiltersProps) {
  const selected = filters.queues
  const isSelected = (q: string) => selected.length === 0 || selected.includes(q)

  function toggleQueue(q: string) {
    const current = selected.length === 0 ? queues : selected
    const next = current.includes(q) ? current.filter(x => x !== q) : [...current, q]
    onChange({ queues: next.length === queues.length ? [] : next })
  }

  return (
    <div className="flex flex-wrap items-center gap-3">
      <div className="relative">
        <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
        <input
          type="text"
          placeholder="Search by function, queue, or ID…"
          value={filters.search}
          onChange={e => onChange({ search: e.target.value })}
          className="h-8 w-72 rounded-md border border-input bg-background pl-8 pr-3 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
        />
      </div>

      {queues.length > 0 && (
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="outline" size="sm" className="h-8 w-40 justify-between text-sm font-normal">
              <span className="truncate">{queueButtonLabel(selected, queues)}</span>
              <ChevronDown className="ml-2 h-3.5 w-3.5 opacity-50" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" className="w-48">
            <DropdownMenuItem
              onSelect={e => { e.preventDefault(); onChange({ queues: [] }) }}
              className="text-xs"
            >
              Select all
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            {queues.map(q => (
              <DropdownMenuCheckboxItem
                key={q}
                checked={isSelected(q)}
                onSelect={e => e.preventDefault()}
                onCheckedChange={() => toggleQueue(q)}
              >
                {q}
              </DropdownMenuCheckboxItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
      )}
    </div>
  )
}
