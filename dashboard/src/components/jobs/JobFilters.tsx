import { Search } from 'lucide-react'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'

export interface JobFiltersState {
  queue: string
  status: string
  search: string
}

interface JobFiltersProps {
  filters: JobFiltersState
  queues: string[]
  onChange: (f: Partial<JobFiltersState>) => void
}

export function JobFilters({ filters, queues, onChange }: JobFiltersProps) {
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
        <Select
          value={filters.queue || 'all'}
          onValueChange={v => onChange({ queue: v === 'all' ? '' : v })}
        >
          <SelectTrigger className="h-8 w-36 text-sm">
            <SelectValue placeholder="All queues" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All queues</SelectItem>
            {queues.map(q => (
              <SelectItem key={q} value={q}>{q}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      )}
    </div>
  )
}
