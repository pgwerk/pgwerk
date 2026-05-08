import { ScheduleDetail } from '@/components/cron/ScheduleDetail'
import { CreateScheduleDialog } from '@/components/cron/CreateScheduleDialog'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from '@/components/ui/table'
import { api } from '@/lib/api'
import { cn, relativeTime, shortFn } from '@/lib/utils'
import type { ScheduleResponse } from '@/types'
import { useQuery } from '@tanstack/react-query'
import { Pause, Plus, RefreshCw, Search } from 'lucide-react'
import { useMemo, useState } from 'react'

type StatusFilter = 'all' | 'active' | 'paused'

const STATUS_TABS: { label: string; value: StatusFilter }[] = [
  { label: 'All',    value: 'all' },
  { label: 'Active', value: 'active' },
  { label: 'Paused', value: 'paused' },
]

function formatSchedule(s: ScheduleResponse): string {
  if (s.cron) return s.cron
  if (s.interval_secs != null) {
    if (s.interval_secs < 60) return `every ${s.interval_secs}s`
    if (s.interval_secs < 3600) return `every ${s.interval_secs / 60}m`
    return `every ${s.interval_secs / 3600}h`
  }
  return '—'
}

export function SchedulesPage() {
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all')
  const [queueFilter, setQueueFilter] = useState('')
  const [search, setSearch] = useState('')
  const [selected, setSelected] = useState<ScheduleResponse | null>(null)
  const [createOpen, setCreateOpen] = useState(false)

  const { data, isLoading, refetch, isFetching } = useQuery({
    queryKey: ['schedules'],
    queryFn: api.listSchedules,
    refetchInterval: 30_000,
  })

  const queues = useMemo(() => [...new Set(data?.map(s => s.queue) ?? [])].sort(), [data])

  const visible = useMemo(() => {
    let rows = data ?? []
    if (statusFilter === 'active') rows = rows.filter(s => !s.paused)
    if (statusFilter === 'paused') rows = rows.filter(s => s.paused)
    if (queueFilter) rows = rows.filter(s => s.queue === queueFilter)
    if (search) {
      const q = search.toLowerCase()
      rows = rows.filter(s => s.name.toLowerCase().includes(q) || s.function.toLowerCase().includes(q))
    }
    return rows
  }, [data, statusFilter, queueFilter, search])

  function countTab(status: StatusFilter) {
    const base = data ?? []
    if (status === 'all') return base.length
    if (status === 'active') return base.filter(s => !s.paused).length
    return base.filter(s => s.paused).length
  }

  return (
    <div className="flex flex-col">
      <div className="flex h-12 items-center justify-between">
        <h1 className="text-sm font-semibold">Schedules</h1>
        <div className="flex items-center gap-1">
          <Button variant="outline" size="sm" className="h-7 gap-1.5 text-xs" onClick={() => setCreateOpen(true)}>
            <Plus className="h-3 w-3" />
            Create Schedule
          </Button>
          <Button variant="ghost" size="icon" className="h-8 w-8" onClick={() => refetch()} disabled={isFetching}>
            <RefreshCw className={`h-3.5 w-3.5 ${isFetching ? 'animate-spin' : ''}`} />
          </Button>
        </div>
      </div>

      <ScheduleDetail schedule={selected} open={!!selected} onClose={() => setSelected(null)} />
      <CreateScheduleDialog open={createOpen} onOpenChange={setCreateOpen} />

      {/* Status tabs */}
      <div className="flex items-center gap-0.5 pt-4">
        {STATUS_TABS.map(tab => {
          const count = countTab(tab.value)
          const isActive = statusFilter === tab.value
          return (
            <button
              key={tab.value}
              onClick={() => setStatusFilter(tab.value)}
              className={cn(
                'flex items-center gap-1.5 rounded-t-md border border-b-0 px-3 py-1.5 text-xs font-medium transition-colors',
                isActive
                  ? 'border-border bg-card text-foreground'
                  : 'border-transparent text-muted-foreground hover:text-foreground',
              )}
            >
              {tab.label}
              {count > 0 && (
                <span className={cn(
                  'rounded px-1 py-0.5 font-mono text-[10px] tabular-nums',
                  isActive ? 'bg-muted text-muted-foreground' : 'text-muted-foreground/60',
                )}>
                  {count}
                </span>
              )}
            </button>
          )
        })}
      </div>

      <div className="flex-1 space-y-4 py-6">
        {/* Filters */}
        <div className="flex flex-wrap items-center gap-3">
          <div className="relative">
            <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
            <input
              type="text"
              placeholder="Search by name or function…"
              value={search}
              onChange={e => setSearch(e.target.value)}
              className="h-8 w-72 rounded-md border border-input bg-background pl-8 pr-3 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
            />
          </div>
          {queues.length > 0 && (
            <Select value={queueFilter || 'all'} onValueChange={v => setQueueFilter(v === 'all' ? '' : v)}>
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

        <Card className="overflow-hidden">
          {isLoading ? (
            <div className="space-y-2 p-4">
              {Array.from({ length: 4 }).map((_, i) => (
                <Skeleton key={i} className="h-10 w-full" />
              ))}
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead>Function</TableHead>
                  <TableHead className="w-28">Queue</TableHead>
                  <TableHead className="w-32">Schedule</TableHead>
                  <TableHead className="w-20">Status</TableHead>
                  <TableHead className="w-32">Next Run</TableHead>
                  <TableHead className="w-32">Last Run</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {visible.map(s => (
                  <TableRow
                    key={s.name}
                    className="cursor-pointer"
                    onClick={() => setSelected(s)}
                  >
                    <TableCell className="font-mono text-xs font-medium" title={s.name}>
                      {shortFn(s.name)}
                    </TableCell>
                    <TableCell>
                      <span className="font-mono text-xs text-muted-foreground" title={s.function}>
                        {shortFn(s.function)}
                      </span>
                    </TableCell>
                    <TableCell className="font-mono text-xs text-muted-foreground">
                      {s.queue}
                    </TableCell>
                    <TableCell className="font-mono text-xs text-muted-foreground">
                      {formatSchedule(s)}
                    </TableCell>
                    <TableCell className="text-xs">
                      {s.paused ? (
                        <span className="flex items-center gap-1 text-yellow-500">
                          <Pause className="h-3 w-3" />
                          paused
                        </span>
                      ) : (
                        <span className="text-emerald-500">active</span>
                      )}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {relativeTime(s.next_run_at ?? undefined)}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {relativeTime(s.last_run_at ?? undefined)}
                    </TableCell>
                  </TableRow>
                ))}
                {!visible.length && (
                  <TableRow>
                    <TableCell colSpan={7} className="py-10 text-center text-sm text-muted-foreground">
                      No schedules found
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          )}
        </Card>
      </div>
    </div>
  )
}
