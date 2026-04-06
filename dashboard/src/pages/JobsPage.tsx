import { JobFilters, type JobFiltersState } from '@/components/jobs/JobFilters'
import { JobsTable } from '@/components/jobs/JobsTable'
import { SendJobDialog } from '@/components/jobs/SendJobDialog'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { api } from '@/lib/api'
import { cn } from '@/lib/utils'
import type { JobStatus, QueueStats } from '@/types'
import { useQuery } from '@tanstack/react-query'
import { Plus, RefreshCw } from 'lucide-react'
import { useEffect, useMemo, useRef, useState } from 'react'

const PAGE_SIZE = 50

const STATUS_TABS: { label: string; value: JobStatus | '' }[] = [
  { label: 'All',      value: '' },
  { label: 'Queued',   value: 'queued' },
  { label: 'Active',   value: 'active' },
  { label: 'Waiting',  value: 'waiting' },
  { label: 'Failed',   value: 'failed' },
  { label: 'Complete', value: 'complete' },
  { label: 'Aborted',  value: 'aborted' },
]

function sumStatus(queues: QueueStats[], status: JobStatus | ''): number {
  if (status === '') return queues.reduce((s, q) => s + q.queued + q.active + q.waiting + q.failed + q.complete + q.aborted + q.scheduled, 0)
  return queues.reduce((s, q) => s + (q[status as keyof QueueStats] as number ?? 0), 0)
}

export function JobsPage() {
  const [page, setPage] = useState(0)
  const [sendDialogOpen, setSendDialogOpen] = useState(false)
  const [filters, setFilters] = useState<JobFiltersState>({ queue: '', status: '', search: '' })
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => {
      setDebouncedSearch(filters.search)
      setPage(0)
    }, 350)
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current) }
  }, [filters.search])

  const { data: stats } = useQuery({ queryKey: ['stats'], queryFn: api.getStats, refetchInterval: 5_000 })
  const queues = useMemo(() => stats?.queues.map(q => q.queue) ?? [], [stats])

  const { data, isLoading, refetch, isFetching } = useQuery({
    queryKey: ['jobs', filters.queue, filters.status, debouncedSearch, page],
    queryFn: () => api.listJobs({
      queue: filters.queue || undefined,
      status: filters.status || undefined,
      search: debouncedSearch || undefined,
      limit: PAGE_SIZE + 1,
      offset: page * PAGE_SIZE,
    }),
    refetchInterval: 10_000,
  })

  const visibleJobs = useMemo(() => data?.slice(0, PAGE_SIZE) ?? [], [data])

  function handleFilterChange(partial: Partial<JobFiltersState>) {
    setFilters(f => ({ ...f, ...partial }))
    if (!('search' in partial)) setPage(0)
  }

  function setStatusTab(value: JobStatus | '') {
    handleFilterChange({ status: value })
  }

  return (
    <div className="flex flex-col">
      <div className="flex h-12 items-center justify-between px-6">
        <h1 className="text-sm font-semibold">Jobs</h1>
        <div className="flex items-center gap-1">
          <Button variant="outline" size="sm" className="h-7 gap-1.5 text-xs" onClick={() => setSendDialogOpen(true)}>
            <Plus className="h-3 w-3" />
            Send Job
          </Button>
          <Button variant="ghost" size="icon" className="h-8 w-8" onClick={() => refetch()} disabled={isFetching}>
            <RefreshCw className={`h-3.5 w-3.5 ${isFetching ? 'animate-spin' : ''}`} />
          </Button>
        </div>
      </div>

      <SendJobDialog
        open={sendDialogOpen}
        onOpenChange={setSendDialogOpen}
        defaultQueue={filters.queue || undefined}
      />

      {/* Status tabs */}
      <div className="flex items-center gap-0.5 border-b border-border px-6 pt-4">
        {STATUS_TABS.map(tab => {
          const count = stats ? sumStatus(stats.queues, tab.value) : null
          const isActive = filters.status === tab.value
          return (
            <button
              key={tab.value}
              onClick={() => setStatusTab(tab.value)}
              className={cn(
                'flex items-center gap-1.5 rounded-t-md border border-b-0 px-3 py-1.5 text-xs font-medium transition-colors',
                isActive
                  ? 'border-border bg-card text-foreground'
                  : 'border-transparent text-muted-foreground hover:text-foreground',
              )}
            >
              {tab.label}
              {count != null && count > 0 && (
                <span className={cn(
                  'rounded px-1 py-0.5 font-mono text-[10px] tabular-nums',
                  isActive ? 'bg-muted text-muted-foreground' : 'text-muted-foreground/60',
                )}>
                  {count.toLocaleString()}
                </span>
              )}
            </button>
          )
        })}
      </div>

      <div className="flex-1 space-y-4 p-6">
        <JobFilters filters={filters} queues={queues} onChange={handleFilterChange} />
        <Card className="overflow-hidden">
          <JobsTable
            jobs={visibleJobs}
            isLoading={isLoading}
            page={page}
            onPageChange={setPage}
            hasMore={(data?.length ?? 0) > PAGE_SIZE}
          />
        </Card>
      </div>
    </div>
  )
}
