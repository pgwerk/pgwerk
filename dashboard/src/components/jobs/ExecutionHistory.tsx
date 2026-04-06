import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { StatusBadge } from '@/components/StatusBadge'
import { Skeleton } from '@/components/ui/skeleton'
import { formatTimestamp, formatDuration, truncateId } from '@/lib/utils'

interface ExecutionHistoryProps {
  jobId: string
}

export function ExecutionHistory({ jobId }: ExecutionHistoryProps) {
  const { data, isLoading } = useQuery({
    queryKey: ['executions', jobId],
    queryFn: () => api.getJobExecutions(jobId),
  })

  if (isLoading) {
    return <div className="space-y-2">{Array.from({ length: 2 }).map((_, i) => <Skeleton key={i} className="h-14 w-full" />)}</div>
  }

  if (!data?.length) {
    return <p className="text-sm text-muted-foreground">No executions recorded.</p>
  }

  return (
    <div className="space-y-2">
      {data.map(ex => (
        <div key={ex.id} className="rounded-md border border-border bg-muted/30 p-3 text-xs">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <span className="font-mono text-muted-foreground">#{ex.attempt}</span>
              <StatusBadge status={ex.status} />
            </div>
            <span className="font-mono text-muted-foreground">
              {formatDuration(ex.started_at, ex.completed_at)}
            </span>
          </div>
          <div className="mt-1.5 flex gap-4 font-mono text-muted-foreground">
            <span>started {formatTimestamp(ex.started_at)}</span>
            {ex.completed_at && <span>ended {formatTimestamp(ex.completed_at)}</span>}
          </div>
          {ex.error && (
            <pre className="mt-2 overflow-x-auto rounded bg-destructive/10 p-2 font-mono text-[10px] text-destructive">
              {ex.error}
            </pre>
          )}
          {ex.worker_id && (
            <p className="mt-1 font-mono text-[10px] text-muted-foreground/60">
              worker {truncateId(ex.worker_id)}
            </p>
          )}
        </div>
      ))}
    </div>
  )
}
