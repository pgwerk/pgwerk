import { useState } from 'react'
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from '@/components/ui/table'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { StatusBadge } from '@/components/StatusBadge'
import { JobDetail } from './JobDetail'
import { cn, relativeTime, formatDuration, truncateId, shortFn } from '@/lib/utils'
import type { JobResponse } from '@/types'

interface JobsTableProps {
  jobs: JobResponse[]
  isLoading?: boolean
  page: number
  onPageChange: (p: number) => void
  hasMore: boolean
}

export function JobsTable({ jobs, isLoading, page, onPageChange, hasMore }: JobsTableProps) {
  const [selected, setSelected] = useState<JobResponse | null>(null)

  if (isLoading) {
    return (
      <div className="space-y-2 p-4">
        {Array.from({ length: 8 }).map((_, i) => (
          <Skeleton key={i} className="h-10 w-full" />
        ))}
      </div>
    )
  }

  return (
    <>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-24 font-mono">ID</TableHead>
            <TableHead>Function</TableHead>
            <TableHead className="w-28">Queue</TableHead>
            <TableHead className="w-28">Status</TableHead>
            <TableHead className="w-20 text-right">Priority</TableHead>
            <TableHead className="w-20 text-right">Attempts</TableHead>
            <TableHead className="w-32">Enqueued</TableHead>
            <TableHead className="w-24 text-right">Duration</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {jobs.map(job => (
            <TableRow
              key={job.id}
              className={cn(
                'cursor-pointer',
                job.status === 'failed'              && 'border-l-2 border-l-red-500/60',
                job.status === 'active'              && 'border-l-2 border-l-green-500/60',
                (job.status as string) === 'running' && 'border-l-2 border-l-green-500/60',
                job.status === 'aborted'             && 'border-l-2 border-l-violet-500/40',
                job.status === 'aborting'            && 'border-l-2 border-l-orange-500/60',
              )}
              onClick={() => setSelected(job)}
            >
              <TableCell className="font-mono text-xs text-muted-foreground">
                {truncateId(job.id)}
              </TableCell>
              <TableCell>
                <span className="font-mono text-xs" title={job.function}>
                  {shortFn(job.function)}
                </span>
              </TableCell>
              <TableCell className="font-mono text-xs text-muted-foreground">
                {job.queue}
              </TableCell>
              <TableCell>
                <StatusBadge status={job.status} />
              </TableCell>
              <TableCell className="text-right font-mono text-xs text-muted-foreground">
                {job.priority}
              </TableCell>
              <TableCell className="text-right font-mono text-xs text-muted-foreground">
                {job.attempts}/{job.max_attempts}
              </TableCell>
              <TableCell className="text-xs text-muted-foreground">
                {relativeTime(job.enqueued_at)}
              </TableCell>
              <TableCell className="text-right font-mono text-xs text-muted-foreground">
                {formatDuration(job.started_at, job.completed_at)}
              </TableCell>
            </TableRow>
          ))}
          {jobs.length === 0 && (
            <TableRow>
              <TableCell colSpan={8} className="py-10 text-center text-sm text-muted-foreground">
                No jobs found
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>

      <div className="flex items-center justify-between border-t border-border px-4 py-3">
        <span className="text-xs text-muted-foreground">
          Page {page + 1} · {jobs.length} rows
        </span>
        <div className="flex gap-2">
          <Button
            size="sm"
            variant="outline"
            disabled={page === 0}
            onClick={() => onPageChange(page - 1)}
          >
            Previous
          </Button>
          <Button
            size="sm"
            variant="outline"
            disabled={!hasMore}
            onClick={() => onPageChange(page + 1)}
          >
            Next
          </Button>
        </div>
      </div>

      <JobDetail job={selected} open={!!selected} onClose={() => setSelected(null)} />
    </>
  )
}
