import { StatusBadge } from '@/components/StatusBadge'
import { TriggerCronDialog } from '@/components/cron/TriggerCronDialog'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from '@/components/ui/table'
import { api } from '@/lib/api'
import { relativeTime, shortFn } from '@/lib/utils'
import type { CronJobStats } from '@/types'
import { useQuery } from '@tanstack/react-query'
import { Play, RefreshCw } from 'lucide-react'
import { useState } from 'react'

export function CronPage() {
  const [triggerJob, setTriggerJob] = useState<CronJobStats | null>(null)

  const { data, isLoading, refetch, isFetching } = useQuery({
    queryKey: ['cron'],
    queryFn: api.listCronJobs,
    refetchInterval: 30_000,
  })

  return (
    <div className="flex flex-col">
      <div className="flex h-12 items-center justify-between  px-6">
        <h1 className="text-sm font-semibold">Cron Jobs</h1>
        <Button variant="ghost" size="icon" className="h-8 w-8" onClick={() => refetch()} disabled={isFetching}>
          <RefreshCw className={`h-3.5 w-3.5 ${isFetching ? 'animate-spin' : ''}`} />
        </Button>
      </div>

      <TriggerCronDialog job={triggerJob} onOpenChange={open => { if (!open) setTriggerJob(null) }} />

      <div className="p-6">
        {isLoading ? (
          <div className="space-y-2">
            {Array.from({ length: 4 }).map((_, i) => <Skeleton key={i} className="h-10 w-full" />)}
          </div>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Function</TableHead>
                <TableHead className="w-28">Queue</TableHead>
                <TableHead className="w-24">Last Status</TableHead>
                <TableHead className="w-32">Last Run</TableHead>
                <TableHead className="w-24 text-right">Runs</TableHead>
                <TableHead className="w-24 text-right">Failed</TableHead>
                <TableHead className="w-16" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {data?.map(job => (
                <TableRow key={job.name}>
                  <TableCell className="font-mono text-xs font-medium" title={job.name}>
                    {shortFn(job.name)}
                  </TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground" title={job.function}>
                    {shortFn(job.function)}
                  </TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground">
                    {job.queue}
                  </TableCell>
                  <TableCell>
                    {job.last_status ? <StatusBadge status={job.last_status} /> : '—'}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {relativeTime(job.last_enqueued_at)}
                  </TableCell>
                  <TableCell className="text-right font-mono text-xs tabular-nums text-muted-foreground">
                    {job.total_runs.toLocaleString()}
                  </TableCell>
                  <TableCell className={`text-right font-mono text-xs tabular-nums ${job.failed_runs > 0 ? 'text-red-400' : 'text-muted-foreground'}`}>
                    {job.failed_runs.toLocaleString()}
                  </TableCell>
                  <TableCell>
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-6 w-6 text-muted-foreground hover:text-foreground"
                      title="Run now"
                      onClick={() => setTriggerJob(job)}
                    >
                      <Play className="h-3 w-3" />
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
              {!data?.length && (
                <TableRow>
                  <TableCell colSpan={8} className="py-10 text-center text-sm text-muted-foreground">
                    No cron jobs have run yet
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        )}
      </div>
    </div>
  )
}
