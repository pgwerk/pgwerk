import { StatusBadge } from '@/components/StatusBadge'
import { JobDetail } from '@/components/jobs/JobDetail'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Separator } from '@/components/ui/separator'
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from '@/components/ui/table'
import { api } from '@/lib/api'
import { cn, formatDuration, formatTimestamp, relativeTime, shortFn, truncateId } from '@/lib/utils'
import type { JobResponse } from '@/types'
import { useQuery } from '@tanstack/react-query'
import { formatDistanceToNow } from 'date-fns'
import { ArrowLeft, Cpu, RefreshCw } from 'lucide-react'
import { useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { Area, AreaChart, ResponsiveContainer, Tooltip } from 'recharts'

const THROUGHPUT_MINUTES = 30
const ALIVE_THRESHOLD_MS = 90_000

interface HeartbeatPoint { t: number; v: number }

function isAlive(heartbeat: string | undefined): boolean {
  if (!heartbeat) return false
  return Date.now() - new Date(heartbeat).getTime() < ALIVE_THRESHOLD_MS
}

interface FieldProps { label: string; value: React.ReactNode; mono?: boolean }

function Field({ label, value, mono }: FieldProps) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</span>
      <span className={cn('text-sm', mono && 'font-mono')}>{value ?? '—'}</span>
    </div>
  )
}

export function WorkerDetailPage() {
  const { id } = useParams<{ id: string }>()
  const [selectedJob, setSelectedJob] = useState<JobResponse | null>(null)

  const { data: worker, isLoading, refetch, isFetching } = useQuery({
    queryKey: ['worker', id],
    queryFn: () => api.getWorker(id!),
    refetchInterval: 10_000,
    enabled: !!id,
  })

  const { data: jobs, isLoading: jobsLoading } = useQuery({
    queryKey: ['worker-jobs', id],
    queryFn: () => api.listWorkerJobs(id!, { limit: 50 }),
    refetchInterval: 10_000,
    enabled: !!id,
  })

  const { data: throughputHistory } = useQuery({
    queryKey: ['stats', 'throughput', 'worker-detail', id, THROUGHPUT_MINUTES],
    queryFn: () => api.getThroughputHistory(THROUGHPUT_MINUTES),
    refetchInterval: 10_000,
    enabled: !!id,
  })

  if (isLoading) {
    return (
      <div className="flex flex-col gap-6 py-6">
        <div className="h-8 w-48 animate-pulse rounded bg-muted" />
        <div className="h-32 w-full animate-pulse rounded bg-muted" />
      </div>
    )
  }

  if (!worker) {
    return (
      <div className="flex flex-col gap-4 py-6">
        <Link to="/workers" className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground">
          <ArrowLeft className="h-3.5 w-3.5" /> Workers
        </Link>
        <p className="text-sm text-muted-foreground">Worker not found.</p>
      </div>
    )
  }

  const alive = isAlive(worker.heartbeat_at)
  const uptime = worker.started_at
    ? formatDistanceToNow(new Date(worker.started_at), { addSuffix: false })
    : null
  const history: HeartbeatPoint[] = (() => {
    const points = throughputHistory ?? []
    const times = [...new Set(points.map(point => point.time))]
      .sort((a, b) => new Date(a).getTime() - new Date(b).getTime())

    return times.map(time => {
      const count = points
        .filter(point => point.worker_id === worker.id && point.time === time)
        .reduce((sum, point) => sum + point.count, 0)

      return {
        t: new Date(time).getTime(),
        v: count,
      }
    })
  })()

  return (
    <div className="flex flex-col gap-6 py-6">
      <div className="flex items-center justify-between">
        <Link to="/workers" className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground transition-colors">
          <ArrowLeft className="h-3.5 w-3.5" /> Workers
        </Link>
        <Button variant="ghost" size="icon" className="h-8 w-8" onClick={() => refetch()} disabled={isFetching}>
          <RefreshCw className={cn('h-3.5 w-3.5', isFetching && 'animate-spin')} />
        </Button>
      </div>

      <Card>
        <CardContent className="p-6">
          <div className="flex items-start justify-between gap-4">
            <div className="flex items-center gap-3 min-w-0">
              <div className={cn(
                'flex h-8 w-8 shrink-0 items-center justify-center rounded-md border',
                alive
                  ? 'border-green-500/30 bg-green-500/10 text-green-400'
                  : 'border-border bg-muted/40 text-muted-foreground/50',
              )}>
                <Cpu className="h-3.5 w-3.5" />
              </div>
              <div className="min-w-0">
                <h2 className="truncate font-mono text-base font-semibold text-foreground">
                  {worker.name}
                </h2>
                <p className="mt-0.5 font-mono text-xs text-muted-foreground">{worker.id}</p>
              </div>
            </div>
            <div className="flex shrink-0 items-center gap-2">
              <span className={cn(
                'h-2.5 w-2.5 rounded-full',
                alive ? 'bg-green-500 shadow-[0_0_8px_2px_rgba(34,197,94,0.4)]' : 'bg-zinc-600',
              )} />
              <span className={cn('text-sm font-medium', alive ? 'text-green-400' : 'text-zinc-500')}>
                {alive ? 'online' : 'offline'}
              </span>
            </div>
          </div>

          {history.length > 1 && (
            <div className="mt-5 h-14">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={history} margin={{ top: 4, right: 0, bottom: 0, left: 0 }}>
                  <Tooltip
                    content={() => null}
                    cursor={{ stroke: 'hsl(var(--border))', strokeWidth: 1 }}
                  />
                  <Area
                    type="monotone"
                    dataKey="v"
                    stroke={alive ? '#22c55e' : '#52525b'}
                    fill={alive ? 'rgba(34,197,94,0.15)' : 'rgba(82,82,91,0.15)'}
                    strokeWidth={2}
                    dot={false}
                    isAnimationActive={false}
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          )}

          <Separator className="my-5" />

          <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-4">
            <Field label="Queue" value={worker.queue} mono />
            <Field label="Status" value={worker.status} mono />
            {uptime && <Field label="Uptime" value={uptime} />}
            <Field label="Started" value={formatTimestamp(worker.started_at)} mono />
            <Field label="Last heartbeat" value={relativeTime(worker.heartbeat_at)} />
            {worker.expires_at && <Field label="Expires" value={formatTimestamp(worker.expires_at)} mono />}
            <Field label="ID" value={truncateId(worker.id)} mono />
          </div>

          {worker.metadata && Object.keys(worker.metadata).length > 0 && (
            <>
              <Separator className="my-5" />
              <p className="mb-2 text-[10px] uppercase tracking-wider text-muted-foreground">Metadata</p>
              <pre className="overflow-x-auto rounded-md bg-muted p-3 font-mono text-xs text-foreground">
                {JSON.stringify(worker.metadata, null, 2)}
              </pre>
            </>
          )}
        </CardContent>
      </Card>

      <div>
        <h3 className="mb-3 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          Jobs
        </h3>

        {jobsLoading ? (
          <div className="space-y-2">
            {Array.from({ length: 5 }).map((_, i) => (
              <div key={i} className="h-10 animate-pulse rounded bg-muted" />
            ))}
          </div>
        ) : !jobs?.length ? (
          <div className="flex h-24 items-center justify-center rounded-lg border border-dashed border-border">
            <p className="text-sm text-muted-foreground">No jobs found for this worker</p>
          </div>
        ) : (
          <Card>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Function</TableHead>
                  <TableHead>Queue</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Started</TableHead>
                  <TableHead>Duration</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {jobs.map(job => (
                  <TableRow
                    key={job.id}
                    className="cursor-pointer"
                    onClick={() => setSelectedJob(job)}
                  >
                    <TableCell className="font-mono text-xs">{shortFn(job.function)}</TableCell>
                    <TableCell className="font-mono text-xs text-muted-foreground">{job.queue}</TableCell>
                    <TableCell><StatusBadge status={job.status} /></TableCell>
                    <TableCell className="text-xs text-muted-foreground">{relativeTime(job.started_at)}</TableCell>
                    <TableCell className="font-mono text-xs text-muted-foreground">
                      {formatDuration(job.started_at, job.completed_at)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Card>
        )}
      </div>

      <JobDetail job={selectedJob} open={!!selectedJob} onClose={() => setSelectedJob(null)} />
    </div>
  )
}
