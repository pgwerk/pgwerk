import { JobStatusPieChart, JobStatusPieChartSkeleton } from '@/components/overview/JobStatusPieChart'
import { QueueDepthChart, QueueDepthChartSkeleton } from '@/components/overview/QueueDepthChart'
import { QueueTable, QueueTableSkeleton } from '@/components/overview/QueueTable'
import { StatCards, StatCardsSkeleton } from '@/components/overview/StatCards'
import { ThroughputChart, ThroughputChartSkeleton } from '@/components/overview/ThroughputChart'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { api } from '@/lib/api'
import type { QueueStats, WorkerResponse } from '@/types'
import { useQuery } from '@tanstack/react-query'
import { CheckCircle2, Clock, Cpu, RefreshCw, XCircle } from 'lucide-react'
import { useState } from 'react'

import { formatDistanceToNow } from 'date-fns'

function WorkerSummary({ workers }: { workers: WorkerResponse[] }) {
  const activeWorkers = workers.filter(w =>
    w.heartbeat_at ? Date.now() - new Date(w.heartbeat_at).getTime() < 30_000 : false
  )

  if (activeWorkers.length === 0) {
    return (
      <div className="flex h-24 items-center justify-center text-sm text-muted-foreground">
        No active workers
      </div>
    )
  }

  return (
    <div className="grid grid-cols-1 gap-2">
      {activeWorkers.map(w => (
        <div
          key={w.id}
          className="flex items-center gap-3 rounded-md border border-border bg-muted/30 px-3 py-2"
        >
          <div className="h-2 w-2 shrink-0 rounded-full bg-green-400" />
          <div className="min-w-0 flex-1">
            <p className="truncate font-mono text-xs font-medium">{w.name}</p>
            <p className="truncate text-xs text-muted-foreground">
              {w.queue}
              {w.heartbeat_at && (
                <span className="ml-2 opacity-60">
                  · {formatDistanceToNow(new Date(w.heartbeat_at), { addSuffix: true })}
                </span>
              )}
            </p>
          </div>
        </div>
      ))}
    </div>
  )
}

function WorkerSummarySkeleton() {
  return (
    <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
      {Array.from({ length: 4 }).map((_, i) => (
        <Skeleton key={i} className="h-12 w-full" />
      ))}
    </div>
  )
}

function SummaryBar({ queues }: { queues: QueueStats[] }) {
  const complete = queues.reduce((s, q) => s + q.complete, 0)
  const failed   = queues.reduce((s, q) => s + q.failed,   0)
  const aborted  = queues.reduce((s, q) => s + q.aborted,  0)
  const active   = queues.reduce((s, q) => s + q.active,   0)
  const queued   = queues.reduce((s, q) => s + q.queued,   0)
  const waiting  = queues.reduce((s, q) => s + q.waiting,  0)

  const items = [
    { label: 'Completed', value: complete, icon: <CheckCircle2 className="h-3.5 w-3.5 text-blue-400" />,           color: 'text-blue-400' },
    { label: 'Active',    value: active,   icon: <Cpu className="h-3.5 w-3.5 text-green-400" />,                   color: 'text-green-400' },
    { label: 'Queued',    value: queued,   icon: <Clock className="h-3.5 w-3.5 text-yellow-400" />,                color: 'text-yellow-400' },
    { label: 'Waiting',   value: waiting,  icon: <Clock className="h-3.5 w-3.5 text-muted-foreground" />,          color: 'text-muted-foreground' },
    { label: 'Failed',    value: failed,   icon: <XCircle className="h-3.5 w-3.5 text-red-400" />,                 color: 'text-red-400' },
    { label: 'Aborted',   value: aborted,  icon: <XCircle className="h-3.5 w-3.5 text-muted-foreground/60" />,     color: 'text-muted-foreground' },
  ]

  return (
    <div className="grid grid-cols-3 gap-3 sm:grid-cols-6">
      {items.map(item => (
        <div key={item.label} className="flex flex-col items-center gap-1 rounded-md bg-muted/40 px-2 py-3">
          {item.icon}
          <span className={`font-mono text-lg font-semibold tabular-nums ${item.color}`}>
            {item.value.toLocaleString()}
          </span>
          <span className="text-xs text-muted-foreground">{item.label}</span>
        </div>
      ))}
    </div>
  )
}

const RANGE_OPTIONS = [
  { label: '15m', minutes: 15   },
  { label: '30m', minutes: 30   },
  { label: '1h',  minutes: 60   },
  { label: '6h',  minutes: 360  },
  { label: '24h', minutes: 1440 },
  { label: '7d',  minutes: 10080 },
]

export function OverviewPage() {
  const [chartMinutes, setChartMinutes] = useState(15)

  const { data: stats, isLoading: statsLoading, refetch, isFetching } = useQuery({
    queryKey: ['stats'],
    queryFn: api.getStats,
    refetchInterval: 5_000,
  })

  const { data: workers, isLoading: workersLoading } = useQuery({
    queryKey: ['workers'],
    queryFn: api.listWorkers,
    refetchInterval: 10_000,
  })

  const { data: throughputHistory, refetch: refetchThroughput } = useQuery({
    queryKey: ['stats', 'throughput', chartMinutes],
    queryFn: () => api.getThroughputHistory(chartMinutes),
    refetchInterval: 5_000,
  })

  const { data: queueDepthHistory, refetch: refetchQueueDepth } = useQuery({
    queryKey: ['stats', 'queue-depth', chartMinutes],
    queryFn: () => api.getQueueDepthHistory(chartMinutes),
    refetchInterval: 5_000,
  })

  return (
    <div className="flex flex-col">
      <div className="flex h-12 items-center justify-between px-6">
        <div className="flex items-baseline gap-2">
          <h1 className="text-sm font-semibold">Overview</h1>
          <span className="text-xs text-muted-foreground">auto-refreshes every 5s</span>
        </div>
        <Button variant="ghost" size="icon" className="h-8 w-8" onClick={() => { refetch(); refetchThroughput(); refetchQueueDepth() }} disabled={isFetching}>
          <RefreshCw className={`h-3.5 w-3.5 ${isFetching ? 'animate-spin' : ''}`} />
        </Button>
      </div>

      <div className="flex-1 space-y-6 p-6">
        {/* Stat cards */}
        {statsLoading || !stats ? <StatCardsSkeleton /> : <StatCards data={stats} />}

        {/* Job status summary strip */}
        {statsLoading || !stats
          ? <Skeleton className="h-20 w-full" />
          : <SummaryBar queues={stats.queues} />}

        {/* Area charts row */}
        <div className="space-y-3">
          <div className="flex items-center justify-end">
            <div className="flex rounded-md border border-border bg-muted/40 p-0.5 gap-0.5">
              {RANGE_OPTIONS.map(opt => (
                <button
                  key={opt.minutes}
                  onClick={() => setChartMinutes(opt.minutes)}
                  className={`rounded px-3 py-1 text-xs font-medium transition-colors ${
                    chartMinutes === opt.minutes
                      ? 'bg-background text-foreground shadow-sm'
                      : 'text-muted-foreground hover:text-foreground'
                  }`}
                >
                  {opt.label}
                </button>
              ))}
            </div>
          </div>
          <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-semibold">Queue Depth</CardTitle>
              </CardHeader>
              <CardContent className="pt-0">
                {!queueDepthHistory
                  ? <QueueDepthChartSkeleton />
                  : <QueueDepthChart points={queueDepthHistory} minutes={chartMinutes} />}
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-semibold">Throughput</CardTitle>
              </CardHeader>
              <CardContent className="pt-0">
                {!throughputHistory
                  ? <ThroughputChartSkeleton />
                  : <ThroughputChart points={throughputHistory} minutes={chartMinutes} />}
              </CardContent>
            </Card>
          </div>
        </div>

        {/* Status distribution row */}
        <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-semibold">Job Status Distribution</CardTitle>
            </CardHeader>
            <CardContent className="pt-0">
              {statsLoading || !stats
                ? <JobStatusPieChartSkeleton />
                : <JobStatusPieChart queues={stats.queues} />}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-semibold">Workers</CardTitle>
            </CardHeader>
            <CardContent className="pt-0">
              {workersLoading || !workers
                ? <WorkerSummarySkeleton />
                : <WorkerSummary workers={workers} />}
            </CardContent>
          </Card>
        </div>

        {/* Queues row */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-semibold">Queues</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            {statsLoading || !stats
              ? <QueueTableSkeleton />
              : <QueueTable queues={stats.queues} />}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
