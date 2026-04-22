import { Button } from '@/components/ui/button'
import { WorkersList } from '@/components/workers/WorkersList'
import { api } from '@/lib/api'
import type { WorkerSparklinePoint } from '@/components/workers/WorkerCard'
import { useQuery } from '@tanstack/react-query'
import { RefreshCw } from 'lucide-react'

const SPARKLINE_MINUTES = 5

export function WorkersPage() {
  const { data, isLoading, refetch, isFetching } = useQuery({
    queryKey: ['workers'],
    queryFn: api.listWorkers,
    refetchInterval: 10_000,
  })

  const { data: throughputHistory } = useQuery({
    queryKey: ['stats', 'throughput', 'workers-page', SPARKLINE_MINUTES],
    queryFn: () => api.getThroughputHistory(SPARKLINE_MINUTES),
    refetchInterval: 10_000,
  })

  const historyByWorker = new Map<string, WorkerSparklinePoint[]>()
  if (throughputHistory) {
    const times = [...new Set(throughputHistory.map(point => point.time))]
      .sort((a, b) => new Date(a).getTime() - new Date(b).getTime())

    const countsByWorker = new Map<string, Map<string, number>>()
    for (const point of throughputHistory) {
      if (!point.worker_id) continue
      const workerCounts = countsByWorker.get(point.worker_id) ?? new Map<string, number>()
      workerCounts.set(point.time, point.count)
      countsByWorker.set(point.worker_id, workerCounts)
    }

    for (const worker of data ?? []) {
      const counts = countsByWorker.get(worker.id) ?? new Map<string, number>()
      historyByWorker.set(
        worker.id,
        times.map(time => ({
          t: new Date(time).getTime(),
          v: counts.get(time) ?? 0,
        })),
      )
    }
  }

  const getHistory = (workerId: string) => historyByWorker.get(workerId) ?? []

  return (
    <div className="flex flex-col">
      <div className="flex h-12 items-center justify-between">
        <h1 className="text-sm font-semibold">Workers</h1>
        <Button variant="ghost" size="icon" className="h-8 w-8" onClick={() => refetch()} disabled={isFetching}>
          <RefreshCw className={`h-3.5 w-3.5 ${isFetching ? 'animate-spin' : ''}`} />
        </Button>
      </div>
      <div className="py-6">
        <WorkersList workers={data ?? []} isLoading={isLoading} getHistory={getHistory} />
      </div>
    </div>
  )
}
