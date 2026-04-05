import { useEffect } from 'react'
import { Button } from '@/components/ui/button'
import { WorkersList } from '@/components/workers/WorkersList'
import { useWorkerHistory } from '@/hooks/useWorkerHistory'
import { api } from '@/lib/api'
import { useQuery } from '@tanstack/react-query'
import { RefreshCw } from 'lucide-react'

export function WorkersPage() {
  const { record, getHistory } = useWorkerHistory()

  const { data, isLoading, refetch, isFetching } = useQuery({
    queryKey: ['workers'],
    queryFn: api.listWorkers,
    refetchInterval: 10_000,
  })

  useEffect(() => {
    if (data) record(data)
  }, [data, record])

  return (
    <div className="flex flex-col">
      <div className="flex h-12 items-center justify-between px-6">
        <h1 className="text-sm font-semibold">Workers</h1>
        <Button variant="ghost" size="icon" className="h-8 w-8" onClick={() => refetch()} disabled={isFetching}>
          <RefreshCw className={`h-3.5 w-3.5 ${isFetching ? 'animate-spin' : ''}`} />
        </Button>
      </div>
      <div className="p-6">
        <WorkersList workers={data ?? []} isLoading={isLoading} getHistory={getHistory} />
      </div>
    </div>
  )
}
