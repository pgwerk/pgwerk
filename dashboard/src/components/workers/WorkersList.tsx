import { useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'
import { Skeleton } from '@/components/ui/skeleton'
import { WorkerCard } from './WorkerCard'
import type { WorkerResponse } from '@/types'
import type { HeartbeatPoint } from '@/hooks/useWorkerHistory'

function isAlive(heartbeat: string | undefined): boolean {
  if (!heartbeat) return false
  return Date.now() - new Date(heartbeat).getTime() < 90_000
}

interface WorkersListProps {
  workers: WorkerResponse[]
  isLoading?: boolean
  getHistory?: (workerId: string) => HeartbeatPoint[]
}

export function WorkersList({ workers, isLoading, getHistory }: WorkersListProps) {
  const [archivedOpen, setArchivedOpen] = useState(false)

  if (isLoading) {
    return (
      <div className="flex flex-col gap-3">
        {Array.from({ length: 4 }).map((_, i) => (
          <Skeleton key={i} className="h-20 w-full" />
        ))}
      </div>
    )
  }

  if (!workers.length) {
    return (
      <div className="flex h-40 items-center justify-center rounded-lg border border-dashed border-border">
        <p className="text-sm text-muted-foreground">No workers registered</p>
      </div>
    )
  }

  const online = workers.filter(w => isAlive(w.heartbeat_at))
  const offline = workers.filter(w => !isAlive(w.heartbeat_at))

  return (
    <div className="space-y-6">
      {online.length > 0 && (
        <div className="flex flex-col gap-3">
          {online.map(w => (
            <WorkerCard key={w.id} worker={w} history={getHistory?.(w.id)} />
          ))}
        </div>
      )}

      {online.length === 0 && offline.length > 0 && (
        <div className="flex h-24 items-center justify-center rounded-lg border border-dashed border-border">
          <p className="text-sm text-muted-foreground">No workers online</p>
        </div>
      )}

      {offline.length > 0 && (
        <div>
          <button
            className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground transition-colors mb-3"
            onClick={() => setArchivedOpen(v => !v)}
          >
            {archivedOpen ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
            <span>Offline workers ({offline.length})</span>
          </button>

          {archivedOpen && (
            <div className="flex flex-col gap-3">
              {offline.map(w => (
                <WorkerCard key={w.id} worker={w} history={getHistory?.(w.id)} />
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
