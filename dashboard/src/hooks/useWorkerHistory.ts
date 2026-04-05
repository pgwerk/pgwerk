import { useRef, useCallback } from 'react'
import type { WorkerResponse } from '@/types'

const MAX_POINTS = 20
const ALIVE_THRESHOLD_MS = 90_000

export interface HeartbeatPoint {
  t: number
  v: number
}

function isAlive(heartbeat: string | undefined): boolean {
  if (!heartbeat) return false
  return Date.now() - new Date(heartbeat).getTime() < ALIVE_THRESHOLD_MS
}

export function useWorkerHistory() {
  const historyRef = useRef<Map<string, HeartbeatPoint[]>>(new Map())

  const record = useCallback((workers: WorkerResponse[]) => {
    const now = Date.now()
    for (const w of workers) {
      const v = isAlive(w.heartbeat_at) ? 1 : 0
      const existing = historyRef.current.get(w.id) ?? []
      historyRef.current.set(w.id, [...existing, { t: now, v }].slice(-MAX_POINTS))
    }
  }, [])

  const getHistory = useCallback((workerId: string): HeartbeatPoint[] => {
    return historyRef.current.get(workerId) ?? []
  }, [])

  return { record, getHistory }
}
