import type {
  StatsResponse,
  JobResponse,
  ExecutionResponse,
  WorkerResponse,
  SweepResponse,
  ScheduleResponse,
  ScheduleStats,
  EnqueueRequest,
  WorkerThroughputPoint,
  QueueDepthPoint,
  ServerInfo,
  PurgeRequest,
  PurgeResponse,
  RequeueFailedRequest,
  CancelQueuedRequest,
} from '@/types'

const BASE = import.meta.env.VITE_API_URL ?? ''

async function req<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  })
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText)
    throw new Error(`${res.status}: ${text}`)
  }
  if (res.status === 204) return undefined as T
  return res.json() as Promise<T>
}

export const api = {
  getStats: () => req<StatsResponse>('/api/stats'),

  listJobs: (params: {
    queue?: string
    status?: string
    worker_id?: string
    search?: string
    schedule_name?: string
    retried?: boolean
    limit?: number
    offset?: number
  }) => {
    const q = new URLSearchParams()
    if (params.queue) q.set('queue', params.queue)
    if (params.status) q.set('status', params.status)
    if (params.worker_id) q.set('worker_id', params.worker_id)
    if (params.search) q.set('search', params.search)
    if (params.schedule_name) q.set('schedule_name', params.schedule_name)
    if (params.retried) q.set('retried', 'true')
    if (params.limit != null) q.set('limit', String(params.limit))
    if (params.offset != null) q.set('offset', String(params.offset))
    return req<JobResponse[]>(`/api/jobs?${q}`)
  },

  getJob: (id: string) => req<JobResponse>(`/api/jobs/${id}`),

  getJobExecutions: (id: string) =>
    req<ExecutionResponse[]>(`/api/jobs/${id}/executions`),

  getJobDependencies: (id: string) =>
    req<string[]>(`/api/jobs/${id}/dependencies`),

  cancelJob: (id: string) =>
    req<{ cancelled: boolean; job_id: string }>(`/api/jobs/${id}/cancel`, {
      method: 'POST',
    }),

  abortJob: (id: string) =>
    req<{ aborted: boolean; job_id: string }>(`/api/jobs/${id}/abort`, {
      method: 'POST',
    }),

  requeueJob: (id: string) =>
    req<{ requeued: boolean; job_id: string }>(`/api/jobs/${id}/requeue`, {
      method: 'POST',
    }),

  deleteJob: (id: string) =>
    req<void>(`/api/jobs/${id}`, { method: 'DELETE' }),

  getWorker: (id: string) => req<WorkerResponse>(`/api/workers/${id}`),

  listWorkerJobs: (id: string, params: { limit?: number; offset?: number } = {}) => {
    const q = new URLSearchParams()
    if (params.limit != null) q.set('limit', String(params.limit))
    if (params.offset != null) q.set('offset', String(params.offset))
    return req<JobResponse[]>(`/api/workers/${id}/jobs?${q}`)
  },

  listWorkers: () => req<WorkerResponse[]>('/api/workers'),

  getThroughputHistory: (minutes = 1440) =>
    req<WorkerThroughputPoint[]>(`/api/stats/throughput?minutes=${minutes}`),

  getQueueDepthHistory: (minutes = 1440) =>
    req<QueueDepthPoint[]>(`/api/stats/queue-depth?minutes=${minutes}`),

  createSchedule: (data: {
    name: string
    function: string
    queue?: string
    cron?: string
    interval_secs?: number
    kwargs?: Record<string, unknown>
  }) => req<ScheduleResponse>('/api/schedules', { method: 'POST', body: JSON.stringify(data) }),

  listSchedules: () => req<ScheduleResponse[]>('/api/schedules'),

  listScheduleStats: () => req<ScheduleStats[]>('/api/schedules/stats'),

  getSchedule: (name: string) => req<ScheduleResponse>(`/api/schedules/${encodeURIComponent(name)}`),

  triggerSchedule: (name: string) =>
    req<ScheduleResponse>(`/api/schedules/${encodeURIComponent(name)}/trigger`, { method: 'POST' }),

  pauseSchedule: (name: string) =>
    req<ScheduleResponse>(`/api/schedules/${encodeURIComponent(name)}/pause`, { method: 'POST' }),

  resumeSchedule: (name: string) =>
    req<ScheduleResponse>(`/api/schedules/${encodeURIComponent(name)}/resume`, { method: 'POST' }),

  deleteSchedule: (name: string) =>
    req<void>(`/api/schedules/${encodeURIComponent(name)}`, { method: 'DELETE' }),

  enqueueJob: (data: EnqueueRequest) =>
    req<JobResponse>('/api/jobs', { method: 'POST', body: JSON.stringify(data) }),

  sweep: () => req<SweepResponse>('/api/sweep', { method: 'POST' }),

  getServerInfo: () => req<ServerInfo>('/api/server'),

  purgeJobs: (data: PurgeRequest) =>
    req<PurgeResponse>('/api/purge', { method: 'POST', body: JSON.stringify(data) }),

  truncate: () => req<{ truncated: true }>('/api/truncate', { method: 'POST' }),

  requeueFailed: (data: RequeueFailedRequest = {}) =>
    req<{ requeued: number }>('/api/requeue-failed', { method: 'POST', body: JSON.stringify(data) }),

  cancelQueued: (data: CancelQueuedRequest = {}) =>
    req<{ cancelled: number }>('/api/cancel-queued', { method: 'POST', body: JSON.stringify(data) }),

  vacuum: () => req<{ vacuumed: true }>('/api/vacuum', { method: 'POST' }),

  rescheduleStuck: () => req<{ rescheduled: number }>('/api/reschedule-stuck', { method: 'POST' }),
}
