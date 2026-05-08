export type JobStatus =
  | 'queued'
  | 'waiting'
  | 'active'
  | 'aborting'
  | 'complete'
  | 'failed'
  | 'aborted'

export interface JobResponse {
  id: string
  function: string
  queue: string
  status: JobStatus
  priority: number
  attempts: number
  max_attempts: number
  scheduled_at: string
  enqueued_at: string
  key?: string
  group_key?: string
  error?: string
  timeout_secs?: number
  heartbeat_secs?: number
  started_at?: string
  completed_at?: string
  worker_id?: string
  meta?: Record<string, unknown>
}

export interface ExecutionResponse {
  id: string
  job_id: string
  attempt: number
  status: string
  worker_id?: string
  error?: string
  result?: unknown
  started_at?: string
  completed_at?: string
}

export interface WorkerResponse {
  id: string
  name: string
  queue: string
  status: string
  role?: string
  metadata?: Record<string, unknown>
  heartbeat_at?: string
  started_at?: string
  expires_at?: string
}

export interface QueueStats {
  queue: string
  scheduled: number
  queued: number
  active: number
  waiting: number
  failed: number
  complete: number
  aborted: number
}

export interface StatsResponse {
  queues: QueueStats[]
  total_jobs: number
  workers_online: number
}

export interface SweepResponse {
  swept: number
  job_ids: string[]
}

export interface WorkerThroughputPoint {
  time: string
  worker_id: string | null
  worker_name: string | null
  count: number
}

export interface QueueDepthPoint {
  time: string
  queued: number
  active: number
}

export interface EnqueueRequest {
  function: string
  queue?: string
  priority?: number
  args?: unknown[]
  kwargs?: Record<string, unknown>
  key?: string
  delay?: number
  scheduled_at?: string
  max_attempts?: number
  timeout_secs?: number
  meta?: Record<string, unknown>
  schedule_name?: string
}

export interface ScheduleResponse {
  name: string
  function: string
  queue: string
  args: unknown[]
  kwargs: Record<string, unknown>
  interval_secs: number | null
  cron: string | null
  timeout_secs: number | null
  result_ttl: number | null
  failure_ttl: number | null
  meta: Record<string, unknown> | null
  paused: boolean
  next_run_at: string | null
  last_run_at: string | null
  last_registered_at: string | null
  created_at: string | null
}

export interface ScheduleStats {
  name: string
  function: string
  queue: string
  total_runs: number
  failed_runs: number
  last_status?: string
  last_enqueued_at?: string
  last_completed_at?: string
}

export interface TableInfo {
  name: string
  size_bytes: number
  row_count: number
}

export interface ServerInfo {
  pg_version: string
  db_size_bytes: number
  pgwerk_size_bytes: number
  schema: string | null
  tables: TableInfo[]
}

export interface PurgeRequest {
  statuses: string[]
  older_than_days: number
}

export interface PurgeResponse {
  purged: number
}

export interface RequeueFailedRequest {
  queue?: string
  function_name?: string
}

export interface CancelQueuedRequest {
  queue?: string
}
