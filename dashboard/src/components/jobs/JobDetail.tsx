import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { Sheet, SheetContent, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { Button } from '@/components/ui/button'
import { Separator } from '@/components/ui/separator'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { StatusBadge } from '@/components/StatusBadge'
import {
  Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle,
} from '@/components/ui/dialog'
import { ExecutionHistory } from './ExecutionHistory'
import { api } from '@/lib/api'
import { relativeTime, formatTimestamp, formatDuration, truncateId } from '@/lib/utils'
import type { JobResponse } from '@/types'
import { useState } from 'react'

interface FieldProps {
  label: string
  value: React.ReactNode
  mono?: boolean
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <p className="mb-2 text-[10px] uppercase tracking-wider text-muted-foreground">{children}</p>
  )
}

function Field({ label, value, mono }: FieldProps) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</span>
      <span className={`text-sm ${mono ? 'font-mono' : ''} text-foreground`}>{value ?? '—'}</span>
    </div>
  )
}

interface JobDetailProps {
  job: JobResponse | null
  open: boolean
  onClose: () => void
}

export function JobDetail({ job, open, onClose }: JobDetailProps) {
  const [confirmDelete, setConfirmDelete] = useState(false)
  const qc = useQueryClient()

  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ['jobs'] })
    qc.invalidateQueries({ queryKey: ['stats'] })
  }

  const cancel = useMutation({
    mutationFn: () => api.cancelJob(job!.id),
    onSuccess: () => { toast.success('Job cancelled'); invalidate(); onClose() },
    onError: (e: Error) => toast.error(e.message),
  })

  const abort = useMutation({
    mutationFn: () => api.abortJob(job!.id),
    onSuccess: () => { toast.success('Job aborted'); invalidate(); onClose() },
    onError: (e: Error) => toast.error(e.message),
  })

  const requeue = useMutation({
    mutationFn: () => api.requeueJob(job!.id),
    onSuccess: () => { toast.success('Job requeued'); invalidate(); onClose() },
    onError: (e: Error) => toast.error(e.message),
  })

  const remove = useMutation({
    mutationFn: () => api.deleteJob(job!.id),
    onSuccess: () => { toast.success('Job deleted'); invalidate(); onClose() },
    onError: (e: Error) => toast.error(e.message),
  })

  const { data: dependencies } = useQuery({
    queryKey: ['job-dependencies', job?.id],
    queryFn: () => api.getJobDependencies(job!.id),
    enabled: job?.status === 'waiting',
  })

  if (!job) return null

  const canCancel = job.status === 'queued' || job.status === 'waiting'
  const canAbort = job.status === 'active' || job.status === 'aborting'
  const canRequeue = job.status === 'failed' || job.status === 'aborted' || job.status === 'complete'

  return (
    <>
    <Sheet open={open} onOpenChange={v => !v && onClose()}>
      <SheetContent className="flex w-[480px] flex-col gap-0 p-0 sm:max-w-[480px]">
        <SheetHeader className="border-b border-border p-5">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <SheetTitle className="truncate font-mono text-sm font-medium">
                {job.function}
              </SheetTitle>
              <p className="mt-0.5 font-mono text-[11px] text-muted-foreground">
                {job.id}
              </p>
            </div>
            <StatusBadge status={job.status} className="shrink-0 mt-0.5" />
          </div>
        </SheetHeader>

        <div className="flex gap-2 border-b border-border px-5 py-3">
          {canCancel && (
            <Button size="sm" variant="outline" onClick={() => cancel.mutate()} disabled={cancel.isPending}>
              Cancel
            </Button>
          )}
          {canAbort && (
            <Button size="sm" variant="outline" onClick={() => abort.mutate()} disabled={abort.isPending}>
              Abort
            </Button>
          )}
          {canRequeue && (
            <Button size="sm" variant="outline" onClick={() => requeue.mutate()} disabled={requeue.isPending}>
              Requeue
            </Button>
          )}
          <Button
            size="sm"
            variant="ghost"
            className="ml-auto text-destructive hover:bg-destructive/10 hover:text-destructive"
            onClick={() => setConfirmDelete(true)}
            disabled={remove.isPending}
          >
            Delete
          </Button>
        </div>

        <Tabs defaultValue="details" className="flex flex-1 flex-col overflow-hidden">
          <TabsList className="h-9 w-full rounded-none border-b border-border bg-transparent px-5 justify-start gap-4">
            <TabsTrigger value="details" className="h-9 rounded-none border-b-2 border-transparent px-0 data-[state=active]:border-foreground data-[state=active]:bg-transparent">
              Details
            </TabsTrigger>
            <TabsTrigger value="executions" className="h-9 rounded-none border-b-2 border-transparent px-0 data-[state=active]:border-foreground data-[state=active]:bg-transparent">
              Executions
            </TabsTrigger>
            {job.error && (
              <TabsTrigger value="error" className="h-9 rounded-none border-b-2 border-transparent px-0 data-[state=active]:border-foreground data-[state=active]:bg-transparent">
                Error
              </TabsTrigger>
            )}
          </TabsList>

          <TabsContent value="details" className="flex-1 overflow-y-auto p-5">
            <SectionLabel>Identity</SectionLabel>
            <div className="grid grid-cols-2 gap-4">
              <Field label="Queue" value={job.queue} mono />
              <Field label="Priority" value={job.priority} mono />
              <Field label="Function" value={job.function} mono />
              <Field label="Status" value={job.status} mono />
              {job.key && <Field label="Key" value={job.key} mono />}
              {job.group_key && <Field label="Group" value={job.group_key} mono />}
              {job.schedule_name && <Field label="Schedule" value={job.schedule_name} mono />}
              {job.failure_mode && <Field label="On failure" value={job.failure_mode} mono />}
            </div>

            {dependencies && dependencies.length > 0 && (
              <>
                <Separator className="my-4" />
                <SectionLabel>Waiting on</SectionLabel>
                <div className="flex flex-col gap-1">
                  {dependencies.map(id => (
                    <span key={id} className="font-mono text-xs text-muted-foreground">{id}</span>
                  ))}
                </div>
              </>
            )}

            <Separator className="my-4" />
            <SectionLabel>Lifecycle</SectionLabel>
            <div className="grid grid-cols-2 gap-4">
              <Field label="Attempts" value={`${job.attempts} / ${job.max_attempts}`} mono />
              <Field label="Timeout" value={job.timeout_secs ? `${job.timeout_secs}s` : null} mono />
              <Field label="Heartbeat" value={job.heartbeat_secs ? `${job.heartbeat_secs}s` : null} mono />
              <Field
                label="Worker"
                value={job.worker_id ? truncateId(job.worker_id) : null}
                mono
              />
              <Field label="Enqueued" value={relativeTime(job.enqueued_at)} />
              <Field label="Scheduled" value={formatTimestamp(job.scheduled_at)} mono />
              <Field label="Started" value={formatTimestamp(job.started_at)} mono />
              <Field label="Completed" value={formatTimestamp(job.completed_at)} mono />
              <Field label="Last heartbeat" value={formatTimestamp(job.touched_at)} mono />
              <Field label="Expires" value={formatTimestamp(job.expires_at)} mono />
              <Field
                label="Duration"
                value={formatDuration(job.started_at, job.completed_at)}
                mono
              />
            </div>

            {(job.retry_intervals?.length || job.repeat_remaining != null || job.repeat_interval_secs != null || job.repeat_intervals?.length) && (
              <>
                <Separator className="my-4" />
                <SectionLabel>Retry / repeat</SectionLabel>
                <div className="grid grid-cols-2 gap-4">
                  {job.retry_intervals?.length ? (
                    <Field label="Retry intervals" value={job.retry_intervals.map(s => `${s}s`).join(', ')} mono />
                  ) : null}
                  {job.repeat_remaining != null && <Field label="Repeats left" value={job.repeat_remaining} mono />}
                  {job.repeat_interval_secs != null && <Field label="Repeat every" value={`${job.repeat_interval_secs}s`} mono />}
                  {job.repeat_intervals?.length ? (
                    <Field label="Repeat intervals" value={job.repeat_intervals.map(s => `${s}s`).join(', ')} mono />
                  ) : null}
                </div>
              </>
            )}

            {(job.ttl != null || job.result_ttl != null || job.failure_ttl != null) && (
              <>
                <Separator className="my-4" />
                <SectionLabel>TTL</SectionLabel>
                <div className="grid grid-cols-2 gap-4">
                  {job.ttl != null && <Field label="Queue TTL" value={`${job.ttl}s`} mono />}
                  {job.result_ttl != null && <Field label="Result TTL" value={`${job.result_ttl}s`} mono />}
                  {job.failure_ttl != null && <Field label="Failure TTL" value={`${job.failure_ttl}s`} mono />}
                </div>
              </>
            )}

            {(job.on_success || job.on_failure || job.on_stopped) && (
              <>
                <Separator className="my-4" />
                <SectionLabel>Callbacks</SectionLabel>
                <div className="grid grid-cols-2 gap-4">
                  {job.on_success && <Field label="On success" value={job.on_success} mono />}
                  {job.on_success_timeout != null && <Field label="…timeout" value={`${job.on_success_timeout}s`} mono />}
                  {job.on_failure && <Field label="On failure" value={job.on_failure} mono />}
                  {job.on_failure_timeout != null && <Field label="…timeout" value={`${job.on_failure_timeout}s`} mono />}
                  {job.on_stopped && <Field label="On stopped" value={job.on_stopped} mono />}
                  {job.on_stopped_timeout != null && <Field label="…timeout" value={`${job.on_stopped_timeout}s`} mono />}
                </div>
              </>
            )}

            {((job.args && job.args.length > 0) || (job.kwargs && Object.keys(job.kwargs).length > 0)) && (
              <>
                <Separator className="my-4" />
                {job.args && job.args.length > 0 && (
                  <>
                    <SectionLabel>Args</SectionLabel>
                    <pre className="overflow-x-auto rounded-md bg-muted p-3 font-mono text-xs text-foreground">
                      {JSON.stringify(job.args, null, 2)}
                    </pre>
                  </>
                )}
                {job.kwargs && Object.keys(job.kwargs).length > 0 && (
                  <>
                    <div className="mt-3" />
                    <SectionLabel>Kwargs</SectionLabel>
                    <pre className="overflow-x-auto rounded-md bg-muted p-3 font-mono text-xs text-foreground">
                      {JSON.stringify(job.kwargs, null, 2)}
                    </pre>
                  </>
                )}
              </>
            )}

            {job.result != null && (
              <>
                <Separator className="my-4" />
                <SectionLabel>Result</SectionLabel>
                <pre className="overflow-x-auto rounded-md bg-muted p-3 font-mono text-xs text-foreground">
                  {JSON.stringify(job.result, null, 2)}
                </pre>
              </>
            )}

            {job.meta && Object.keys(job.meta).length > 0 && (
              <>
                <Separator className="my-4" />
                <SectionLabel>Meta</SectionLabel>
                <pre className="overflow-x-auto rounded-md bg-muted p-3 font-mono text-xs text-foreground">
                  {JSON.stringify(job.meta, null, 2)}
                </pre>
              </>
            )}
          </TabsContent>

          <TabsContent value="executions" className="flex-1 overflow-y-auto p-5">
            <ExecutionHistory jobId={job.id} />
          </TabsContent>

          {job.error && (
            <TabsContent value="error" className="flex-1 overflow-y-auto p-5">
              <pre className="overflow-x-auto rounded-md bg-destructive/10 p-3 font-mono text-xs text-destructive">
                {job.error}
              </pre>
            </TabsContent>
          )}
        </Tabs>
      </SheetContent>
    </Sheet>

    <Dialog open={confirmDelete} onOpenChange={setConfirmDelete}>
      <DialogContent className="sm:max-w-sm">
        <DialogHeader>
          <DialogTitle className="text-sm font-semibold">Delete Job</DialogTitle>
          <DialogDescription className="text-xs">
            Permanently delete job <span className="font-mono font-medium">{truncateId(job.id)}</span>. This cannot be undone.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button variant="outline" size="sm" onClick={() => setConfirmDelete(false)} disabled={remove.isPending}>
            Cancel
          </Button>
          <Button
            variant="destructive"
            size="sm"
            onClick={() => { setConfirmDelete(false); remove.mutate() }}
            disabled={remove.isPending}
          >
            Delete
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
    </>
  )
}
