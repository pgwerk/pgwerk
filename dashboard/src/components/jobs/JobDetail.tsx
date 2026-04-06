import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { Sheet, SheetContent, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { Button } from '@/components/ui/button'
import { Separator } from '@/components/ui/separator'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { StatusBadge } from '@/components/StatusBadge'
import { ExecutionHistory } from './ExecutionHistory'
import { api } from '@/lib/api'
import { relativeTime, formatTimestamp, formatDuration, truncateId } from '@/lib/utils'
import type { JobResponse } from '@/types'

interface FieldProps {
  label: string
  value: React.ReactNode
  mono?: boolean
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

  if (!job) return null

  const canCancel = job.status === 'queued' || job.status === 'waiting'
  const canAbort = job.status === 'active' || job.status === 'aborting'
  const canRequeue = job.status === 'failed' || job.status === 'aborted' || job.status === 'complete'

  return (
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
            onClick={() => remove.mutate()}
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
            <div className="grid grid-cols-2 gap-4">
              <Field label="Queue" value={job.queue} mono />
              <Field label="Priority" value={job.priority} mono />
              <Field label="Attempts" value={`${job.attempts} / ${job.max_attempts}`} mono />
              <Field label="Timeout" value={job.timeout_secs ? `${job.timeout_secs}s` : null} mono />
              <Field label="Enqueued" value={relativeTime(job.enqueued_at)} />
              <Field label="Scheduled" value={formatTimestamp(job.scheduled_at)} mono />
              <Field label="Started" value={formatTimestamp(job.started_at)} mono />
              <Field label="Completed" value={formatTimestamp(job.completed_at)} mono />
              <Field
                label="Duration"
                value={formatDuration(job.started_at, job.completed_at)}
                mono
              />
              <Field
                label="Worker"
                value={job.worker_id ? truncateId(job.worker_id) : null}
                mono
              />
              {job.key && <Field label="Key" value={job.key} mono />}
              {job.group_key && <Field label="Group" value={job.group_key} mono />}
            </div>

            {job.meta && Object.keys(job.meta).length > 0 && (
              <>
                <Separator className="my-4" />
                <p className="mb-2 text-[10px] uppercase tracking-wider text-muted-foreground">Meta</p>
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
  )
}
