import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { Sheet, SheetContent, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { Button } from '@/components/ui/button'
import { Separator } from '@/components/ui/separator'
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { StatusBadge } from '@/components/StatusBadge'
import {
  Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle,
} from '@/components/ui/dialog'
import { api } from '@/lib/api'
import { cn, relativeTime, formatTimestamp, formatDuration, truncateId, shortFn } from '@/lib/utils'
import type { JobResponse, ScheduleResponse } from '@/types'
import { ChevronDown, Check, Copy, Pause, Play } from 'lucide-react'

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

function formatScheduleExpr(s: ScheduleResponse): string {
  if (s.cron) return s.cron
  if (s.interval_secs != null) {
    if (s.interval_secs < 60) return `every ${s.interval_secs}s`
    if (s.interval_secs < 3600) return `every ${s.interval_secs / 60}m`
    return `every ${s.interval_secs / 3600}h`
  }
  return '—'
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)
  function copy(e: React.MouseEvent) {
    e.stopPropagation()
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    })
  }
  return (
    <button onClick={copy} className="shrink-0 rounded p-1 text-muted-foreground hover:bg-muted hover:text-foreground transition-colors" title="Copy to clipboard">
      {copied ? <Check className="h-3 w-3 text-emerald-500" /> : <Copy className="h-3 w-3" />}
    </button>
  )
}

function CodeBlock({ content, variant }: { content: string; variant: 'error' | 'result' }) {
  return (
    <div
      className={cn('relative rounded', variant === 'error' ? 'bg-destructive/10' : 'bg-muted')}
      onClick={e => e.stopPropagation()}
    >
      <div className="absolute right-1.5 top-1.5">
        <CopyButton text={content} />
      </div>
      <pre className={cn('overflow-x-auto p-2 pr-8 font-mono text-[10px] whitespace-pre-wrap break-words', variant === 'error' ? 'text-destructive' : 'text-foreground')}>
        {content}
      </pre>
    </div>
  )
}

function JobCard({ job }: { job: JobResponse }) {
  const hasDetail = !!(job.error || job.meta != null)
  const [open, setOpen] = useState(!!job.error)

  return (
    <div className={cn('rounded-md border border-border bg-muted/30 text-xs', hasDetail && 'cursor-pointer')}>
      <div className="flex items-center justify-between p-3" onClick={() => hasDetail && setOpen(o => !o)}>
        <div className="flex items-center gap-2">
          <span className="font-mono text-muted-foreground">{truncateId(job.id)}</span>
          <StatusBadge status={job.status} />
        </div>
        <div className="flex items-center gap-2">
          <span className="font-mono text-muted-foreground">{formatDuration(job.started_at, job.completed_at)}</span>
          {hasDetail && <ChevronDown className={cn('h-3.5 w-3.5 text-muted-foreground transition-transform', open && 'rotate-180')} />}
        </div>
      </div>

      <div className="flex gap-4 px-3 pb-2 font-mono text-muted-foreground" onClick={() => hasDetail && setOpen(o => !o)}>
        <span>enqueued {relativeTime(job.enqueued_at)}</span>
        {job.started_at && <span>started {formatTimestamp(job.started_at)}</span>}
      </div>

      {job.worker_id && (
        <p className="px-3 pb-2 font-mono text-[10px] text-muted-foreground/60" onClick={() => hasDetail && setOpen(o => !o)}>
          worker {truncateId(job.worker_id)}
        </p>
      )}

      {open && (
        <div className="border-t border-border px-3 pb-3 pt-2 space-y-2">
          {job.error && <CodeBlock content={job.error} variant="error" />}
          {job.meta != null && <CodeBlock content={JSON.stringify(job.meta, null, 2)} variant="result" />}
        </div>
      )}
    </div>
  )
}

function ScheduleExecutions({ scheduleName }: { scheduleName: string }) {
  const { data, isLoading } = useQuery({
    queryKey: ['schedule-jobs', scheduleName],
    queryFn: () => api.listJobs({ schedule_name: scheduleName, limit: 25 }),
    refetchInterval: 10_000,
  })

  if (isLoading) {
    return <div className="space-y-2">{Array.from({ length: 3 }).map((_, i) => <Skeleton key={i} className="h-14 w-full" />)}</div>
  }

  if (!data?.length) {
    return <p className="text-sm text-muted-foreground">No executions yet.</p>
  }

  return (
    <div className="space-y-2">
      {data.map(job => <JobCard key={job.id} job={job} />)}
    </div>
  )
}

interface ScheduleDetailProps {
  schedule: ScheduleResponse | null
  open: boolean
  onClose: () => void
}

export function ScheduleDetail({ schedule, open, onClose }: ScheduleDetailProps) {
  const [confirmDelete, setConfirmDelete] = useState(false)
  const qc = useQueryClient()

  const invalidate = () => qc.invalidateQueries({ queryKey: ['schedules'] })

  const trigger = useMutation({
    mutationFn: () => api.triggerSchedule(schedule!.name),
    onSuccess: () => { toast.success('Schedule triggered'); invalidate() },
    onError: (e: Error) => toast.error(e.message),
  })

  const pause = useMutation({
    mutationFn: () => api.pauseSchedule(schedule!.name),
    onSuccess: () => { toast.success('Schedule paused'); invalidate() },
    onError: (e: Error) => toast.error(e.message),
  })

  const resume = useMutation({
    mutationFn: () => api.resumeSchedule(schedule!.name),
    onSuccess: () => { toast.success('Schedule resumed'); invalidate() },
    onError: (e: Error) => toast.error(e.message),
  })

  const remove = useMutation({
    mutationFn: () => api.deleteSchedule(schedule!.name),
    onSuccess: () => { toast.success('Schedule deleted'); invalidate(); onClose() },
    onError: (e: Error) => toast.error(e.message),
  })

  if (!schedule) return null

  return (
    <>
      <Sheet open={open} onOpenChange={v => !v && onClose()}>
        <SheetContent className="flex w-[480px] flex-col gap-0 p-0 sm:max-w-[480px]">
          <SheetHeader className="border-b border-border p-5">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <SheetTitle className="truncate font-mono text-sm font-medium">
                  {shortFn(schedule.name)}
                </SheetTitle>
                <p className="mt-0.5 font-mono text-[11px] text-muted-foreground truncate">
                  {schedule.function}
                </p>
              </div>
              <span className={`mt-0.5 shrink-0 text-xs font-medium ${schedule.paused ? 'text-yellow-500' : 'text-emerald-500'}`}>
                {schedule.paused ? 'paused' : 'active'}
              </span>
            </div>
          </SheetHeader>

          <div className="flex gap-2 border-b border-border px-5 py-3">
            <Button size="sm" variant="outline" className="gap-1.5" onClick={() => trigger.mutate()} disabled={trigger.isPending}>
              <Play className="h-3 w-3" />
              Trigger Now
            </Button>
            {schedule.paused ? (
              <Button size="sm" variant="outline" className="gap-1.5" onClick={() => resume.mutate()} disabled={resume.isPending}>
                <Play className="h-3 w-3" />
                Resume
              </Button>
            ) : (
              <Button size="sm" variant="outline" className="gap-1.5" onClick={() => pause.mutate()} disabled={pause.isPending}>
                <Pause className="h-3 w-3" />
                Pause
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
            </TabsList>

            <TabsContent value="details" className="flex-1 overflow-y-auto p-5">
              <p className="mb-2 text-[10px] uppercase tracking-wider text-muted-foreground">Identity</p>
              <div className="grid grid-cols-2 gap-4">
                <Field label="Name" value={schedule.name} mono />
                <Field label="Queue" value={schedule.queue} mono />
                <Field label="Function" value={schedule.function} mono />
                <Field label="State" value={schedule.paused ? 'paused' : 'active'} mono />
              </div>

              <Separator className="my-4" />
              <p className="mb-2 text-[10px] uppercase tracking-wider text-muted-foreground">Schedule</p>
              <div className="grid grid-cols-2 gap-4">
                <Field label="Expression" value={formatScheduleExpr(schedule)} mono />
                {schedule.cron && <Field label="Cron" value={schedule.cron} mono />}
                {schedule.interval_secs != null && <Field label="Interval" value={`${schedule.interval_secs}s`} mono />}
                <Field label="Next Run" value={relativeTime(schedule.next_run_at ?? undefined)} />
                <Field label="Last Run" value={relativeTime(schedule.last_run_at ?? undefined)} />
                <Field label="Registered" value={formatTimestamp(schedule.last_registered_at ?? undefined)} mono />
                <Field label="Created" value={formatTimestamp(schedule.created_at ?? undefined)} mono />
              </div>

              {(schedule.timeout_secs != null || schedule.result_ttl != null || schedule.failure_ttl != null) && (
                <>
                  <Separator className="my-4" />
                  <p className="mb-2 text-[10px] uppercase tracking-wider text-muted-foreground">Limits</p>
                  <div className="grid grid-cols-2 gap-4">
                    {schedule.timeout_secs != null && <Field label="Timeout" value={`${schedule.timeout_secs}s`} mono />}
                    {schedule.result_ttl != null && <Field label="Result TTL" value={`${schedule.result_ttl}s`} mono />}
                    {schedule.failure_ttl != null && <Field label="Failure TTL" value={`${schedule.failure_ttl}s`} mono />}
                  </div>
                </>
              )}

              {schedule.args.length > 0 && (
                <>
                  <Separator className="my-4" />
                  <p className="mb-2 text-[10px] uppercase tracking-wider text-muted-foreground">Args</p>
                  <pre className="overflow-x-auto rounded-md bg-muted p-3 font-mono text-xs text-foreground">
                    {JSON.stringify(schedule.args, null, 2)}
                  </pre>
                </>
              )}

              {Object.keys(schedule.kwargs).length > 0 && (
                <>
                  <Separator className="my-4" />
                  <p className="mb-2 text-[10px] uppercase tracking-wider text-muted-foreground">Kwargs</p>
                  <pre className="overflow-x-auto rounded-md bg-muted p-3 font-mono text-xs text-foreground">
                    {JSON.stringify(schedule.kwargs, null, 2)}
                  </pre>
                </>
              )}

              {schedule.meta && Object.keys(schedule.meta).length > 0 && (
                <>
                  <Separator className="my-4" />
                  <p className="mb-2 text-[10px] uppercase tracking-wider text-muted-foreground">Meta</p>
                  <pre className="overflow-x-auto rounded-md bg-muted p-3 font-mono text-xs text-foreground">
                    {JSON.stringify(schedule.meta, null, 2)}
                  </pre>
                </>
              )}
            </TabsContent>

            <TabsContent value="executions" className="flex-1 overflow-y-auto p-5">
              <ScheduleExecutions scheduleName={schedule.name} />
            </TabsContent>
          </Tabs>
        </SheetContent>
      </Sheet>

      <Dialog open={confirmDelete} onOpenChange={setConfirmDelete}>
        <DialogContent className="sm:max-w-sm">
          <DialogHeader>
            <DialogTitle className="text-sm font-semibold">Delete Schedule</DialogTitle>
            <DialogDescription className="text-xs">
              Permanently remove <span className="font-mono font-medium">{schedule.name}</span>. This cannot be undone — the schedule will stop firing immediately.
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
