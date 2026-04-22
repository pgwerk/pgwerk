import { Header } from '@/components/layout/Header'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { api } from '@/lib/api'
import { useMutation, useQuery } from '@tanstack/react-query'
import { AlertTriangle, Ban, Clock, Database, RefreshCw, RotateCcw, Trash2, Wrench } from 'lucide-react'
import { useState } from 'react'
import { toast } from 'sonner'

const TERMINAL_STATUSES = ['complete', 'failed', 'aborted'] as const
type TerminalStatus = (typeof TERMINAL_STATUSES)[number]

function fmtBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

function fmtPgVersion(ver: string): string {
  const m = ver.match(/PostgreSQL ([\d.]+)/)
  return m ? `PostgreSQL ${m[1]}` : ver
}

type ConfirmKey =
  | 'sweep'
  | 'purge'
  | 'requeue'
  | 'cancel'
  | 'vacuum'
  | 'reschedule'
  | 'truncate'
  | null

export function MaintenancePage() {
  const [confirm, setConfirm] = useState<ConfirmKey>(null)

  // purge state
  const [purgeStatuses, setPurgeStatuses] = useState<Set<TerminalStatus>>(
    new Set(['complete', 'failed', 'aborted']),
  )
  const [purgeDays, setPurgeDays] = useState(30)

  // requeue state
  const [requeueQueue, setRequeueQueue] = useState('')
  const [requeueFn, setRequeueFn] = useState('')

  // cancel state
  const [cancelQueue, setCancelQueue] = useState('')

  const serverInfo = useQuery({
    queryKey: ['server-info'],
    queryFn: api.getServerInfo,
    refetchInterval: 30_000,
  })

  const sweep = useMutation({
    mutationFn: api.sweep,
    onSuccess: r => { toast.success(`Swept ${r.swept} stuck job${r.swept !== 1 ? 's' : ''}`); setConfirm(null) },
    onError: (e: Error) => toast.error(e.message),
  })

  const purge = useMutation({
    mutationFn: api.purgeJobs,
    onSuccess: r => { toast.success(`Purged ${r.purged} job${r.purged !== 1 ? 's' : ''}`); setConfirm(null); serverInfo.refetch() },
    onError: (e: Error) => toast.error(e.message),
  })

  const requeue = useMutation({
    mutationFn: api.requeueFailed,
    onSuccess: r => { toast.success(`Requeued ${r.requeued} job${r.requeued !== 1 ? 's' : ''}`); setConfirm(null) },
    onError: (e: Error) => toast.error(e.message),
  })

  const cancelQueued = useMutation({
    mutationFn: api.cancelQueued,
    onSuccess: r => { toast.success(`Cancelled ${r.cancelled} job${r.cancelled !== 1 ? 's' : ''}`); setConfirm(null) },
    onError: (e: Error) => toast.error(e.message),
  })

  const vacuum = useMutation({
    mutationFn: api.vacuum,
    onSuccess: () => { toast.success('VACUUM ANALYZE complete'); setConfirm(null) },
    onError: (e: Error) => toast.error(e.message),
  })

  const reschedule = useMutation({
    mutationFn: api.rescheduleStuck,
    onSuccess: r => { toast.success(`Rescheduled ${r.rescheduled} job${r.rescheduled !== 1 ? 's' : ''}`); setConfirm(null) },
    onError: (e: Error) => toast.error(e.message),
  })

  const truncate = useMutation({
    mutationFn: api.truncate,
    onSuccess: () => { toast.success('All wrk tables truncated'); setConfirm(null); serverInfo.refetch() },
    onError: (e: Error) => toast.error(e.message),
  })

  function toggleStatus(s: TerminalStatus) {
    setPurgeStatuses(prev => {
      const next = new Set(prev)
      if (next.has(s)) next.delete(s)
      else next.add(s)
      return next
    })
  }

  return (
    <div className="flex flex-col">
      <Header title="Maintenance" />
      <div className="py-6 space-y-6">

        {/* Database info */}
        <div>
          <h2 className="text-sm font-semibold text-muted-foreground mb-3 uppercase tracking-wide">Database</h2>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-semibold flex items-center gap-2">
                <Database className="h-4 w-4" />
                Server Info
              </CardTitle>
            </CardHeader>
            <CardContent>
              {serverInfo.isLoading && <p className="text-xs text-muted-foreground">Loading…</p>}
              {serverInfo.isError && <p className="text-xs text-destructive">Failed to load server info</p>}
              {serverInfo.data && (
                <div className="space-y-4">
                  <div className="flex flex-wrap gap-6 text-sm">
                    <div>
                      <p className="text-xs text-muted-foreground mb-0.5">Version</p>
                      <p className="font-mono font-medium">{fmtPgVersion(serverInfo.data.pg_version)}</p>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground mb-0.5">Database size</p>
                      <p className="font-mono font-medium">{fmtBytes(serverInfo.data.db_size_bytes)}</p>
                    </div>
                  </div>
                  {serverInfo.data.tables.length > 0 && (
                    <table className="w-full text-xs">
                      <thead>
                        <tr className="border-b">
                          <th className="text-left py-1.5 font-medium text-muted-foreground">Table</th>
                          <th className="text-right py-1.5 font-medium text-muted-foreground">Rows</th>
                          <th className="text-right py-1.5 font-medium text-muted-foreground">Size</th>
                        </tr>
                      </thead>
                      <tbody>
                        {serverInfo.data.tables.map(t => (
                          <tr key={t.name} className="border-b last:border-0">
                            <td className="py-1.5 font-mono">{t.name}</td>
                            <td className="py-1.5 text-right tabular-nums">{t.row_count.toLocaleString()}</td>
                            <td className="py-1.5 text-right tabular-nums">{fmtBytes(t.size_bytes)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Operations */}
        <div>
          <h2 className="text-sm font-semibold text-muted-foreground mb-3 uppercase tracking-wide">Operations</h2>
          <div className="flex flex-col gap-4">

            {/* Sweep */}
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-semibold">Sweep Stuck Jobs</CardTitle>
              </CardHeader>
              <CardContent className="flex items-start justify-between gap-6">
                <p className="text-xs text-muted-foreground max-w-prose">
                  Find jobs stuck in <code className="font-mono">active</code> state with a missed
                  heartbeat and mark them failed so they can be retried.
                </p>
                <Button variant="outline" size="sm" className="gap-2 shrink-0" onClick={() => setConfirm('sweep')}>
                  <RefreshCw className="h-3.5 w-3.5" />
                  Run Sweep
                </Button>
              </CardContent>
            </Card>

            {/* Requeue failed */}
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-semibold">Requeue Failed Jobs</CardTitle>
              </CardHeader>
              <CardContent className="flex items-start justify-between gap-6">
                <div className="space-y-2">
                  <p className="text-xs text-muted-foreground max-w-prose">
                    Reset all failed jobs back to queued so they will be picked up again.
                    Leave filters blank to requeue everything.
                  </p>
                  <div className="flex flex-wrap items-center gap-3">
                    <div className="flex items-center gap-1.5">
                      <label className="text-xs text-muted-foreground whitespace-nowrap">Queue</label>
                      <input
                        value={requeueQueue}
                        onChange={e => setRequeueQueue(e.target.value)}
                        placeholder="all"
                        className="h-7 w-28 text-xs border rounded px-2 bg-background"
                      />
                    </div>
                    <div className="flex items-center gap-1.5">
                      <label className="text-xs text-muted-foreground whitespace-nowrap">Function</label>
                      <input
                        value={requeueFn}
                        onChange={e => setRequeueFn(e.target.value)}
                        placeholder="all"
                        className="h-7 w-40 text-xs border rounded px-2 bg-background"
                      />
                    </div>
                  </div>
                </div>
                <Button variant="outline" size="sm" className="gap-2 shrink-0" onClick={() => setConfirm('requeue')}>
                  <RotateCcw className="h-3.5 w-3.5" />
                  Requeue Failed
                </Button>
              </CardContent>
            </Card>

            {/* Reschedule stuck */}
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-semibold">Reschedule Stuck Jobs</CardTitle>
              </CardHeader>
              <CardContent className="flex items-start justify-between gap-6">
                <p className="text-xs text-muted-foreground max-w-prose">
                  Promote overdue <code className="font-mono">scheduled</code> jobs (past their{' '}
                  <code className="font-mono">scheduled_at</code>) to <code className="font-mono">queued</code>.
                  Fixes jobs left behind if the scheduler was down.
                </p>
                <Button variant="outline" size="sm" className="gap-2 shrink-0" onClick={() => setConfirm('reschedule')}>
                  <Clock className="h-3.5 w-3.5" />
                  Reschedule Stuck
                </Button>
              </CardContent>
            </Card>

            {/* Cancel queued */}
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-semibold">Cancel Queued Jobs</CardTitle>
              </CardHeader>
              <CardContent className="flex items-start justify-between gap-6">
                <div className="space-y-2">
                  <p className="text-xs text-muted-foreground max-w-prose">
                    Abort all <code className="font-mono">queued</code>,{' '}
                    <code className="font-mono">scheduled</code>, and{' '}
                    <code className="font-mono">waiting</code> jobs. Does not affect active jobs.
                    Leave blank to cancel across all queues.
                  </p>
                  <div className="flex items-center gap-1.5">
                    <label className="text-xs text-muted-foreground whitespace-nowrap">Queue</label>
                    <input
                      value={cancelQueue}
                      onChange={e => setCancelQueue(e.target.value)}
                      placeholder="all"
                      className="h-7 w-28 text-xs border rounded px-2 bg-background"
                    />
                  </div>
                </div>
                <Button variant="outline" size="sm" className="gap-2 shrink-0" onClick={() => setConfirm('cancel')}>
                  <Ban className="h-3.5 w-3.5" />
                  Cancel Queued
                </Button>
              </CardContent>
            </Card>

            {/* Purge old */}
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-semibold">Purge Old Jobs</CardTitle>
              </CardHeader>
              <CardContent className="flex items-start justify-between gap-6">
                <div className="space-y-2">
                  <p className="text-xs text-muted-foreground max-w-prose">
                    Permanently delete terminal jobs older than a specified age to reclaim database space.
                  </p>
                  <div className="flex flex-wrap items-center gap-4">
                    <div className="flex flex-wrap gap-3">
                      {TERMINAL_STATUSES.map(s => (
                        <label key={s} className="flex items-center gap-1.5 text-xs cursor-pointer">
                          <input
                            type="checkbox"
                            className="h-3 w-3"
                            checked={purgeStatuses.has(s)}
                            onChange={() => toggleStatus(s)}
                          />
                          {s}
                        </label>
                      ))}
                    </div>
                    <div className="flex items-center gap-2">
                      <p className="text-xs text-muted-foreground whitespace-nowrap">older than</p>
                      <input
                        type="number"
                        min={1}
                        max={3650}
                        value={purgeDays}
                        onChange={e => setPurgeDays(Math.max(1, Number(e.target.value)))}
                        className="w-16 h-7 text-xs border rounded px-2 bg-background"
                      />
                      <p className="text-xs text-muted-foreground">days</p>
                    </div>
                  </div>
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  className="gap-2 shrink-0"
                  disabled={purgeStatuses.size === 0}
                  onClick={() => setConfirm('purge')}
                >
                  <Trash2 className="h-3.5 w-3.5" />
                  Purge Jobs
                </Button>
              </CardContent>
            </Card>

            {/* Vacuum */}
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-semibold">VACUUM ANALYZE</CardTitle>
              </CardHeader>
              <CardContent className="flex items-start justify-between gap-6">
                <p className="text-xs text-muted-foreground max-w-prose">
                  Run <code className="font-mono">VACUUM ANALYZE</code> on all wrk tables to reclaim
                  storage from deleted rows and refresh query planner statistics. Recommended after
                  a large purge or truncate.
                </p>
                <Button variant="outline" size="sm" className="gap-2 shrink-0" onClick={() => setConfirm('vacuum')}>
                  <Wrench className="h-3.5 w-3.5" />
                  Run VACUUM
                </Button>
              </CardContent>
            </Card>

            {/* Truncate */}
            <Card className="border-destructive/40">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-semibold flex items-center gap-2 text-destructive">
                  <AlertTriangle className="h-3.5 w-3.5" />
                  Truncate All Tables
                </CardTitle>
              </CardHeader>
              <CardContent className="flex items-start justify-between gap-6">
                <p className="text-xs text-muted-foreground max-w-prose">
                  Delete every row from all wrk tables — jobs, workers, executions, and dependencies.
                  Instant and irreversible. Use only in dev or to fully reset a deployment.
                </p>
                <Button
                  variant="destructive"
                  size="sm"
                  className="gap-2 shrink-0"
                  onClick={() => setConfirm('truncate')}
                >
                  <AlertTriangle className="h-3.5 w-3.5" />
                  Truncate
                </Button>
              </CardContent>
            </Card>

          </div>
        </div>
      </div>

      {/* Confirm dialogs */}

      <Dialog open={confirm === 'sweep'} onOpenChange={o => !o && setConfirm(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Run Sweep</DialogTitle>
            <DialogDescription>
              Scans for jobs stuck in <code className="font-mono text-xs">active</code> state with
              an expired heartbeat and marks them failed so they can be retried. Safe to run at any time.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirm(null)}>Cancel</Button>
            <Button onClick={() => sweep.mutate()} disabled={sweep.isPending}>
              {sweep.isPending ? 'Running…' : 'Run Sweep'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={confirm === 'requeue'} onOpenChange={o => !o && setConfirm(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Requeue Failed Jobs</DialogTitle>
            <DialogDescription>
              Reset all failed jobs{requeueQueue ? ` in queue "${requeueQueue}"` : ''}
              {requeueFn ? ` for function "${requeueFn}"` : ''} back to queued with zero attempts.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirm(null)}>Cancel</Button>
            <Button
              onClick={() => requeue.mutate({
                queue: requeueQueue || undefined,
                function_name: requeueFn || undefined,
              })}
              disabled={requeue.isPending}
            >
              {requeue.isPending ? 'Requeueing…' : 'Requeue Failed'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={confirm === 'reschedule'} onOpenChange={o => !o && setConfirm(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Reschedule Stuck Jobs</DialogTitle>
            <DialogDescription>
              Promote all overdue <code className="font-mono text-xs">scheduled</code> jobs to{' '}
              <code className="font-mono text-xs">queued</code> so workers pick them up immediately.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirm(null)}>Cancel</Button>
            <Button onClick={() => reschedule.mutate()} disabled={reschedule.isPending}>
              {reschedule.isPending ? 'Rescheduling…' : 'Reschedule Stuck'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={confirm === 'cancel'} onOpenChange={o => !o && setConfirm(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Cancel Queued Jobs</DialogTitle>
            <DialogDescription>
              Abort all queued, scheduled, and waiting jobs
              {cancelQueue ? ` in queue "${cancelQueue}"` : ' across all queues'}.
              Active jobs will not be affected.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirm(null)}>Cancel</Button>
            <Button
              variant="destructive"
              onClick={() => cancelQueued.mutate({ queue: cancelQueue || undefined })}
              disabled={cancelQueued.isPending}
            >
              {cancelQueued.isPending ? 'Cancelling…' : 'Cancel Queued'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={confirm === 'purge'} onOpenChange={o => !o && setConfirm(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Purge Old Jobs</DialogTitle>
            <DialogDescription>
              Permanently delete all <strong>{[...purgeStatuses].join(', ')}</strong> jobs enqueued
              more than <strong>{purgeDays} day{purgeDays !== 1 ? 's' : ''}</strong> ago. Cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirm(null)}>Cancel</Button>
            <Button
              variant="destructive"
              onClick={() => purge.mutate({ statuses: [...purgeStatuses], older_than_days: purgeDays })}
              disabled={purge.isPending}
            >
              {purge.isPending ? 'Purging…' : 'Purge Jobs'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={confirm === 'vacuum'} onOpenChange={o => !o && setConfirm(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>VACUUM ANALYZE</DialogTitle>
            <DialogDescription>
              Run <code className="font-mono text-xs">VACUUM ANALYZE</code> on all wrk tables. This
              reclaims storage and refreshes planner statistics. It may take a moment on large tables.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirm(null)}>Cancel</Button>
            <Button onClick={() => vacuum.mutate()} disabled={vacuum.isPending}>
              {vacuum.isPending ? 'Running…' : 'Run VACUUM'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={confirm === 'truncate'} onOpenChange={o => !o && setConfirm(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Truncate All Tables</DialogTitle>
            <DialogDescription>
              Permanently delete <strong>all data</strong> from every wrk table. Jobs, workers,
              executions, and dependencies will be wiped. There is no undo.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirm(null)}>Cancel</Button>
            <Button variant="destructive" onClick={() => truncate.mutate()} disabled={truncate.isPending}>
              {truncate.isPending ? 'Truncating…' : 'Yes, truncate everything'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

    </div>
  )
}
