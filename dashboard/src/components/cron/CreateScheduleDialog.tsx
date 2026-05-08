import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { api } from '@/lib/api'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'

interface CreateScheduleDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
}

const inputCls =
  'h-8 w-full rounded-md border border-input bg-background px-3 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring'
const labelCls = 'block text-xs font-medium text-muted-foreground mb-1'

type ScheduleType = 'cron' | 'interval'

export function CreateScheduleDialog({ open, onOpenChange }: CreateScheduleDialogProps) {
  const queryClient = useQueryClient()
  const [name, setName] = useState('')
  const [fn, setFn] = useState('')
  const [queue, setQueue] = useState('default')
  const [scheduleType, setScheduleType] = useState<ScheduleType>('cron')
  const [cron, setCron] = useState('')
  const [intervalSecs, setIntervalSecs] = useState('')
  const [kwargs, setKwargs] = useState('')
  const [kwargsError, setKwargsError] = useState('')

  const mutation = useMutation({
    mutationFn: api.createSchedule,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schedules'] })
      toast.success('Schedule created')
      onOpenChange(false)
      reset()
    },
    onError: (err: Error) => toast.error(err.message),
  })

  function reset() {
    setName('')
    setFn('')
    setQueue('default')
    setScheduleType('cron')
    setCron('')
    setIntervalSecs('')
    setKwargs('')
    setKwargsError('')
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setKwargsError('')

    let parsedKwargs: Record<string, unknown> = {}
    if (kwargs.trim()) {
      try {
        const parsed = JSON.parse(kwargs)
        if (typeof parsed !== 'object' || Array.isArray(parsed) || parsed === null) {
          setKwargsError('Must be a JSON object, e.g. {"key": "value"}')
          return
        }
        parsedKwargs = parsed
      } catch {
        setKwargsError('Invalid JSON')
        return
      }
    }

    mutation.mutate({
      name: name.trim(),
      function: fn.trim(),
      queue: queue.trim() || 'default',
      cron: scheduleType === 'cron' ? cron.trim() : undefined,
      interval_secs: scheduleType === 'interval' ? parseInt(intervalSecs, 10) : undefined,
      kwargs: Object.keys(parsedKwargs).length ? parsedKwargs : undefined,
    })
  }

  const canSubmit = name.trim() && fn.trim() &&
    (scheduleType === 'cron' ? cron.trim() : !!parseInt(intervalSecs, 10))

  return (
    <Dialog open={open} onOpenChange={(v) => { onOpenChange(v); if (!v) reset() }}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle className="text-sm font-semibold">Create Schedule</DialogTitle>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className={labelCls}>Name <span className="text-destructive">*</span></label>
            <input
              className={inputCls}
              placeholder="send_weekly_report"
              value={name}
              onChange={e => setName(e.target.value)}
              required
              autoFocus
            />
          </div>

          <div>
            <label className={labelCls}>Function <span className="text-destructive">*</span></label>
            <input
              className={inputCls}
              placeholder="myapp.tasks.send_report"
              value={fn}
              onChange={e => setFn(e.target.value)}
              required
            />
          </div>

          <div>
            <label className={labelCls}>Queue</label>
            <input
              className={inputCls}
              placeholder="default"
              value={queue}
              onChange={e => setQueue(e.target.value)}
            />
          </div>

          {/* Schedule type toggle */}
          <div>
            <label className={labelCls}>Schedule <span className="text-destructive">*</span></label>
            <div className="flex gap-1 mb-2">
              {(['cron', 'interval'] as ScheduleType[]).map(t => (
                <button
                  key={t}
                  type="button"
                  onClick={() => setScheduleType(t)}
                  className={`rounded px-2.5 py-1 text-xs font-medium transition-colors ${
                    scheduleType === t
                      ? 'bg-accent text-accent-foreground'
                      : 'text-muted-foreground hover:text-foreground'
                  }`}
                >
                  {t === 'cron' ? 'Cron' : 'Interval'}
                </button>
              ))}
            </div>
            {scheduleType === 'cron' ? (
              <input
                className={inputCls}
                placeholder="0 9 * * 1  (every Monday at 9am)"
                value={cron}
                onChange={e => setCron(e.target.value)}
                required
              />
            ) : (
              <div className="flex items-center gap-2">
                <input
                  className={inputCls}
                  type="number"
                  min="1"
                  placeholder="60"
                  value={intervalSecs}
                  onChange={e => setIntervalSecs(e.target.value)}
                  required
                />
                <span className="text-xs text-muted-foreground shrink-0">seconds</span>
              </div>
            )}
          </div>

          <div>
            <label className={labelCls}>kwargs (JSON)</label>
            <textarea
              className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm font-mono placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring resize-none"
              rows={3}
              placeholder={'{\n  "key": "value"\n}'}
              value={kwargs}
              onChange={e => { setKwargs(e.target.value); setKwargsError('') }}
            />
            {kwargsError && <p className="mt-1 text-xs text-destructive">{kwargsError}</p>}
          </div>

          <DialogFooter>
            <Button
              type="button"
              variant="ghost"
              size="sm"
              onClick={() => { onOpenChange(false); reset() }}
              disabled={mutation.isPending}
            >
              Cancel
            </Button>
            <Button type="submit" size="sm" disabled={!canSubmit || mutation.isPending}>
              {mutation.isPending ? 'Creating…' : 'Create Schedule'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}
