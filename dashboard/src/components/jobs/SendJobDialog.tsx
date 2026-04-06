import { api } from '@/lib/api'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'

interface SendJobDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  defaultQueue?: string
}

const inputCls =
  'h-8 w-full rounded-md border border-input bg-background px-3 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring'
const labelCls = 'block text-xs font-medium text-muted-foreground mb-1'

export function SendJobDialog({ open, onOpenChange, defaultQueue }: SendJobDialogProps) {
  const queryClient = useQueryClient()
  const [fn, setFn] = useState('')
  const [queue, setQueue] = useState(defaultQueue ?? 'default')
  const [priority, setPriority] = useState('0')
  const [payload, setPayload] = useState('')
  const [payloadError, setPayloadError] = useState('')

  const mutation = useMutation({
    mutationFn: api.enqueueJob,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['jobs'] })
      queryClient.invalidateQueries({ queryKey: ['stats'] })
      toast.success('Job enqueued')
      onOpenChange(false)
      setFn('')
      setQueue(defaultQueue ?? 'default')
      setPriority('0')
      setPayload('')
    },
    onError: (err: Error) => toast.error(err.message),
  })

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setPayloadError('')

    let kwargs: Record<string, unknown> = {}
    if (payload.trim()) {
      try {
        const parsed = JSON.parse(payload)
        if (typeof parsed !== 'object' || Array.isArray(parsed) || parsed === null) {
          setPayloadError('Payload must be a JSON object, e.g. {"key": "value"}')
          return
        }
        kwargs = parsed
      } catch {
        setPayloadError('Invalid JSON')
        return
      }
    }

    mutation.mutate({
      function: fn.trim(),
      queue: queue.trim() || 'default',
      priority: parseInt(priority, 10) || 0,
      kwargs: Object.keys(kwargs).length ? kwargs : undefined,
    })
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle className="text-sm font-semibold">Send Job</DialogTitle>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className={labelCls}>Function <span className="text-destructive">*</span></label>
            <input
              className={inputCls}
              placeholder="myapp.tasks.send_email"
              value={fn}
              onChange={e => setFn(e.target.value)}
              required
              autoFocus
            />
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className={labelCls}>Queue</label>
              <input
                className={inputCls}
                placeholder="default"
                value={queue}
                onChange={e => setQueue(e.target.value)}
              />
            </div>
            <div>
              <label className={labelCls}>Priority</label>
              <input
                className={inputCls}
                type="number"
                placeholder="0"
                value={priority}
                onChange={e => setPriority(e.target.value)}
              />
            </div>
          </div>

          <div>
            <label className={labelCls}>Payload (JSON kwargs)</label>
            <textarea
              className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm font-mono placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring resize-none"
              rows={4}
              placeholder={'{\n  "key": "value"\n}'}
              value={payload}
              onChange={e => { setPayload(e.target.value); setPayloadError('') }}
            />
            {payloadError && (
              <p className="mt-1 text-xs text-destructive">{payloadError}</p>
            )}
          </div>

          <DialogFooter>
            <Button
              type="button"
              variant="ghost"
              size="sm"
              onClick={() => onOpenChange(false)}
              disabled={mutation.isPending}
            >
              Cancel
            </Button>
            <Button type="submit" size="sm" disabled={!fn.trim() || mutation.isPending}>
              {mutation.isPending ? 'Sending…' : 'Send Job'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}
