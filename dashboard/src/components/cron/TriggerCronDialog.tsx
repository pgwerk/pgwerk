import { api } from '@/lib/api'
import type { CronJobStats } from '@/types'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog'
import { shortFn } from '@/lib/utils'

interface TriggerCronDialogProps {
  job: CronJobStats | null
  onOpenChange: (open: boolean) => void
}

export function TriggerCronDialog({ job, onOpenChange }: TriggerCronDialogProps) {
  const queryClient = useQueryClient()

  const mutation = useMutation({
    mutationFn: () => api.triggerCronJob(job!.name),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['jobs'] })
      queryClient.invalidateQueries({ queryKey: ['stats'] })
      toast.success('Cron job triggered')
      onOpenChange(false)
    },
    onError: (err: Error) => toast.error(err.message),
  })

  return (
    <Dialog open={job !== null} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-sm">
        <DialogHeader>
          <DialogTitle className="text-sm font-semibold">Trigger Cron Job</DialogTitle>
          <DialogDescription className="text-xs">
            Enqueue a one-off run of this cron job immediately.
          </DialogDescription>
        </DialogHeader>

        {job && (
          <div className="space-y-2 rounded-md border border-border bg-muted/40 px-3 py-2.5 text-xs">
            <div className="flex gap-2">
              <span className="w-20 shrink-0 text-muted-foreground">Name</span>
              <span className="font-mono font-medium truncate" title={job.name}>{shortFn(job.name)}</span>
            </div>
            <div className="flex gap-2">
              <span className="w-20 shrink-0 text-muted-foreground">Function</span>
              <span className="font-mono truncate text-muted-foreground" title={job.function}>{shortFn(job.function)}</span>
            </div>
            <div className="flex gap-2">
              <span className="w-20 shrink-0 text-muted-foreground">Queue</span>
              <span className="font-mono">{job.queue}</span>
            </div>
          </div>
        )}

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
          <Button
            size="sm"
            onClick={() => mutation.mutate()}
            disabled={mutation.isPending}
          >
            {mutation.isPending ? 'Triggering…' : 'Run Now'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
