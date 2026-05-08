import { api } from '@/lib/api'
import type { ScheduleResponse } from '@/types'
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

interface TriggerScheduleDialogProps {
  schedule: ScheduleResponse | null
  onOpenChange: (open: boolean) => void
}

export function TriggerScheduleDialog({ schedule, onOpenChange }: TriggerScheduleDialogProps) {
  const queryClient = useQueryClient()

  const mutation = useMutation({
    mutationFn: () => api.triggerSchedule(schedule!.name),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schedules'] })
      queryClient.invalidateQueries({ queryKey: ['jobs'] })
      queryClient.invalidateQueries({ queryKey: ['stats'] })
      toast.success('Schedule triggered')
      onOpenChange(false)
    },
    onError: (err: Error) => toast.error(err.message),
  })

  return (
    <Dialog open={schedule !== null} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-sm">
        <DialogHeader>
          <DialogTitle className="text-sm font-semibold">Trigger Schedule</DialogTitle>
          <DialogDescription className="text-xs">
            Force this schedule to become due on the next tick.
          </DialogDescription>
        </DialogHeader>

        {schedule && (
          <div className="space-y-2 rounded-md border border-border bg-muted/40 px-3 py-2.5 text-xs">
            <div className="flex gap-2">
              <span className="w-20 shrink-0 text-muted-foreground">Name</span>
              <span className="font-mono font-medium truncate" title={schedule.name}>{shortFn(schedule.name)}</span>
            </div>
            <div className="flex gap-2">
              <span className="w-20 shrink-0 text-muted-foreground">Function</span>
              <span className="font-mono truncate text-muted-foreground" title={schedule.function}>{shortFn(schedule.function)}</span>
            </div>
            <div className="flex gap-2">
              <span className="w-20 shrink-0 text-muted-foreground">Queue</span>
              <span className="font-mono">{schedule.queue}</span>
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
            {mutation.isPending ? 'Triggering…' : 'Trigger Now'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
