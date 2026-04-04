import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Skeleton } from '@/components/ui/skeleton'
import { statusColor } from '@/components/StatusBadge'
import type { QueueStats } from '@/types'

const STATUS_COLS = ['queued', 'active', 'waiting', 'failed', 'complete', 'aborted'] as const

interface QueueTableProps {
  queues: QueueStats[]
}

export function QueueTable({ queues }: QueueTableProps) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead className="w-40">Queue</TableHead>
          {STATUS_COLS.map(s => (
            <TableHead key={s} className="w-24 text-right font-mono capitalize">
              {s}
            </TableHead>
          ))}
          <TableHead>Distribution</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {queues.map(q => {
          const total = STATUS_COLS.reduce((sum, s) => sum + (q[s] ?? 0), 0)
          return (
            <TableRow key={q.queue}>
              <TableCell className="font-mono text-sm font-medium">{q.queue}</TableCell>
              {STATUS_COLS.map(s => (
                <TableCell key={s} className="text-right font-mono text-sm tabular-nums text-muted-foreground">
                  {q[s] ?? 0}
                </TableCell>
              ))}
              <TableCell>
                {total > 0 ? (
                  <div className="flex h-1.5 w-full overflow-hidden rounded-full bg-muted">
                    {STATUS_COLS.filter(s => (q[s] ?? 0) > 0).map(s => (
                      <div
                        key={s}
                        className={`h-full ${statusColor(s)} opacity-80`}
                        style={{ width: `${((q[s] ?? 0) / total) * 100}%` }}
                      />
                    ))}
                  </div>
                ) : (
                  <div className="h-1.5 w-full rounded-full bg-muted" />
                )}
              </TableCell>
            </TableRow>
          )
        })}
        {queues.length === 0 && (
          <TableRow>
            <TableCell colSpan={8} className="py-8 text-center text-sm text-muted-foreground">
              No queues found
            </TableCell>
          </TableRow>
        )}
      </TableBody>
    </Table>
  )
}

export function QueueTableSkeleton() {
  return (
    <div className="space-y-3 p-4">
      {Array.from({ length: 3 }).map((_, i) => (
        <Skeleton key={i} className="h-8 w-full" />
      ))}
    </div>
  )
}
