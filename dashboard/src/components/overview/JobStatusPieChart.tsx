import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { Skeleton } from '@/components/ui/skeleton'
import type { QueueStats } from '@/types'

const STATUS_COLORS: Record<string, string> = {
  Queued:   'hsl(217 91% 60%)',
  Active:   'hsl(142 71% 45%)',
  Waiting:  'hsl(38 92% 50%)',
  Failed:   'hsl(0 84% 60%)',
  Complete: 'hsl(210 40% 55%)',
  Aborted:  'hsl(0 0% 45%)',
}

interface Props {
  queues: QueueStats[]
}

export function JobStatusPieChart({ queues: qs }: Props) {
  const totals = {
    Queued:   qs.reduce((s, q) => s + q.queued,   0),
    Active:   qs.reduce((s, q) => s + q.active,   0),
    Waiting:  qs.reduce((s, q) => s + q.waiting,  0),
    Failed:   qs.reduce((s, q) => s + q.failed,   0),
    Complete: qs.reduce((s, q) => s + q.complete, 0),
    Aborted:  qs.reduce((s, q) => s + q.aborted,  0),
  }

  const data = Object.entries(totals)
    .filter(([, v]) => v > 0)
    .map(([name, value]) => ({ name, value }))

  if (data.length === 0) {
    return (
      <div className="flex h-[220px] items-center justify-center text-sm text-muted-foreground">
        No jobs yet
      </div>
    )
  }

  return (
    <ResponsiveContainer width="100%" height={220}>
      <PieChart>
        <Pie
          data={data}
          cx="50%"
          cy="45%"
          innerRadius={55}
          outerRadius={80}
          paddingAngle={2}
          dataKey="value"
        >
          {data.map(entry => (
            <Cell key={entry.name} fill={STATUS_COLORS[entry.name] ?? 'hsl(var(--muted-foreground))'} />
          ))}
        </Pie>
        <Tooltip
          contentStyle={{
            background: 'hsl(var(--card))',
            border: '1px solid hsl(var(--border))',
            borderRadius: 6,
            fontSize: 12,
          }}
        />
        <Legend
          wrapperStyle={{ fontSize: 11 }}
          iconType="square"
          iconSize={8}
        />
      </PieChart>
    </ResponsiveContainer>
  )
}

export function JobStatusPieChartSkeleton() {
  return <Skeleton className="h-[220px] w-full" />
}
