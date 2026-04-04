import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { format } from 'date-fns'
import { Skeleton } from '@/components/ui/skeleton'
import type { WorkerThroughputPoint } from '@/types'

interface Props {
  points: WorkerThroughputPoint[]
  minutes: number
}

type ChartEntry = { name: string; _t: number; [key: string]: string | number }

const COLORS = [
  'hsl(210 40% 55%)',
  'hsl(142 71% 45%)',
  'hsl(38 92% 50%)',
  'hsl(280 65% 60%)',
  'hsl(0 84% 60%)',
  'hsl(190 80% 50%)',
  'hsl(330 80% 55%)',
  'hsl(60 70% 45%)',
]

export function ThroughputChart({ points, minutes }: Props) {
  const timeFmt = minutes > 2880 ? 'EEE HH:mm' : 'HH:mm'

  const workers = [
    ...new Set(
      points
        .filter(p => p.worker_id != null)
        .map(p => p.worker_name ?? p.worker_id!)
    ),
  ]

  // Key by the raw ISO timestamp so two sample points that happen to format
  // to the same "HH:mm" label (e.g. 24h view wrapping the same minute) don't
  // get merged into a single row.
  const byTime = new Map<string, ChartEntry>()
  for (const p of points) {
    if (!byTime.has(p.time)) {
      byTime.set(p.time, {
        name: format(new Date(p.time), timeFmt),
        _t: new Date(p.time).getTime(),
      })
    }
    if (p.worker_id == null) continue
    const key = p.worker_name ?? p.worker_id
    const entry = byTime.get(p.time)!
    entry[key] = ((entry[key] as number | undefined) ?? 0) + p.count
  }
  const data = Array.from(byTime.values()).sort((a, b) => a._t - b._t)

  if (workers.length === 0) {
    return (
      <div className="flex h-[280px] items-center justify-center text-sm text-muted-foreground">
        No completed jobs in this range
      </div>
    )
  }

  return (
    <ResponsiveContainer width="100%" height={280}>
      <AreaChart data={data} margin={{ right: 20 }}>
        <defs>
          {workers.map((w, i) => (
            <linearGradient key={w} id={`color-worker-${i}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor={COLORS[i % COLORS.length]} stopOpacity={0.3} />
              <stop offset="95%" stopColor={COLORS[i % COLORS.length]} stopOpacity={0} />
            </linearGradient>
          ))}
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" vertical={false} />
        <XAxis
          dataKey="name"
          tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          allowDecimals={false}
          tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
          axisLine={false}
          tickLine={false}
          width={30}
        />
        <Tooltip
          contentStyle={{
            background: 'hsl(var(--card))',
            border: '1px solid hsl(var(--border))',
            borderRadius: 6,
            fontSize: 12,
          }}
        />
        <Legend
          wrapperStyle={{ fontSize: 11, paddingTop: 8 }}
          iconType="square"
          iconSize={8}
        />
        {workers.map((w, i) => (
          <Area
            key={w}
            type="monotone"
            dataKey={w}
            stroke={COLORS[i % COLORS.length]}
            fill={`url(#color-worker-${i})`}
            strokeWidth={2}
          />
        ))}
      </AreaChart>
    </ResponsiveContainer>
  )
}

export function ThroughputChartSkeleton() {
  return <Skeleton className="h-[280px] w-full" />
}
