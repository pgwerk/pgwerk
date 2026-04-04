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
import type { QueueDepthPoint } from '@/types'

interface Props {
  points: QueueDepthPoint[]
  minutes: number
}

export function QueueDepthChart({ points, minutes }: Props) {
  const fmt = minutes > 2880 ? 'EEE HH:mm' : 'HH:mm'
  const data = [...points]
    .sort((a, b) => new Date(a.time).getTime() - new Date(b.time).getTime())
    .map(p => ({
      name: format(new Date(p.time), fmt),
      Queued: p.queued,
      Active: p.active,
    }))

  return (
    <ResponsiveContainer width="100%" height={280}>
      <AreaChart data={data} margin={{ right: 20 }}>
        <defs>
          <linearGradient id="colorQueued" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="hsl(217 91% 60%)" stopOpacity={0.3} />
            <stop offset="95%" stopColor="hsl(217 91% 60%)" stopOpacity={0} />
          </linearGradient>
          <linearGradient id="colorActive" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="hsl(142 71% 45%)" stopOpacity={0.3} />
            <stop offset="95%" stopColor="hsl(142 71% 45%)" stopOpacity={0} />
          </linearGradient>
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
        <Area type="monotone" dataKey="Queued" stackId="a" stroke="hsl(217 91% 60%)" fill="url(#colorQueued)" strokeWidth={2} />
        <Area type="monotone" dataKey="Active" stackId="a" stroke="hsl(142 71% 45%)" fill="url(#colorActive)" strokeWidth={2} />
      </AreaChart>
    </ResponsiveContainer>
  )
}

export function QueueDepthChartSkeleton() {
  return <Skeleton className="h-[280px] w-full" />
}
