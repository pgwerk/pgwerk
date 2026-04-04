import { Layers, Cpu, Zap, AlertCircle } from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import type { StatsResponse } from '@/types'

interface StatCardProps {
  label: string
  value: number | string
  icon: React.ReactNode
  accent?: string
}

function StatCard({ label, value, icon, accent }: StatCardProps) {
  return (
    <Card>
      <CardContent className="flex items-start justify-between p-5">
        <div>
          <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
            {label}
          </p>
          <p className={`mt-2 font-mono text-3xl font-semibold tabular-nums ${accent ?? 'text-foreground'}`}>
            {value}
          </p>
        </div>
        <div className="rounded-md bg-muted p-2 text-muted-foreground">{icon}</div>
      </CardContent>
    </Card>
  )
}

export function StatCardsSkeleton() {
  return (
    <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
      {Array.from({ length: 4 }).map((_, i) => (
        <Card key={i}>
          <CardContent className="p-5">
            <Skeleton className="h-4 w-24" />
            <Skeleton className="mt-2 h-9 w-16" />
          </CardContent>
        </Card>
      ))}
    </div>
  )
}

interface StatCardsProps {
  data: StatsResponse
}

export function StatCards({ data }: StatCardsProps) {
  const totalActive = data.queues.reduce((s, q) => s + q.active, 0)
  const totalFailed = data.queues.reduce((s, q) => s + q.failed, 0)

  return (
    <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
      <StatCard
        label="Total Jobs"
        value={data.total_jobs.toLocaleString()}
        icon={<Layers className="h-4 w-4" />}
      />
      <StatCard
        label="Workers Online"
        value={data.workers_online}
        icon={<Cpu className="h-4 w-4" />}
        accent={data.workers_online > 0 ? 'text-green-400' : 'text-red-400'}
      />
      <StatCard
        label="Active Jobs"
        value={totalActive}
        icon={<Zap className="h-4 w-4" />}
        accent={totalActive > 0 ? 'text-green-400' : undefined}
      />
      <StatCard
        label="Failed Jobs"
        value={totalFailed}
        icon={<AlertCircle className="h-4 w-4" />}
        accent={totalFailed > 0 ? 'text-red-400' : undefined}
      />
    </div>
  )
}
