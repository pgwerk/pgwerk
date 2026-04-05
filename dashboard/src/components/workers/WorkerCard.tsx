import { formatDistanceToNow } from 'date-fns'
import { Link } from 'react-router-dom'
import { AreaChart, Area, ResponsiveContainer } from 'recharts'
import { Cpu } from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { cn, truncateId } from '@/lib/utils'
import type { WorkerResponse } from '@/types'
import type { HeartbeatPoint } from '@/hooks/useWorkerHistory'

function isAlive(heartbeat: string | undefined): boolean {
  if (!heartbeat) return false
  return Date.now() - new Date(heartbeat).getTime() < 90_000
}

interface WorkerCardProps {
  worker: WorkerResponse
  history?: HeartbeatPoint[]
}

export function WorkerCard({ worker, history = [] }: WorkerCardProps) {
  const alive = isAlive(worker.heartbeat_at)
  const uptime = worker.started_at
    ? formatDistanceToNow(new Date(worker.started_at), { addSuffix: false })
    : null

  return (
    <Link to={`/workers/${worker.id}`} className="block">
      <Card className={cn('transition-colors hover:border-border/80 hover:bg-muted/20', !alive && 'opacity-60')}>
        <CardContent className="px-5 py-6">
          <div className="flex items-center gap-6">
            {/* Icon + identity */}
            <div className="flex items-center gap-3 w-64 shrink-0">
              <div className={cn(
                'flex h-8 w-8 shrink-0 items-center justify-center rounded-md border',
                alive
                  ? 'border-green-500/30 bg-green-500/10 text-green-400'
                  : 'border-border bg-muted/40 text-muted-foreground/50',
              )}>
                <Cpu className="h-3.5 w-3.5" />
              </div>
              <div className="min-w-0">
                <div className="flex items-center gap-2">
                  <p className="truncate font-mono text-sm font-medium text-foreground leading-tight">
                    {worker.name}
                  </p>
                  <span
                    className={cn(
                      'h-1.5 w-1.5 rounded-full shrink-0',
                      alive ? 'bg-green-500 shadow-[0_0_5px_1px_rgba(34,197,94,0.5)]' : 'bg-zinc-600',
                    )}
                  />
                </div>
                <p className="font-mono text-xs text-muted-foreground/50 leading-tight mt-0.5">
                  {truncateId(worker.id)}
                </p>
              </div>
            </div>

            {/* Stats row */}
            <div className="flex items-center gap-8 flex-1">
              <Stat label="Queue" value={worker.queue} />
              {uptime && <Stat label="Uptime" value={uptime} />}
              {worker.heartbeat_at && (
                <Stat
                  label="Last heartbeat"
                  value={formatDistanceToNow(new Date(worker.heartbeat_at), { addSuffix: true })}
                  muted
                />
              )}
            </div>

            {/* Sparkline */}
            {history.length > 1 && (
              <div className="w-36 h-12 shrink-0">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={history} margin={{ top: 2, right: 0, bottom: 0, left: 0 }}>
                    <Area
                      type="monotone"
                      dataKey="v"
                      stroke={alive ? '#22c55e' : '#52525b'}
                      fill={alive ? 'rgba(34,197,94,0.12)' : 'rgba(82,82,91,0.12)'}
                      strokeWidth={1.5}
                      dot={false}
                      isAnimationActive={false}
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </Link>
  )
}

function Stat({ label, value, muted }: { label: string; value: string; muted?: boolean }) {
  return (
    <div className="shrink-0">
      <p className="text-xs text-muted-foreground/70 leading-tight mb-1">{label}</p>
      <p className={cn('font-mono text-sm leading-tight', muted ? 'text-muted-foreground' : 'text-foreground')}>
        {value}
      </p>
    </div>
  )
}
