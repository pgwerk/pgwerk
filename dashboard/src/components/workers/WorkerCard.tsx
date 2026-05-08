import { formatDistanceToNow } from 'date-fns'
import { Link } from 'react-router-dom'
import { AreaChart, Area, ResponsiveContainer } from 'recharts'
import { Cpu, CalendarClock } from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Tooltip, TooltipTrigger, TooltipContent, TooltipProvider } from '@/components/ui/tooltip'
import { cn, truncateId } from '@/lib/utils'
import type { WorkerResponse } from '@/types'

export interface WorkerSparklinePoint {
  t: number
  v: number
}

const MAX_QUEUES = 3

function isAlive(heartbeat: string | undefined): boolean {
  if (!heartbeat) return false
  return Date.now() - new Date(heartbeat).getTime() < 90_000
}

function parseQueues(queue: string): string[] {
  return queue ? queue.split(',').map(q => q.trim()).filter(Boolean) : []
}

interface WorkerCardProps {
  worker: WorkerResponse
  history?: WorkerSparklinePoint[]
}

export function WorkerCard({ worker, history = [] }: WorkerCardProps) {
  const alive = isAlive(worker.heartbeat_at)
  const uptime = worker.started_at
    ? formatDistanceToNow(new Date(worker.started_at), { addSuffix: false })
    : null
  const isScheduler = worker.role === 'scheduler'
  const queues = parseQueues(worker.queue)
  const visibleQueues = queues.slice(0, MAX_QUEUES)
  const hiddenQueues = queues.slice(MAX_QUEUES)

  return (
    <TooltipProvider>
      <Link to={`/workers/${worker.id}`} className="block">
        <Card className={cn('transition-colors hover:border-border/80 hover:bg-muted/20', !alive && 'opacity-60')}>
          <CardContent className="px-5 py-6">
            <div className="grid items-center gap-6" style={{ gridTemplateColumns: '25% 22% 13% 1fr auto' }}>

              {/* Identity */}
              <div className="flex items-center gap-3 min-w-0">
                <div className={cn(
                  'flex h-8 w-8 shrink-0 items-center justify-center rounded-md border',
                  alive
                    ? isScheduler
                      ? 'border-violet-500/30 bg-violet-500/10 text-violet-400'
                      : 'border-green-500/30 bg-green-500/10 text-green-400'
                    : 'border-border bg-muted/40 text-muted-foreground/50',
                )}>
                  {isScheduler ? <CalendarClock className="h-3.5 w-3.5" /> : <Cpu className="h-3.5 w-3.5" />}
                </div>
                <div className="min-w-0">
                  <div className="flex items-center gap-2">
                    <p className="truncate font-mono text-sm font-medium text-foreground leading-tight">
                      {worker.name}
                    </p>
                    <span className={cn(
                      'h-1.5 w-1.5 rounded-full shrink-0',
                      alive ? 'bg-green-500 shadow-[0_0_5px_1px_rgba(34,197,94,0.5)]' : 'bg-zinc-600',
                    )} />
                  </div>
                  <div className="flex items-center gap-1.5 mt-0.5">
                    <p className="font-mono text-xs text-muted-foreground/50 leading-tight">
                      {truncateId(worker.id)}
                    </p>
                    {isScheduler && (
                      <span className="text-[10px] font-medium px-1.5 py-0.5 rounded bg-violet-500/10 text-violet-400 border border-violet-500/20 leading-none shrink-0">
                        scheduler
                      </span>
                    )}
                  </div>
                </div>
              </div>

              {/* Queue */}
              <div className="min-w-0">
                <p className="text-xs text-muted-foreground/70 leading-tight mb-1.5">Queue</p>
                {isScheduler ? (
                  <span className="font-mono text-xs px-1.5 py-0.5 rounded bg-muted/40 text-muted-foreground/40 border border-border/30 leading-none">
                    —
                  </span>
                ) : (
                  <div className="flex items-center gap-1 flex-nowrap overflow-hidden">
                    {visibleQueues.map(q => (
                      <span key={q} className="font-mono text-xs px-1.5 py-0.5 rounded bg-muted/60 text-foreground border border-border/50 leading-none truncate">
                        {q}
                      </span>
                    ))}
                    {hiddenQueues.length > 0 && (
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <span className="font-mono text-xs px-1.5 py-0.5 rounded bg-muted/40 text-muted-foreground border border-border/40 leading-none cursor-default shrink-0">
                            +{hiddenQueues.length}
                          </span>
                        </TooltipTrigger>
                        <TooltipContent>
                          <p className="font-mono text-xs">{hiddenQueues.join(', ')}</p>
                        </TooltipContent>
                      </Tooltip>
                    )}
                  </div>
                )}
              </div>

              {/* Uptime */}
              <div className="min-w-0">
                <p className="text-xs text-muted-foreground/70 leading-tight mb-1">Uptime</p>
                <p className="font-mono text-sm leading-tight text-foreground truncate">
                  {uptime ?? '—'}
                </p>
              </div>

              {/* Last heartbeat */}
              <div className="min-w-0">
                <p className="text-xs text-muted-foreground/70 leading-tight mb-1">Last heartbeat</p>
                <p className="font-mono text-sm leading-tight text-muted-foreground truncate">
                  {worker.heartbeat_at
                    ? formatDistanceToNow(new Date(worker.heartbeat_at), { addSuffix: true })
                    : '—'}
                </p>
              </div>

              {/* Sparkline */}
              {history.length > 1 ? (
                <div className="w-24 h-12 shrink-0">
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
              ) : (
                <div className="w-24 h-12 shrink-0" />
              )}

            </div>
          </CardContent>
        </Card>
      </Link>
    </TooltipProvider>
  )
}
