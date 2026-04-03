import { cn } from '@/lib/utils'
import type { JobStatus } from '@/types'

const config: Record<string, { label: string; dot: string; className: string }> = {
  queued:    { label: 'queued',    dot: 'bg-sky-400',     className: 'bg-sky-500/10 text-sky-400 border-sky-500/30' },
  scheduled: { label: 'scheduled', dot: 'bg-indigo-400',  className: 'bg-indigo-500/10 text-indigo-400 border-indigo-500/30' },
  waiting:   { label: 'waiting',   dot: 'bg-amber-400',   className: 'bg-amber-500/10 text-amber-400 border-amber-500/30' },
  active:    { label: 'active',    dot: 'bg-green-400',   className: 'bg-green-500/10 text-green-400 border-green-500/30' },
  running:   { label: 'running',   dot: 'bg-green-400',   className: 'bg-green-500/10 text-green-400 border-green-500/30' },
  aborting:  { label: 'aborting',  dot: 'bg-orange-400',  className: 'bg-orange-500/10 text-orange-400 border-orange-500/30' },
  complete:  { label: 'complete',  dot: 'bg-emerald-400', className: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/30' },
  failed:    { label: 'failed',    dot: 'bg-red-400',     className: 'bg-red-500/10 text-red-400 border-red-500/30' },
  aborted:   { label: 'aborted',   dot: 'bg-violet-400',  className: 'bg-violet-500/10 text-violet-400 border-violet-500/30' },
}

const PULSING = new Set(['active', 'running'])

interface StatusBadgeProps {
  status: string
  className?: string
}

export function StatusBadge({ status, className }: StatusBadgeProps) {
  const c = config[status] ?? { label: status, dot: 'bg-zinc-400', className: 'bg-zinc-700/30 text-zinc-400 border-zinc-600/30' }
  return (
    <span
      className={cn(
        'inline-flex items-center gap-1.5 rounded border px-1.5 py-0.5 font-mono text-[10px] font-medium uppercase tracking-wider',
        c.className,
        className,
      )}
    >
      <span className={cn('size-1.5 shrink-0 rounded-full', c.dot, PULSING.has(status) && 'animate-pulse')} />
      {c.label}
    </span>
  )
}

export function statusColor(status: JobStatus | string): string {
  const map: Record<string, string> = {
    queued:    'bg-sky-500',
    scheduled: 'bg-indigo-500',
    waiting:   'bg-amber-500',
    active:    'bg-green-500',
    aborting:  'bg-orange-500',
    complete:  'bg-emerald-500',
    failed:    'bg-red-500',
    aborted:   'bg-violet-500',
  }
  return map[status] ?? 'bg-zinc-600'
}
