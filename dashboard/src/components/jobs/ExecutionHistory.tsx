import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { StatusBadge } from '@/components/StatusBadge'
import { Skeleton } from '@/components/ui/skeleton'
import { formatTimestamp, formatDuration, truncateId } from '@/lib/utils'
import { ChevronDown, Copy, Check } from 'lucide-react'
import { cn } from '@/lib/utils'
import type { ExecutionResponse } from '@/types'

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)

  function copy(e: React.MouseEvent) {
    e.stopPropagation()
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    })
  }

  return (
    <button
      onClick={copy}
      className="shrink-0 rounded p-1 text-muted-foreground hover:bg-muted hover:text-foreground transition-colors"
      title="Copy to clipboard"
    >
      {copied ? <Check className="h-3 w-3 text-emerald-500" /> : <Copy className="h-3 w-3" />}
    </button>
  )
}

function CodeBlock({ content, variant }: { content: string; variant: 'error' | 'result' }) {
  return (
    <div
      className={cn(
        'relative rounded',
        variant === 'error' ? 'bg-destructive/10' : 'bg-muted',
      )}
      onClick={e => e.stopPropagation()}
    >
      <div className="absolute right-1.5 top-1.5">
        <CopyButton text={content} />
      </div>
      <pre className={cn(
        'overflow-x-auto p-2 pr-8 font-mono text-[10px] whitespace-pre-wrap break-words',
        variant === 'error' ? 'text-destructive' : 'text-foreground',
      )}>
        {content}
      </pre>
    </div>
  )
}

function DetailRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-baseline gap-2">
      <span className="w-16 shrink-0 text-[10px] uppercase tracking-wider text-muted-foreground">{label}</span>
      <span className="font-mono text-[11px] text-foreground break-all">{value}</span>
    </div>
  )
}

function ExecutionCard({ ex }: { ex: ExecutionResponse }) {
  const [open, setOpen] = useState(!!ex.error)

  return (
    <div className="rounded-md border border-border bg-muted/30 text-xs cursor-pointer">
      <div
        className="flex items-center justify-between p-3"
        onClick={() => setOpen(o => !o)}
      >
        <div className="flex items-center gap-2">
          <span className="font-mono text-muted-foreground">#{ex.attempt}</span>
          <StatusBadge status={ex.status} />
        </div>
        <div className="flex items-center gap-2">
          <span className="font-mono text-muted-foreground">
            {formatDuration(ex.started_at, ex.completed_at)}
          </span>
          <ChevronDown className={cn('h-3.5 w-3.5 text-muted-foreground transition-transform', open && 'rotate-180')} />
        </div>
      </div>

      <div
        className="flex gap-4 px-3 pb-2 font-mono text-muted-foreground"
        onClick={() => setOpen(o => !o)}
      >
        {ex.started_at && <span>started {formatTimestamp(ex.started_at)}</span>}
        {ex.completed_at && <span>ended {formatTimestamp(ex.completed_at)}</span>}
      </div>

      {ex.worker_id && (
        <p
          className="px-3 pb-2 font-mono text-[10px] text-muted-foreground/60"
          onClick={() => setOpen(o => !o)}
        >
          worker {truncateId(ex.worker_id)}
        </p>
      )}

      {open && (
        <div className="border-t border-border px-3 pb-3 pt-2 space-y-2" onClick={e => e.stopPropagation()}>
          <div className="space-y-1">
            <DetailRow label="ID" value={ex.id} />
            <DetailRow label="Attempt" value={`#${ex.attempt}`} />
            <DetailRow label="Status" value={ex.status} />
            {ex.worker_id && <DetailRow label="Worker" value={ex.worker_id} />}
            {ex.started_at && <DetailRow label="Started" value={formatTimestamp(ex.started_at)} />}
            {ex.completed_at && <DetailRow label="Ended" value={formatTimestamp(ex.completed_at)} />}
            <DetailRow label="Duration" value={formatDuration(ex.started_at, ex.completed_at)} />
          </div>
          {ex.error && (
            <CodeBlock content={ex.error} variant="error" />
          )}
          {ex.result != null && (
            <CodeBlock content={JSON.stringify(ex.result, null, 2)} variant="result" />
          )}
          {!ex.error && ex.result == null && (
            <p className="text-[10px] text-muted-foreground/70 italic">No result or error recorded.</p>
          )}
        </div>
      )}
    </div>
  )
}

interface ExecutionHistoryProps {
  jobId: string
}

export function ExecutionHistory({ jobId }: ExecutionHistoryProps) {
  const { data, isLoading } = useQuery({
    queryKey: ['executions', jobId],
    queryFn: () => api.getJobExecutions(jobId),
  })

  if (isLoading) {
    return <div className="space-y-2">{Array.from({ length: 2 }).map((_, i) => <Skeleton key={i} className="h-14 w-full" />)}</div>
  }

  if (!data?.length) {
    return <p className="text-sm text-muted-foreground">No executions recorded.</p>
  }

  return (
    <div className="space-y-2">
      {data.map(ex => <ExecutionCard key={ex.id} ex={ex} />)}
    </div>
  )
}
