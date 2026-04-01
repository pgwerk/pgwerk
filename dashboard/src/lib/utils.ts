import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'
import { formatDistanceToNow, differenceInSeconds, format } from 'date-fns'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function relativeTime(dateStr: string | undefined): string {
  if (!dateStr) return '—'
  try {
    return formatDistanceToNow(new Date(dateStr), { addSuffix: true })
  } catch {
    return '—'
  }
}

export function formatTimestamp(dateStr: string | undefined): string {
  if (!dateStr) return '—'
  try {
    return format(new Date(dateStr), 'MMM d, HH:mm:ss')
  } catch {
    return '—'
  }
}

export function formatDuration(
  startStr: string | undefined,
  endStr?: string | undefined,
): string {
  if (!startStr) return '—'
  try {
    const start = new Date(startStr)
    const end = endStr ? new Date(endStr) : new Date()
    const secs = differenceInSeconds(end, start)
    if (secs < 60) return `${secs}s`
    if (secs < 3600) return `${Math.floor(secs / 60)}m ${secs % 60}s`
    return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`
  } catch {
    return '—'
  }
}

export function truncateId(id: string): string {
  return id.slice(0, 8)
}

export function shortFn(fn: string): string {
  const parts = fn.split('.')
  return parts.length > 2 ? `…${parts.slice(-2).join('.')}` : fn
}
