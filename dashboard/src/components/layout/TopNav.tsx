import { NavLink } from 'react-router-dom'
import { Moon, Sun, Settings, RefreshCw, Database } from 'lucide-react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { useTheme } from '@/hooks/useTheme'
import { api } from '@/lib/api'
import { Logo } from '@/components/layout/Logo'

const nav = [
  { to: '/', label: 'Overview', end: true },
  { to: '/jobs', label: 'Jobs', end: false },
  { to: '/workers', label: 'Workers', end: false },
  { to: '/cron', label: 'Cron', end: false },
]

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

function shortPgVersion(full: string): string {
  const m = full.match(/PostgreSQL\s+([\d.]+)/i)
  return m ? `PG ${m[1]}` : full.split(' ')[0]
}

export function TopNav() {
  const { theme, toggle } = useTheme()
  const queryClient = useQueryClient()
  const [isRefreshing, setIsRefreshing] = useState(false)

  const { data: serverInfo, isError: dbError } = useQuery({
    queryKey: ['serverInfo'],
    queryFn: api.getServerInfo,
    staleTime: 30_000,
    retry: false,
  })

  async function handleRefresh() {
    setIsRefreshing(true)
    await queryClient.invalidateQueries()
    setIsRefreshing(false)
  }

  return (
    <header className="sticky top-0 z-40 flex h-12 items-center border-b border-border bg-background/95 backdrop-blur-sm">
      <div className="mx-auto flex w-full max-w-6xl items-center px-6">
        {/* Logo */}
        <div className="mr-8 shrink-0">
          <Logo theme={theme} size={28} />
        </div>

        {/* Nav links */}
        <nav className="flex items-center gap-1">
          {nav.map(({ to, label, end }) => (
            <NavLink
              key={to}
              to={to}
              end={end}
              className={({ isActive }) =>
                cn(
                  'rounded-md px-3 py-1.5 text-sm font-medium transition-colors',
                  isActive
                    ? 'bg-accent text-accent-foreground'
                    : 'text-muted-foreground hover:bg-accent/50 hover:text-foreground',
                )
              }
            >
              {label}
            </NavLink>
          ))}
        </nav>

        {/* Right side */}
        <div className="ml-auto flex items-center gap-2">
          {/* DB health badge */}
          <div className="flex items-center gap-1.5 rounded-md border border-border bg-muted/40 px-2.5 py-1 text-xs text-muted-foreground">
            <Database className="h-3 w-3 shrink-0" />
            <span
              className={cn(
                'h-1.5 w-1.5 rounded-full shrink-0',
                dbError ? 'bg-red-500' : 'bg-emerald-500',
              )}
            />
            {serverInfo ? (
              <>
                <span className="font-mono">{shortPgVersion(serverInfo.pg_version)}</span>
                <span className="text-border">·</span>
                <span className="font-mono">{formatBytes(serverInfo.db_size_bytes)}</span>
              </>
            ) : dbError ? (
              <span>unreachable</span>
            ) : (
              <span className="text-muted-foreground/50">connecting…</span>
            )}
          </div>

          <Button
            variant="ghost"
            size="icon"
            className="h-8 w-8"
            onClick={handleRefresh}
            disabled={isRefreshing}
          >
            <RefreshCw className={cn('h-3.5 w-3.5', isRefreshing && 'animate-spin')} />
          </Button>

          <Button variant="ghost" size="icon" className="h-8 w-8" onClick={toggle}>
            {theme === 'dark'
              ? <Sun className="h-3.5 w-3.5" />
              : <Moon className="h-3.5 w-3.5" />
            }
          </Button>

          <NavLink to="/maintenance">
            {({ isActive }) => (
              <Button
                variant="ghost"
                size="icon"
                className={cn('h-8 w-8', isActive && 'bg-accent text-accent-foreground')}
              >
                <Settings className="h-3.5 w-3.5" />
              </Button>
            )}
          </NavLink>
        </div>
      </div>
    </header>
  )
}
