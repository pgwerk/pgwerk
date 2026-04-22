import { Button } from '@/components/ui/button'
import { RefreshCw } from 'lucide-react'

interface HeaderProps {
  title: string
  subtitle?: string
  onRefresh?: () => void
  isRefreshing?: boolean
}

export function Header({ title, subtitle, onRefresh, isRefreshing }: HeaderProps) {
  return (
    <header className="flex h-14 items-center justify-between">
      <div className="flex items-baseline gap-3">
        <h1 className="text-sm font-semibold text-foreground">{title}</h1>
        {subtitle && (
          <span className="text-xs text-muted-foreground">{subtitle}</span>
        )}
      </div>
      <div className="flex items-center gap-2">
        {onRefresh && (
          <Button variant="ghost" size="icon" onClick={onRefresh} disabled={isRefreshing}>
            <RefreshCw className={`h-4 w-4 ${isRefreshing ? 'animate-spin' : ''}`} />
          </Button>
        )}
      </div>
    </header>
  )
}
