import { NavLink } from 'react-router-dom'
import { LayoutDashboard, List, Cpu, Wrench } from 'lucide-react'
import { cn } from '@/lib/utils'

const nav = [
  { to: '/', label: 'Overview', icon: LayoutDashboard, end: true },
  { to: '/jobs', label: 'Jobs', icon: List, end: false },
  { to: '/workers', label: 'Workers', icon: Cpu, end: false },
  { to: '/maintenance', label: 'Maintenance', icon: Wrench, end: false },
]

export function Sidebar() {
  return (
    <aside className="fixed inset-y-0 left-0 z-40 flex w-56 flex-col border-r border-border bg-card">
      <div className="flex h-14 items-center border-b border-border px-5">
        <span className="font-mono text-sm font-medium tracking-widest text-foreground">
          wrk
        </span>
        <span className="ml-2 font-mono text-xs text-muted-foreground">
          dashboard
        </span>
      </div>

      <nav className="flex-1 space-y-0.5 p-3">
        {nav.map(({ to, label, icon: Icon, end }) => (
          <NavLink
            key={to}
            to={to}
            end={end}
            className={({ isActive }) =>
              cn(
                'flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors',
                isActive
                  ? 'bg-accent text-accent-foreground'
                  : 'text-muted-foreground hover:bg-accent/50 hover:text-foreground',
              )
            }
          >
            <Icon className="h-4 w-4 shrink-0" />
            {label}
          </NavLink>
        ))}
      </nav>

      <div className="border-t border-border p-3">
        <p className="px-3 font-mono text-[10px] text-muted-foreground/50">
          v0.1.0
        </p>
      </div>
    </aside>
  )
}
