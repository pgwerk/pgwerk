import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { Toaster } from 'sonner'
import { TopNav } from '@/components/layout/TopNav'
import { OverviewPage } from '@/pages/OverviewPage'
import { JobsPage } from '@/pages/JobsPage'
import { WorkersPage } from '@/pages/WorkersPage'
import { WorkerDetailPage } from '@/pages/WorkerDetailPage'
import { CronPage } from '@/pages/CronPage'
import { MaintenancePage } from '@/pages/MaintenancePage'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 4_000,
      retry: 1,
    },
  },
})

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <div className="min-h-screen bg-background text-foreground">
          <TopNav />
          <main className="mx-auto w-full max-w-6xl px-6">
            <Routes>
              <Route path="/" element={<OverviewPage />} />
              <Route path="/jobs" element={<JobsPage />} />
              <Route path="/workers" element={<WorkersPage />} />
              <Route path="/workers/:id" element={<WorkerDetailPage />} />
              <Route path="/cron" element={<CronPage />} />
              <Route path="/maintenance" element={<MaintenancePage />} />
            </Routes>
          </main>
        </div>
      </BrowserRouter>
      <Toaster position="bottom-right" richColors />
    </QueryClientProvider>
  )
}
