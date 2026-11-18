import React, { useEffect, useState } from 'react'
import { api } from './api'
import {
  Dashboard, Pipelines, Onboard, RunDQ,
  Anomalies, Profiles, Approvals, RuleSets, Reports, Settings, ChatPanel
} from './pages'

type Page = {
  id: string
  label: string
  icon: string
  section?: string
  component: React.FC<any>
  badge?: () => number | null
}

const PAGES: Page[] = [
  { id: 'dashboard',  label: 'Dashboard',      icon: '◈', section: 'OVERVIEW',    component: Dashboard },
  { id: 'pipelines',  label: 'Pipelines',       icon: '⬡', section: 'GOVERN',      component: Pipelines },
  { id: 'onboard',    label: 'Onboard',         icon: '＋', component: Onboard },
  { id: 'rundq',      label: 'Run DQ',          icon: '▶', component: RunDQ },
  { id: 'anomalies',  label: 'Anomalies',       icon: '⚠', section: 'OBSERVE',    component: Anomalies },
  { id: 'profiles',   label: 'Profiles',        icon: '∿', component: Profiles },
  { id: 'approvals',  label: 'Approvals',       icon: '✓', component: Approvals },
  { id: 'rulesets',   label: 'Rule Sets',       icon: '⊞', section: 'CONFIGURE',  component: RuleSets },
  { id: 'reports',    label: 'Reports',         icon: '▤', component: Reports },
  { id: 'settings',   label: 'Settings',        icon: '⚙', section: 'SYSTEM',     component: Settings },
]

// Demo-scoped nav: admin/internal tabs hidden by default; Settings → Sidebar tabs
// can re-enable any of them (stored in localStorage dq_nav).
const DEMO_HIDE = new Set<string>()
const navVisible = (id: string): boolean => {
  try {
    const n = JSON.parse(localStorage.getItem('dq_nav') || 'null')
    if (n && typeof n === 'object' && id in n) return n[id] !== false
    return !DEMO_HIDE.has(id)
  } catch { return !DEMO_HIDE.has(id) }
}

const SparkBadge = () => {
  const [master, setMaster] = useState('…')
  useEffect(() => { api.getConfig().then(c => setMaster(c?.runtime?.spark_master ?? 'local')).catch(() => setMaster('local')) }, [])
  return <span className="muted" style={{ fontSize: 12 }}>Spark {master}</span>
}

export default function App() {
  const [pageId, setPageId] = useState('dashboard')
  const [pipeFocus, setPipeFocus] = useState<string | null>(null)
  const openPipeline = (id: string) => { setPipeFocus(id); setPageId('pipelines') }
  const [health, setHealth] = useState<string>('')
  const [pendingCount, setPendingCount] = useState(0)

  useEffect(() => {
    const go = (e: Event) => setPageId((e as CustomEvent).detail)
    window.addEventListener('dq:navigate', go)
    return () => window.removeEventListener('dq:navigate', go)
  }, [])

  useEffect(() => {
    api.health().then(h => setHealth(h.status)).catch(() => setHealth('offline'))
    // poll pending approvals count for badge
    const poll = () => api.quarantinePending().then(r => setPendingCount(r.total ?? 0)).catch(() => {})
    poll()
    const t = setInterval(poll, 15000)
    return () => clearInterval(t)
  }, [])

  const page = PAGES.find(p => p.id === pageId) ?? PAGES[0]
  const PageComponent = page.component

  return (
    <div className="app-shell">
      {/* ---- Sidebar ---- */}
      <aside className="sidebar">
        <div className="sidebar-logo">
          <h1>DQ Accelerator</h1>
          <span>Data Quality Platform</span>
        </div>
        <nav className="sidebar-nav">
          {PAGES.filter(p => p.id === 'settings' || navVisible(p.id)).map(p => (
            <React.Fragment key={p.id}>
              {p.section && <div className="nav-section">{p.section}</div>}
              <div
                className={`nav-item${pageId === p.id ? ' active' : ''}`}
                onClick={() => setPageId(p.id)}
              >
                <span className="icon">{p.icon}</span>
                <span>{p.label}</span>
                {p.id === 'approvals' && pendingCount > 0 && (
                  <span className="nav-badge">{pendingCount}</span>
                )}
              </div>
            </React.Fragment>
          ))}
        </nav>
        <div className="sidebar-footer">
          <span className={`health-dot${health === 'ok' ? ' ok' : health === 'offline' ? ' err' : ''}`} />
          API {health || '…'}
        </div>
      </aside>

      {/* ---- Main area ---- */}
      <div className="main-content">
        <div className="top-bar">
          <div className="breadcrumb">
            <span>DQ Accelerator</span>
            <span> / </span>
            <b>{page.label}</b>
          </div>
          <div className="top-bar-right">
            <SparkBadge />
          </div>
        </div>
        <div className="page-content">
          <PageComponent onOpenPipeline={openPipeline} focusPipeline={pageId === 'pipelines' ? pipeFocus : null} />
        </div>
      </div>
      <ChatPanel />
    </div>
  )
}
