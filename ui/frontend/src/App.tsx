import React, { useEffect, useState } from 'react'
import { api } from './api'
import { Dashboard, Pipelines, RunDQ, RuleSets, Reports } from './pages'

const PAGES: [string, React.FC][] = [
  ['Dashboard', Dashboard], ['Pipelines', Pipelines], ['Run DQ', RunDQ],
  ['Rule Sets', RuleSets], ['Reports', Reports],
]

export default function App() {
  const [tab, setTab] = useState(0)
  const [health, setHealth] = useState('')
  useEffect(() => { api.health().then(h => setHealth(h.status)).catch(() => setHealth('backend offline')) }, [])
  const Page = PAGES[tab][1]
  return <>
    <header>
      <h1>DQ Accelerator</h1>
      <nav>{PAGES.map(([n], i) => <button key={n} className={i === tab ? 'active' : ''} onClick={() => setTab(i)}>{n}</button>)}</nav>
      <span style={{ marginLeft: 'auto' }} className="muted">API: <span className={health === 'ok' ? 'ok' : 'err'}>{health || '…'}</span></span>
    </header>
    <main><Page /></main>
  </>
}
