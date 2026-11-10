import React, { useEffect, useRef, useState } from 'react'
import { api, Job } from './api'

/* ============================================================
   Shared hooks + tiny components
   ============================================================ */

const usePoll = (jobId: string | null, ms = 2500) => {
  const [job, setJob] = useState<Job | null>(null)
  useEffect(() => {
    if (!jobId) { setJob(null); return }
    setJob({ job_id: jobId, kind: '', status: 'QUEUED', progress: 'submitting…', submitted_at: new Date().toISOString() } as Job)
    let dead = false; let pollTimer: number | undefined
    const es = new EventSource(`/api/jobs/${jobId}/stream`)
    es.addEventListener('status', ev => {
      const d = JSON.parse(ev.data)
      setJob(prev => prev ? { ...prev, status: d.status, progress: d.progress } : prev)
      if (d.status === 'COMPLETED' || d.status === 'FAILED') {
        es.close(); api.job(jobId).then(j => { if (!dead) setJob(j) }).catch(() => {})
      }
    })
    es.addEventListener('progress', ev => {
      const d = JSON.parse(ev.data)
      setJob(prev => prev ? { ...prev, progress: d.progress } : prev)
    })
    es.onerror = () => {
      es.close(); if (pollTimer !== undefined) return
      pollTimer = window.setInterval(async () => {
        try { const j = await api.job(jobId); if (!dead) setJob(j); if (j.status === 'COMPLETED' || j.status === 'FAILED') window.clearInterval(pollTimer) }
        catch { /* keep polling */ }
      }, ms)
    }
    return () => { dead = true; es.close(); if (pollTimer !== undefined) window.clearInterval(pollTimer) }
  }, [jobId])
  return job
}

const PHASE_LABELS = ['Queued', 'Starting Spark', 'Reading data', 'Profiling', 'Validating',
  'AI remediation', 'Revalidating', 'Quarantine', 'Writing report', 'Done']
// Backend progress messages may carry an explicit "phase=<key> |" tag — preferred.
const PHASE_KEY: Record<string, number> = {
  queued: 0, spark: 1, read: 2, profile: 3, validate: 4, remediate: 5, ai: 5,
  revalidate: 6, quarantine: 7, report: 8, done: 9,
}
// Fallback regexes for untaged messages — revalidat must be tested before validat.
const PHASE_FALLBACK: Array<[RegExp, number]> = [
  [/submit|queue/i, 0], [/spark|engine/i, 1], [/read|load|sampl/i, 2], [/profil/i, 3],
  [/revalidat/i, 6], [/validat/i, 4], [/remediat|llm/i, 5], [/quarantin/i, 7],
  [/report|writ/i, 8], [/done|complet/i, 9],
]
const phaseIndex = (progress: string): number => {
  const m = /phase=(\w+)/.exec(progress || '')
  if (m && PHASE_KEY[m[1]] != null) return PHASE_KEY[m[1]]
  for (const [re, idx] of PHASE_FALLBACK) if (re.test(progress || '')) return idx
  return -1
}

const fmtClock = (ms: number) => {
  const s = Math.max(0, Math.floor(ms / 1000))
  return `${String(Math.floor(s / 60)).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`
}

const JobStatus = ({ job }: { job: Job | null }) => {
  const [now, setNow] = useState(() => Date.now())
  const active = !!job && (job.status === 'RUNNING' || job.status === 'QUEUED')
  useEffect(() => {
    if (!active) return
    const t = window.setInterval(() => setNow(Date.now()), 1000)
    return () => window.clearInterval(t)
  }, [active])
  if (!job) return null
  const t0 = Date.parse(job.started_at || job.submitted_at || '') || now
  const t1 = job.finished_at ? Date.parse(job.finished_at) : now
  const cur = phaseIndex(job.progress || '')
  const chip = (i: number) => {
    let style: React.CSSProperties
    let marker = '○'
    if (job.status === 'COMPLETED') { style = { background: 'var(--success)', color: '#fff' }; marker = '✓' }
    else if (job.status === 'FAILED') {
      if (i < cur) { style = { background: 'var(--success)', color: '#fff' }; marker = '✓' }
      else if (i === cur) { style = { background: 'var(--danger)', color: '#fff' }; marker = '✕' }
      else { style = { background: 'var(--border)', color: 'var(--muted)' } }
    } else {
      if (i < cur) { style = { background: 'var(--success)', color: '#fff', opacity: 0.75 }; marker = '✓' }
      else if (i === cur) { style = { background: 'var(--info)', color: '#fff' }; marker = '▶' }
      else { style = { background: 'var(--border)', color: 'var(--muted)' } }
    }
    return <span key={i} className="badge" style={{ fontSize: 10, ...style }}>{marker} {PHASE_LABELS[i]}</span>
  }
  const phases = job.result?.phases as Record<string, number> | undefined
  return (
    <div className="card" style={{ borderLeft: `4px solid ${job.status === 'FAILED' ? 'var(--danger)' : 'var(--info)'}` }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
        <span className={`badge ${job.status}`} style={{ fontSize: 12 }}>{job.status}</span>
        {active && <span className="spin" />}
        <b style={{ flex: 1, fontSize: 13 }}>{job.progress || 'waiting…'}</b>
        <span className="muted" style={{ fontSize: 11, fontVariantNumeric: 'tabular-nums' }}>
          {active ? `elapsed ${fmtClock(now - t0)}` : job.finished_at ? `took ${fmtClock(t1 - t0)}` : ''}
        </span>
        <span className="muted" style={{ fontSize: 11 }}>{job.job_id}</span>
      </div>
      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 10 }}>
        {PHASE_LABELS.map((_, i) => chip(i))}
      </div>
      {active && <div className="progress-bar" style={{ height: 8 }}>
        <div className="progress-fill" style={{ width: `${Math.min(95, 8 + (cur >= 0 ? cur : 0) * 10)}%`, animation: 'pulse 1.2s infinite' }} />
      </div>}
      {phases && <div className="muted" style={{ fontSize: 11, marginTop: 6 }}>
        phase timings: {Object.entries(phases).map(([k, v]) => `${k} ${(v / 1000).toFixed(1)}s`).join(' · ')}
      </div>}
      {job.error && <pre className="err" style={{ marginTop: 8 }}>{job.error}</pre>}
      {job.result?.summary && <pre style={{ marginTop: 8 }}>{job.result.summary}</pre>}
    </div>
  )
}

/* ---- Bar chart ---- */
const Bar = ({ pct, color }: { pct: number; color?: string }) => (
  <div className="bar">
    <div className="bar-fill" style={{ width: `${Math.min(100, Math.max(0, pct))}%`, background: color ?? (pct >= 90 ? '#22c55e' : pct >= 70 ? '#f59e0b' : '#ef4444') }} />
    <span className="bar-label">{pct.toFixed(1)}%</span>
  </div>
)

/* ---- Sparkline ---- */
const Sparkline = ({ data, color = '#3b82f6' }: { data: number[]; color?: string }) => {
  if (!data.length) return null
  const max = Math.max(...data, 1)
  return (
    <div className="sparkline">
      {data.map((v, i) => (
        <div key={i} className="spark-bar" style={{ height: `${Math.max(4, (v / max) * 100)}%`, background: color }} title={`${v}%`} />
      ))}
    </div>
  )
}

/* ---- Profile / anomaly views ---- */
const AnomalyCard = ({ a }: { a: any }) => (
  <div className="anomaly-card">
    <span className="tag">{a.type}</span>
    <div style={{ flex: 1 }}>
      <b style={{ color: 'var(--info)' }}>{a.column}</b>
      <span className="muted" style={{ marginLeft: 8 }}>{a.detail}</span>
    </div>
  </div>
)

const ColCard = ({ c }: { c: any }) => (
  <div className="colcard">
    <h4>{c.name}</h4>
    <div className="kv"><span>type</span><b>{c.data_type}{c.inferred_type !== c.data_type ? ` → ${c.inferred_type}` : ''}</b></div>
    <div className="kv"><span>distinct / unique</span><b>{c.distinct_count} / {c.unique_count ?? '-'}</b></div>
    {c.mean != null && <div className="kv"><span>mean ± σ</span><b>{c.mean.toFixed(2)} ± {c.stddev?.toFixed(2)}</b></div>}
    {c.min != null && <div className="kv"><span>min – max</span><b>{String(c.min).slice(0, 14)} – {String(c.max).slice(0, 14)}</b></div>}
    {c.date_min && <div className="kv"><span>dates</span><b>{c.date_min} → {c.date_max}</b></div>}
    {c.outlier_count ? <div className="kv"><span>outliers</span><b className="err">{c.outlier_count}</b></div> : null}
    {c.email_match_percentage != null && <div className="kv"><span>email match</span><b>{c.email_match_percentage.toFixed(0)}%</b></div>}
    <div style={{ marginTop: 6 }}><span className="muted">null rate</span><Bar pct={c.null_percentage} /></div>
    {c.top_values?.length ? <div className="muted" style={{ marginTop: 6 }}>top: {c.top_values.slice(0, 3).map((v: any) => `${v.value ?? v[0]} (${v.count ?? v[1]})`).join(', ')}</div> : null}
  </div>
)

const ProfileView = ({ r, jid }: { r: any; jid?: string }) => (
  <div className="card">
    <h3>Profile{jid ? ` · ${jid}` : ''} <span className="muted">— {r.row_count?.toLocaleString()} rows × {r.column_count} cols {r.profile_mode ? `(${r.profile_mode})` : ''}</span></h3>
    {r.pk_candidates?.length > 0 && <p style={{ fontSize: 12, marginBottom: 10 }}>PK candidates: {r.pk_candidates.map((k: string) => <span key={k} className="badge ACTIVE" style={{ marginRight: 4 }}>{k}</span>)}</p>}
    {(r.anomalies?.length > 0 || r.schema_anomalies?.length > 0) && <>
      <h4>Anomalies ({(r.anomalies?.length || 0) + (r.schema_anomalies?.length || 0)})</h4>
      {[...(r.anomalies || []), ...(r.schema_anomalies || [])].map((a: any, i: number) => <AnomalyCard a={a} key={i} />)}
    </>}
    <div className="colgrid">{(r.columns || []).map((c: any) => <ColCard c={c} key={c.name} />)}</div>
  </div>
)

const RulesView = ({ rules }: { rules: any[] }) => !rules?.length ? null : (
  <div className="card">
    <h3>Rule results <span className="muted">— {rules.filter(r => r.status === 'PASSED').length}/{rules.length} passed</span></h3>
    {rules.map(r => (
      <div className="rule-row" key={r.rule_id}>
        <span>{r.rule_id}</span>
        <span className={`badge ${r.status === 'PASSED' ? 'COMPLETED' : r.severity === 'ERROR' ? 'FAILED' : 'REGISTERED'}`}>{r.status}</span>
        <Bar pct={r.pass_pct} />
        <span className="muted">{r.passed}/{r.total} · {r.column ?? r.rule_type}</span>
      </div>
    ))}
  </div>
)

const PipelineRunView = ({ r, jid }: { r: any; jid?: string }) => {
  const scores = (r.stages || []).map((s: any) => s.quality_score).filter((v: any) => v != null)
  const avgScore = scores.length ? scores.reduce((a: number, b: number) => a + b, 0) / scores.length : null
  const outcomeBadges = (r.stages || []).flatMap((s: any) =>
    Object.entries(s.outcome_counts || {}).map(([k, v]: any) => `${s.stage} · ${k}: ${v}`))
  const recs: string[] = [...new Set((r.stages || []).map((s: any) => s.recommendation).filter(Boolean))] as string[]
  const paths = Object.entries(r.written_paths || {})
  return (
  <div className="card">
    <h3>Pipeline run <span className="muted">— {jid} · {r.pipeline_id} / {r.dataset} [{r.mode}]</span></h3>
    <div className="stat-grid" style={{ marginBottom: 12 }}>
      <div className="stat-tile"><div className="num">{r.stages?.length ?? 0}</div><div className="lbl">Stages</div></div>
      <div className="stat-tile"><div className={`num ${r.gate_passed ? 'ok' : 'bad'}`}>{r.gate_passed ? 'PASS' : 'FAIL'}</div><div className="lbl">Overall gate</div></div>
      <div className="stat-tile"><div className={`num ${avgScore != null && avgScore >= 90 ? 'ok' : avgScore != null && avgScore >= 70 ? 'warn' : 'bad'}`}>{avgScore != null ? avgScore.toFixed(1) : '—'}</div><div className="lbl">Avg quality score</div></div>
      <div className="stat-tile"><div className="num">{r.halted ? 'HALTED' : 'DONE'}</div><div className="lbl">Flow</div></div>
      <div className="stat-tile"><div className="num">{((r.duration_ms ?? 0) / 1000).toFixed(1)}s</div><div className="lbl">Duration</div></div>
    </div>
    <table className="tbl">
      <thead><tr><th>Stage</th><th>Status</th><th>Ruleset</th><th>Input</th><th>Accepted</th><th>Quarantined</th><th>Gate</th><th>Score</th><th>Time</th></tr></thead>
      <tbody>
        {(r.stages || []).map((s: any) => (
          <tr key={s.stage}>
            <td><b>{s.stage}</b></td>
            <td><span className={`badge ${s.status === 'COMPLETED' ? 'COMPLETED' : s.status === 'SKIPPED' ? 'REGISTERED' : 'FAILED'}`}>{s.status}</span></td>
            <td className="muted">{s.ruleset ?? '—'}</td>
            <td>{s.input?.toLocaleString() ?? '—'}</td>
            <td className="ok">{s.accepted?.toLocaleString() ?? '—'}</td>
            <td className="bad">{s.quarantined?.toLocaleString() ?? '—'}</td>
            <td>{s.gate_passed == null ? '—' : s.gate_passed ? <span className="ok">PASS</span> : <span className="bad">FAIL</span>}</td>
            <td>{s.quality_score != null ? <span className={s.quality_score >= 90 ? 'ok' : 'warn'}>{s.quality_score.toFixed(1)}</span> : '—'}</td>
            <td className="muted">{s.duration_ms ? `${(s.duration_ms / 1000).toFixed(1)}s` : '—'}</td>
          </tr>
        ))}
      </tbody>
    </table>
    {outcomeBadges.length > 0 && (
      <div style={{ marginTop: 10 }}>
        <h4>Remediation outcomes</h4>
        {outcomeBadges.map((b: string) => <span key={b} className="tag" style={{ marginRight: 6, marginBottom: 4 }}>{b}</span>)}
      </div>
    )}
    {recs.map((rec: string, i: number) => <div key={i} className="muted" style={{ marginTop: 6, fontSize: 12 }}>Recommendation — {rec}</div>)}
    {paths.length > 0 && (
      <details style={{ marginTop: 10 }}>
        <summary className="muted" style={{ cursor: 'pointer' }}>Written outputs ({paths.length})</summary>
        <table className="tbl" style={{ marginTop: 6 }}>
          <tbody>
            {paths.map(([k, v]: any) => (
              <tr key={k}><td className="muted" style={{ width: 220 }}>{k}</td><td style={{ fontSize: 11, wordBreak: 'break-all' }}>{String(v)}</td></tr>
            ))}
          </tbody>
        </table>
      </details>
    )}
    {r.summary && <pre style={{ marginTop: 10 }}>{r.summary}</pre>}
  </div>
  )
}

const JobResult = ({ job }: { job: Job | null }) => {
  const r = job?.result
  if (!job || !r) return null
  if (job.kind === 'profile' || job.kind === 'bootstrap_preview') return <ProfileView r={r} jid={job.job_id} />
  if (job.kind === 'pipeline_run') return <PipelineRunView r={r} jid={job.job_id} />
  const score = r.input ? Math.round(r.accepted / r.input * 100) : null
  return <>
    <div className="card">
      <h3>DQ Result <span className="muted">— {job.job_id}</span></h3>
      <div className="stat-grid">
        <div className="stat-tile"><div className="num">{r.input?.toLocaleString()}</div><div className="lbl">Input rows</div></div>
        <div className="stat-tile"><div className="num ok">{r.accepted?.toLocaleString()}</div><div className="lbl">Accepted</div></div>
        <div className="stat-tile"><div className="num bad">{r.quarantined?.toLocaleString()}</div><div className="lbl">Quarantined</div></div>
        <div className="stat-tile"><div className={`num ${r.gate_passed ? 'ok' : 'bad'}`}>{r.gate_passed ? 'PASS' : 'FAIL'}</div><div className="lbl">Quality gate</div></div>
        {score != null && <div className="stat-tile"><div className={`num ${score >= 90 ? 'ok' : score >= 70 ? 'warn' : 'bad'}`}>{score}%</div><div className="lbl">Acceptance rate</div></div>}
      </div>
      {r.quarantine_by_status && Object.keys(r.quarantine_by_status).length > 0 && (
        <div style={{ marginTop: 10 }}>
          <h4>Quarantine breakdown</h4>
          {Object.entries(r.quarantine_by_status).map(([k, v]: any) => (
            <div key={k} style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 4, fontSize: 12 }}>
              <span className="tag">{k}</span><b>{v}</b>
            </div>
          ))}
        </div>
      )}
      <p className="muted" style={{ marginTop: 8 }}>Report: <code>{r.report_path}</code></p>
    </div>
    <RulesView rules={r.rules ?? []} />
  </>
}

/* ---- Lifecycle stepper ---- */
const LC_MAIN = ['REGISTERED', 'TESTED', 'ACTIVE']
const LC_NEXT: Record<string, string> = {
  REGISTERED: 'Next: test — validates the manifest test_data through the engine',
  TESTED:     'Next: activate — promotes to governed ACTIVE',
  ACTIVE:     'Ready — run DQ / Profile from Run DQ tab; pause or offboard anytime',
  PAUSED:     'Paused — activate resumes; offboard also allowed',
  OFFBOARDED: 'Offboarded — history retained; re-onboard starts fresh',
  DEACTIVATING: 'Deactivating → OFFBOARDED',
}
const Lifecycle = ({ state }: { state: string }) => {
  const side = state === 'PAUSED' || state === 'OFFBOARDED' || state === 'DEACTIVATING'
  const cur = LC_MAIN.indexOf(state)
  return <>
    <div className="steps">
      {LC_MAIN.map((s, i) => {
        const cls = side || i < cur ? 'done' : i === cur ? 'current' : ''
        return <React.Fragment key={s}>
          {i > 0 && <div className={`conn ${side || i <= cur ? 'done' : ''}`} />}
          <div className={`step ${cls}`}><span className="dot">{side || i < cur ? '✓' : i + 1}</span><span className="lbl">{s}</span></div>
        </React.Fragment>
      })}
      {side && <><div className="conn done" /><div className={`step ${state === 'PAUSED' ? 'warned' : 'term'}`}><span className="dot">{state === 'PAUSED' ? '⏸' : '✕'}</span><span className="lbl">{state}</span></div></>}
    </div>
    <p className="muted">{LC_NEXT[state] ?? state}</p>
  </>
}

/* ============================================================
   DASHBOARD  — 100% real data from /api/dashboard/summary
   ============================================================ */
const DonutBar = ({ label, value, total, color }: { label: string; value: number; total: number; color: string }) => {
  const pct = total ? Math.round(value / total * 100) : 0
  return (
    <div style={{ marginBottom: 8 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, marginBottom: 3 }}>
        <span style={{ color: 'var(--text2)' }}>{label}</span>
        <span style={{ fontWeight: 700, color }}>{value.toLocaleString()} <span className="muted">({pct}%)</span></span>
      </div>
      <div className="bar"><div className="bar-fill" style={{ width: `${pct}%`, background: color }} /></div>
    </div>
  )
}

export function Dashboard({ onOpenPipeline }: { onOpenPipeline?: (id: string) => void } = {}) {
  const [summary, setSummary] = useState<any>(null)
  const [pipes, setPipes] = useState<any[]>([])
  const [jobs, setJobs] = useState<Job[]>([])
  const [anomStats, setAnoStats] = useState<any>({})
  const [viewJob, setViewJob] = useState<Job | null>(null)
  const [trends, setTrends] = useState<Record<string, any[]>>({})
  const [loading, setLoading] = useState(true)

  const load = () => {
    api.dashboardSummary().then(d => { setSummary(d); setLoading(false) }).catch(() => setLoading(false))
    api.pipelines().then(setPipes)
    api.jobs().then(setJobs)
    api.anomalyStats().then(setAnoStats).catch(() => {})
  }
  useEffect(() => { load(); const t = setInterval(load, 15000); return () => clearInterval(t) }, [])
  useEffect(() => {
    pipes.slice(0, 8).forEach(p =>
      api.pipelineTrend(p.pipeline_id).then(t => setTrends(prev => ({ ...prev, [p.pipeline_id]: t }))).catch(() => {})
    )
  }, [pipes.length])

  const t = summary?.totals ?? {}
  const scoreVal: number | null = t.overall_pass_rate ?? null
  const scoreClass = scoreVal == null ? '' : scoreVal >= 90 ? 'hi' : scoreVal >= 70 ? 'mid' : 'low'

  return <>
    <div className="section-title">Data Quality Command Centre</div>
    {loading && <div className="info-box">Loading real metrics from {'{'}reports/{'}'}… <span className="spin" /></div>}

    {/* ── Row 1: headline KPIs ── */}
    <div className="stat-grid" style={{ gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))' }}>
      <div className="stat-tile"><div className={`num ${scoreClass}`}>{scoreVal != null ? scoreVal + '%' : '—'}</div><div className="lbl">Acceptance rate</div></div>
      <div className="stat-tile"><div className="num">{(t.total_runs ?? 0)}</div><div className="lbl">Total DQ runs</div></div>
      <div className="stat-tile"><div className="num ok">{(t.gates_passed ?? 0)}</div><div className="lbl">Gates passed</div></div>
      <div className="stat-tile"><div className="num bad">{((t.total_runs ?? 0) - (t.gates_passed ?? 0))}</div><div className="lbl">Gates failed</div></div>
      <div className="stat-tile"><div className="num">{(t.input_records ?? 0).toLocaleString()}</div><div className="lbl">Total rows processed</div></div>
      <div className="stat-tile"><div className="num ok">{(t.accepted_records ?? 0).toLocaleString()}</div><div className="lbl">Total accepted</div></div>
      <div className="stat-tile"><div className="num bad">{(t.quarantined_records ?? 0).toLocaleString()}</div><div className="lbl">Total quarantined</div></div>
      <div className="stat-tile"><div className="num warn">{anomStats['OPEN'] ?? 0}</div><div className="lbl">Open anomalies</div></div>
    </div>

    {/* ── Row 2: processing breakdown + remediation ── */}
    {t.input_records > 0 && <div className="card-row">
      <div className="card">
        <h3>Processing breakdown <span className="muted">— all runs combined</span></h3>
        <DonutBar label="Accepted" value={t.accepted_records ?? 0} total={t.input_records ?? 0} color="#22c55e" />
        <DonutBar label="Quarantined" value={t.quarantined_records ?? 0} total={t.input_records ?? 0} color="#ef4444" />
        <DonutBar label="Duplicates removed" value={t.duplicates_removed ?? 0} total={t.input_records ?? 0} color="#94a3b8" />
        <div className="divider" />
        <div style={{ display: 'flex', gap: 24, fontSize: 12, flexWrap: 'wrap', marginTop: 8 }}>
          <span>Rules evaluated: <b>{(t.rules_evaluated ?? 0).toLocaleString()}</b></span>
          <span>Rules failed: <b style={{ color: 'var(--danger)' }}>{(t.rules_failed ?? 0).toLocaleString()}</b></span>
          <span>Rule pass rate: <b style={{ color: 'var(--success)' }}>{t.rules_evaluated ? Math.round((t.rules_evaluated - t.rules_failed) / t.rules_evaluated * 100) + '%' : '—'}</b></span>
        </div>
      </div>
      <div className="card">
        <h3>Remediation summary</h3>
        <div className="stat-grid" style={{ gridTemplateColumns: '1fr 1fr', gap: 10 }}>
          <div className="stat-tile"><div className="num ok">{t.deterministic_fixes ?? 0}</div><div className="lbl">Deterministic fixes</div></div>
          <div className="stat-tile"><div className="num warn">{t.ai_fixes ?? 0}</div><div className="lbl">AI fixes applied</div></div>
          <div className="stat-tile"><div className="num">{t.duplicates_removed ?? 0}</div><div className="lbl">Duplicates removed</div></div>
          <div className="stat-tile"><div className="num">{(t.gate_pass_rate ?? 0)}%</div><div className="lbl">Gate pass rate</div></div>
        </div>
        {summary?.mode_distribution && Object.keys(summary.mode_distribution).length > 0 && <>
          <div className="divider" />
          <h4>Runs by mode</h4>
          {Object.entries(summary.mode_distribution).map(([m, c]: any) => (
            <div key={m} style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, padding: '3px 0' }}>
              <span className="tag">{m}</span><b>{c}</b>
            </div>
          ))}
        </>}
      </div>
    </div>}

    {/* ── Row 3: quarantine breakdown + top failing rules ── */}
    {summary && <div className="card-row">
      {Object.keys(summary.quarantine_breakdown ?? {}).length > 0 && <div className="card">
        <h3>Quarantine breakdown <span className="muted">— all runs</span></h3>
        {Object.entries(summary.quarantine_breakdown).map(([k, v]: any) => (
          <div key={k} style={{ marginBottom: 8 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, marginBottom: 3 }}>
              <span className="tag">{k.replace(/_/g, ' ')}</span><b>{v}</b>
            </div>
            <div className="bar" style={{ height: 12 }}>
              <div className="bar-fill" style={{ width: `${Math.round(v / (t.quarantined_records || 1) * 100)}%`, background: k === 'PENDING_HUMAN_APPROVAL' ? '#f59e0b' : k.includes('BELOW') ? '#94a3b8' : '#ef4444' }} />
            </div>
          </div>
        ))}
      </div>}

      {(summary.top_failing_rules ?? []).length > 0 && <div className="card">
        <h3>Top failing rules <span className="muted">— across all datasets</span></h3>
        {summary.top_failing_rules.slice(0, 8).map((r: any) => (
          <div className="rule-row" key={r.rule_id}>
            <span style={{ minWidth: 120, fontFamily: 'Consolas, monospace', fontSize: 11 }}>{r.rule_id}</span>
            <span className="muted">{r.datasets?.join(', ')}</span>
            <div className="bar" style={{ flex: 1 }}>
              <div className="bar-fill" style={{ width: `${r.fail_rate}%`, background: '#ef4444' }} />
              <span className="bar-label">{r.fail_rate}%</span>
            </div>
            <span className="muted">{r.failed}/{r.total} runs</span>
          </div>
        ))}
      </div>}
    </div>}

    {/* ── Row 4: per-dataset processing table (REAL data) ── */}
    {(summary?.by_dataset ?? []).length > 0 && <div className="card">
      <h3>Dataset processing summary <span className="muted">— real data from all {summary.totals?.total_runs ?? 0} runs + {summary.profile_runs ?? 0} profiles</span></h3>
      <table>
        <thead><tr><th>Dataset</th><th>Runs</th><th>Total rows</th><th>Accepted</th><th>Quarantined</th><th>Acceptance %</th><th>Gate pass%</th><th>Det. fixes</th><th>AI fixes</th><th>Last run</th></tr></thead>
        <tbody>
          {summary.by_dataset.map((d: any) => (
            <tr key={d.dataset}>
              <td><b>{d.dataset}</b><div className="muted">{d.modes?.join(' · ')}</div></td>
              <td>{d.runs}</td>
              <td>{(d.input ?? 0).toLocaleString()}</td>
              <td style={{ color: 'var(--success)', fontWeight: 600 }}>{(d.accepted ?? 0).toLocaleString()}</td>
              <td style={{ color: 'var(--danger)', fontWeight: 600 }}>{(d.quarantined ?? 0).toLocaleString()}</td>
              <td>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <div className="bar" style={{ width: 80, flex: 'none' }}>
                    <div className="bar-fill" style={{ width: `${d.pass_rate ?? 0}%`, background: (d.pass_rate ?? 0) >= 90 ? '#22c55e' : (d.pass_rate ?? 0) >= 70 ? '#f59e0b' : '#ef4444' }} />
                    <span className="bar-label">{d.pass_rate ?? '—'}%</span>
                  </div>
                </div>
              </td>
              <td>{d.gate_pass_rate != null ? d.gate_pass_rate + '%' : '—'}</td>
              <td>{d.det_fixes ?? 0}</td>
              <td>{d.ai_fixes ?? 0}</td>
              <td className="muted">{d.last_run ? new Date(d.last_run).toLocaleDateString() : '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>}

    {/* ── Row 5: pipeline status grid with real sparklines ── */}
    <div className="card">
      <h3>Pipeline registry</h3>
      {pipes.length === 0 && <div className="empty-state"><div className="icon">⬡</div><p>No pipelines yet — use Onboard to add one.</p></div>}
      <div className="pipe-grid">
        {pipes.map(p => {
          const trd = trends[p.pipeline_id] || []
          const rates = trd.map((r: any) => r.pass_rate ?? 0)
          const last = trd[trd.length - 1]
          const dsRow = (summary?.by_dataset ?? []).find((d: any) => d.dataset === p.pipeline_id?.split('_')[0]?.toLowerCase())
          return (
            <div className="pipe-card" key={p.pipeline_id}
              style={{ cursor: onOpenPipeline ? 'pointer' : 'default' }}
              title={onOpenPipeline ? 'Open pipeline detail' : undefined}
              onClick={() => onOpenPipeline?.(p.pipeline_id)}>
              <div className="pipe-card-header">
                <div>
                  <div className="pipe-card-name">{p.pipeline_name ?? p.pipeline_id}</div>
                  <div className="pipe-card-platform">{p.platform} · {p.dq_mode}</div>
                </div>
                <span className={`badge ${p.state}`}>{p.state}</span>
              </div>
              <div className="pipe-card-stats">
                {last && <span style={{ color: (last.pass_rate ?? 0) >= 90 ? 'var(--success)' : 'var(--warn)' }}>
                  Last: {last.pass_rate != null ? last.pass_rate + '%' : '—'}
                </span>}
                <span>{(p.executions ?? 0)} runs</span>
                {last?.gate_passed != null && <span className={`badge ${last.gate_passed ? 'COMPLETED' : 'FAILED'}`} style={{ padding: '1px 6px', fontSize: 10 }}>{last.gate_passed ? 'PASS' : 'FAIL'}</span>}
              </div>
              {rates.length > 1 && <div style={{ marginTop: 10 }}>
                <Sparkline data={rates} color={rates[rates.length-1] >= 90 ? '#22c55e' : '#f59e0b'} />
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, color: 'var(--text3)', marginTop: 2 }}>
                  <span>{rates[0]}%</span><span>→</span><span>{rates[rates.length-1]}%</span>
                </div>
              </div>}
            </div>
          )
        })}
      </div>
    </div>

    {/* ── Row 6: run timeline ── */}
    {(summary?.timeline ?? []).length > 0 && <div className="card">
      <h3>Recent DQ runs <span className="muted">— from reports on disk (real execution data)</span></h3>
      <table>
        <thead><tr><th>Run ID</th><th>Dataset</th><th>Mode</th><th>Input</th><th>Accepted</th><th>Quarantined</th><th>Pass rate</th><th>Gate</th><th>Det.fix</th><th>AI fix</th><th>Duration</th><th>Finished</th></tr></thead>
        <tbody>
          {summary.timeline.map((r: any) => (
            <tr key={r.run_id}>
              <td><code style={{ fontSize: 10 }}>{r.run_id}</code></td>
              <td><span className="tag">{r.dataset}</span></td>
              <td><span className="tag">{r.mode}</span></td>
              <td>{r.input?.toLocaleString()}</td>
              <td style={{ color: 'var(--success)', fontWeight: 600 }}>{r.accepted?.toLocaleString()}</td>
              <td style={{ color: r.quarantined > 0 ? 'var(--danger)' : 'var(--text3)' }}>{r.quarantined?.toLocaleString()}</td>
              <td>
                <div className="bar" style={{ width: 70, flex: 'none' }}>
                  <div className="bar-fill" style={{ width: `${r.pass_rate ?? 0}%`, background: (r.pass_rate ?? 0) >= 90 ? '#22c55e' : (r.pass_rate ?? 0) >= 70 ? '#f59e0b' : '#ef4444' }} />
                  <span className="bar-label">{r.pass_rate}%</span>
                </div>
              </td>
              <td><span className={`badge ${r.gate_passed ? 'COMPLETED' : 'FAILED'}`}>{r.gate_passed ? 'PASS' : 'FAIL'}</span></td>
              <td>{r.det_fixes ?? 0}</td>
              <td>{r.ai_fixes ?? 0}</td>
              <td className="muted">{r.duration_s}s</td>
              <td className="muted">{r.finished_at ? new Date(r.finished_at).toLocaleString() : '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>}

    {/* ── Recent jobs (for running/queued status) ── */}
    <div className="card">
      <h3>Job queue</h3>
      <table>
        <thead><tr><th>Job</th><th>Type</th><th>Status</th><th>Finished</th><th>Progress</th><th></th></tr></thead>
        <tbody>
          {jobs.slice(0, 8).map(j => (
            <tr key={j.job_id}>
              <td><code style={{ fontSize: 11 }}>{j.job_id}</code></td>
              <td><span className="tag">{j.kind}</span></td>
              <td><span className={`badge ${j.status}`}>{j.status}</span>{j.status === 'RUNNING' && <span className="spin" />}</td>
              <td className="muted">{j.finished_at ? new Date(j.finished_at).toLocaleString() : '—'}</td>
              <td className="muted">{j.progress}</td>
              <td>{j.status === 'COMPLETED' && <button className="btn sm sec" onClick={() => api.job(j.job_id).then(setViewJob)}>View</button>}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>

    {viewJob && <>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
        <h3>Job result — {viewJob.job_id}</h3>
        <button className="btn sec sm" onClick={() => setViewJob(null)}>✕</button>
      </div>
      <JobResult job={viewJob} />
      {viewJob.kind === 'profile' && <ProfileView r={viewJob.result} jid={viewJob.job_id} />}
    </>}
  </>
}

/* ============================================================
   PIPELINES
   ============================================================ */
export function Pipelines({ focusPipeline }: { focusPipeline?: string | null } = {}) {
  const [pipes, setPipes] = useState<any[]>([])
  const [sel, setSel] = useState<any>(null)
  const [metrics, setMetrics] = useState<any>(null)
  const [yaml, setYaml] = useState('')
  const [msg, setMsg] = useState(''); const [err, setErr] = useState('')
  const [trend, setTrend] = useState<any[]>([])

  const load = () => api.pipelines().then(setPipes)
  useEffect(() => { load() }, [])

  const onboard = async () => {
    setMsg(''); setErr('')
    try { await api.onboard(yaml); setMsg('Onboarded successfully'); load() }
    catch (e: any) { setErr(e.message) }
  }
  const act = async (id: string, action: string) => {
    setMsg(''); setErr('')
    try { await api.pipelineAction(id, action); setMsg(`${id} → ${action}`); load(); if (sel?.pipeline_id === id) setSel(await api.pipeline(id)) }
    catch (e: any) { setErr(e.message) }
  }
  const open = async (id: string) => {
    const d = await api.pipeline(id); setSel(d)
    api.pipelineTrend(id).then(setTrend).catch(() => setTrend([]))
    api.pipelineMetrics(id).then(setMetrics).catch(() => setMetrics(null))
  }
  useEffect(() => { if (focusPipeline) open(focusPipeline) }, [focusPipeline])

  const ACTIONS: Record<string, string[]> = {
    REGISTERED: ['test', 'offboard'],
    TESTED:     ['activate', 'test', 'offboard'],
    ACTIVE:     ['pause', 'offboard'],
    PAUSED:     ['activate', 'test', 'offboard'],
    OFFBOARDED: [],
  }

  return <>
    <div className="section-title">Pipeline Registry</div>

    <div className="card">
      <h3>Registered pipelines</h3>
      {pipes.length === 0 && <div className="empty-state"><div className="icon">⬡</div><p>No pipelines yet — onboard one below or use the Onboard wizard.</p></div>}
      <table>
        <thead><tr><th>Pipeline ID</th><th>Platform</th><th>DQ Mode</th><th>State</th><th>Updated</th><th></th></tr></thead>
        <tbody>
          {pipes.map(p => (
            <tr key={p.pipeline_id} style={{ cursor: 'pointer' }} onClick={() => open(p.pipeline_id)}>
              <td><b>{p.pipeline_id}</b><div className="muted">{p.pipeline_name}</div></td>
              <td><span className="tag">{p.platform}</span></td>
              <td><span className="tag">{p.dq_mode}</span></td>
              <td><span className={`badge ${p.state}`}>{p.state}</span></td>
              <td className="muted">{p.updated_at ? new Date(p.updated_at).toLocaleString() : '—'}</td>
              <td style={{ whiteSpace: 'nowrap' }} onClick={e => e.stopPropagation()}>
                {(ACTIONS[p.state] ?? []).map(a => (
                  <button key={a} className={`btn sm ${a === 'offboard' ? 'danger' : a === 'activate' ? 'success' : 'sec'}`}
                    onClick={() => act(p.pipeline_id, a)} style={{ marginRight: 4 }}>{a}</button>
                ))}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {msg && <div className="success-box" style={{ marginTop: 10 }}>{msg}</div>}
      {err && <div className="warn-box" style={{ marginTop: 10 }}>{err}</div>}
    </div>

    {sel && (
      <div className="card">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
          <h3>{sel.pipeline_id} <span className={`badge ${sel.state}`}>{sel.state}</span></h3>
          <button className="btn sm sec" onClick={() => setSel(null)}>✕</button>
        </div>
        <Lifecycle state={sel.state} />
        <div className="form-grid" style={{ marginTop: 12 }}>
          <div className="kv"><span>Platform</span><b>{sel.platform}</b></div>
          <div className="kv"><span>DQ Mode</span><b>{sel.dq_mode}</b></div>
          <div className="kv"><span>Owner</span><b>{sel.owner || '—'}</b></div>
          <div className="kv"><span>Executions</span><b>{sel.executions?.length ?? 0}</b></div>
        </div>
        {trend.length > 1 && <>
          <h4>Pass-rate trend (last {trend.length} runs)</h4>
          <Sparkline data={trend.map((r: any) => r.pass_rate ?? 0)} />
          <div style={{ display: 'flex', gap: 20, marginTop: 8, fontSize: 12, color: 'var(--text3)' }}>
            {trend.slice(-5).map((r: any) => <span key={r.job_id}>{new Date(r.ran_at).toLocaleDateString()} {r.pass_rate != null ? r.pass_rate + '%' : '—'}</span>)}
          </div>
        </>}
        {sel.history?.length > 0 && <>
          <h4>Lifecycle history</h4>
          <table><thead><tr><th>From</th><th>To</th><th>By</th><th>Reason</th><th>At</th></tr></thead>
            <tbody>{[...sel.history].reverse().slice(0, 8).map((h: any, i: number) => (
              <tr key={i}><td>{h.from ?? '—'}</td><td><span className={`badge ${h.to}`}>{h.to}</span></td><td>{h.by}</td><td className="muted">{h.reason}</td><td className="muted">{h.at ? new Date(h.at).toLocaleString() : ''}</td></tr>
            ))}</tbody>
          </table>
        </>}
        {sel.executions?.length > 0 && <>
          <h4>Recent executions</h4>
          <table><thead><tr><th>Run ID</th><th>Dataset</th><th>Mode</th><th>Input</th><th>Accepted</th><th>Gate</th><th>Finished</th></tr></thead>
            <tbody>{[...sel.executions].reverse().slice(0, 5).map((e: any, i: number) => (
              <tr key={i}><td><code style={{ fontSize: 11 }}>{e.dq_run_id?.slice(0, 20)}</code></td><td>{e.dataset}</td><td>{e.mode}</td><td>{e.input?.toLocaleString()}</td><td>{e.accepted?.toLocaleString()}</td><td><span className={`badge ${e.gate_passed ? 'COMPLETED' : 'FAILED'}`}>{e.gate_passed ? 'PASS' : 'FAIL'}</span></td><td className="muted">{e.finished_at ? new Date(e.finished_at).toLocaleString() : ''}</td></tr>
            ))}</tbody>
          </table>
        </>}
        {metrics && metrics.runs > 0 && <>
          <div className="divider" />
          <h4>Detailed metrics from reports <span className="muted">— {metrics.runs} runs · {(metrics.total_input ?? 0).toLocaleString()} rows total</span></h4>
          <div className="stat-grid" style={{ gridTemplateColumns: 'repeat(4, 1fr)', marginTop: 10 }}>
            <div className="stat-tile"><div className="num ok">{metrics.overall_pass_rate ?? '—'}%</div><div className="lbl">Acceptance rate</div></div>
            <div className="stat-tile"><div className="num">{(metrics.total_input ?? 0).toLocaleString()}</div><div className="lbl">Total rows</div></div>
            <div className="stat-tile"><div className="num ok">{(metrics.total_accepted ?? 0).toLocaleString()}</div><div className="lbl">Accepted</div></div>
            <div className="stat-tile"><div className="num bad">{(metrics.total_quarantined ?? 0).toLocaleString()}</div><div className="lbl">Quarantined</div></div>
          </div>
          {metrics.executions?.slice(0, 3).map((ex: any, i: number) => (
            <div key={i} style={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 6, padding: '10px 14px', marginTop: 10 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                <code style={{ fontSize: 11, color: 'var(--info)' }}>{ex.run_id?.slice(0, 28)}</code>
                <div style={{ display: 'flex', gap: 6 }}>
                  <span className="tag">{ex.mode}</span>
                  <span className={`badge ${ex.gate_passed ? 'COMPLETED' : 'FAILED'}`}>{ex.gate_passed ? 'PASS' : 'FAIL'}</span>
                </div>
              </div>
              <div style={{ display: 'flex', gap: 20, fontSize: 12, flexWrap: 'wrap' }}>
                <span>{ex.input?.toLocaleString()} in → <span style={{ color: 'var(--success)', fontWeight: 700 }}>{ex.accepted?.toLocaleString()} accepted</span> · <span style={{ color: 'var(--danger)', fontWeight: 700 }}>{ex.quarantined?.toLocaleString()} quarantined</span></span>
                <span>Det.fix: {ex.det_fixes ?? 0} · AI: {ex.ai_fixes ?? 0} · Dups: {ex.removed_duplicates ?? 0}</span>
                <span className="muted">{ex.duration_s}s · {ex.rules_evaluated} rules ({ex.rules_failed} failed)</span>
              </div>
              {Object.keys(ex.phase_durations_s ?? {}).length > 0 && <div style={{ display: 'flex', gap: 12, fontSize: 11, color: 'var(--text3)', marginTop: 6 }}>
                {Object.entries(ex.phase_durations_s).map(([ph, s]: any) => <span key={ph}>{ph}: {s}s</span>)}
              </div>}
              {ex.rules?.filter((r: any) => r.status === 'FAILED').length > 0 && <div style={{ marginTop: 8 }}>
                <div style={{ fontSize: 11, color: 'var(--text3)', marginBottom: 4 }}>Failing rules:</div>
                {ex.rules.filter((r: any) => r.status === 'FAILED').slice(0, 5).map((r: any) => (
                  <div className="rule-row" key={r.rule_id} style={{ padding: '4px 0' }}>
                    <span style={{ minWidth: 110, fontSize: 11 }}>{r.rule_id}</span>
                    <span className="tag" style={{ fontSize: 10 }}>{r.column ?? '—'}</span>
                    <div className="bar" style={{ flex: 1, height: 14 }}>
                      <div className="bar-fill" style={{ width: `${r.pass_pct ?? 0}%`, background: '#ef4444' }} />
                      <span className="bar-label" style={{ fontSize: 10 }}>{r.pass_pct?.toFixed(1)}%</span>
                    </div>
                    <span className="muted" style={{ fontSize: 10 }}>{r.failed}/{r.total}</span>
                  </div>
                ))}
              </div>}
            </div>
          ))}
        </>}
      </div>
    )}

    <div className="card">
      <h3>Onboard by manifest YAML</h3>
      <p className="muted">Paste a pipeline manifest YAML directly. For the guided wizard use the Onboard section.</p>
      <textarea value={yaml} onChange={e => setYaml(e.target.value)} placeholder="pipeline:&#10;  pipeline_id: MY_PIPELINE&#10;  platform: DATABRICKS&#10;  ..." />
      <div className="btn-row">
        <button className="btn" onClick={onboard} disabled={!yaml.trim()}>Onboard pipeline</button>
      </div>
    </div>
  </>
}

/* ============================================================
   ONBOARD WIZARD
   ============================================================ */
const WIZ_STEPS = ['Source', 'Options', 'Profile', 'Review rules', 'Finalize']
const resolveRel = (rel: string) => {
  const out: string[] = []
  for (const s of `config/pipelines/${rel}`.split('/')) {
    if (s === '..') out.pop(); else if (s && s !== '.') out.push(s)
  }
  return out.join('/')
}

export function Onboard() {
  const [step, setStep] = useState(0)
  const [datasets, setDatasets] = useState<any[]>([])
  const [source, setSource] = useState(''); const [dataset, setDataset] = useState('')
  const [pipelineId, setPipelineId] = useState(''); const [platform, setPlatform] = useState('DATABRICKS')
  const [stage, setStage] = useState('SILVER'); const [dqMode, setDqMode] = useState('OBSERVE_ONLY')
  const [policy, setPolicy] = useState('../policies/default.yaml'); const [owner, setOwner] = useState('')
  const [pub, setPub] = useState(true); const [onb, setOnb] = useState(true); const [force, setForce] = useState(false)
  const [jobId, setJobId] = useState<string | null>(null)
  const [approved, setApproved] = useState<Set<string>>(new Set())
  const [result, setResult] = useState<any>(null); const [err, setErr] = useState('')
  const job = usePoll(jobId)
  const preview = job?.status === 'COMPLETED' ? job.result : null

  useEffect(() => { api.datasets().then(setDatasets) }, [])
  useEffect(() => { if (preview?.candidates) setApproved(new Set(preview.candidates.map((c: any) => c.rule_id))) }, [job?.status])

  const pickDs = (path: string, name: string) => { setSource(path); setDataset(name); if (!pipelineId) setPipelineId(`${name.toUpperCase()}_PIPELINE`) }
  const upload = async (f: File) => { setErr(''); try { const r = await api.upload(f); pickDs(r.path, r.filename.replace(/\.[^.]+$/, '').toLowerCase()) } catch (e: any) { setErr(e.message) } }
  const runPreview = async () => { setErr(''); setResult(null); try { setJobId((await api.onboardPreview({ source, dataset, stage })).job_id); setStep(2) } catch (e: any) { setErr(e.message) } }
  const [finishing, setFinishing] = useState(false)
  const previewBusy = !!job && (job.status === 'RUNNING' || job.status === 'QUEUED')
  const finalize = async () => {
    if (finishing) return
    setErr(''); setFinishing(true)
    try {
      setResult(await api.onboardFinalize({ preview_job_id: jobId, approved_rule_ids: [...approved], dataset, ruleset_id: preview.ruleset_id, pipeline_id: pipelineId, platform, stage, dq_mode: dqMode, policy, owner, source, publish: pub, onboard: onb, force }))
      setStep(4)
    } catch (e: any) { setErr(e.message) } finally { setFinishing(false) }
  }
  const confColor = (c: number) => c >= 0.9 ? 'var(--success)' : c >= 0.7 ? 'var(--warn)' : 'var(--danger)'

  return <>
    <div className="section-title">Onboard a pipeline</div>

    <div className="card">
      <div className="steps" style={{ marginBottom: 16 }}>
        {WIZ_STEPS.map((s, i) => <React.Fragment key={s}>
          {i > 0 && <div className={`conn ${i <= step ? 'done' : ''}`} />}
          <div className={`step ${i < step ? 'done' : i === step ? 'current' : ''}`}>
            <span className="dot">{i < step ? '✓' : i + 1}</span><span className="lbl">{s}</span>
          </div>
        </React.Fragment>)}
      </div>
      <p className="wiz-hint">Profile the source → auto-generate candidate rules → steward review → write ruleset + manifest → optional publish &amp; onboard. No YAML hand-writing required.</p>
    </div>

    {err && <div className="warn-box">{err}</div>}

    {step === 0 && <div className="card">
      <h3>Step 1 · Select source data</h3>
      <div className="field"><label>Sample datasets (sample_data/bronze)</label>
        <select value={source} onChange={e => pickDs(e.target.value, e.target.selectedOptions[0].dataset.n ?? '')}>
          <option value="">— choose a sample dataset —</option>
          {datasets.map(d => <option key={d.id} value={d.path} data-n={d.id}>{d.id} — {d.path}</option>)}
        </select></div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, margin: '14px 0' }}>
        <div className="divider" style={{ flex: 1 }} /><span className="muted">or upload your own file</span><div className="divider" style={{ flex: 1 }} />
      </div>
      <div className="field"><label>Upload CSV / Parquet (any size)</label>
        <input type="file" accept=".csv,.parquet" onChange={e => e.target.files?.[0] && upload(e.target.files[0])} /></div>
      {source && <div className="success-box">Source: <code>{source}</code></div>}
      <div className="btn-row"><button className="btn" disabled={!source} onClick={() => setStep(1)}>Next: set options →</button></div>
    </div>}

    {step === 1 && <div className="card">
      <h3>Step 2 · Pipeline options</h3>
      <div className="form-grid">
        <div className="field"><label>Dataset ID</label><input value={dataset} onChange={e => setDataset(e.target.value)} /></div>
        <div className="field"><label>Pipeline ID</label><input value={pipelineId} onChange={e => setPipelineId(e.target.value)} /></div>
        <div className="field"><label>Platform</label><select value={platform} onChange={e => setPlatform(e.target.value)}>
          <option>DATABRICKS</option><option>FABRIC</option><option>STANDALONE_SPARK</option><option>DATAPROC</option><option>PYTHON</option><option>OTHER</option>
        </select></div>
        <div className="field"><label>Target stage</label><select value={stage} onChange={e => setStage(e.target.value)}>
          <option>SILVER</option><option>GOLD</option><option>BRONZE</option>
        </select></div>
        <div className="field"><label>Initial DQ mode</label><select value={dqMode} onChange={e => setDqMode(e.target.value)}>
          <option>OBSERVE_ONLY</option><option>ENFORCE</option><option>REMEDIATE</option>
        </select></div>
        <div className="field"><label>Policy reference</label><select value={policy} onChange={e => setPolicy(e.target.value)}>
          <option value="../policies/default.yaml">Default (banking)</option>
          <option value="../policies/healthcare.yaml">Healthcare (PHI)</option>
        </select></div>
        <div className="field span2"><label>Owner email</label><input value={owner} onChange={e => setOwner(e.target.value)} placeholder="team@example.com" /></div>
      </div>
      <div style={{ marginTop: 10 }}>
        <label className="chk"><input type="checkbox" checked={pub} onChange={e => setPub(e.target.checked)} /> Publish + activate ruleset after generation</label>
        <label className="chk"><input type="checkbox" checked={onb} onChange={e => setOnb(e.target.checked)} /> Register pipeline immediately</label>
        <label className="chk"><input type="checkbox" checked={force} onChange={e => setForce(e.target.checked)} /> Overwrite existing files</label>
      </div>
      <div className="btn-row">
        <button className="btn sec" onClick={() => setStep(0)}>← Back</button>
        <button className="btn" onClick={runPreview} disabled={!dataset || !pipelineId || previewBusy}>{previewBusy ? 'Profiling…' : 'Profile + generate rules →'}</button>
      </div>
    </div>}

    {step === 2 && <div className="card">
      <h3>Step 3 · Profiling {dataset}</h3>
      <JobStatus job={job} />
      {preview && <>
        <div className="success-box">{preview.row_count?.toLocaleString()} rows × {preview.column_count} cols · {preview.candidate_count} candidate rules · {(preview.anomalies?.length || 0) + (preview.schema_anomalies?.length || 0)} anomalies found {preview.profile_mode === 'SAMPLE' ? '(SAMPLE mode — file &gt;50 MB)' : ''}</div>
        {preview.anomalies?.length > 0 && <>
          <h4>Anomalies detected</h4>
          {preview.anomalies.map((a: any, i: number) => <AnomalyCard a={a} key={i} />)}
        </>}
        <div className="btn-row"><button className="btn" onClick={() => setStep(3)}>Review rules →</button></div>
      </>}
    </div>}

    {step === 3 && preview && <div className="card">
      <h3>Step 4 · Review candidate rules</h3>
      <div className="info-box">{approved.size} of {preview.candidate_count} rules approved. Unchecked rules are excluded from the published ruleset. Colour = confidence (green ≥ 0.9 / amber ≥ 0.7 / red &lt; 0.7 — review carefully).</div>
      <div style={{ marginTop: 10 }}>
        {preview.candidates.map((c: any) => (
          <label className="rule-check-row" key={c.rule_id}>
            <input type="checkbox" checked={approved.has(c.rule_id)} onChange={e => { const s = new Set(approved); e.target.checked ? s.add(c.rule_id) : s.delete(c.rule_id); setApproved(s) }} />
            <span style={{ minWidth: 170, fontFamily: 'Consolas, monospace', fontSize: 11 }}>{c.rule_id}</span>
            <span className={`badge ${c.severity === 'ERROR' ? 'FAILED' : 'REGISTERED'}`}>{c.severity}</span>
            <span className="tag" style={{ minWidth: 160 }}>{c.rule_type}</span>
            <span className="muted" style={{ minWidth: 110 }}>{c.column ?? '—'}</span>
            <span style={{ color: confColor(c.confidence), fontWeight: 700, minWidth: 42 }}>{c.confidence}</span>
            <span className="muted" style={{ flex: 1 }}>{c.description}</span>
          </label>
        ))}
      </div>
      <div className="btn-row">
        <button className="btn sec" onClick={() => setStep(1)}>← Options</button>
        <button className="btn" onClick={finalize} disabled={!approved.size || finishing}>{finishing ? 'Writing…' : `Write ruleset + manifest${onb ? ' + register' : ''} →`}</button>
      </div>
    </div>}

    {step === 4 && result && <div className="card">
      <h3>Step 5 · Done</h3>
      <div className="success-box">
        <b>{result.ruleset}</b> written — {result.rules_approved} rules approved, {result.rules_rejected} rejected
        {result.published && <> · published <code>{result.published.checksum}</code></>}
        {result.onboarded && <> · <b>{result.onboarded.pipeline_id}</b> → <span className={`badge ${result.onboarded.state}`}>{result.onboarded.state}</span></>}
      </div>
      <div className="form-grid" style={{ marginTop: 12 }}>
        <div className="kv"><span>Ruleset file</span><code>{result.ruleset_path}</code></div>
        <div className="kv"><span>Manifest file</span><code>{result.manifest_path}</code></div>
      </div>
      {result.onboarded && <div className="info-box" style={{ marginTop: 10 }}>Next: go to <b>Pipelines</b> → test → activate → <b>Run DQ</b></div>}
      <h4>Manifest YAML</h4>
      <pre>{result.manifest_yaml}</pre>
      <div className="btn-row">
        <button className="btn sec" onClick={() => { setStep(0); setSource(''); setJobId(null); setResult(null); setPipelineId('') }}>Onboard another</button>
      </div>
    </div>}
  </>
}

/* ============================================================
   RUN DQ
   ============================================================ */
const ADHOC_RS = [
  ['customers_v1', 'config/rulesets/customers_v1.yaml'],
  ['customers_v1_1', 'config/rulesets/customers_v1_1.yaml'],
  ['transactions_v1', 'config/rulesets/transactions_v1.yaml'],
  ['patients_v1', 'config/rulesets/patients_v1.yaml'],
  ['orders_v1', 'config/rulesets/orders_v1.yaml'],
  ['loans_v1', 'config/rulesets/loans_v1.yaml'],
  ['employees_v1', 'config/rulesets/employees_v1.yaml'],
  ['cdr_records_v1', 'config/rulesets/cdr_records_v1.yaml'],
  ['customer_summary_gold_v1', 'config/rulesets/customer_summary_gold_v1.yaml'],
]

export function RunDQ() {
  const [datasets, setDatasets] = useState<any[]>([])
  const [pipes, setPipes] = useState<any[]>([])
  const [pipeSel, setPipeSel] = useState('ADHOC')
  const [govDs, setGovDs] = useState<any[]>([])
  const [rsOpts, setRsOpts] = useState<{ label: string; value: string }[]>([])
  const [src, setSrc] = useState(''); const [rs, setRs] = useState('config/rulesets/customers_v1.yaml')
  const [mode, setMode] = useState('OBSERVE_ONLY'); const [ds, setDs] = useState('customers')
  const [pid, setPid] = useState('UI_RUN'); const [pol, setPol] = useState('config/policies/default.yaml')
  const [jobId, setJobId] = useState<string | null>(null)
  const [pjob, setPjob] = useState<string | null>(null)
  const [past, setPast] = useState<Job[]>([])
  const job = usePoll(jobId); const prof = usePoll(pjob)
  const governed = pipeSel !== 'ADHOC'

  useEffect(() => {
    api.datasets().then(d => { setDatasets(d); if (d[0]) { setSrc(d[0].path); setDs(d[0].id) } })
    api.pipelines().then(setPipes)
  }, [])
  useEffect(() => { api.jobs().then(setPast) }, [job?.status, prof?.status])

  const applyDs = (e: any) => {
    setDs(e.dataset_id)
    const opts = Object.entries(e.rulesets ?? {}).map(([st, p]: any) => ({ label: `${st}: ${String(p).split('/').pop()?.replace('.yaml', '')}`, value: resolveRel(String(p)) }))
    setRsOpts(opts); if (opts[0]) setRs(opts[0].value)
    if (e.test_data) setSrc(resolveRel(e.test_data))
  }
  const onPipeline = async (id: string) => {
    setPipeSel(id)
    if (id === 'ADHOC') { setGovDs([]); setPid('UI_RUN'); return }
    const d = await api.pipeline(id); const dss = d.manifest?.datasets ?? []
    setGovDs(dss); setPid(id)
    if (d.manifest?.dq_mode) setMode(d.manifest.dq_mode)
    if (d.manifest?.policy) setPol(resolveRel(d.manifest.policy))
    if (dss.length) applyDs(dss[0])
  }
  const [err, setErr] = useState('')
  const jobActive = (j: Job | null) => !!j && (j.status === 'RUNNING' || j.status === 'QUEUED')
  const busy = jobActive(job) || jobActive(prof)
  const submit = async () => {
    setErr('')
    try { const r = await api.run({ source: src, ruleset: rs, mode, dataset: ds, pipeline_id: pid, policy: pol }); setJobId(r.job_id) }
    catch (e: any) { setErr(e.message) }
  }
  const submitProfile = async () => {
    setErr('')
    try { const r = await api.profile({ source: src, dataset: ds, mode: 'FULL' }); setPjob(r.job_id) }
    catch (e: any) { setErr(e.message) }
  }
  const submitPipeline = async () => {
    setErr('')
    try { const r = await api.pipelineRun(pipeSel, { dataset_id: ds, mode }); setJobId(r.job_id) }
    catch (e: any) { setErr(e.message) }
  }
  const rsOptions = governed ? rsOpts : ADHOC_RS.map(([l, v]) => ({ label: l, value: v }))

  return <>
    <div className="section-title">Run Data Quality</div>

    <div className="card">
      <div className="form-grid">
        <div className="field">
          <label>Pipeline {governed ? '(governed — manifest binding active)' : '(ad-hoc)'}</label>
          <select value={pipeSel} onChange={e => onPipeline(e.target.value)}>
            <option value="ADHOC">— ad-hoc run (unregistered) —</option>
            {pipes.map(p => <option key={p.pipeline_id} value={p.pipeline_id}>{p.pipeline_id} ({p.state})</option>)}
          </select>
        </div>
        <div className="field">
          <label>{governed ? 'Dataset (from manifest)' : 'Source file'}</label>
          {governed
            ? <select value={ds} onChange={e => applyDs(govDs.find((x: any) => x.dataset_id === e.target.value))}>
                {govDs.map((d: any) => <option key={d.dataset_id} value={d.dataset_id}>{d.dataset_id}</option>)}
              </select>
            : <select value={src} onChange={e => { setSrc(e.target.value); setDs(e.target.selectedOptions[0].text.split(' — ')[0]) }}>
                {datasets.map(d => <option key={d.path} value={d.path}>{d.id} — {d.path}</option>)}
              </select>}
        </div>
        {governed && <div className="field"><label>Source (test_data)</label><input value={src} readOnly /></div>}
        <div className="field"><label>Rule set</label>
          <select value={rs} onChange={e => setRs(e.target.value)}>
            {rsOptions.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
          </select>
        </div>
        <div className="field"><label>Policy{governed ? ' (from manifest, locked)' : ''}</label>
          <select value={pol} onChange={e => setPol(e.target.value)} disabled={governed}>
            <option value="config/policies/default.yaml">Default (banking)</option>
            <option value="config/policies/healthcare.yaml">Healthcare (PHI)</option>
          </select>
        </div>
        <div className="field"><label>Mode</label>
          <select value={mode} onChange={e => setMode(e.target.value)}>
            {['OBSERVE_ONLY', 'ENFORCE', 'REMEDIATE'].map(m => <option key={m}>{m}</option>)}
          </select>
        </div>
        <div className="field"><label>Pipeline ID</label>
          <input value={pid} readOnly={governed} onChange={e => setPid(e.target.value)} />
        </div>
        <div className="field"><label>View past result</label>
          <select value="" onChange={e => { const id = e.target.value; if (!id) return; if (past.find(x => x.job_id === id)?.kind === 'profile') setPjob(id); else setJobId(id); e.target.value = '' }}>
            <option value="">— completed jobs —</option>
            {past.filter(x => x.status === 'COMPLETED').map(x => <option key={x.job_id} value={x.job_id}>{x.job_id} · {x.kind}</option>)}
          </select>
        </div>
      </div>
      {governed && <div className="info-box">Governed mode — dataset, source, ruleset and policy are locked to the manifest. Only the execution mode is adjustable.</div>}
      {err && <div className="warn-box">{err}</div>}
      <div className="btn-row">
        <button className="btn" onClick={submit} disabled={busy}>▶ Run DQ</button>
        {governed && <button className="btn" style={{ background: 'var(--success)' }} onClick={submitPipeline} disabled={busy}>▶▶ Run all stages</button>}
        <button className="btn sec" onClick={submitProfile} disabled={busy}>⬡ Profile only</button>
        <span className="muted">{busy ? 'A job is already running — wait for it to finish.' : governed ? 'Run DQ = single stage · Run all stages = BRONZE→SILVER→GOLD chained.' : 'Spark runs take 15–120 s. Live progress streams automatically.'}</span>
      </div>
    </div>

    <JobStatus job={job} /><JobResult job={job} />
    <JobStatus job={prof} /><JobResult job={prof} />
    {prof?.result && <ProfileView r={prof.result} jid={prof.job_id} />}
  </>
}

/* ============================================================
   ANOMALIES
   ============================================================ */
export function Anomalies() {
  const [rows, setRows] = useState<any[]>([])
  const [filter, setFilter] = useState('OPEN')
  const [editing, setEditing] = useState<any>(null)
  const [owner, setOwner] = useState(''); const [note, setNote] = useState(''); const [msg, setMsg] = useState('')

  const load = () => api.anomalies({ status: filter || undefined }).then(setRows)
  useEffect(() => { load() }, [filter])

  const patch = async (id: string, status: string) => {
    await api.patchAnomaly(id, { status, owner: owner || undefined, note: note || undefined })
    setMsg(`Updated → ${status}`); setEditing(null); load()
  }

  return <>
    <div className="section-title">Anomaly Inbox</div>
    <div className="card">
      <div style={{ display: 'flex', gap: 10, marginBottom: 14 }}>
        {['', 'OPEN', 'ACKNOWLEDGED', 'RESOLVED'].map(s => (
          <button key={s} className={`btn sm ${filter === s ? '' : 'sec'}`} onClick={() => setFilter(s)}>{s || 'All'}</button>
        ))}
        <span className="muted" style={{ marginLeft: 'auto', alignSelf: 'center' }}>{rows.length} anomalies</span>
      </div>
      {rows.length === 0 && <div className="empty-state"><div className="icon">✓</div><p>No anomalies in {filter || 'all'} status. Run a DQ or Profile job to populate.</p></div>}
      {rows.map(a => (
        <div className="anomaly-row" key={a.id}>
          <span className={`anomaly-status ${a.status}`}>{a.status}</span>
          <span className="anomaly-type">{a.atype}</span>
          <span className="anomaly-col">{a.col}</span>
          <span className="anomaly-detail">{a.detail}</span>
          <span className="muted">{a.dataset}</span>
          <span className="anomaly-meta">{a.created_at ? new Date(a.created_at).toLocaleDateString() : ''}</span>
          <div style={{ display: 'flex', gap: 4 }}>
            {a.status === 'OPEN' && <button className="btn sm warn" onClick={() => { setEditing(a); setOwner(a.owner ?? ''); setNote(a.note ?? '') }}>Acknowledge</button>}
            {a.status === 'ACKNOWLEDGED' && <button className="btn sm success" onClick={() => patch(a.id, 'RESOLVED')}>Resolve</button>}
            {a.status !== 'OPEN' && <button className="btn sm sec" onClick={() => patch(a.id, 'OPEN')}>Re-open</button>}
          </div>
        </div>
      ))}
      {msg && <div className="success-box" style={{ marginTop: 10 }}>{msg}</div>}
    </div>
    {editing && (
      <div className="card">
        <h3>Acknowledge anomaly — {editing.id}</h3>
        <div className="form-grid">
          <div className="field"><label>Assign owner</label><input value={owner} onChange={e => setOwner(e.target.value)} placeholder="name@example.com" /></div>
          <div className="field"><label>Resolution note</label><input value={note} onChange={e => setNote(e.target.value)} placeholder="e.g. Known data feed issue, monitoring" /></div>
        </div>
        <div className="btn-row">
          <button className="btn success" onClick={() => patch(editing.id, 'ACKNOWLEDGED')}>Acknowledge</button>
          <button className="btn sec" onClick={() => setEditing(null)}>Cancel</button>
        </div>
      </div>
    )}
  </>
}

/* ============================================================
   APPROVALS — human-in-the-loop for PENDING_HUMAN_APPROVAL
   ============================================================ */
export function Approvals() {
  const [data, setData] = useState<any>(null)
  const [decisions, setDecisions] = useState<any>(null)
  const [msg, setMsg] = useState(''); const [err, setErr] = useState('')
  const load = () => {
    api.quarantinePending().then(setData).catch(() => {})
    api.quarantineDecisions().then(setDecisions).catch(() => {})
  }
  useEffect(() => { load() }, [])

  const approve = async (rec: any) => {
    setMsg(''); setErr('')
    try {
      await api.approveCorrection({ column: rec.column, original_value: rec.original_value ?? '', corrected_value: rec.proposed_value ?? '', rule_id: rec.rule_id ?? '', dataset: rec.dataset ?? 'unknown', approved_by: 'ui-steward' })
      setMsg(`Approved: ${rec.column} "${rec.original_value}" → "${rec.proposed_value}"`)
      load()
    } catch (e: any) { setErr(e.message) }
  }
  const reject = async (rec: any) => {
    setErr(''); setMsg('')
    try {
      await api.rejectCorrection({ column: rec.column, original_value: rec.original_value ?? '', corrected_value: rec.proposed_value ?? '', rule_id: rec.rule_id ?? '', dataset: rec.dataset ?? 'unknown', approved_by: 'ui-steward' })
      setMsg(`Rejected: ${rec.column} "${rec.original_value}"`)
      load()
    } catch (e: any) { setErr(e.message) }
  }

  const records = data?.records?.filter((r: any) => !r.summary_only) ?? []
  const summaries = data?.records?.filter((r: any) => r.summary_only) ?? []

  return <>
    <div className="section-title">Human Approval Inbox</div>
    <div className="card">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 14 }}>
        <p className="muted">Records held at <code>PENDING_HUMAN_APPROVAL</code> — AI proposed a correction on a sensitive column. Review and approve or reject each.</p>
        <span style={{ background: 'rgba(239,68,68,.15)', color: '#fca5a5', padding: '4px 14px', borderRadius: 10, fontWeight: 700, fontSize: 14 }}>{data?.total ?? 0} pending</span>
      </div>
      {data?.total === 0 && <div className="empty-state"><div className="icon">✓</div><p>No pending approvals. Run in REMEDIATE mode to generate them.</p></div>}
      {summaries.map((s: any, i: number) => (
        <div key={i} className="warn-box">{s.count} pending rows in job {s.job_id} — run with REMEDIATE mode and check the report for per-row detail.</div>
      ))}
      {records.length > 0 && <>
        <div className="approval-row" style={{ fontWeight: 700, fontSize: 11, color: 'var(--text3)', borderBottom: '2px solid var(--border)' }}>
          <span>Column</span><span>Rule</span><span>Original value</span><span>Proposed value</span><span>Confidence</span><span>Dataset</span><span>Actions</span>
        </div>
        {records.map((r: any, i: number) => (
          <div className="approval-row" key={i}>
            <span style={{ color: 'var(--info)', fontWeight: 600 }}>{r.column}</span>
            <span className="tag">{r.rule_id}</span>
            <span className="val-orig">{r.original_value}</span>
            <span className="val-new">{r.proposed_value}</span>
            <span className={r.confidence >= 0.9 ? 'conf-high' : r.confidence >= 0.7 ? 'conf-mid' : 'conf-low'}>{r.confidence}</span>
            <span className="muted">{r.dataset}</span>
            <div style={{ display: 'flex', gap: 4 }}>
              <button className="btn sm success" onClick={() => approve(r)}>Approve</button>
              <button className="btn sm danger" onClick={() => reject(r)}>Reject</button>
            </div>
          </div>
        ))}
      </>}
      {msg && <div className="success-box" style={{ marginTop: 12 }}>{msg}</div>}
      {err && <div className="warn-box" style={{ marginTop: 12 }}>{err}</div>}
    </div>
    <div className="card">
      <h3>Decision history</h3>
      <p className="muted">Approvals stored in <code>config/approvals/approved_corrections.yaml</code>, rejections in <code>rejected_corrections.yaml</code>. Approved corrections apply as <code>HUMAN_APPROVED_AI</code> on the next REMEDIATE run; actioned rows leave this inbox.</p>
      {decisions && (decisions.approvals?.length || decisions.rejections?.length) ? <>
        {decisions.approvals?.slice(0, 10).map((a: any, i: number) => (
          <div key={`a${i}`} className="approval-row" style={{ gridTemplateColumns: '1fr 1fr 1fr 1fr auto' }}>
            <span style={{ color: 'var(--success)', fontWeight: 700 }}>✓ APPROVED</span>
            <span className="muted">{a.dataset} · {a.column}</span>
            <span className="val-orig">{a.original_value}</span>
            <span className="val-new">{a.approved_value}</span>
            <span className="muted" style={{ fontSize: 11 }}>{a.approved_by} · {String(a.approved_at ?? '').slice(0, 10)}</span>
          </div>
        ))}
        {decisions.rejections?.slice(0, 10).map((rj: any, i: number) => (
          <div key={`r${i}`} className="approval-row" style={{ gridTemplateColumns: '1fr 1fr 1fr 1fr auto' }}>
            <span style={{ color: 'var(--danger)', fontWeight: 700 }}>✗ REJECTED</span>
            <span className="muted">{rj.dataset} · {rj.column}</span>
            <span className="val-orig">{rj.original_value}</span>
            <span className="val-new">{rj.proposed_value}</span>
            <span className="muted" style={{ fontSize: 11 }}>{rj.rejected_by} · {String(rj.rejected_at ?? '').slice(0, 10)}</span>
          </div>
        ))}
      </> : <p className="muted" style={{ marginTop: 8 }}>No decisions recorded yet.</p>}
    </div>
  </>
}

/* ============================================================
   RULE SETS
   ============================================================ */
export function RuleSets() {
  const [rs, setRs] = useState<any[]>([]); const [yaml, setYaml] = useState(''); const [msg, setMsg] = useState(''); const [err, setErr] = useState('')
  const [diff, setDiff] = useState<any>(null); const [selRs, setSelRs] = useState<any>(null)

  const load = () => api.rulesets().then(setRs)
  useEffect(() => { load() }, [])

  const pub = async () => {
    setMsg(''); setErr('')
    try { await api.publishRuleset(yaml, true); setMsg('Published and activated'); setYaml(''); load() }
    catch (e: any) { setErr(e.message) }
  }
  const act = async (id: string, action: string, version: string) => {
    setMsg(''); setErr('')
    try { await api.rulesetAction(id, action, version); setMsg(`${id}@${version} → ${action}`); load() }
    catch (e: any) { setErr(e.message) }
  }
  const showDiff = async (id: string) => {
    try { setDiff(await api.rulesetDiff(id)) } catch (e: any) { setErr(e.message) }
  }

  const grouped: Record<string, any[]> = rs.reduce((a: Record<string, any[]>, r: any) => { (a[r.ruleset_id] = a[r.ruleset_id] ?? []).push(r); return a }, {})

  return <>
    <div className="section-title">Rule Set Registry</div>

    {Object.entries(grouped).map(([rid, versions]: [string, any[]]) => {
      const active = versions.find((v: any) => v.active)
      return (
        <div className="card" key={rid}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
            <div>
              <h3 style={{ marginBottom: 4 }}>{rid}</h3>
              <span className="muted">{versions[0]?.dataset} · {versions.length} version{versions.length !== 1 ? 's' : ''}</span>
              {active && <span className="badge ACTIVE" style={{ marginLeft: 10 }}>v{active.version} active</span>}
            </div>
            <button className="btn sm sec" onClick={() => showDiff(rid)}>Diff</button>
          </div>
          <table style={{ marginTop: 12 }}>
            <thead><tr><th>Version</th><th>Rules</th><th>Checksum</th><th>Published</th><th>Status</th><th></th></tr></thead>
            <tbody>
              {versions.map((v: any) => (
                <tr key={v.version}>
                  <td><b>{v.version}</b></td>
                  <td>{v.rules}</td>
                  <td><code>{v.checksum}</code></td>
                  <td className="muted">{v.published_at ? new Date(v.published_at).toLocaleString() : '—'}</td>
                  <td>{v.active ? <span className="badge ACTIVE">ACTIVE</span> : <span className="badge DRAFT">inactive</span>}</td>
                  <td style={{ whiteSpace: 'nowrap' }}>
                    {!v.active && <button className="btn sm success" onClick={() => act(rid, 'activate', v.version)}>Activate</button>}
                    {v.active && versions.length > 1 && <button className="btn sm warn" onClick={() => { const prev = versions.find((x: any) => !x.active); if (prev) act(rid, 'rollback', prev.version) }}>Rollback</button>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )
    })}

    {diff && (
      <div className="card">
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <h3>Diff — {diff.from_version} → {diff.to_version}</h3>
          <button className="btn sm sec" onClick={() => setDiff(null)}>✕</button>
        </div>
        <div className="stat-grid" style={{ gridTemplateColumns: 'repeat(3, 1fr)', marginTop: 10 }}>
          <div className="stat-tile"><div className="num ok">{diff.added?.length ?? 0}</div><div className="lbl">Added rules</div></div>
          <div className="stat-tile"><div className="num bad">{diff.removed?.length ?? 0}</div><div className="lbl">Removed rules</div></div>
          <div className="stat-tile"><div className="num warn">{diff.modified?.length ?? 0}</div><div className="lbl">Modified rules</div></div>
        </div>
        {diff.added?.length > 0 && <div style={{ marginTop: 8 }}><h4>Added</h4>{diff.added.map((r: string) => <span key={r} className="tag">{r}</span>)}</div>}
        {diff.removed?.length > 0 && <div style={{ marginTop: 8 }}><h4>Removed</h4>{diff.removed.map((r: string) => <span key={r} className="tag">{r}</span>)}</div>}
        {diff.modified?.length > 0 && <div style={{ marginTop: 8 }}><h4>Modified</h4>{diff.modified?.map((m: any) => <span key={m.rule_id} className="tag">{m.rule_id}</span>)}</div>}
      </div>
    )}

    <div className="card">
      <h3>Publish a rule set</h3>
      <p className="muted">Paste a ruleset YAML (or the content of any <code>config/rulesets/*.yaml</code>) to publish a new version.</p>
      <textarea value={yaml} onChange={e => setYaml(e.target.value)} placeholder="ruleset:&#10;  ruleset_id: my_dataset_silver&#10;  version: '1.0'&#10;  ..." />
      <div className="btn-row">
        <button className="btn" onClick={pub} disabled={!yaml.trim()}>Publish + activate</button>
      </div>
      {msg && <div className="success-box" style={{ marginTop: 10 }}>{msg}</div>}
      {err && <div className="warn-box" style={{ marginTop: 10 }}>{err}</div>}
    </div>
  </>
}

/* ============================================================
   REPORTS
   ============================================================ */
export function Reports() {
  const [reps, setReps] = useState<any[]>([]); const [sel, setSel] = useState<any>(null); const [selName, setSelName] = useState('')
  useEffect(() => { api.reports().then(setReps) }, [])
  const open = async (name: string) => { const d = await api.report(name); setSel(d); setSelName(name) }

  const scoreFor = (r: any) => {
    const m = r.execution_metrics
    if (!m?.input_records) return null
    return Math.round(m.accepted_records / m.input_records * 100)
  }

  return <>
    <div className="section-title">Execution Reports</div>
    <div className="card">
      {reps.length === 0 && <div className="empty-state"><div className="icon">▤</div><p>No reports yet. Run a DQ job to generate one.</p></div>}
      <table>
        <thead><tr><th>Report</th><th>Size</th><th>Modified</th><th></th></tr></thead>
        <tbody>
          {reps.map(r => (
            <tr key={r.name} style={{ cursor: 'pointer' }} onClick={() => open(r.name)}>
              <td><b>{r.name}</b></td>
              <td className="muted">{(r.size / 1024).toFixed(1)} KB</td>
              <td className="muted">{new Date(r.modified * 1000).toLocaleString()}</td>
              <td><button className="btn sm sec">View</button></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>

    {sel && (() => {
      const m = sel.execution_metrics; const sc = scoreFor(sel)
      const topFail = (sel.validation_results?.row_level_results ?? []).filter((r: any) => r.status === 'FAILED').sort((a: any, b: any) => a.pass_percentage - b.pass_percentage).slice(0, 5)
      return (
        <div className="card">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
            <h3>{selName}</h3>
            <button className="btn sm sec" onClick={() => { setSel(null); setSelName('') }}>✕</button>
          </div>
          {m && <>
            <div className="report-score">
              <div className={`big ${sc != null ? (sc >= 90 ? 'ok' : sc >= 70 ? 'warn-text' : 'err') : ''}`}>{sc != null ? sc + '%' : '—'}</div>
              <div>
                <div className="meta">{m.dataset} · {m.integration_mode} · {m.stage}</div>
                <div className="meta">Run: {m.dq_run_id}</div>
                <div className="meta">Duration: {m.duration_ms ? (m.duration_ms / 1000).toFixed(1) + 's' : '—'}</div>
              </div>
              <div className="stat-grid" style={{ flex: 1, gridTemplateColumns: 'repeat(4, 1fr)' }}>
                <div className="stat-tile"><div className="num">{m.input_records?.toLocaleString()}</div><div className="lbl">Input</div></div>
                <div className="stat-tile"><div className="num ok">{m.accepted_records?.toLocaleString()}</div><div className="lbl">Accepted</div></div>
                <div className="stat-tile"><div className="num bad">{m.quarantined_records?.toLocaleString()}</div><div className="lbl">Quarantined</div></div>
                <div className="stat-tile"><div className={`num ${sel.quality_gate?.passed ? 'ok' : 'bad'}`}>{sel.quality_gate?.passed ? 'PASS' : 'FAIL'}</div><div className="lbl">Gate</div></div>
              </div>
            </div>
            {sel.quality_pillars?.pillars && (() => {
              const PILLAR_ORDER = ['completeness', 'uniqueness', 'validity', 'consistency', 'accuracy', 'timeliness', 'integrity']
              const qp = sel.quality_pillars
              return <>
                <h4>Quality pillars {qp.overall_score != null && <span className="badge ACTIVE" style={{ marginLeft: 8 }}>overall {qp.overall_score}</span>}</h4>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(210px, 1fr))', gap: 8, marginBottom: 6 }}>
                  {PILLAR_ORDER.map(name => {
                    const p = qp.pillars[name]
                    if (!p) return null
                    const measured = p.status === 'MEASURED' && p.score != null
                    const cls = !measured ? 'muted' : p.score >= 90 ? 'ok' : p.score >= 70 ? 'warn-text' : 'err'
                    return (
                      <div key={name} style={{ border: '1px solid var(--border)', borderRadius: 8, padding: '8px 10px' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12 }}>
                          <b style={{ textTransform: 'capitalize' }}>{name}</b>
                          <span className={cls} style={{ fontWeight: 700 }}>{measured ? p.score : '—'}</span>
                        </div>
                        <div className="muted" style={{ fontSize: 10, marginTop: 2 }}>{p.status === 'MEASURED' ? `${p.rules ?? 0} rules` : p.status}</div>
                      </div>
                    )
                  })}
                </div>
                {qp.domain_recommendation && <div className="info-box" style={{ fontSize: 12, marginBottom: 10 }}>💡 {qp.domain_recommendation}</div>}
              </>
            })()}
            {(() => {
              const oa = sel.remediation_results?.outcome_audit
              if (!oa?.counts || !Object.keys(oa.counts).length) return null
              const COLORS: Record<string, string> = {
                AI_APPLIED: 'var(--success)', HUMAN_APPROVED_AI: 'var(--success)',
                DETERMINISTIC_FIX: 'var(--info)', BELOW_CONFIDENCE_THRESHOLD: '#f59e0b',
                PENDING_HUMAN_APPROVAL: '#f59e0b', BLOCKED_SENSITIVE_DATA: '#a855f7',
                REJECTED_BY_HUMAN: '#f97316',
                NO_SAFE_PROPOSAL: 'var(--danger)', REVALIDATION_FAILED: 'var(--danger)',
                NOT_EVALUATED_LIMIT: '#94a3b8',
              }
              return <>
                <h4>Remediation outcomes {oa.all_terminal && <span className="badge COMPLETED" style={{ marginLeft: 8 }}>all terminal</span>}</h4>
                <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 10 }}>
                  {Object.entries(oa.counts).map(([k, v]: any) => (
                    <span key={k} className="tag" style={{ borderColor: COLORS[k] ?? 'var(--border)', color: COLORS[k] ?? 'var(--text)' }}>
                      {k.replace(/_/g, ' ')}: <b>{v}</b>
                    </span>
                  ))}
                </div>
              </>
            })()}
            {topFail.length > 0 && <>
              <h4>Top failing rules</h4>
              {topFail.map((r: any) => (
                <div className="rule-row" key={r.rule_id}>
                  <span>{r.rule_id}</span>
                  <span className="badge FAILED">{r.status}</span>
                  <Bar pct={r.pass_percentage} />
                  <span className="muted">{r.passed_records}/{r.total_records} · {r.column}</span>
                </div>
              ))}
            </>}
            {m.quarantine_status_breakdown && Object.keys(m.quarantine_status_breakdown).length > 0 && <>
              <h4>Quarantine breakdown</h4>
              {Object.entries(m.quarantine_status_breakdown).map(([k, v]: any) => (
                <div key={k} style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 4, fontSize: 12 }}>
                  <span className="tag">{k}</span><b>{v}</b>
                </div>
              ))}
            </>}
          </>}
          <details style={{ marginTop: 14 }}>
            <summary className="muted" style={{ cursor: 'pointer' }}>Raw JSON</summary>
            <pre>{JSON.stringify(sel, null, 2).slice(0, 30000)}</pre>
          </details>
        </div>
      )
    })()}
  </>
}

/* ============================================================
   SETTINGS
   ============================================================ */
export function Settings() {
  const [health, setHealth] = useState<any>(null)
  const [ai, setAi] = useState<any>(null)
  const [cfg, setCfg] = useState<any>(null)
  const [sys, setSys] = useState<any>(null)
  const [assistantOn, setAssistantOn] = useState<boolean>(true)
  const [chatLog, setChatLog] = useState<any[]>([])
  const [reloadMsg, setReloadMsg] = useState<string>('')
  useEffect(() => {
    api.health().then(setHealth)
    api.aiStatus().then(setAi).catch(() => {})
    api.getConfig().then(setCfg).catch(() => {})
    api.systemStatus().then(setSys).catch(() => {})
    api.assistantEnabled().then(r => setAssistantOn(r.enabled)).catch(() => {})
    api.chatLog().then(r => setChatLog(r.entries ?? [])).catch(() => {})
  }, [])

  const dqxReady = ai && ai.provider === 'DQXProvider' && !ai.simulated
  const mockReady = ai && ai.provider === 'MockAIProvider'

  const SYS_CHECK: Record<string, {label: string; url?: string}> = {
    python: {label: 'Python 3.12+', url: 'https://www.python.org/downloads/'},
    java: {label: 'Java JDK 21+', url: 'https://learn.microsoft.com/en-us/java/openjdk/download'},
    node: {label: 'Node.js 18+', url: 'https://nodejs.org/en/download'},
    pyspark: {label: 'PySpark'},
    databricks_dqx: {label: 'databricks-labs-dqx'},
    litellm: {label: 'litellm'},
    fastapi: {label: 'FastAPI'},
    uvicorn: {label: 'Uvicorn'},
  }

  return <>
    <div className="section-title">Settings &amp; Configuration</div>

    {sys && <div className="card">
      <h3>System status {sys.all_ok ? <span className="badge COMPLETED">all ok</span> : <span className="badge FAILED">issues</span>}</h3>
      <table>
        <thead><tr><th>Component</th><th>Status</th><th>Detected</th><th>Install</th></tr></thead>
        <tbody>
          {Object.entries(sys.checks ?? {}).map(([k, c]: [string, any]) => (
            <tr key={k}>
              <td>{SYS_CHECK[k]?.label ?? k}</td>
              <td><span className={`badge ${c.ok ? 'COMPLETED' : 'FAILED'}`}>{c.ok ? 'OK' : 'MISSING'}</span></td>
              <td className="muted" style={{ fontSize: 11 }}>{String(c.version ?? '—').slice(0, 60)}</td>
              <td>{!c.ok && SYS_CHECK[k]?.url && <a href={SYS_CHECK[k].url} target="_blank" rel="noreferrer">download</a>}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <h4 style={{ marginTop: 12 }}>LLM environment variables</h4>
      <div style={{ display: 'flex', gap: 16, fontSize: 12, flexWrap: 'wrap' }}>
        {Object.entries(sys.env_vars ?? {}).map(([k, v]: [string, any]) => (
          <span key={k}><code>{k}</code>: <b style={{ color: v ? 'var(--success)' : 'var(--danger)' }}>{v ? 'set' : 'not set'}</b></span>
        ))}
      </div>
      <div className="info-box" style={{ marginTop: 10 }}>Set env vars before running <code>DQ_start.bat</code> to enable the real LLM. One-time setup guide: <code>docs/SETUP.md</code></div>
    </div>}

    <div className="card">
      <h3>Engine</h3>
      <div className="form-grid">
        <div className="kv"><span>Engine</span><b>{health?.engine ?? '—'}</b></div>
        <div className="kv"><span>Root</span><code style={{ fontSize: 11, wordBreak: 'break-all' }}>{health?.root ?? '—'}</code></div>
        <div className="kv"><span>Runtime</span><b>{cfg?.runtime?.spark_master ?? '—'} · {cfg?.runtime?.driver_memory ?? '—'} driver</b></div>
        <div className="kv"><span>Registry</span><b>{cfg?.registry?.backend ?? 'file'} · anomalies.db · jobs.db</b></div>
        <div className="kv"><span>Default mode</span><b>{cfg?.engine?.default_mode ?? '—'}</b></div>
        <div className="kv"><span>Profiling</span><b>{cfg?.profiling?.mode ?? '—'} · {((cfg?.profiling?.sample_fraction ?? 0) * 100).toFixed(0)}% sample</b></div>
      </div>
      <div style={{ display: 'flex', gap: 10, marginTop: 12, alignItems: 'center' }}>
        <button className="primary" style={{ padding: '6px 14px' }}
          onClick={() => api.reloadEngine(false).then(r => { api.aiStatus().then(setAi); api.getConfig().then(setCfg); setReloadMsg(`Engine reloaded — provider: ${r.provider}`) }).catch((e: any) => setReloadMsg(`Reload failed: ${e.message}`))}>
          Apply config changes (no restart)
        </button>
        <button style={{ padding: '6px 14px' }}
          onClick={() => api.reloadEngine(true).then(r => { api.aiStatus().then(setAi); api.getConfig().then(setCfg); setReloadMsg(`Engine + Spark reloaded — provider: ${r.provider}`) }).catch((e: any) => setReloadMsg(`Reload failed: ${e.message}`))}>
          Reload + restart Spark
        </button>
        {reloadMsg && <span className="muted" style={{ fontSize: 11 }}>{reloadMsg}</span>}
      </div>
      <div className="info-box" style={{ marginTop: 10, fontSize: 11 }}>
        Use <b>Apply</b> after editing <code>config/engine.yaml</code> (provider, thresholds) — no backend restart needed.
        Use <b>Reload + restart Spark</b> only when changing <code>runtime.*</code> (memory, master, partitions) — takes ~30 s.
      </div>
    </div>

    <div className="card">
      <h3>AI remediation provider</h3>
      {ai && <>
        <div className={dqxReady ? 'success-box' : mockReady ? 'warn-box' : 'info-box'} style={{ marginBottom: 14 }}>
          <b>Provider: {ai.provider ?? '—'}</b> · model: {ai.model_version ?? '—'} ·{' '}
          {dqxReady
            ? '✓ Connected to Databricks Model Serving — real LLM corrections active'
            : mockReady
              ? '⚠ SIMULATED (MockAIProvider) — deterministic YAML lookup, not a real LLM'
              : ai.provider === 'DQXProvider'
                ? '⏳ DQX configured but not yet initialised — first LLM call will verify connection'
                : '⚠ Provider not configured'
          }
          {ai.credentials_set === false && <div style={{ marginTop: 6 }}>DATABRICKS_HOST / DATABRICKS_TOKEN not set</div>}
          {ai.dqx_available === false && <div style={{ marginTop: 6 }}>databricks-labs-dqx[llm] not installed</div>}
          {ai.workspace_host && <div style={{ marginTop: 6, fontSize: 11 }}>Workspace: <code>{ai.workspace_host}</code></div>}
          {ai.model_endpoint && <div style={{ marginTop: 2, fontSize: 11 }}>Endpoint: <code>{ai.model_endpoint}</code></div>}
        </div>
      </>}
      {!ai && <div className="warn-box">Could not load AI provider status</div>}

      <h4>How to switch AI provider</h4>
      <div className="form-grid" style={{ marginBottom: 12 }}>
        <div className="kv"><span>Current config</span><code>ai.provider: {cfg?.ai?.provider ?? '—'}</code></div>
        <div className="kv"><span>DQX endpoint</span><code>{cfg?.ai?.has_dqx_endpoint ? 'set' : 'not set'}</code></div>
      </div>

      <h4>Option A — Real LLM (Databricks Model Serving) — CURRENT</h4>
      <div className="gstep"><span className="n">1</span><span>Install: <code>pip install 'databricks-labs-dqx[llm]'</code></span></div>
      <div className="gstep"><span className="n">2</span><span>Set env: <code>DATABRICKS_HOST=https://&lt;your-workspace&gt;.cloud.databricks.com</code></span></div>
      <div className="gstep"><span className="n">3</span><span>Set env: <code>DATABRICKS_TOKEN=&lt;personal-access-token&gt;</code></span></div>
      <div className="gstep"><span className="n">4</span><span>Set env: <code>DQX_MODEL_ENDPOINT=databricks-meta-llama-3-3-70b-instruct</code></span></div>
      <div className="gstep"><span className="n">5</span><span>Edit <code>config/engine.yaml</code> → <code>ai.provider: dqx</code> (already done)</span></div>
      <div className="gstep"><span className="n">6</span><span>Restart backend — Settings shows <b>DQXProvider</b> when connected</span></div>

      <h4 style={{ marginTop: 16 }}>Option B — Mock AI (no external calls)</h4>
      <div className="gstep"><span className="n">1</span><span>Edit <code>config/engine.yaml</code> → <code>ai.provider: mock</code></span></div>
      <div className="gstep"><span className="n">2</span><span>Restart backend — Settings shows <b>MockAIProvider</b> with simulated=true</span></div>
      <div className="gstep"><span className="n">3</span><span>Edit <code>config/ai/mock_ai_responses.yaml</code> to add your corrections</span></div>

      <div className="info-box" style={{ marginTop: 12 }}>
        <b>How it works:</b> DQXProvider calls Databricks Model Serving via LiteLLM for genuine NLP corrections.
        MockAIProvider uses a deterministic YAML lookup table — confidence is real but corrections are pre-scripted.
        Both go through the same 90% confidence gate and sensitive-column policy.
        DQX auto-falls back to MockAI if credentials are absent or the endpoint is unreachable.
      </div>
    </div>

    <div className="card">
      <h3>DQ Assistant (chatbot)</h3>
      <div className="form-grid" style={{ marginBottom: 12 }}>
        <div className="kv"><span>Status</span>
          <b style={{ color: assistantOn ? 'var(--success)' : 'var(--danger)' }}>{assistantOn ? 'ENABLED' : 'DISABLED'}</b></div>
        <div className="kv"><span>Log</span><code>logs/assistant/chat_log.jsonl</code></div>
      </div>
      <div style={{ display: 'flex', gap: 10, marginBottom: 12 }}>
        <button className="primary" style={{ padding: '6px 14px' }}
          onClick={() => api.setAssistantEnabled(!assistantOn).then(r => setAssistantOn(r.enabled))}>
          {assistantOn ? 'Disable' : 'Enable'}
        </button>
        <button style={{ padding: '6px 14px' }}
          onClick={() => api.chatLog().then(r => setChatLog(r.entries ?? []))}>Refresh log</button>
      </div>
      {chatLog.length > 0 && <div style={{ maxHeight: 300, overflowY: 'auto' }}>
        {chatLog.slice(0, 10).map((e: any, i: number) => (
          <details key={i} style={{ borderBottom: '1px solid var(--border)', padding: '8px 0' }}>
            <summary style={{ cursor: 'pointer', fontSize: 12 }}>
              <span className={`badge ${e.source === 'llm' ? 'COMPLETED' : 'PENDING'}`} style={{ marginRight: 8 }}>{e.source}</span>
              {e.q}
              <span className="muted" style={{ float: 'right', fontSize: 10 }}>{(e.ts ?? '').slice(11, 19)}</span>
            </summary>
            <div style={{ fontSize: 11, whiteSpace: 'pre-wrap', padding: '8px 4px', color: 'var(--muted)' }}>{e.a}</div>
          </details>
        ))}
      </div>}
      <div className="info-box" style={{ marginTop: 10 }}>
        The Assistant answers questions about this app only — pipelines, rulesets, jobs, anomalies, reports,
        setup, and roadmap. Uses the same Databricks LLM as remediation; falls back to built-in answers offline.
      </div>
    </div>

    <div className="card">
      <h3>DQ modes</h3>
      {[
        ['OBSERVE_ONLY', 'All rows pass through untouched. Violations flagged only. Zero data risk — safe start for all new pipelines.'],
        ['ENFORCE',      'Failed rows quarantined (ENFORCE_NO_REMEDIATION_ATTEMPTED). Accepted rows continue. No corrections attempted.'],
        ['REMEDIATE',    'Deterministic fixes → AI (≥90% confidence) → revalidation. Remaining failures quarantined with reasons. Highest data quality but requires reviewed + published rulesets.'],
      ].map(([m, d]) => <div key={m} className="gstep" style={{ marginBottom: 8 }}><span className="n">{m[0]}</span><span><b style={{ color: 'var(--info)' }}>{m}</b> — {d}</span></div>)}
    </div>

    <div className="card">
      <h3>Confidence threshold &amp; policy</h3>
      <div className="form-grid">
        <div className="kv"><span>Default threshold</span><b>90%</b></div>
        <div className="kv"><span>Config</span><code>config/policies/default.yaml</code></div>
      </div>
      <div style={{ marginTop: 12 }}>
        {[
          ['≥ 90% + non-sensitive', 'AUTO_APPLY — correction applied automatically'],
          ['≥ 90% + sensitive column', 'PENDING_HUMAN_APPROVAL — held for steward review (Approvals tab)'],
          ['&lt; 90%', 'BELOW_CONFIDENCE_THRESHOLD — quarantined, not applied'],
          ['0% / no match', 'NO_PROPOSAL — quarantined as UNRESOLVED_NO_SAFE_REMEDIATION'],
        ].map(([c, r]) => (
          <div key={c} style={{ display: 'flex', gap: 12, padding: '6px 0', borderBottom: '1px solid var(--border)', fontSize: 12 }}>
            <span className="tag" style={{ minWidth: 220 }} dangerouslySetInnerHTML={{ __html: c }} />
            <span className="muted">{r}</span>
          </div>
        ))}
      </div>
    </div>
  </>
}

/* ============================================================
   DQ ASSISTANT — floating chatbot panel (Genie-style, app-scoped)
   ============================================================ */
type ChatMsg = { role: 'user' | 'bot'; text: string; source?: string }

export function ChatPanel() {
  const [enabled, setEnabled] = useState(true)
  const [open, setOpen] = useState(false)
  const [msgs, setMsgs] = useState<ChatMsg[]>([
    { role: 'bot', text: "Hi! I'm DQ Assistant. Ask me about onboarded pipelines, rulesets, jobs, anomalies, how to run DQ, or the roadmap." },
  ])
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)
  const endRef = useRef<HTMLDivElement>(null)

  useEffect(() => { endRef.current?.scrollIntoView({ behavior: 'smooth' }) }, [msgs, open])
  useEffect(() => {
    const poll = () => api.assistantEnabled().then(r => setEnabled(r.enabled)).catch(() => {})
    poll(); const t = setInterval(poll, 15000); return () => clearInterval(t)
  }, [])
  if (!enabled) return null

  const send = async () => {
    const text = input.trim()
    if (!text || busy) return
    setMsgs(m => [...m, { role: 'user', text }])
    setInput('')
    setBusy(true)
    try {
      const r = await api.chat(text)
      setMsgs(m => [...m, { role: 'bot', text: r.answer, source: r.source }])
    } catch (e: any) {
      setMsgs(m => [...m, { role: 'bot', text: `Error: ${e.message ?? e}` }])
    }
    setBusy(false)
  }

  const SUGGESTIONS = ['Which pipelines are onboarded?', 'Show rulesets', 'Is the real LLM active?', 'How do I run DQ?', 'Next steps roadmap']

  return <>
    {!open && <button onClick={() => setOpen(true)} title="DQ Assistant"
      style={{ position: 'fixed', bottom: 20, right: 20, zIndex: 1000, height: 40, padding: '0 16px',
               borderRadius: 20, border: 'none', cursor: 'pointer', fontSize: 13, fontWeight: 700,
               letterSpacing: '.5px',
               background: 'var(--info)', color: '#fff', boxShadow: '0 4px 14px rgba(0,0,0,.4)' }}>✦ DQ Assistant</button>}

    {open && <div style={{ position: 'fixed', bottom: 20, right: 20, zIndex: 1000, width: 360,
        maxHeight: 500, display: 'flex', flexDirection: 'column',
        background: 'var(--card, #1e1e2e)', border: '1px solid var(--border, #444)',
        borderRadius: 10, boxShadow: '0 8px 30px rgba(0,0,0,.5)' }}>
      <div style={{ padding: '10px 14px', borderBottom: '1px solid var(--border, #444)',
          display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <b style={{ fontSize: 13 }}>DQ Assistant</b>
        <button onClick={() => setOpen(false)} style={{ background: 'none', border: 'none',
            color: 'var(--muted, #999)', cursor: 'pointer', fontSize: 16 }}>✕</button>
      </div>
      <div style={{ flex: 1, overflowY: 'auto', padding: 12, minHeight: 260, maxHeight: 340 }}>
        {msgs.map((m, i) => (
          <div key={i} style={{ marginBottom: 10, textAlign: m.role === 'user' ? 'right' : 'left' }}>
            <div style={{ display: 'inline-block', padding: '8px 11px', borderRadius: 10,
                fontSize: 12, whiteSpace: 'pre-wrap', maxWidth: '85%', textAlign: 'left',
                background: m.role === 'user' ? 'var(--info)' : 'var(--bg, #2a2a3a)',
                color: m.role === 'user' ? '#fff' : 'var(--text, #ddd)' }}>
              {m.text}
              {m.source && <div style={{ fontSize: 9, opacity: .6, marginTop: 4 }}>{m.source === 'llm' ? 'Databricks LLM' : 'built-in answers'}</div>}
            </div>
          </div>
        ))}
        {busy && <div className="muted" style={{ fontSize: 11 }}>thinking…</div>}
        <div ref={endRef} />
      </div>
      <div style={{ padding: '6px 10px', display: 'flex', gap: 6, flexWrap: 'wrap', borderTop: '1px solid var(--border, #444)' }}>
        {SUGGESTIONS.map(s => <button key={s} onClick={() => { setInput(s) }}
          style={{ fontSize: 10, padding: '3px 8px', borderRadius: 10, cursor: 'pointer',
                   border: '1px solid var(--border, #444)', background: 'transparent',
                   color: 'var(--muted, #999)' }}>{s}</button>)}
      </div>
      <div style={{ padding: 10, display: 'flex', gap: 8, borderTop: '1px solid var(--border, #444)' }}>
        <input value={input} onChange={e => setInput(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && send()} placeholder="Ask about pipelines, jobs, setup…"
          style={{ flex: 1, padding: '7px 10px', fontSize: 12, borderRadius: 8,
                   border: '1px solid var(--border, #444)', background: 'var(--bg, #14141f)',
                   color: 'var(--text, #ddd)' }} />
        <button onClick={send} disabled={busy} className="primary"
          style={{ padding: '7px 14px', fontSize: 12 }}>Send</button>
      </div>
    </div>}
  </>
}
