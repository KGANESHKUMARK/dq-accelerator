import React, { useEffect, useState } from 'react'
import { api, Job } from './api'

const usePoll = (jobId: string | null, ms = 2500) => {
  const [job, setJob] = useState<Job | null>(null)
  useEffect(() => {
    if (!jobId) { setJob(null); return }
    // instant feedback — show QUEUED before the first server event arrives
    setJob({ job_id: jobId, kind: '', status: 'QUEUED', progress: 'submitting…', submitted_at: '' } as Job)
    let dead = false; let pollTimer: number | undefined
    const es = new EventSource(`/api/jobs/${jobId}/stream`)
    es.addEventListener('status', ev => {
      const d = JSON.parse(ev.data)
      setJob(prev => prev ? { ...prev, status: d.status, progress: d.progress } : prev)
      if (d.status === 'COMPLETED' || d.status === 'FAILED') {
        es.close()
        api.job(jobId).then(j => { if (!dead) setJob(j) }).catch(() => {})
      }
    })
    es.addEventListener('progress', ev => {
      const d = JSON.parse(ev.data)
      setJob(prev => prev ? { ...prev, progress: d.progress } : prev)
    })
    es.onerror = () => {   // stream dropped -> fall back to polling
      es.close()
      if (pollTimer !== undefined) return
      pollTimer = window.setInterval(async () => {
        try {
          const j = await api.job(jobId)
          if (!dead) setJob(j)
          if (j.status === 'COMPLETED' || j.status === 'FAILED') window.clearInterval(pollTimer)
        } catch { /* keep polling */ }
      }, ms)
    }
    return () => { dead = true; es.close(); if (pollTimer !== undefined) window.clearInterval(pollTimer) }
  }, [jobId])
  return job
}
const JobStatus = ({ job }: { job: Job | null }) => !job ? null : (
  <div className="card">
    <b>{job.job_id}</b> <span className={`badge ${job.status === 'COMPLETED' ? 'ACTIVE' : job.status === 'FAILED' ? 'OFFBOARDED' : 'REGISTERED'}`}>{job.status}</span>
    {job.status !== 'COMPLETED' && job.status !== 'FAILED' && <span className="spin" />}
    <div className="muted">{job.progress}</div>
    {job.error && <pre className="err">{job.error}</pre>}
    {job.result?.summary && <pre>{job.result.summary}</pre>}
  </div>
)

/* ------------------------------------------------------------ chart bits */
const Bar = ({ pct, color }: { pct: number; color?: string }) => (
  <div className="bar"><div className="bar-fill" style={{ width: `${Math.min(100, Math.max(0, pct))}%`, background: color ?? (pct >= 90 ? '#22c55e' : pct >= 70 ? '#f59e0b' : '#ef4444') }} /><span className="bar-label">{pct.toFixed(1)}%</span></div>
)

const Anomaly = ({ a, i }: { a: any; i: number }) => (
  <div className="anomaly" key={i}><span className="badge PAUSED">{a.type}</span><b>{a.column}</b><span className="muted">{a.detail}</span></div>
)

const ColCard = ({ c }: { c: any }) => (
  <div className="colcard"><h4>{c.name}</h4>
    <div className="kv"><span>type</span><b>{c.data_type}{c.inferred_type !== c.data_type ? ` → ${c.inferred_type}` : ''}</b></div>
    <div className="kv"><span>distinct / unique</span><b>{c.distinct_count} / {c.unique_count ?? '-'}</b></div>
    {c.mean != null && <div className="kv"><span>mean ± σ</span><b>{c.mean.toFixed(2)} ± {c.stddev?.toFixed(2)}</b></div>}
    {c.min != null && <div className="kv"><span>min – max</span><b>{String(c.min).slice(0, 14)} – {String(c.max).slice(0, 14)}</b></div>}
    {c.date_min && <div className="kv"><span>dates</span><b>{c.date_min} → {c.date_max} {c.dominant_date_format ? `(${c.dominant_date_format})` : ''}</b></div>}
    {c.outlier_count ? <div className="kv"><span>outliers</span><b className="err">{c.outlier_count}</b></div> : null}
    {c.whitespace_count ? <div className="kv"><span>whitespace</span><b>{c.whitespace_count}</b></div> : null}
    {c.email_match_percentage != null && <div className="kv"><span>email match</span><b>{c.email_match_percentage.toFixed(0)}%</b></div>}
    <div className="kv" style={{ marginTop: 4 }}><span>null rate</span></div><Bar pct={c.null_percentage} />
    {c.top_values?.length ? <div className="muted" style={{ marginTop: 6 }}>top: {c.top_values.slice(0, 3).map((v: any) => `${v.value ?? v[0]} (${v.count ?? v[1]})`).join(', ')}</div> : null}
  </div>
)

const ProfileView = ({ r, jid }: { r: any; jid?: string }) => (
  <div className="card"><h3>Profile{jid ? ` — ${jid}` : ''} · {r.row_count} rows × {r.column_count} cols</h3>
    {r.pk_candidates?.length > 0 && <p style={{ fontSize: 12 }}>Primary-key candidates: {r.pk_candidates.map((k: string) => <span key={k} className="badge ACTIVE" style={{ marginRight: 6 }}>{k}</span>)}</p>}
    {(r.anomalies?.length > 0 || r.schema_anomalies?.length > 0) && <>
      <h4 style={{ margin: '8px 0 4px' }}>Anomalies ({(r.anomalies?.length || 0) + (r.schema_anomalies?.length || 0)})</h4>
      {[...(r.anomalies || []), ...(r.schema_anomalies || [])].map((a: any, i: number) => <Anomaly a={a} i={i} key={i} />)}
    </>}
    <div className="colgrid">{(r.columns || []).map((c: any) => <ColCard c={c} key={c.name} />)}</div>
  </div>
)

const RulesView = ({ rules }: { rules: any[] }) => !rules?.length ? null : (
  <div className="card"><h3>Rule results</h3>
    {rules.map(r => <div className="rule-row" key={r.rule_id}>
      <span>{r.rule_id}</span>
      <span className={`badge ${r.status === 'PASSED' ? 'ACTIVE' : r.severity === 'ERROR' ? 'OFFBOARDED' : 'PAUSED'}`}>{r.status}</span>
      <Bar pct={r.pass_pct} />
      <span className="muted">{r.passed}/{r.total} · {r.column ?? r.rule_type}</span>
    </div>)}
  </div>
)

/* ------------------------------------------------------------ lifecycle */
const LC_MAIN = ['REGISTERED', 'TESTED', 'ACTIVE']
const LC_NEXT: Record<string, string> = {
  REGISTERED: 'Next: run test — validates the manifest\'s test_data through the engine',
  TESTED: 'Next: activate — promotes the pipeline to governed ACTIVE (or test again)',
  ACTIVE: 'Ready — run DQ / Profile from the Run DQ tab; pause or offboard anytime',
  PAUSED: 'Paused — activate resumes; test or offboard also allowed',
  OFFBOARDED: 'Offboarded — audit history retained; re-onboarding starts a fresh registration',
  DEACTIVATING: 'Deactivating — transitioning to OFFBOARDED',
}
const Lifecycle = ({ state }: { state: string }) => {
  const side = state === 'PAUSED' || state === 'OFFBOARDED'
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
      {side && <>
        <div className="conn done" />
        <div className={`step ${state === 'PAUSED' ? 'warned' : 'term'}`}>
          <span className="dot">{state === 'PAUSED' ? '⏸' : '✕'}</span><span className="lbl">{state}</span>
        </div></>}
    </div>
    <div className="muted">{LC_NEXT[state] ?? `state ${state}`}</div>
  </>
}

/* ------------------------------------------------------------ flow guide */
const FLOW = [
  ['Onboard', 'paste a pipeline manifest below → state REGISTERED'],
  ['Test', 'engine runs the manifest\'s test_data → TESTED'],
  ['Activate', 'promote to ACTIVE — the pipeline is now governed'],
  ['Run DQ', 'Run DQ tab → OBSERVE_ONLY / ENFORCE / REMEDIATE, or Profile only'],
  ['Review', 'rule bars, anomaly cards, quarantine breakdown — Dashboard → view · Reports'],
  ['Iterate', 'publish ruleset v1.1, approve pending corrections, re-run; pause / offboard anytime'],
]
const FlowGuide = () => (
  <div className="card"><h3>How a pipeline flows through the accelerator</h3>
    {FLOW.map(([t, d], i) => <div className="gstep" key={t}><span className="n">{i + 1}</span><span><b>{t}</b>{d}</span></div>)}
  </div>
)

/* Renders a persisted job result — works for current and historical jobs */
const JobResult = ({ job }: { job: Job | null }) => {
  const r = job?.result
  if (!job || !r) return null
  if (job.kind === 'profile') return <ProfileView r={r} jid={job.job_id} />
  return <>
    <div className="card"><h3>Result — {job.job_id}</h3>
      <div className="grid2">
        <div><div className="stat">{r.input}</div><div className="muted">input</div></div>
        <div><div className="stat ok">{r.accepted}</div><div className="muted">accepted</div></div>
        <div><div className="stat err">{r.quarantined}</div><div className="muted">quarantined</div></div>
        <div><div className="stat">{r.gate_passed ? 'PASS' : 'FAIL'}</div><div className="muted">quality gate</div></div>
      </div>
      {r.quarantine_by_status && <p className="muted">quarantine by status: {JSON.stringify(r.quarantine_by_status)}</p>}
      <p className="muted">report: {r.report_path}</p>
    </div>
    <RulesView rules={r.rules ?? []} />
  </>
}

/* ------------------------------------------------------------ Dashboard */
export function Dashboard() {
  const [pipes, setPipes] = useState<any[]>([]); const [jobs, setJobs] = useState<Job[]>([])
  const [viewJob, setViewJob] = useState<Job | null>(null)
  const load = () => { api.pipelines().then(setPipes); api.jobs().then(setJobs) }
  useEffect(() => { load(); const t = setInterval(load, 5000); return () => clearInterval(t) }, [])
  const states = pipes.reduce((a: any, p) => ({ ...a, [p.state]: (a[p.state] || 0) + 1 }), {})
  return <>
    <FlowGuide />
    <div className="grid2">
      <div className="card"><div className="stat">{pipes.length}</div><div className="muted">registered pipelines</div></div>
      <div className="card"><div className="stat">{jobs.filter(j => j.status === 'COMPLETED').length}</div><div className="muted">completed jobs</div></div>
    </div>
    <div className="card"><h3>Pipeline states</h3>
      <div>{Object.entries(states).map(([s, n]: any) => <span key={s} className={`badge ${s}`} style={{ marginRight: 8 }}>{s}: {n}</span>)}</div>
    </div>
    <div className="card"><h3>Recent jobs</h3>
      <table><thead><tr><th>Job</th><th>Kind</th><th>Status</th><th>Submitted</th><th></th></tr></thead>
        <tbody>{jobs.map(j => <tr key={j.job_id}><td>{j.job_id}</td><td>{j.kind}</td>
          <td><span className={`badge ${j.status === 'COMPLETED' ? 'ACTIVE' : j.status === 'FAILED' ? 'OFFBOARDED' : 'REGISTERED'}`}>{j.status}</span></td>
          <td className="muted">{j.submitted_at}</td>
          <td>{j.status === 'COMPLETED' && <button className="btn sec" onClick={() => api.job(j.job_id).then(setViewJob)}>view</button>}</td></tr>)}</tbody></table>
    </div>
    <JobResult job={viewJob} />
  </>
}

/* ------------------------------------------------------------ Pipelines */
export function Pipelines() {
  const [pipes, setPipes] = useState<any[]>([]); const [yaml, setYaml] = useState('')
  const [msg, setMsg] = useState(''); const [detail, setDetail] = useState<any>(null)
  const load = () => api.pipelines().then(setPipes)
  useEffect(() => { load() }, [])
  const act = async (id: string, a: string) => { const r = await api.pipelineAction(id, a); setMsg(`${id} → ${r.state}`); load() }
  const onboard = async () => { try { const r = await api.onboard(yaml); setMsg(`onboarded ${r.pipeline_id} → ${r.state}`); setYaml(''); load() } catch (e: any) { setMsg('ERR ' + e.message) } }
  const show = async (id: string) => setDetail(await api.pipeline(id))
  return <>
    <FlowGuide />
    <div className="card"><h3>Registered pipelines</h3>
      <table><thead><tr><th>ID</th><th>Name</th><th>Platform</th><th>DQ mode</th><th>State</th><th>Exec</th><th>Actions</th></tr></thead>
        <tbody>{pipes.map(p => <tr key={p.pipeline_id}>
          <td><a href="#" onClick={e => { e.preventDefault(); show(p.pipeline_id) }}>{p.pipeline_id}</a></td>
          <td>{p.pipeline_name}</td><td>{p.platform}</td><td>{p.dq_mode}</td>
          <td><span className={`badge ${p.state}`}>{p.state}</span></td><td>{p.executions}</td>
          <td>{['TESTED', 'ACTIVE'].includes(p.state) || true ?
            <>{['test', 'activate', 'pause', 'offboard'].map(a =>
              <button key={a} className="btn sec" onClick={() => act(p.pipeline_id, a)}>{a}</button>)}</> : null}</td>
        </tr>)}</tbody></table>
      {msg && <p className="ok">{msg}</p>}
    </div>
    <div className="card"><h3>Onboard pipeline (manifest YAML)</h3>
      <textarea value={yaml} onChange={e => setYaml(e.target.value)} placeholder="paste config/pipelines/*.yaml content" />
      <button className="btn" onClick={onboard} disabled={!yaml.trim()}>Onboard</button>
      <span className="muted"> or load an existing manifest into the box manually</span>
    </div>
    {detail && <div className="card"><h3>{detail.pipeline_id} — detail</h3>
      <Lifecycle state={detail.state} />
      <div className="muted">state={detail.state} history={detail.history?.length} executions={detail.executions?.length}</div>
      <h4>History</h4><pre>{JSON.stringify(detail.history, null, 1)}</pre>
      <h4>Manifest</h4><pre>{JSON.stringify(detail.manifest, null, 1)}</pre>
    </div>}
  </>
}

/* ------------------------------------------------------------ Run DQ */
export function RunDQ() {
  const [datasets, setDatasets] = useState<any[]>([]); const [rulesets, setRulesets] = useState<any[]>([])
  const [src, setSrc] = useState(''); const [rs, setRs] = useState('config/rulesets/customers_v1.yaml')
  const [mode, setMode] = useState('OBSERVE_ONLY'); const [ds, setDs] = useState('customers')
  const [pid, setPid] = useState('UI_RUN'); const [jobId, setJobId] = useState<string | null>(null)
  const [pol, setPol] = useState('config/policies/default.yaml')
  const [pjob, setPjob] = useState<string | null>(null)
  const [past, setPast] = useState<Job[]>([])
  const job = usePoll(jobId); const prof = usePoll(pjob)
  useEffect(() => { api.datasets().then(d => { setDatasets(d); setSrc(d[0]?.path ?? '') }); api.rulesets().then(setRulesets) }, [])
  useEffect(() => { api.jobs().then(setPast) }, [job?.status, prof?.status])
  const submit = async () => { const r = await api.run({ source: src, ruleset: rs, mode, dataset: ds, pipeline_id: pid, policy: pol }); setJobId(r.job_id) }
  const submitProfile = async () => { const r = await api.profile({ source: src, dataset: ds, mode: 'FULL' }); setPjob(r.job_id) }
  return <>
    <div className="card"><h3>Run data quality</h3>
      <div className="row">
        <div><label>Source file</label><select value={src} onChange={e => setSrc(e.target.value)}>
          {datasets.map(d => <option key={d.path} value={d.path}>{d.id} — {d.path}</option>)}</select></div>
        <div><label>Rule set</label><select value={rs} onChange={e => setRs(e.target.value)}>
          <option value="config/rulesets/customers_v1.yaml">customers_v1</option>
          <option value="config/rulesets/customers_v1_1.yaml">customers_v1_1</option>
          <option value="config/rulesets/transactions_v1.yaml">transactions_v1</option>
          <option value="config/rulesets/patients_v1.yaml">patients_v1</option></select></div>
        <div><label>Policy</label><select value={pol} onChange={e => setPol(e.target.value)}>
          <option value="config/policies/default.yaml">default (banking)</option>
          <option value="config/policies/healthcare.yaml">healthcare (PHI)</option></select></div>
        <div><label>Mode</label><select value={mode} onChange={e => setMode(e.target.value)}>
          {['OBSERVE_ONLY', 'ENFORCE', 'REMEDIATE'].map(m => <option key={m}>{m}</option>)}</select></div>
        <div><label>Dataset</label><input value={ds} onChange={e => setDs(e.target.value)} /></div>
        <div><label>Pipeline ID</label><input value={pid} onChange={e => setPid(e.target.value)} /></div>
        <div><label>View past result</label><select value="" onChange={e => {
          const id = e.target.value
          if (!id) return
          if (past.find(x => x.job_id === id)?.kind === 'profile') setPjob(id); else setJobId(id)
          e.target.value = ''
        }}>
          <option value="">— completed jobs —</option>
          {past.filter(x => x.status === 'COMPLETED').map(x => <option key={x.job_id} value={x.job_id}>{x.job_id} · {x.kind}</option>)}
        </select></div>
      </div>
      <button className="btn" onClick={submit}>Run DQ</button>
      <button className="btn sec" onClick={submitProfile}>Profile only</button>
      <span className="muted"> Spark runs take 10–70 s — status polls automatically.</span>
    </div>
    <JobStatus job={job} />
    <JobResult job={job} />
    <JobStatus job={prof} />
    <JobResult job={prof} />
  </>
}

/* ------------------------------------------------------------ Rule Sets */
export function RuleSets() {
  const [rs, setRs] = useState<any[]>([]); const [yaml, setYaml] = useState(''); const [msg, setMsg] = useState('')
  const [diff, setDiff] = useState<any>(null)
  const load = () => api.rulesets().then(setRs)
  useEffect(() => { load() }, [])
  const pub = async () => { try { const r = await api.publishRuleset(yaml); setMsg(`${r.ref} published checksum=${r.checksum}`); setYaml(''); load() } catch (e: any) { setMsg('ERR ' + e.message) } }
  const act = async (id: string, a: string, v: string) => { try { await api.rulesetAction(id, a, v); setMsg(`${id} ${a} → ${v}`); load() } catch (e: any) { setMsg('ERR ' + e.message) } }
  const showDiff = async (id: string) => { try { setDiff(await api.rulesetDiff(id)) } catch (e: any) { setMsg('ERR ' + e.message) } }
  return <>
    <div className="card"><h3>Published rule sets</h3>
      <table><thead><tr><th>RuleSet</th><th>Dataset</th><th>Version</th><th>Rules</th><th>Checksum</th><th>Active</th><th></th></tr></thead>
        <tbody>{rs.map(r => <tr key={r.ruleset_id + r.version}>
          <td>{r.ruleset_id}</td><td>{r.dataset}</td><td>{r.version}</td><td>{r.rules}</td>
          <td className="muted">{r.checksum}</td>
          <td>{r.active ? <span className="badge ACTIVE">ACTIVE</span> : ''}</td>
          <td>{!r.active && <button className="btn sec" onClick={() => act(r.ruleset_id, 'activate', r.version)}>activate</button>}
            <button className="btn sec" onClick={() => showDiff(r.ruleset_id)}>diff</button></td>
        </tr>)}</tbody></table>
      {msg && <p className="ok">{msg}</p>}
    </div>
    {diff && <div className="card"><h3>Diff {diff.from_version} → {diff.to_version}</h3>
      <pre>{JSON.stringify(diff.summary, null, 1)}
added: {diff.added?.join(', ')}
removed: {diff.removed?.join(', ')}
modified: {diff.modified?.map((m: any) => m.rule_id).join(', ')}</pre></div>}
    <div className="card"><h3>Publish rule set YAML</h3>
      <textarea value={yaml} onChange={e => setYaml(e.target.value)} placeholder="paste a ruleset YAML (e.g. config/rulesets/customers_v1_1.yaml)" />
      <button className="btn" onClick={pub} disabled={!yaml.trim()}>Publish + activate</button>
    </div>
  </>
}

/* ------------------------------------------------------------ Reports */
export function Reports() {
  const [reps, setReps] = useState<any[]>([]); const [sel, setSel] = useState<any>(null)
  const load = () => api.reports().then(setReps)
  useEffect(() => { load() }, [])
  const open = async (name: string) => setSel(await api.report(name))
  return <>
    <div className="card"><h3>Execution reports</h3>
      <table><thead><tr><th>File</th><th>Size</th><th>Modified</th></tr></thead>
        <tbody>{reps.map(r => <tr key={r.name} style={{ cursor: 'pointer' }} onClick={() => open(r.name)}>
          <td>{r.name}</td><td className="muted">{(r.size / 1024).toFixed(1)} KB</td>
          <td className="muted">{new Date(r.modified * 1000).toLocaleString()}</td></tr>)}</tbody></table>
      {!reps.length && <p className="muted">No reports yet — run a DQ job or the standalone example.</p>}
    </div>
    {sel && <div className="card"><h3>{sel.run_id ?? 'report'}</h3>
      <div className="muted">mode={sel.integration_mode} dataset={sel.execution_metrics?.dataset} gate={JSON.stringify(sel.quality_gate?.passed)}</div>
      <pre>{JSON.stringify(sel, null, 1).slice(0, 20000)}</pre></div>}
  </>
}
