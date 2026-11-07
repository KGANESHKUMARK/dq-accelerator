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
/* resolve a manifest-relative path (../../sample_data/x, ../rulesets/x) to repo-relative */
const resolveRel = (rel: string) => {
  const out: string[] = []
  for (const s of `config/pipelines/${rel}`.split('/')) {
    if (s === '..') out.pop(); else if (s && s !== '.') out.push(s)
  }
  return out.join('/')
}

const ADHOC_RULESETS = [
  ['customers_v1', 'config/rulesets/customers_v1.yaml'],
  ['customers_v1_1', 'config/rulesets/customers_v1_1.yaml'],
  ['transactions_v1', 'config/rulesets/transactions_v1.yaml'],
  ['customer_summary_gold_v1', 'config/rulesets/customer_summary_gold_v1.yaml'],
  ['patients_v1', 'config/rulesets/patients_v1.yaml'],
  ['orders_v1', 'config/rulesets/orders_v1.yaml'],
  ['loans_v1', 'config/rulesets/loans_v1.yaml'],
  ['employees_v1', 'config/rulesets/employees_v1.yaml'],
]

export function RunDQ() {
  const [datasets, setDatasets] = useState<any[]>([])
  const [pipes, setPipes] = useState<any[]>([])
  const [pipeSel, setPipeSel] = useState('ADHOC')
  const [govDs, setGovDs] = useState<any[]>([])            // manifest datasets when governed
  const [rsOpts, setRsOpts] = useState<{ label: string; value: string }[]>([])
  const [src, setSrc] = useState(''); const [rs, setRs] = useState('config/rulesets/customers_v1.yaml')
  const [mode, setMode] = useState('OBSERVE_ONLY'); const [ds, setDs] = useState('customers')
  const [pid, setPid] = useState('UI_RUN'); const [jobId, setJobId] = useState<string | null>(null)
  const [pol, setPol] = useState('config/policies/default.yaml')
  const [pjob, setPjob] = useState<string | null>(null)
  const [past, setPast] = useState<Job[]>([])
  const job = usePoll(jobId); const prof = usePoll(pjob)
  const governed = pipeSel !== 'ADHOC'

  useEffect(() => {
    api.datasets().then(d => { setDatasets(d); setSrc(d[0]?.path ?? '') })
    api.pipelines().then(setPipes)
  }, [])
  useEffect(() => { api.jobs().then(setPast) }, [job?.status, prof?.status])

  const applyDataset = (e: any) => {           // e = manifest dataset entry
    setDs(e.dataset_id)
    const opts = Object.entries(e.rulesets ?? {}).map(([st, p]: any) => ({
      label: `${st}: ${String(p).split('/').pop()?.replace('.yaml', '')}`,
      value: resolveRel(String(p)),
    }))
    setRsOpts(opts)
    if (opts[0]) setRs(opts[0].value)
    if (e.test_data) setSrc(resolveRel(e.test_data))
  }

  const onPipeline = async (id: string) => {
    setPipeSel(id)
    if (id === 'ADHOC') { setGovDs([]); setPid('UI_RUN'); return }
    const d = await api.pipeline(id)
    const dss = d.manifest?.datasets ?? []
    setGovDs(dss)
    setPid(id)
    if (d.manifest?.dq_mode) setMode(d.manifest.dq_mode)
    if (d.manifest?.policy) setPol(resolveRel(d.manifest.policy))
    if (dss.length) applyDataset(dss[0])
  }

  const submit = async () => { const r = await api.run({ source: src, ruleset: rs, mode, dataset: ds, pipeline_id: pid, policy: pol }); setJobId(r.job_id) }
  const submitProfile = async () => { const r = await api.profile({ source: src, dataset: ds, mode: 'FULL' }); setPjob(r.job_id) }
  const rulesetOptions = governed ? rsOpts : ADHOC_RULESETS.map(([l, v]) => ({ label: l, value: v }))

  return <>
    <div className="card"><h3>Run data quality</h3>
      <div className="row">
        <div><label>Pipeline</label><select value={pipeSel} onChange={e => onPipeline(e.target.value)}>
          <option value="ADHOC">— ad-hoc run (unregistered) —</option>
          {pipes.map(p => <option key={p.pipeline_id} value={p.pipeline_id}>{p.pipeline_id} ({p.state})</option>)}
        </select></div>
        <div><label>{governed ? 'Dataset (manifest)' : 'Source file'}</label>
          {governed
            ? <select value={ds} onChange={e => applyDataset(govDs.find(x => x.dataset_id === e.target.value))}>
                {govDs.map((d: any) => <option key={d.dataset_id} value={d.dataset_id}>{d.dataset_id}</option>)}
              </select>
            : <select value={src} onChange={e => { setSrc(e.target.value); setDs(e.target.selectedOptions[0].text.split(' — ')[0]) }}>
                {datasets.map(d => <option key={d.path} value={d.path}>{d.id} — {d.path}</option>)}</select>}
        </div>
        {governed && <div><label>Source (test_data)</label><input value={src} readOnly /></div>}
        <div><label>Rule set</label><select value={rs} onChange={e => setRs(e.target.value)}>
          {rulesetOptions.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}</select></div>
        <div><label>Policy{governed ? ' (manifest)' : ''}</label><select value={pol} onChange={e => setPol(e.target.value)} disabled={governed}>
          <option value="config/policies/default.yaml">default (banking)</option>
          <option value="config/policies/healthcare.yaml">healthcare (PHI)</option></select></div>
        <div><label>Mode</label><select value={mode} onChange={e => setMode(e.target.value)}>
          {['OBSERVE_ONLY', 'ENFORCE', 'REMEDIATE'].map(m => <option key={m}>{m}</option>)}</select></div>
        <div><label>Pipeline ID</label><input value={pid} readOnly={governed} onChange={e => setPid(e.target.value)} /></div>
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
      {governed && <p className="muted" style={{ fontSize: 12 }}>
        Governed run — dataset, source, ruleset and policy come from the manifest binding;
        only the mode is adjustable. Pick "ad-hoc" for an ungoverned run.
      </p>}
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

/* ------------------------------------------------------------ Onboard wizard */
const WIZ_STEPS = ['Source', 'Options', 'Profile', 'Review rules', 'Finalize']

export function Onboard() {
  const [step, setStep] = useState(0)
  const [datasets, setDatasets] = useState<any[]>([])
  const [source, setSource] = useState('')
  const [dataset, setDataset] = useState('')
  const [pipelineId, setPipelineId] = useState('')
  const [platform, setPlatform] = useState('DATABRICKS')
  const [stage, setStage] = useState('SILVER')
  const [dqMode, setDqMode] = useState('OBSERVE_ONLY')
  const [policy, setPolicy] = useState('../policies/default.yaml')
  const [owner, setOwner] = useState('')
  const [pub, setPub] = useState(true)
  const [onb, setOnb] = useState(true)
  const [force, setForce] = useState(false)
  const [jobId, setJobId] = useState<string | null>(null)
  const [approved, setApproved] = useState<Set<string>>(new Set())
  const [result, setResult] = useState<any>(null)
  const [err, setErr] = useState('')
  const job = usePoll(jobId)
  const preview = job?.status === 'COMPLETED' ? job.result : null

  useEffect(() => { api.datasets().then(setDatasets) }, [])
  useEffect(() => {           // preview done -> pre-approve all candidates
    if (preview?.candidates) setApproved(new Set(preview.candidates.map((c: any) => c.rule_id)))
  }, [job?.status])

  const pickDataset = (path: string, name: string) => {
    setSource(path); setDataset(name)
    if (!pipelineId) setPipelineId(`${name.toUpperCase()}_PIPELINE`)
  }
  const upload = async (f: File) => {
    setErr('')
    try {
      const r = await api.upload(f)
      pickDataset(r.path, r.filename.replace(/\.[^.]+$/, '').toLowerCase())
    } catch (e: any) { setErr(e.message) }
  }
  const runPreview = async () => {
    setErr(''); setResult(null)
    try { setJobId((await api.onboardPreview({ source, dataset, stage })).job_id); setStep(2) }
    catch (e: any) { setErr(e.message) }
  }
  const finalize = async () => {
    setErr('')
    try {
      setResult(await api.onboardFinalize({
        preview_job_id: jobId, approved_rule_ids: [...approved], dataset,
        ruleset_id: preview.ruleset_id, pipeline_id: pipelineId, platform, stage,
        dq_mode: dqMode, policy, owner, source, publish: pub, onboard: onb, force,
      }))
      setStep(4)
    } catch (e: any) { setErr(e.message) }
  }
  const confColor = (c: number) => c >= 0.9 ? '#22c55e' : c >= 0.7 ? '#f59e0b' : '#ef4444'

  return <>
    <div className="card">
      <h3>Onboard a new pipeline — bootstrap wizard</h3>
      <div className="steps" style={{ marginBottom: 8 }}>
        {WIZ_STEPS.map((s, i) => <React.Fragment key={s}>
          {i > 0 && <div className={`conn ${i <= step ? 'done' : ''}`} />}
          <div className={`step ${i < step ? 'done' : i === step ? 'current' : ''}`}>
            <span className="dot">{i < step ? '✓' : i + 1}</span><span className="lbl">{s}</span>
          </div>
        </React.Fragment>)}
      </div>
      <p className="muted" style={{ fontSize: 12 }}>
        Profile the source → auto-generate candidate rules → review &amp; approve →
        ruleset + manifest written under <code>config/</code> → optional publish &amp; onboard.
        Same path as <code>dqctl bootstrap</code>.
      </p>
    </div>
    {err && <div className="card"><pre className="err">{err}</pre></div>}

    {step === 0 && <div className="card">
      <h3>1 · Source data</h3>
      <p className="muted">Pick a Bronze sample, or upload a CSV/Parquet extract of your source.</p>
      <select value={source} onChange={e => pickDataset(e.target.value, e.target.selectedOptions[0].dataset.n ?? '')}>
        <option value="">— choose sample_data/bronze file —</option>
        {datasets.map(d => <option key={d.id} value={d.path} data-n={d.id}>{d.id} — {d.path}</option>)}
      </select>
      <span className="muted" style={{ margin: '0 10px' }}>or</span>
      <input type="file" accept=".csv,.parquet" onChange={e => e.target.files?.[0] && upload(e.target.files[0])} />
      {source && <p className="ok" style={{ marginTop: 8 }}>source: {source}</p>}
      <div style={{ marginTop: 10 }}><button className="btn" disabled={!source} onClick={() => setStep(1)}>Next →</button></div>
    </div>}

    {step === 1 && <div className="card">
      <h3>2 · Pipeline options</h3>
      <div className="grid2">
        <label>Dataset id<input value={dataset} onChange={e => setDataset(e.target.value)} /></label>
        <label>Pipeline id<input value={pipelineId} onChange={e => setPipelineId(e.target.value)} /></label>
        <label>Platform<select value={platform} onChange={e => setPlatform(e.target.value)}>
          <option>DATABRICKS</option><option>FABRIC</option><option>STANDALONE_SPARK</option>
          <option>DATAPROC</option><option>AIRFLOW</option><option>PYTHON</option><option>OTHER</option>
        </select></label>
        <label>Stage<select value={stage} onChange={e => setStage(e.target.value)}>
          <option>SILVER</option><option>GOLD</option><option>BRONZE</option>
        </select></label>
        <label>DQ mode<select value={dqMode} onChange={e => setDqMode(e.target.value)}>
          <option>OBSERVE_ONLY</option><option>ENFORCE</option><option>REMEDIATE</option>
        </select></label>
        <label>Policy<input value={policy} onChange={e => setPolicy(e.target.value)} /></label>
        <label>Owner<input value={owner} onChange={e => setOwner(e.target.value)} placeholder="team@example.com" /></label>
      </div>
      <div style={{ marginTop: 8 }}>
        <label className="chk"><input type="checkbox" checked={pub} onChange={e => setPub(e.target.checked)} /> publish + activate ruleset</label>
        <label className="chk"><input type="checkbox" checked={onb} onChange={e => setOnb(e.target.checked)} /> onboard pipeline</label>
        <label className="chk"><input type="checkbox" checked={force} onChange={e => setForce(e.target.checked)} /> overwrite existing files</label>
      </div>
      <div style={{ marginTop: 10 }}>
        <button className="btn sec" onClick={() => setStep(0)}>← Back</button>
        <button className="btn" onClick={runPreview} disabled={!dataset || !pipelineId}>Profile + generate rules →</button>
      </div>
    </div>}

    {step === 2 && <div className="card">
      <h3>3 · Profile + candidate rules</h3>
      <JobStatus job={job} />
      {preview && <>
        <p className="ok">{preview.row_count} rows × {preview.column_count} cols · {preview.candidate_count} candidate rules generated</p>
        {(preview.anomalies?.length > 0 || preview.schema_anomalies?.length > 0) && <>
          <h4 style={{ margin: '8px 0 4px' }}>Anomalies found ({(preview.anomalies?.length || 0) + (preview.schema_anomalies?.length || 0)})</h4>
          {[...(preview.anomalies || []), ...(preview.schema_anomalies || [])].map((a: any, i: number) => <Anomaly a={a} i={i} key={i} />)}
        </>}
        <button className="btn" style={{ marginTop: 10 }} onClick={() => setStep(3)}>Review rules →</button>
      </>}
    </div>}

    {step === 3 && preview && <div className="card">
      <h3>4 · Review generated rules — <span className="muted">{approved.size} of {preview.candidate_count} approved</span></h3>
      <p className="muted" style={{ fontSize: 12 }}>Unchecked rules are excluded from the ruleset. Confidence comes from profiling evidence — review anything below 0.9 carefully.</p>
      <div>
        {preview.candidates.map((c: any) => <label className="rule-row" key={c.rule_id} style={{ cursor: 'pointer' }}>
          <input type="checkbox" checked={approved.has(c.rule_id)}
            onChange={e => { const s = new Set(approved); e.target.checked ? s.add(c.rule_id) : s.delete(c.rule_id); setApproved(s) }} />
          <span style={{ minWidth: 170 }}>{c.rule_id}</span>
          <span className={`badge ${c.severity === 'ERROR' ? 'OFFBOARDED' : 'PAUSED'}`}>{c.severity}</span>
          <span style={{ minWidth: 150 }}>{c.rule_type}</span>
          <span className="muted" style={{ minWidth: 110 }}>{c.column ?? '-'}</span>
          <span style={{ color: confColor(c.confidence), fontWeight: 600, minWidth: 48 }}>{c.confidence}</span>
          <span className="muted" style={{ flex: 1 }}>{c.description}</span>
        </label>)}
      </div>
      <div style={{ marginTop: 10 }}>
        <button className="btn sec" onClick={() => setStep(1)}>← Options</button>
        <button className="btn" onClick={finalize} disabled={!approved.size}>Write ruleset + manifest{onb ? ' + onboard' : ''} →</button>
      </div>
    </div>}

    {step === 4 && result && <div className="card">
      <h3>5 · Done</h3>
      <p className="ok">ruleset <b>{result.ruleset}</b> — {result.rules_approved} approved / {result.rules_rejected} rejected</p>
      <div className="muted">written: {result.ruleset_path} · {result.manifest_path}
        {result.published && <> · published checksum {result.published.checksum}</>}
        {result.onboarded && <> · <b>{result.onboarded.pipeline_id}</b> → <span className="badge ACTIVE">{result.onboarded.state}</span></>}
      </div>
      {result.onboarded && <p className="muted" style={{ marginTop: 6 }}>Next: Pipelines tab → test → activate → Run DQ.</p>}
      <h4 style={{ margin: '10px 0 4px' }}>Manifest</h4>
      <pre style={{ maxHeight: 300, overflow: 'auto' }}>{result.manifest_yaml}</pre>
      <button className="btn sec" style={{ marginTop: 8 }}
        onClick={() => { setStep(0); setSource(''); setJobId(null); setResult(null); setPipelineId('') }}>Onboard another</button>
    </div>}
  </>
}
