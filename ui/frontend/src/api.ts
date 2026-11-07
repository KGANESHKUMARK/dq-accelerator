const B = '/api'
// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function j(r: Response): Promise<any> {
  if (!r.ok) throw new Error((await r.json().catch(() => ({}))).detail ?? r.statusText)
  return r.json()
}
export const api = {
  health: () => fetch(`${B}/health`).then(j),
  pipelines: () => fetch(`${B}/pipelines`).then(j),
  pipeline: (id: string) => fetch(`${B}/pipelines/${id}`).then(j),
  onboard: (manifest_yaml: string) => fetch(`${B}/pipelines/onboard`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ manifest_yaml }) }).then(j),
  pipelineAction: (id: string, action: string, reason = 'via UI') => fetch(`${B}/pipelines/${id}/${action}?reason=${encodeURIComponent(reason)}`, { method: 'POST' }).then(j),
  rulesets: () => fetch(`${B}/rulesets`).then(j),
  publishRuleset: (ruleset_yaml: string, activate = true) => fetch(`${B}/rulesets/publish`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ruleset_yaml, activate }) }).then(j),
  rulesetAction: (id: string, action: string, version: string) => fetch(`${B}/rulesets/${id}/${action}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ version }) }).then(j),
  rulesetDiff: (id: string) => fetch(`${B}/rulesets/${id}/diff`).then(j),
  datasets: () => fetch(`${B}/datasets`).then(j),
  run: (body: any) => fetch(`${B}/dq/run`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }).then(j),
  profile: (body: any) => fetch(`${B}/profile`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }).then(j),
  job: (id: string) => fetch(`${B}/jobs/${id}`).then(j),
  jobs: () => fetch(`${B}/jobs`).then(j),
  reports: () => fetch(`${B}/reports`).then(j),
  report: (name: string) => fetch(`${B}/reports/${name}`).then(j),
}
export type Job = { job_id: string; kind: string; status: string; progress?: string; result?: any; error?: string; submitted_at: string }
