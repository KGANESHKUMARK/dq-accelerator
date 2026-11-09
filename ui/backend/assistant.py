"""DQ Assistant — plug-and-play chatbot for the DQ Accelerator UI.

Answers questions about onboarded pipelines, rulesets, jobs, anomalies,
quarantine status, config, AI provider, and next steps. Uses Databricks
Model Serving via LiteLLM when credentials are set; falls back to a local
rule-based answerer otherwise.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]


def gather_context() -> dict[str, Any]:
    """Collect live state for grounding the assistant."""
    import jobs as J
    from dq_core.runtime.config import EngineConfig
    cfg = EngineConfig.from_yaml(ROOT / "config" / "engine.yaml")
    ctx: dict[str, Any] = {"engine": {"name": cfg.name, "default_mode": cfg.default_mode.value}}

    # pipelines
    from dq_core.registries.factory import build_pipeline_registry, build_ruleset_registry
    preg = build_pipeline_registry(cfg.registry)
    ctx["pipelines"] = preg.list() if preg else []

    # rulesets
    rreg = build_ruleset_registry(cfg.registry)
    if rreg:
        ctx["rulesets"] = [{"id": rid, "versions": [v.get("version") for v in rreg.list_versions(rid)]}
                           for rid in rreg.list_rulesets()]

    # jobs
    ctx["recent_jobs"] = J.list_jobs(limit=10)

    # anomalies + quarantine
    try:
        from main import _load_anomalies, _load_quarantine_pending
        ctx["anomaly_stats"] = {"open": sum(1 for a in _load_anomalies() if a.get("status") == "OPEN")}
        ctx["quarantine_pending"] = _load_quarantine_pending()[:20]
    except Exception:
        pass

    # reports summary
    try:
        from main import _read_reports_index
        reps = [r for r in _read_reports_index() if r.get("execution_metrics")]
        ctx["reports_total"] = len(reps)
        ctx["latest_report"] = reps[0].get("execution_metrics", {}) if reps else None
    except Exception:
        pass

    # ai provider
    try:
        ctx["ai"] = J.get_engine().ai_provider.describe()
    except Exception as exc:
        ctx["ai"] = {"error": str(exc)}

    # datasets on disk
    d = ROOT / "sample_data" / "bronze"
    ctx["datasets"] = [p.stem for p in sorted(d.glob("*.csv"))] if d.exists() else []
    return ctx


SYSTEM_PROMPT = """You are DQ Assistant, the built-in helper of the DQ Accelerator app.
Answer ONLY questions about this app and its data — pipelines, rulesets, jobs,
anomalies, quarantine, profiling, remediation modes, DQX/LLM setup, restart steps,
and the project roadmap. Keep answers short, specific, and action-oriented.
If asked something unrelated, say you only help with this app.

LIVE APP STATE (JSON):
{context}

Key facts about this app:
- Modes: OBSERVE_ONLY (flag only), ENFORCE (quarantine), REMEDIATE (fix + revalidate)
- AI provider: DQXProvider (real Databricks LLM) or MockAIProvider (YAML lookup fallback)
- Confidence gate: >=90% auto-apply; sensitive columns need human approval in Approvals tab
- Onboarding: Onboard tab → upload CSV → profile → approve rules → publish → pipeline
- Restart: run DQ_start.bat; set DATABRICKS_HOST/TOKEN/DQX_MODEL_ENDPOINT for real LLM
- Registry: versioned rulesets (publish/activate/rollback/diff), pipeline lifecycle
"""


def _llm_answer(question: str, context: dict[str, Any]) -> str | None:
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN")
    endpoint = os.environ.get("DQX_MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")
    if not host or not token:
        return None
    try:
        from litellm import completion
        resp = completion(
            model=f"databricks/{endpoint}",
            api_key=token,
            api_base=f"{host}/serving-endpoints",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT.format(context=json.dumps(context, default=str)[:12000])},
                {"role": "user", "content": question},
            ],
            max_tokens=600,
            temperature=0.2,
            timeout=30,
        )
        return resp.choices[0].message.content
    except Exception:
        return None


def _fallback_answer(question: str, context: dict[str, Any]) -> str:
    q = question.lower()
    pipes = context.get("pipelines") or []
    jobs = context.get("recent_jobs") or []
    ai = context.get("ai") or {}

    if any(w in q for w in ("pipeline", "onboard", "registered", "list")):
        if not pipes:
            return "No pipelines are onboarded yet. Use the Onboard tab to upload a CSV and create one."
        lines = [f"**{p.get('pipeline_id')}** — {p.get('state')} · {p.get('dq_mode')} · {p.get('platform', '')}"
                 for p in pipes]
        return "Onboarded pipelines:\n" + "\n".join(lines)

    if any(w in q for w in ("ruleset", "rule")):
        rs = context.get("rulesets") or []
        if not rs:
            return "No published rulesets yet."
        return "Published rulesets:\n" + "\n".join(f"- **{r['id']}** versions {r['versions']}" for r in rs)

    if "anomal" in q:
        stats = context.get("anomaly_stats") or {}
        return f"Anomalies: {stats.get('open', 0)} open. Check the Anomalies tab for details."

    if "quarantine" in q or "approv" in q:
        pend = context.get("quarantine_pending") or []
        return f"{len(pend)} records pending human approval. See the Approvals tab."

    if any(w in q for w in ("ai", "llm", "dqx", "mock")):
        return (f"AI provider: **{ai.get('provider')}** — simulated={ai.get('simulated')}, "
                f"credentials_set={ai.get('credentials_set')}. "
                "Set DATABRICKS_HOST/TOKEN/DQX_MODEL_ENDPOINT and restart for real LLM.")

    if "restart" in q or "start" in q:
        return ("Run `DQ_start.bat` — it checks Python/Java/Node, installs deps, and launches "
                "backend :8000 + frontend :5173. To change Spark memory, edit `config/engine.yaml` "
                "→ `runtime` section, then re-run the launcher.")

    if "job" in q or "run" in q or "status" in q:
        if not jobs:
            return "No jobs yet. Use Run DQ to start one."
        lines = [f"- {j.get('job_id')} · {j.get('kind')} · {j.get('status')}" for j in jobs[:5]]
        return "Recent jobs:\n" + "\n".join(lines)

    if "step" in q or "how" in q or "next" in q or "roadmap" in q or "future" in q:
        return ("Suggested flow: 1) Onboard a dataset (Onboard tab) → 2) Profile it → "
                "3) Approve generated rules → 4) Publish ruleset → 5) Activate pipeline → "
                "6) Run DQ in OBSERVE_ONLY → 7) Escalate to REMEDIATE when ready. "
                "Future roadmap: real Databricks cluster runs, scheduling, alerting, "
                "drift detection, and multi-user auth.")

    return ("I can help with: onboarded pipelines, rulesets, jobs, anomalies, quarantine, "
            "DQ modes, AI provider setup, restart steps, and the roadmap. Try asking "
            "'which pipelines are onboarded?' or 'how do I run DQ?'.")


def answer(question: str) -> dict[str, Any]:
    ctx = gather_context()
    llm = _llm_answer(question, ctx)
    if llm:
        return {"answer": llm, "source": "llm", "provider": "DQXProvider"}
    return {"answer": _fallback_answer(question, ctx), "source": "builtin",
            "provider": "builtin-faq"}
