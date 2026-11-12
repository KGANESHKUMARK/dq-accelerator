"""Alerting — webhook notifications on quality-gate failures / threshold breaches.

Config (engine.yaml)::

    alerts:
      enabled: true
      webhook_url: https://hooks.slack.com/services/...   # or Teams/any POST endpoint
      events: [gate_fail]          # gate_fail | quarantine_threshold | always
      fail_pipeline: false         # true → raise QualityGateFailedError after reporting
      timeout_seconds: 10
      min_quarantine_rate: 0.05    # quarantine_threshold event fires above this rate

Delivery is best-effort: webhook failures are logged, never break the pipeline
(unless fail_pipeline is set, which fails on the GATE result not the webhook).
"""

from __future__ import annotations

from typing import Any

from dq_core.runtime.errors import DQError
from dq_core.runtime.logging import get_logger

_logger = get_logger("alerts")


class QualityGateFailedError(DQError):
    """Raised when alerts.fail_pipeline=true and the quality gate fails."""


def notify_gate(alerts: Any, *, pipeline_id: str, dataset: str, stage: str,
                gate_passed: bool, quality_score: float | None,
                input_records: int, accepted: int, quarantined: int,
                ruleset_ref: str = "", report_path: str = "") -> None:
    """Fire webhook alerts based on AlertSettings. Never raises on HTTP errors."""
    cfg = alerts
    if cfg is None or not getattr(cfg, "enabled", False):
        return
    events = {e.lower() for e in (getattr(cfg, "events", None) or ["gate_fail"])}
    url = getattr(cfg, "webhook_url", None)
    quar_rate = (quarantined / input_records) if input_records else 0.0

    fired: list[str] = []
    if not gate_passed and ("gate_fail" in events or "always" in events):
        fired.append("gate_fail")
    if quar_rate >= float(getattr(cfg, "min_quarantine_rate", 1.0) or 1.0) and \
            ("quarantine_threshold" in events or "always" in events):
        fired.append("quarantine_threshold")
    if not fired:
        return

    payload = {
        "source": "dq-accelerator",
        "events": fired,
        "pipeline_id": pipeline_id, "dataset": dataset, "stage": stage,
        "gate_passed": gate_passed, "quality_score": quality_score,
        "input_records": input_records, "accepted": accepted, "quarantined": quarantined,
        "quarantine_rate": round(quar_rate, 4),
        "ruleset": ruleset_ref, "report": report_path,
    }
    if url:
        try:
            import requests
            requests.post(url, json=payload, timeout=float(getattr(cfg, "timeout_seconds", 10) or 10))
            _logger.info("alert webhook delivered", events=fired, url=url[:60])
        except Exception as exc:  # best-effort — alerting must not break pipelines
            _logger.warning("alert webhook failed", error=str(exc)[:200])
    else:
        _logger.warning("alert fired (no webhook_url configured)", **{k: payload[k] for k in
                          ("pipeline_id", "dataset", "stage", "events", "gate_passed")})

    if fired and gate_passed is False and getattr(cfg, "fail_pipeline", False):
        raise QualityGateFailedError(
            f"Quality gate FAILED for {pipeline_id}/{dataset}/{stage} "
            f"(score={quality_score}, quarantined={quarantined}/{input_records}) — "
            f"alerts.fail_pipeline=true halts the pipeline")
