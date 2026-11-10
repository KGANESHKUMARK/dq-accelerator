"""Seven-pillar quality scoring.

The score is a report summary derived from existing rule results. It does not
change validation, remediation, quarantine, or quality-gate decisions.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any


PILLARS = ("completeness", "uniqueness", "validity", "consistency", "accuracy", "timeliness", "integrity")
_PILLAR_BY_TYPE = {
    "NOT_NULL": "completeness",
    "NULL_THRESHOLD": "completeness",
    "UNIQUE": "uniqueness",
    "DUPLICATE": "uniqueness",
    "DATA_TYPE": "validity",
    "REGEX": "validity",
    "RANGE": "validity",
    "ALLOWED_VALUES": "validity",
    "STRING_LENGTH": "validity",
    "SCHEMA_VALIDATION": "consistency",
    "CROSS_COLUMN": "consistency",
    "CUSTOM_SQL": "accuracy",
    "BUSINESS_RULE": "accuracy",
    "DATE_FORMAT": "timeliness",
    "FRESHNESS": "timeliness",
    "REFERENTIAL_INTEGRITY": "integrity",
    "FOREIGN_KEY": "integrity",
}


def score_pillars(validation_report, dataset: str = "") -> dict[str, Any]:
    """Score each pillar from failed/passed rule evidence."""
    totals = defaultdict(lambda: {"rules": 0, "passed_records": 0, "total_records": 0})
    results = getattr(validation_report, "results", []) if validation_report else []
    for result in results:
        pillar = _PILLAR_BY_TYPE.get(result.rule_type, "accuracy")
        bucket = totals[pillar]
        bucket["rules"] += 1
        bucket["passed_records"] += result.passed_records
        bucket["total_records"] += result.total_records
    pillars = {}
    for name in PILLARS:
        bucket = totals[name]
        score = 100.0 if bucket["total_records"] == 0 else round(100 * bucket["passed_records"] / bucket["total_records"], 2)
        pillars[name] = {"score": score, "rules": bucket["rules"], "status": "NOT_MEASURED" if bucket["rules"] == 0 else "MEASURED"}
    measured = [item["score"] for item in pillars.values() if item["status"] == "MEASURED"]
    return {"pillars": pillars, "overall_score": round(sum(measured) / len(measured), 2) if measured else 100.0,
            "domain_recommendation": domain_recommendation(dataset, pillars)}


def domain_recommendation(dataset: str, pillars: dict[str, dict[str, Any]]) -> str:
    weak = [name for name, item in pillars.items() if item["status"] == "MEASURED" and item["score"] < 90]
    name = dataset.lower()
    if any(k in name for k in ("patient", "health", "clinic", "hospital")):
        domain = "healthcare"
    elif any(k in name for k in ("goat", "farm", "crop", "livestock", "agri")):
        domain = "agriculture"
    elif any(k in name for k in ("pharmacy", "prescription", "drug")):
        domain = "pharmacy"
    elif any(k in name for k in ("telecom", "cdr", "call")):
        domain = "telecom"
    elif any(k in name for k in ("hotel", "resort", "booking", "guest", "hospitality", "reservation")):
        domain = "hospitality"
    elif any(k in name for k in ("bank", "loan", "transaction", "account")):
        domain = "banking"
    else:
        domain = "generic"
    if not weak:
        return f"{domain}: no pillar is below 90; continue monitoring quarantine and approvals."
    return f"{domain}: review {', '.join(weak)} before publication; records below the remediation threshold remain quarantined."
