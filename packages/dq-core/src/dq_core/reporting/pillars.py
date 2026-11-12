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
    advice = _DOMAIN_ADVICE.get(domain, {})
    tips = [advice.get(p) for p in weak if advice.get(p)]
    suffix = f" Suggested: {'; '.join(tips[:2])}." if tips else ""
    return (f"{domain}: review {', '.join(weak)} before publication; records below the "
            f"remediation threshold remain quarantined.{suffix}")


_DOMAIN_ADVICE = {
    "pharmacy": {
        "validity": "verify NDC/drug-code formats and SIG (directions) text patterns",
        "completeness": "prescription fields (drug, dose, prescriber) must be non-null for dispensing",
        "consistency": "cross-check dose units (mg vs mcg) and refill counts against formulary",
        "timeliness": "prior-auth approvals and refill windows must stay current",
        "accuracy": "dispense quantities should reconcile with prescribed quantities",
    },
    "telecom": {
        "validity": "MSISDN/IMSI formats and cell-tower IDs must match network dictionaries",
        "completeness": "CDR records need originating/destination numbers and event timestamps",
        "consistency": "call durations and data volumes must be non-negative and internally coherent",
        "timeliness": "CDR ingestion lag should stay within the billing-cycle window",
        "uniqueness": "CDR event IDs must be unique to avoid double-billing",
        "accuracy": "rated usage should reconcile with raw counters",
    },
    "healthcare": {
        "validity": "ICD/SNOMED/NDC codes must match reference terminologies",
        "completeness": "clinical fields (patient, encounter, provider) must be populated",
        "consistency": "dosages, units and dates must be clinically coherent",
        "timeliness": "claims and eligibility checks must be current",
        "uniqueness": "patient encounter IDs must be unique",
    },
    "banking": {
        "validity": "account/IFSC/UTR formats and amounts must be well-formed",
        "completeness": "transaction references and timestamps are mandatory",
        "consistency": "debits/credits must reconcile with balances",
        "timeliness": "settlement and posting timestamps must be current",
        "uniqueness": "transaction IDs must be unique (double-posting risk)",
    },
    "hospitality": {
        "validity": "guest emails, phone numbers and confirmation codes must be well-formed",
        "completeness": "reservation dates, guests and rates must be populated",
        "consistency": "check-in/check-out must be chronologically coherent",
        "timeliness": "arrival/departure windows must be current",
    },
    "agriculture": {
        "validity": "field/batch/livestock IDs and GPS coordinates must be plausible",
        "completeness": "sensor readings and timestamps must be present",
        "consistency": "yield/moisture/nutrient values must be agronomically coherent",
        "timeliness": "seasonal readings must be current",
    },
}
