"""PII / sensitive-entity detection for column profiles.

Two signals, combined into a confidence score:

* **Name signal** — column-name patterns (``email``, ``phone``, ``ssn``,
  ``card_number`` …) mapped to entity types.
* **Value signal** — regex tests against the profiler's ``top_values``
  samples (emails, E.164-ish phones, SSN shapes, card numbers, IPs, DOBs).

Pure-Python, zero extra deps — portable across every Spark platform.
Output feeds ``Policy.sensitive_categories`` gaps and surfaces as
``PII_SUSPECTED`` profiling anomalies for steward review.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping

# ---------------------------------------------------------------- name rules
_NAME_RULES: list[tuple[str, str, float]] = [
    # (entity, column-name regex, name-signal weight)
    ("EMAIL", r"e-?mail", 0.9),
    ("PHONE", r"phone|mobile|msisdn|contact_?number|telephone", 0.9),
    ("NATIONAL_ID", r"ssn|social_?security|national_?id|passport|pan_?number|tax_?id|aadhaar|nino", 0.95),
    ("CREDIT_CARD", r"card_?(number|num|no)|credit_?card|cc_?num|pan$", 0.9),
    ("IP_ADDRESS", r"ip_?address|^ip$|client_?ip|source_?ip", 0.85),
    ("DATE_OF_BIRTH", r"birth|dob|date_?of_?birth", 0.9),
    ("PERSON_NAME", r"(^|_)(first|last|full|sur|given|family)_?name|patient_?name|customer_?name|employee_?name", 0.6),
    ("STREET_ADDRESS", r"address|street|city$|zip|postal", 0.5),
    ("GEO_LOCATION", r"latitude|longitude|lat$|long$|lng$|geo", 0.5),
]

# ------------------------------------------------------------- value rules
_VALUE_RULES: list[tuple[str, re.Pattern[str], float]] = [
    ("EMAIL", re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"), 0.95),
    ("PHONE", re.compile(r"^\+?[0-9][0-9\s\-().]{7,16}[0-9]$"), 0.7),
    ("NATIONAL_ID", re.compile(r"^\d{3}-\d{2}-\d{4}$|^\d{9}$|^[A-Z]{2}\d{6}[A-Z]?$"), 0.85),
    ("CREDIT_CARD", re.compile(r"^\d{4}[ -]?\d{4}[ -]?\d{4}[ -]?\d{4}$"), 0.9),
    ("IP_ADDRESS", re.compile(r"^\d{1,3}(\.\d{1,3}){3}$"), 0.6),
    ("DATE_OF_BIRTH", re.compile(r"^(19|20)\d{2}[-/]\d{2}[-/]\d{2}$|^\d{2}[-/]\d{2}[-/](19|20)\d{2}$"), 0.5),
]

# entity → existing sensitive_categories key (policies/models.py)
_ENTITY_TO_CATEGORY = {
    "EMAIL": "customer_identity",
    "PHONE": "customer_identity",
    "PERSON_NAME": "customer_identity",
    "STREET_ADDRESS": "customer_identity",
    "NATIONAL_ID": "regulatory",
    "CREDIT_CARD": "account_identifiers",
    "IP_ADDRESS": "account_identifiers",
    "GEO_LOCATION": "account_identifiers",
    "DATE_OF_BIRTH": "customer_identity",
}

_HIGH_VALUE_CONFIDENCE = 0.8


@dataclass(frozen=True)
class PiiFinding:
    column: str
    entity_type: str
    confidence: float
    category: str | None
    evidence: str
    sample: str | None = None


def _name_signal(column: str) -> tuple[str, float] | None:
    c = column.lower()
    for entity, pattern, weight in _NAME_RULES:
        if re.search(pattern, c):
            return entity, weight
    return None


def _value_signal(top_values: list[dict[str, Any]], limit: int = 20) -> tuple[str, float, str, str] | None:
    """Test sampled values against entity regexes; return best (entity, conf, value, matched)."""
    best: tuple[str, float, str, str] | None = None
    for tv in top_values[:limit]:
        v = tv.get("value")
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        for entity, rx, weight in _VALUE_RULES:
            if rx.match(s):
                conf = weight * min(1.0, 0.5 + float(tv.get("percentage") or 0) / 100.0)
                if best is None or conf > best[1]:
                    best = (entity, conf, s, entity)
    return best


def detect_pii(profiles: Mapping[str, Any], *, min_confidence: float = 0.5) -> dict[str, PiiFinding]:
    """Scan column profiles → {column: PiiFinding}. Never raises on odd inputs."""
    findings: dict[str, PiiFinding] = {}
    for col, prof in profiles.items():
        name_hit = _name_signal(col)
        val_hit = _value_signal(getattr(prof, "top_values", None) or [])
        if not name_hit and not val_hit:
            continue
        # combine: value evidence stronger than name alone
        if name_hit and val_hit:
            entity = val_hit[0] if val_hit[1] >= name_hit[1] else name_hit[0]
            conf = max(val_hit[1], name_hit[1] * 0.9)
            evidence = f"name='{col}' matches {name_hit[0]}; values match {val_hit[0]}"
            sample = val_hit[2]
        elif val_hit:
            entity, conf, sample, _ = val_hit
            evidence = f"sampled values match {entity} pattern"
        else:
            entity, conf = name_hit  # type: ignore[misc]
            evidence = f"column name matches {entity} pattern"
            sample = None
        if conf < min_confidence:
            continue
        findings[col] = PiiFinding(
            column=col, entity_type=entity, confidence=round(conf, 3),
            category=_ENTITY_TO_CATEGORY.get(entity), evidence=evidence, sample=sample,
        )
    return findings


def high_confidence_columns(findings: Mapping[str, PiiFinding]) -> list[str]:
    """Columns confident enough to auto-classify as sensitive."""
    return [c for c, f in findings.items() if f.confidence >= _HIGH_VALUE_CONFIDENCE]
