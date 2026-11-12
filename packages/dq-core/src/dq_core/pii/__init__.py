"""PII / sensitive-entity detection (portable, zero-dep)."""

from dq_core.pii.detector import PiiFinding, detect_pii, high_confidence_columns

__all__ = ["PiiFinding", "detect_pii", "high_confidence_columns"]
