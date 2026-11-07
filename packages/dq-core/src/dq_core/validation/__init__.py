"""Validation engine: native PySpark rule execution with multi-violation record annotation."""

from dq_core.validation.engine import ValidationEngine, ValidationOutcome
from dq_core.validation.models import RuleResult, ValidationReport

__all__ = ["ValidationEngine", "ValidationOutcome", "RuleResult", "ValidationReport"]
