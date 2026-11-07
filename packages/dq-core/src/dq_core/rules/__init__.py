"""Rule registry (versioning, immutability, activation/rollback) and version comparison."""

from dq_core.rules.diff import RuleSetDiff, diff_rulesets
from dq_core.rules.registry import RuleSetRegistry

__all__ = ["RuleSetDiff", "diff_rulesets", "RuleSetRegistry"]
