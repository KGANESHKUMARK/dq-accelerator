"""Exception hierarchy for the DQ core engine."""

from __future__ import annotations

from typing import Any


class DQError(Exception):
    """Base class for every error raised by dq-core."""


class ConfigurationError(DQError):
    """Invalid engine, policy, manifest or rule configuration."""


class RuleDefinitionError(ConfigurationError):
    """A rule or rule set definition is invalid."""


class PolicyError(ConfigurationError):
    """A remediation / confidence policy is invalid."""


class RuleSetNotFoundError(DQError):
    """The requested rule set or version does not exist in the registry."""


class ImmutableVersionError(DQError):
    """Attempt to modify, overwrite or silently change a published rule set version."""


class IntegrityError(DQError):
    """Stored metadata failed checksum verification."""


class LifecycleError(DQError):
    """Invalid pipeline lifecycle transition."""


class ValidationExecutionError(DQError):
    """Rule execution failed (as opposed to data failing a rule)."""


class RemediationError(DQError):
    """Remediation could not be executed safely."""


class AdapterError(DQError):
    """A platform / pipeline adapter could not perform the requested operation."""


class SparkRuntimeError(DQError):
    """The Spark runtime could not be created or inspected."""


class QualityGateFailed(DQError):
    """Raised by ``DQResult.raise_if_gate_failed`` when the quality gate did not pass."""

    def __init__(self, message: str, gate: Any = None) -> None:
        super().__init__(message)
        self.gate = gate
