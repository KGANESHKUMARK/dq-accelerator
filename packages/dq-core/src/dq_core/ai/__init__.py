"""AI-assisted remediation: provider interface, simulated provider and factory."""

from __future__ import annotations

from typing import Any

from dq_core.ai.base import AIProposal, AIRemediationProvider, ProposalRequest
from dq_core.ai.mock import MockAIProvider
from dq_core.runtime.errors import ConfigurationError


def create_provider(name: str, **options: Any) -> AIRemediationProvider:
    """Factory for AI providers. Only the simulated provider ships with the POC."""
    if name.lower() in ("mock", "mockaiprovider"):
        return MockAIProvider.from_file(options.get("mock_responses_path"),
                                        similarity_fallback=options.get("similarity_fallback", True))
    raise ConfigurationError(f"Unknown AI provider '{name}'. The POC ships only 'mock'; real providers implement "
                             "dq_core.ai.AIRemediationProvider.")


__all__ = ["AIProposal", "AIRemediationProvider", "ProposalRequest", "MockAIProvider", "create_provider"]
