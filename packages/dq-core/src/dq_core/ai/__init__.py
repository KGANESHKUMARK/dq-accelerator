"""AI-assisted remediation: provider interface, simulated provider and factory.

Supported providers (config: ai.provider):
  mock   – MockAIProvider (default, no credentials needed, fully local)
  dqx    – DQXProvider (databricks-labs-dqx[llm], requires DATABRICKS_HOST +
            DATABRICKS_TOKEN + DQX_MODEL_ENDPOINT; auto-falls back to mock if unavailable)
"""

from __future__ import annotations

from typing import Any

from dq_core.ai.base import AIProposal, AIRemediationProvider, ProposalRequest
from dq_core.ai.mock import MockAIProvider
from dq_core.runtime.errors import ConfigurationError


def create_provider(name: str, **options: Any) -> AIRemediationProvider:
    """Factory for AI remediation providers."""
    n = (name or "mock").lower()
    if n in ("mock", "mockaiprovider"):
        return MockAIProvider.from_file(
            options.get("mock_responses_path"),
            similarity_fallback=options.get("similarity_fallback", True),
        )
    if n in ("dqx", "dqxprovider"):
        from dq_core.ai.dqx_provider import DQXProvider
        return DQXProvider(
            model_endpoint=options.get("dqx_model_endpoint"),
            fallback_on_error=options.get("dqx_fallback_on_error", True),
            mock_responses_path=options.get("mock_responses_path"),
        )
    raise ConfigurationError(
        f"Unknown AI provider '{name}'. "
        "Supported values: 'mock' (default), 'dqx' (requires databricks-labs-dqx[llm])."
    )


__all__ = [
    "AIProposal", "AIRemediationProvider", "ProposalRequest",
    "MockAIProvider", "create_provider",
]
