"""DQX provider — wraps databricks-labs-dqx[llm] for real LLM-driven remediation.

Phase C integration. This module is a bridge between our AIRemediationProvider
interface and the DQX DQLLMEngine / DQGenerator APIs.

Requires:
  pip install 'databricks-labs-dqx[llm]'
  DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
  DATABRICKS_TOKEN=<personal-access-token>
  DQX_MODEL_ENDPOINT=<Databricks Model Serving endpoint name>  # e.g. databricks-meta-llama-3-3-70b-instruct

Usage (engine.yaml):
  ai:
    provider: dqx
    dqx_model_endpoint: databricks-meta-llama-3-3-70b-instruct

Fallback: if the dqx package is not installed or credentials are missing,
DQXProvider automatically delegates to MockAIProvider with a one-time warning
so existing tests and local runs continue to work.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Sequence

from dq_core.ai.base import AIProposal, AIRemediationProvider, ProposalRequest

_logger = logging.getLogger("dq_core.ai.dqx")

_DQX_AVAILABLE: bool | None = None   # None = not yet checked


def _check_dqx() -> bool:
    global _DQX_AVAILABLE
    if _DQX_AVAILABLE is not None:
        return _DQX_AVAILABLE
    try:
        import databricks.labs.dqx  # noqa: F401
        _DQX_AVAILABLE = True
    except ImportError:
        _DQX_AVAILABLE = False
    return _DQX_AVAILABLE


def _check_credentials() -> bool:
    host = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN") or os.environ.get("DATABRICKS_API_KEY", "")
    return bool(host and token)


class DQXProvider(AIRemediationProvider):
    """Real LLM-backed provider using databricks-labs-dqx[llm].

    When DQX is not installed or Databricks credentials are absent this class
    automatically falls back to MockAIProvider, logging a clear warning so
    the caller knows AI is simulated.

    The DQX integration strategy:
    1.  Batch incoming ProposalRequests by (rule_type, column, allowed_values).
    2.  Build a natural-language description from the failure context and pass it
        to DQLLMEngine.detect_business_rules_with_llm() — this returns DQX check
        candidates, not direct corrections.
    3.  Translate DQX outputs back into our AIProposal format with real confidence.
    4.  Every result is still subject to the same confidence threshold (90%) and
        sensitive-column policy gating — DQX does NOT bypass our governance.
    """

    name = "DQXProvider"
    is_external = True
    simulated = False

    def __init__(
        self,
        model_endpoint: str | None = None,
        fallback_on_error: bool = True,
        mock_responses_path: str | None = None,
    ) -> None:
        self.model_endpoint = model_endpoint or os.environ.get("DQX_MODEL_ENDPOINT", "")
        self.fallback_on_error = fallback_on_error
        self._mock_responses_path = mock_responses_path
        self._dqx_engine: Any = None
        self._fallback: AIRemediationProvider | None = None
        self.model_version = f"dqx-llm-{self.model_endpoint or 'unconfigured'}"
        self._ready: bool | None = None

    def _get_fallback(self) -> AIRemediationProvider:
        if self._fallback is None:
            from dq_core.ai.mock import MockAIProvider
            self._fallback = MockAIProvider.from_file(self._mock_responses_path)
        return self._fallback

    def _init_dqx(self) -> bool:
        """Lazy initialise DQX engine. Returns True if ready."""
        if self._ready is not None:
            return self._ready
        if not _check_dqx():
            _logger.warning(
                "databricks-labs-dqx[llm] is not installed. "
                "Install with: pip install 'databricks-labs-dqx[llm]'  "
                "Falling back to MockAIProvider."
            )
            self._ready = False
            return False
        if not _check_credentials():
            _logger.warning(
                "DATABRICKS_HOST and/or DATABRICKS_TOKEN not set. "
                "DQXProvider cannot connect to Databricks Model Serving. "
                "Falling back to MockAIProvider."
            )
            self._ready = False
            return False
        if not self.model_endpoint:
            _logger.warning(
                "DQX_MODEL_ENDPOINT not set (env or config). "
                "Falling back to MockAIProvider."
            )
            self._ready = False
            return False
        try:
            # We use LiteLLM directly for value correction (more appropriate than
            # DQX's rule detection which generates rules, not corrections).
            # DQX package provides LiteLLM as a dependency.
            import litellm  # noqa: F401
            self._dqx_engine = True  # Marker that LLM is available
            self.model_version = f"dqx-llm-{self.model_endpoint}"
            host = os.environ.get("DATABRICKS_HOST", "")
            _logger.info("DQXProvider initialised — endpoint=%s host=%s", self.model_endpoint, host)
            self._ready = True
        except Exception as exc:
            _logger.warning("DQXProvider init failed: %s — falling back to MockAIProvider", exc, exc_info=True)
            self._ready = False
        return self._ready

    # ------------------------------------------------------------------
    def propose(self, requests: Sequence[ProposalRequest]) -> list[AIProposal]:
        if not self._init_dqx():
            return self._get_fallback().propose(requests)
        try:
            return self._propose_via_dqx(requests)
        except Exception as exc:
            _logger.warning("DQX proposal failed: %s — falling back to MockAIProvider", exc, exc_info=True)
            if self.fallback_on_error:
                return self._get_fallback().propose(requests)
            raise

    def _propose_via_dqx(self, requests: Sequence[ProposalRequest]) -> list[AIProposal]:
        """
        Map ProposalRequests → Databricks LLM calls → AIProposals.

        Uses LiteLLM to call the Databricks Model Serving endpoint directly
        for value correction. This is more appropriate than DQX's rule
        detection which is designed for generating data quality rules, not
        correcting individual values.

        Strategy:
          - Group requests by (column, rule_type) for batched LLM calls.
          - Build a prompt asking the LLM to suggest corrected values.
          - Parse JSON response and map to AIProposal format.
          - Fall back to MockAI for values the LLM can't correct.
        """
        from dq_core.ai.mock import MockAIProvider
        from collections import defaultdict
        import json as _json

        proposals: list[AIProposal] = []
        fallback = MockAIProvider.from_file(self._mock_responses_path)

        # Group by (column, rule_type) for batched LLM calls
        groups: dict[tuple[str, str], list[ProposalRequest]] = defaultdict(list)
        for req in requests:
            groups[(req.column, req.rule_type)].append(req)

        for (column, rule_type), group_reqs in groups.items():
            unique_values = list({r.original_value for r in group_reqs if r.original_value})
            allowed = group_reqs[0].context.get("allowed_values") or []
            failure_reason = group_reqs[0].failure_reason

            # Build prompt for LLM value correction
            prompt = self._build_correction_prompt(
                column, rule_type, unique_values, allowed, failure_reason
            )

            try:
                corrections = self._call_llm_for_corrections(prompt, unique_values)
                _logger.info("LLM returned %d corrections for %s/%s", len(corrections), column, rule_type)
                
                for req in group_reqs:
                    if req.original_value in corrections:
                        corrected, conf, reason = corrections[req.original_value]
                        proposals.append(AIProposal(
                            request_id=req.request_id, record_id=req.record_id,
                            rule_id=req.rule_id, column=req.column,
                            original_value=req.original_value, proposed_value=corrected,
                            confidence=conf,
                            reason=reason,
                            evidence={"source": "databricks_llm", "model": self.model_endpoint,
                                      "prompt": prompt[:200]},
                            provider=self.name, model_version=self.model_version, simulated=False,
                        ))
                    else:
                        # No LLM correction — use similarity fallback
                        fb = fallback.propose([req])
                        proposals.extend(fb)
            except Exception as exc:
                _logger.warning("LLM group %s/%s failed: %s — using fallback", column, rule_type, exc, exc_info=True)
                proposals.extend(fallback.propose(group_reqs))

        return proposals

    def _build_correction_prompt(
        self, column: str, rule_type: str, values: list[str], 
        allowed: list[str], failure_reason: str
    ) -> str:
        """Build a prompt for LLM-based value correction."""
        allowed_str = f"Allowed values are: {allowed}. " if allowed else ""
        return (
            f"You are a data quality expert. A data column failed validation.\n\n"
            f"Column: {column}\n"
            f"Rule type: {rule_type}\n"
            f"Failure reason: {failure_reason}\n"
            f"{allowed_str}\n"
            f"Invalid values found: {values[:10]}\n\n"
            f"For each invalid value, suggest the correct replacement if one exists. "
            f"Respond ONLY with a JSON object mapping each invalid value to its correction. "
            f"Format: {{\"invalid_value\": {{\"correction\": \"corrected_value\", \"confidence\": 0.95}}}}\n"
            f"If no correction is possible, omit that value from the response."
        )

    def _call_llm_for_corrections(
        self, prompt: str, values: list[str]
    ) -> dict[str, tuple[str, float, str]]:
        """Call Databricks LLM for value corrections. Returns {value: (correction, confidence, reason)}."""
        import litellm
        import json as _json

        host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        token = os.environ.get("DATABRICKS_TOKEN") or os.environ.get("DATABRICKS_API_KEY", "")
        
        if not host or not token:
            raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN required")

        # LiteLLM Databricks provider configuration
        model = f"databricks/{self.model_endpoint}"
        api_base = f"{host}/serving-endpoints"
        
        response = litellm.completion(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            api_key=token,
            api_base=api_base,
            max_tokens=1000,
            temperature=0.0,
            timeout=60,
        )
        
        content = response.choices[0].message.content.strip()
        
        # Parse JSON response — try multiple extraction strategies
        parsed = self._extract_json_from_response(content)
        if parsed is None:
            _logger.warning("LLM returned invalid JSON: %s", content[:200])
            return {}
        
        # Convert to our format
        corrections = {}
        for invalid_val, correction_data in parsed.items():
            if isinstance(correction_data, dict):
                corrected = correction_data.get("correction")
                conf = float(correction_data.get("confidence", 0.8))
                if corrected:
                    corrections[invalid_val] = (str(corrected), conf, f"LLM correction via {self.model_endpoint}")
        
        return corrections

    def _extract_json_from_response(self, content: str) -> dict | None:
        """Extract JSON from LLM response, handling various formats."""
        import re
        import json as _json
        
        # Strategy 1: Extract from markdown code blocks
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()
        
        # Try parsing the full content first (most common case)
        try:
            return _json.loads(content)
        except _json.JSONDecodeError:
            pass
        
        # Strategy 2: Find outer JSON object containing corrections
        # Look for {"key": {"correction": "value", ...}} pattern
        json_match = re.search(r'\{[^{}]*\{[^{}]*\}[^{}]*\}', content)
        if json_match:
            try:
                return _json.loads(json_match.group(0))
            except _json.JSONDecodeError:
                pass
        
        # Strategy 3: Find any JSON object in text
        json_match = re.search(r'\{[^{}]*\}', content)
        if json_match:
            try:
                return _json.loads(json_match.group(0))
            except _json.JSONDecodeError:
                pass
        
        # Strategy 4: Try to build JSON from natural language response
        corrections = {}
        for value in re.findall(r"correct\s+['\"]([^'\"]+)['\"]\s+to\s+['\"]([^'\"]+)['\"]", content, re.IGNORECASE):
            corrections[value[0]] = {"correction": value[1], "confidence": 0.85}
        if corrections:
            return corrections
        return None

    def describe(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "model_version": self.model_version,
            "model_endpoint": self.model_endpoint,
            "is_external": self.is_external,
            "simulated": not self._ready,
            "dqx_available": _check_dqx(),
            "credentials_set": _check_credentials(),
            "workspace_host": os.environ.get("DATABRICKS_HOST", "")[:50],
        }
