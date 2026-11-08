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
    return bool(os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"))


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
            from databricks.labs.dqx.llm.llm_engine import DQLLMEngine  # type: ignore[import]
            from databricks.labs.dqx.llm.models import LLMModelConfig     # type: ignore[import]
            cfg = LLMModelConfig(endpoint_name=self.model_endpoint)
            self._dqx_engine = DQLLMEngine(model_config=cfg)
            self.model_version = f"dqx-llm-{self.model_endpoint}"
            _logger.info("DQXProvider initialised — endpoint=%s", self.model_endpoint)
            self._ready = True
        except Exception as exc:
            _logger.warning("DQXProvider init failed: %s — falling back to MockAIProvider", exc)
            self._ready = False
        return self._ready

    # ------------------------------------------------------------------
    def propose(self, requests: Sequence[ProposalRequest]) -> list[AIProposal]:
        if not self._init_dqx():
            return self._get_fallback().propose(requests)
        try:
            return self._propose_via_dqx(requests)
        except Exception as exc:
            _logger.warning("DQX proposal failed: %s — falling back to MockAIProvider", exc)
            if self.fallback_on_error:
                return self._get_fallback().propose(requests)
            raise

    def _propose_via_dqx(self, requests: Sequence[ProposalRequest]) -> list[AIProposal]:
        """
        Map ProposalRequests → DQX LLM calls → AIProposals.

        DQX's LLM engine is designed for *rule generation* from natural language,
        not direct value correction. We adapt it as follows:
          - Build a natural-language description of each unique (column, rule_type,
            allowed_values) combination.
          - Ask DQX which correction it would apply to invalid values.
          - Parse the returned rule candidates to extract ALLOWED_VALUES or REGEX
            corrections and map them to concrete proposed_value strings.
          - For values without a DQX match, fall back to string-similarity from MockAI.

        This keeps DQX doing what it does best (business-rule understanding) while
        our engine handles the row-level application and governance.
        """
        from dq_core.ai.mock import MockAIProvider

        proposals: list[AIProposal] = []
        fallback = MockAIProvider.from_file(self._mock_responses_path)

        # Group by (column, rule_type) for batched LLM calls
        from collections import defaultdict
        groups: dict[tuple[str, str], list[ProposalRequest]] = defaultdict(list)
        for req in requests:
            groups[(req.column, req.rule_type)].append(req)

        for (column, rule_type), group_reqs in groups.items():
            unique_values = list({r.original_value for r in group_reqs if r.original_value})
            allowed = group_reqs[0].context.get("allowed_values") or []

            # Build natural-language prompt for DQX
            prompt = (
                f"Column '{column}' (rule type: {rule_type}). "
                f"Invalid values found: {unique_values[:10]}. "
                + (f"Allowed values are: {allowed}. " if allowed else "")
                + "For each invalid value, suggest the correct replacement if one exists."
            )
            schema_info = f'{{"column_name": "{column}", "type": "string"}}'

            try:
                result = self._dqx_engine.detect_business_rules_with_llm(
                    user_input=prompt, schema_info=schema_info
                )
                # DQX returns a Prediction with quality_rules — we parse it
                dqx_rules = getattr(result, "quality_rules", None) or []
                # Build a correction map from DQX rules
                correction_map: dict[str, tuple[str, float]] = {}
                for rule in (dqx_rules if isinstance(dqx_rules, list) else []):
                    # DQX rules may specify allowed_values or regex corrections
                    if isinstance(rule, dict):
                        for bad_val in unique_values:
                            av = rule.get("allowed_values") or rule.get("config", {}).get("values") or []
                            import difflib
                            if av:
                                match = difflib.get_close_matches(
                                    str(bad_val).upper(),
                                    [str(v).upper() for v in av], n=1, cutoff=0.7
                                )
                                if match:
                                    # Map back to original case
                                    matched = next(v for v in av if str(v).upper() == match[0])
                                    ratio = difflib.SequenceMatcher(None, str(bad_val).upper(), match[0]).ratio()
                                    correction_map[str(bad_val)] = (str(matched), round(min(ratio, 1.0) * 0.95, 4))

                for req in group_reqs:
                    if req.original_value in correction_map:
                        corrected, conf = correction_map[req.original_value]
                        proposals.append(AIProposal(
                            request_id=req.request_id, record_id=req.record_id,
                            rule_id=req.rule_id, column=req.column,
                            original_value=req.original_value, proposed_value=corrected,
                            confidence=conf,
                            reason=f"DQX LLM correction via {self.model_endpoint}",
                            evidence={"source": "dqx_llm", "model": self.model_endpoint,
                                      "prompt_tokens": len(prompt)},
                            provider=self.name, model_version=self.model_version, simulated=False,
                        ))
                    else:
                        # No DQX match — use similarity fallback
                        fb = fallback.propose([req])
                        proposals.extend(fb)
            except Exception as exc:
                _logger.debug("DQX group %s/%s failed: %s — using fallback", column, rule_type, exc)
                proposals.extend(fallback.propose(group_reqs))

        return proposals

    def describe(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "model_version": self.model_version,
            "model_endpoint": self.model_endpoint,
            "is_external": self.is_external,
            "simulated": not self._ready,
            "dqx_available": _check_dqx(),
            "credentials_set": _check_credentials(),
        }
