"""MockAIProvider - SIMULATED AI outputs for the POC (no LLM, no API key, no network).

Proposals come from a transparent, version-controlled scenario table (``config/ai/
mock_ai_responses.yaml``) keyed by (column, value). For ALLOWED_VALUES failures that are not in
the table an optional string-similarity fallback proposes the closest allowed value with a
confidence deliberately capped below the default 0.90 threshold.

Every proposal is labelled ``simulated=True`` and ``model_version`` ends with ``-SIMULATED``.
"""

from __future__ import annotations

import difflib
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from dq_core.ai.base import AIProposal, AIRemediationProvider, ProposalRequest
from dq_core.metadata.serialization import load_document

DEFAULT_MODEL_VERSION = "mock-dq-remediator-0.1-SIMULATED"


class MockAIProvider(AIRemediationProvider):
    name = "MockAIProvider"
    is_external = False
    simulated = True

    def __init__(self, scenarios: Iterable[Mapping[str, Any]] = (), *, model_version: str = DEFAULT_MODEL_VERSION,
                 similarity_fallback: bool = True, fallback_max_confidence: float = 0.85) -> None:
        self.model_version = model_version
        self.similarity_fallback = similarity_fallback
        self.fallback_max_confidence = fallback_max_confidence
        self._table: dict[tuple[str, str], Mapping[str, Any]] = {}
        for s in scenarios:
            self._table[(str(s["column"]).lower(), str(s["original_value"]))] = s
        self.calls = 0
        self.requests_seen = 0

    @classmethod
    def from_file(cls, path: str | Path | None, **kwargs: Any) -> "MockAIProvider":
        if not path or not Path(path).exists():
            return cls(**kwargs)
        doc = load_document(path).get("mock_ai", {})
        return cls(doc.get("responses") or [], model_version=doc.get("model_version", DEFAULT_MODEL_VERSION), **kwargs)

    def propose(self, requests: Sequence[ProposalRequest]) -> list[AIProposal]:
        self.calls += 1  # one batched call per remediation run
        self.requests_seen += len(requests)
        return [self._propose_one(r) for r in requests]

    def _proposal(self, req: ProposalRequest, value: Any, confidence: float, reason: str,
                  evidence: Mapping[str, Any]) -> AIProposal:
        return AIProposal(request_id=req.request_id, record_id=req.record_id, rule_id=req.rule_id, column=req.column,
                          original_value=req.original_value, proposed_value=None if value is None else str(value),
                          confidence=float(confidence), reason=f"[SIMULATED] {reason}", evidence=dict(evidence),
                          provider=self.name, model_version=self.model_version, simulated=True)

    def _propose_one(self, req: ProposalRequest) -> AIProposal:
        scenario = self._table.get((req.column.lower(), str(req.original_value)))
        if scenario is not None:
            return self._proposal(req, scenario.get("proposed_value"), scenario.get("confidence", 0.0),
                                  scenario.get("reason", "scenario table match"),
                                  {"source": "mock_scenario_table", **(scenario.get("evidence") or {})})
        allowed = req.context.get("allowed_values")
        if self.similarity_fallback and allowed and req.original_value:
            candidates = {str(v).upper(): str(v) for v in allowed}
            match = difflib.get_close_matches(str(req.original_value).upper(), list(candidates), n=1, cutoff=0.6)
            if match:
                ratio = difflib.SequenceMatcher(None, str(req.original_value).upper(), match[0]).ratio()
                confidence = round(min(ratio, 1.0) * self.fallback_max_confidence, 4)
                return self._proposal(req, candidates[match[0]], confidence,
                                      f"closest allowed value by string similarity ({ratio:.2f})",
                                      {"source": "similarity_fallback", "similarity": round(ratio, 4),
                                       "confidence_cap": self.fallback_max_confidence})
        return self._proposal(req, None, 0.0, "no reliable correction could be inferred",
                              {"source": "no_match"})
