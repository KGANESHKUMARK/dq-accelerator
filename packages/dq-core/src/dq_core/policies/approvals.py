"""Human approval store for corrections that policy routes to review (POC: a YAML file).

Format::

    approvals:
      - dataset: customers
        column: email
        original_value: "neha.gupta@yahoo"
        approved_value: "neha.gupta@yahoo.com"
        approved_by: data.steward@bank.example
        approved_at: "2026-11-02T09:30:00Z"
        ticket: DQ-1234
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from dq_core.metadata.serialization import load_document


@dataclass(frozen=True)
class Approval:
    dataset: str
    column: str
    original_value: str
    approved_value: str
    approved_by: str
    approved_at: str = ""
    ticket: str = ""


class ApprovalStore:
    def __init__(self, approvals: Iterable[Approval] = ()) -> None:
        self._index = {(a.dataset, a.column, a.original_value): a for a in approvals}

    @classmethod
    def load(cls, path: str | Path | None) -> "ApprovalStore":
        if not path or not Path(path).exists():
            return cls()
        doc = load_document(path)
        items = doc.get("approvals") or []
        return cls.from_documents(items)

    @classmethod
    def from_documents(cls, items: Iterable[Mapping[str, Any]]) -> "ApprovalStore":
        return cls(Approval(dataset=str(i["dataset"]), column=str(i["column"]), original_value=str(i["original_value"]),
                            approved_value=str(i["approved_value"]), approved_by=str(i.get("approved_by", "")),
                            approved_at=str(i.get("approved_at", "")), ticket=str(i.get("ticket", ""))) for i in items)

    def find(self, dataset: str, column: str, original_value: Any, proposed_value: Any) -> Approval | None:
        approval = self._index.get((dataset, column, str(original_value)))
        if approval is not None and approval.approved_value == str(proposed_value):
            return approval
        return None

    def __len__(self) -> int:
        return len(self._index)


@dataclass(frozen=True)
class Rejection:
    dataset: str
    column: str
    original_value: str
    proposed_value: str
    rejected_by: str = ""
    rejected_at: str = ""
    reason: str = ""


class RejectionStore:
    """Human rejections — a steward said 'no' to this exact correction proposal.

    Format::

        rejections:
          - dataset: customers
            column: email
            original_value: "bad@x"
            proposed_value: "bad@x.com"
            rejected_by: data.steward@bank.example
            rejected_at: "2026-11-10T09:30:00Z"
            reason: "proposal changes semantics"
    """

    def __init__(self, rejections: Iterable[Rejection] = ()) -> None:
        self._index = {(r.dataset, r.column, r.original_value, r.proposed_value): r for r in rejections}

    @classmethod
    def load(cls, path: str | Path | None) -> "RejectionStore":
        if not path or not Path(path).exists():
            return cls()
        doc = load_document(path)
        items = doc.get("rejections") or []
        return cls.from_documents(items)

    @classmethod
    def from_documents(cls, items: Iterable[Mapping[str, Any]]) -> "RejectionStore":
        return cls(Rejection(dataset=str(i.get("dataset", "")), column=str(i.get("column", "")),
                             original_value=str(i.get("original_value", "")),
                             proposed_value=str(i.get("proposed_value", i.get("corrected_value", ""))),
                             rejected_by=str(i.get("rejected_by", i.get("approved_by", ""))),
                             rejected_at=str(i.get("rejected_at", i.get("approved_at", ""))),
                             reason=str(i.get("reason", ""))) for i in items)

    def find(self, dataset: str, column: str, original_value: Any, proposed_value: Any) -> Rejection | None:
        return self._index.get((dataset, column, str(original_value), str(proposed_value)))

    def __len__(self) -> int:
        return len(self._index)
