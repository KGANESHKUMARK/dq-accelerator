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
