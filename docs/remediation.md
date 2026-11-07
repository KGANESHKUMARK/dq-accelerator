# Remediation

Two remediation paths — **deterministic** (rule-declared, policy-gated) and
**AI-assisted** (mock provider, confidence-gated). Code:
`dq_core/remediation/`, `dq_core/ai/`, policy in `dq_core/policies/`.

## 1. Pipeline position

```
validate ──► failed records ──► deterministic remediation ──► revalidate
                                          │ fails/none
                                          ▼
                              AI proposals (batched, deduped)
                                          │ confidence + sensitive policy
                                          ▼
                              apply eligible corrections ──► revalidate ──► quarantine rest
```

Every corrected record is **revalidated**; anything still failing is quarantined.

## 2. Deterministic remediation (`remediation/deterministic.py`)

Actions declared per-rule in `RuleDefinition.remediation.deterministic`:

| Action | Behaviour | Example in POC |
|---|---|---|
| `TRIM` | strip leading/trailing whitespace | `' C0019'` → `'C0019'` |
| `UPPERCASE` / `LOWERCASE` | case normalisation | `'active'` → `'ACTIVE'` |
| `LOOKUP_MAPPING` | map via **approved** lookup table in `ruleset.lookups` | `'Bharat'`/`'India'` → `'IN'` (ISO) |
| `DATE_STANDARDIZATION` | parse known formats → canonical `yyyy-MM-dd` | `'15/01/2024'` → `'2024-01-15'` |
| `NULL_REPLACEMENT` | fill nulls — **only when `approved: true`** | missing `city` → configured default |
| `DEDUPE` / duplicate removal | explicit business keys + survivor policy | exact dup of C0005 removed; conflicting C0012 quarantined |

Rules of engagement:
- Only actions attached to the **failing rule** are attempted for that record.
- Policy `permitted_deterministic_actions` gates sensitive columns — e.g. on
  `customer_id`/`transaction_amount`, only `TRIM/UPPERCASE/LOOKUP_MAPPING/
  DATE_STANDARDIZATION/DEDUPE` may run deterministically; `NULL_REPLACEMENT`
  and AI fixes there require approval.
- Each fix emits an audit row: record ref, rule ID, column, original value,
  proposed value, action, result → written as the `silver_audit_log` artifact.

## 3. AI-assisted remediation (`remediation/ai_remediation.py` + `ai/mock.py`)

`AIRemediationProvider` interface (`ai/base.py`) → `MockAIProvider`
(`ai/mock.py`) reads scripted responses from
`config/ai/mock_ai_responses.yaml`. **Simulated outputs — no external LLM calls.**

Proposal schema: `record_ref`, `rule_id`, `column`, `original_value`,
`proposed_value`, `confidence` (0–1), `reason`, `evidence`, `provider`,
`model_version`, `simulated: true`.

Demonstration fixtures (all labelled simulated):
- **High confidence:** AI country `'IND'→'IN'` @0.95 → applied;
  AI status @0.97 / @0.96 → applied; AI status @**0.90** → applied (boundary);
  @**0.8999** → quarantined.
- **Low confidence:** @0.34, @0.62, @0.70 → `BELOW_CONFIDENCE_THRESHOLD`.
- **Invalid correction:** AI proposes `'IND'` (ISO-3) where lookup expects ISO-2
  @0.91 → passes threshold but **fails revalidation** →
  `CORRECTION_FAILED_REVALIDATION`.
- **Sensitive columns:** AI name fix @0.98 on `customer_name`, AI amount @0.97 on
  `transaction_amount` → `PENDING_HUMAN_APPROVAL` (high confidence ≠ correctness).

Batched by distinct `(rule_id, column, original_value)` up to
`ai.max_requests_per_run` — no per-row provider calls; no AI-generated code is
ever executed (proposals are data, validated like any other value).

## 4. Policy interaction order

For each failed record/column:
1. Try deterministic remediation declared on the failing rule (if policy permits).
2. Revalidate deterministic result → accept if clean.
3. Else collect AI proposal → evaluate: exists? confidence ≥ threshold?
   rule `auto_apply`? sensitive column approval? pre-approved correction?
4. Apply only what policy allows → revalidate.
5. Everything else → quarantine with the precise status reason.

## 5. Evidence from the POC run (customers, REMEDIATE, v1.0)

```
input=38 accepted=24 accepted_remediated=8 quarantined=13 removed_duplicates=1
deterministic_fixes=11 ai_fixes=4   quality gate: PASSED
```

Quarantine breakdown by status (from the run):
`UNRESOLVED_NO_SAFE_REMEDIATION` (missing email, non-numeric age, bad dates,
conflicting dup id, missing id), `BELOW_CONFIDENCE_THRESHOLD` (0.34/0.62/0.8999),
`PENDING_HUMAN_APPROVAL` (sensitive identity/amount),
`CORRECTION_FAILED_REVALIDATION` (invalid `'IND'`).

Transactions REMEDIATE: `input=40 accepted=30 accepted_remediated=4
quarantined=9 removed_duplicates=1`.
