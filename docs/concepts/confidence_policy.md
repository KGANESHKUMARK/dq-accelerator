# Confidence Policy

Configurable confidence threshold for AI-assisted corrections. Default **90 %**.
Code: `dq_core/policies/confidence.py`, `models.py`, `approvals.py`;
config: `config/policies/default.yaml`.

## 1. Threshold semantics

- Represented as `Decimal` in **[0, 1]** — `0.90` means 90 %.
- `parse_threshold()` rejects non-numeric and out-of-range values with
  `PolicyError`.
- Decision rule: `confidence >= threshold` → **eligible** for auto-apply
  (subject to rule/policy gates); `confidence < threshold` → quarantine with
  status `BELOW_CONFIDENCE_THRESHOLD`.
- Boundary is **inclusive**: exactly 0.90 is applied; 0.8999 is quarantined —
  both cases exist in the synthetic data (C0034 vs C0035) and in unit tests.

Verified boundary matrix (unit tests, `test_policies.py`):

| Confidence | Decision |
|---|---|
| 0.89 | BELOW_THRESHOLD → quarantine |
| 0.8999 | BELOW_THRESHOLD → quarantine |
| 0.90 | AUTO_APPLY eligible |
| 0.95 | AUTO_APPLY eligible |
| 1.00 | AUTO_APPLY eligible (still gated by sensitive rules) |

## 2. Override path

`engine.run(..., confidence_threshold=0.85)` or
`policy.with_threshold("0.85")` — per-run override without editing YAML.
`RemediationPolicyConfig.load()` reads `policy.confidence.threshold`.

## 3. Beyond confidence — the full gate

Confidence is necessary but **not sufficient**. A proposal is auto-applied only
when ALL hold:

1. `confidence >= threshold`;
2. the failing rule's remediation policy allows AI auto-apply
   (`ai.allow_auto_apply`, rule-level flags);
3. column is **not sensitive**, OR the correction is a permitted deterministic
   reference-based action, OR a matching entry exists in
   `config/approvals/approved_corrections.yaml` (human pre-approval);
4. the corrected value **passes revalidation**.

Otherwise → quarantine with the precise status:
`BELOW_CONFIDENCE_THRESHOLD`, `PENDING_HUMAN_APPROVAL`,
`CORRECTION_FAILED_REVALIDATION`, `UNRESOLVED_NO_SAFE_REMEDIATION`.

## 4. Sensitive data categories (`default.yaml`)

| Category | Columns (examples) |
|---|---|
| `customer_identity` | `customer_id`, `customer_name`, `national_id`, `passport_number`, `date_of_birth` |
| `account_identifiers` | `account_id`, `account_number`, `iban`, `card_number` |
| `transaction_amounts` | `transaction_amount`, `amount` |
| `regulatory` | `kyc_status`, `tax_id`, `pan_number`, `risk_rating`, `sanctions_flag` |

`permitted_deterministic_actions` on sensitive columns:
`TRIM, UPPERCASE, LOOKUP_MAPPING, DATE_STANDARDIZATION, DEDUPE` — formatting /
reference-based only. `NULL_REPLACEMENT`, value invention, and AI rewrites on
these columns require human approval even at confidence 1.00
(unit-tested: `test_sensitive_column_requires_approval_even_at_100`).

Also `ai.allow_sensitive_values_to_external_providers: false` — sensitive
values are never sent to an external provider; the mock provider is local-only.

## 5. Design rationale

> "A high AI confidence alone is not proof of correctness."

The POC demonstrates why: the mock AI proposes `'IND'` (ISO-3) for `country` at
**0.91** — above threshold — but the approved lookup expects ISO-2, so the
correction **fails revalidation** and is quarantined. Confidence gates
*eligibility*; revalidation gates *correctness*; approvals gate *impact*.
