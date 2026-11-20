# Quarantine & Revalidation

Code: `dq_core/quarantine/builder.py`, `dq_core/revalidation/revalidator.py`.

## 1. What lands in quarantine

Any record that cannot be safely accepted after the configured mode:

- **OBSERVE_ONLY** — nothing is removed; failures are *flagged* in the
  quarantine/report frame with status `OBSERVED_ONLY` (pipeline output untouched).
- **ENFORCE** — every record failing ≥1 ERROR rule → quarantine.
- **REMEDIATE** — records still failing after deterministic + AI remediation,
  plus records whose only path was blocked by policy.

## 2. Quarantine schema

Each quarantine row contains the **original record** plus `dq_*` audit columns:

| Column family | Contents |
|---|---|
| Identity | `dq_run_id`, `dq_pipeline_id`, `dq_pipeline_run_id`, `dq_dataset_id`, `dq_stage`, `dq_ruleset_id`, `dq_ruleset_version`, `dq_ruleset_checksum` |
| Failure | `dq_failed_rule_ids` (list), `dq_failure_reasons` (list of `rule_id: reason`), per-rule severity |
| Remediation | `dq_proposed_corrections` (JSON: column, original, proposed, action/provider), `dq_confidence`, `dq_remediation_status`, `dq_deterministic_fixes`, `dq_ai_proposals` |
| Record ref | deterministic record reference derived from `record_key` (or row hash) so quarantine rows join back to source |

Multiple violations per record are preserved as lists — never collapsed to the
first failure.

## 3. Remediation status taxonomy (observed)

| Status | Meaning |
|---|---|
| `OBSERVED_ONLY` | flagged in observe mode; not removed |
| `UNRESOLVED_NO_SAFE_REMEDIATION` | failed, no approved deterministic fix, no eligible AI proposal (e.g. missing email, non-numeric age, invalid calendar date, conflicting duplicate key, missing id) |
| `BELOW_CONFIDENCE_THRESHOLD` | AI proposal exists but confidence < 0.90 |
| `PENDING_HUMAN_APPROVAL` | correction on a sensitive column / requires review (high AI confidence ≠ approval) |
| `CORRECTION_FAILED_REVALIDATION` | a proposed/applied correction still fails the rules after re-check (e.g. `'IND'` vs ISO-2) |
| `DEDUPLICATE_REMOVED` | exact duplicate removed by configured dedupe (kept in a separate removed-duplicates frame, not the main quarantine) |

## 4. Revalidation (`revalidation/revalidator.py`)

After deterministic fixes and after AI corrections, the affected records are
re-run through the **same** validation rules:

```
corrected_df ──► validate(same ruleset) ──► still failing? ──► quarantine
                                            passing?  ──► accepted
```

Guarantees:
- A correction that introduces a *new* violation is caught (post-validation).
- AI proposals are treated as untrusted data — validated exactly like source data.
- Revalidation results feed `final_validation_results` in `DQResult` and the
  JSON report, so auditors see pre- and post-correction outcomes.

## 5. Downstream artifacts

`MedallionRunner` / `dqctl run --out` write:

```
data/quarantine/silver/<dataset>/        # quarantine rows (parquet)
data/quarantine/silver/<dataset>_removed_duplicates/
data/audit/silver/<dataset>_remediation_log/   # every fix attempt w/ before→after
data/silver/<dataset>/                   # accepted records only
reports/<dataset>_silver_<run>.json      # full audit report
```

## 6. POC evidence (customers, REMEDIATE v1.0)

`input=38 → accepted=24 (+8 remediated) quarantined=13 removed_duplicates=1`.
Quarantine statuses observed: 6× `UNRESOLVED_NO_SAFE_REMEDIATION`,
3× `BELOW_CONFIDENCE_THRESHOLD` (0.34/0.62/0.8999),
2× `PENDING_HUMAN_APPROVAL` (name @0.98, transaction amount @0.97),
1× `CORRECTION_FAILED_REVALIDATION` (`'IND'` @0.91), plus the OBSERVE_ONLY flag
set of 22 records in observe mode. Every expected per-record outcome in
`sample_data/expected_results.json` matched the engine output.
