# Rule Management

Metadata-driven, versioned, reviewable rule sets. Code: `dq_core/metadata/`,
`dq_core/rules/`, storage under `config/rulesets/` + `registry/rulesets/`.

## 1. Models (`metadata/models.py`)

**`RuleDefinition`** (frozen dataclass, validated in `__post_init__`):

| Field | Notes |
|---|---|
| `rule_id` | unique within a RuleSet, e.g. `CUST_007` |
| `name`, `description` | human-readable |
| `dataset`, `column` | target (`column=None` for dataset-level rules) |
| `rule_type` | see enum below |
| `scope` | `ROW_LEVEL` (default per type), `DATASET_LEVEL`, `CROSS_DATASET` |
| `severity` | `ERROR` / `WARNING` / `INFO` |
| `stage` | `BRONZE` / `SILVER` / `GOLD` |
| `config` | rule-type-specific parameters (pattern, min/max, values, keys, …) |
| `remediation` | deterministic steps + AI policy for the rule |
| `version`, `status` | rule version; `CANDIDATE/APPROVED/REJECTED/DEPRECATED` |
| `condition` | optional precondition (`RuleCondition`) |
| `generation` | provenance for generated candidates (confidence + evidence) |

**`RuleSet`**: `ruleset_id`, `dataset`, `version`, `rules`, `stage`, `status`
(`DRAFT`/`PUBLISHED`/`DEPRECATED`), `record_key`, `lookups` (approved lookup
tables), `parent_version`, approval metadata, `change_log`, `checksum()`.

Validation enforced at construction: unique rule IDs, rule-type config
requirements (REGEX needs a valid pattern, RANGE needs a bound,
ALLOWED_VALUES needs a list, DUPLICATE needs keys, SCHEMA_VALIDATION takes no
column, NULL_REPLACEMENT requires `approved: true`, DEDUPLICATE requires a
survivor policy), lookup references must exist and be `approved: true`, and a
**PUBLISHED** RuleSet may not contain `CANDIDATE` rules.

## 2. Rule types

`NOT_NULL, UNIQUE, DUPLICATE, REGEX, RANGE, ALLOWED_VALUES, DATA_TYPE,
DATE_FORMAT, STRING_LENGTH, REFERENTIAL_INTEGRITY, SCHEMA_VALIDATION, ROW_COUNT`
(+ `RuleCondition` for composite logic). Each maps to a scope default;
REFERENTIAL_INTEGRITY/CROSS_DATASET rules consume `reference_data` frames.

## 3. Versioning & immutability

- `RuleSet.derive(new_version, add=, remove=, modify=, …)` creates a **new**
  version; the source object is never mutated. `new_version` must be greater
  (`version_key` ordering).
- `checksum()` = SHA-256 over the canonical content dict — any content change
  produces a different checksum.
- `RuleSetRegistry.publish()`:
  - refuses to publish different content under an **existing** version
    (`ImmutableVersionError`);
  - writes an immutable version file + updates the `active` pointer only when
    `activate=True`;
  - `verify(ruleset)` returns whether the supplied object matches the published
    checksum — the engine calls this before ENFORCE/REMEDIATE, so **editing a
    published YAML and re-running fails loudly**.
- `rollback(ruleset_id, version)` re-points `active` to a previously published
  version — instant, no file edits.
- `diff(ruleset_id, from, to)` → added / removed / modified rule IDs with
  field-level changes (used by `dqctl ruleset diff`).

## 4. Serialization (`metadata/serialization.py`)

`load_ruleset(path)` accepts YAML or JSON; `to_dict()/from_dict()` round-trip.
`RuleSet.ref` gives `ruleset_id@version`. Registry refs like
`customers_silver@active` resolve through `DataQualityEngine.load_ruleset`.

## 5. Worked example — customers v1.0 → v1.1

`config/rulesets/customers_v1.yaml` (17 rules) vs `customers_v1_1.yaml`:

```
RuleSet customers_silver: 1.0 -> 1.1
  added=1 removed=0 modified=3 unchanged=13
  + CUST_017                                   (new rule)
  ~ CUST_007  config.max: 120 -> 110          (tighter age range)
  ~ CUST_010  values += 'SUSPENDED'           (new allowed status)
  ~ CUST_013  severity: WARNING -> ERROR      (whitespace now blocking)
```

Effect on data (verified): C0026 (age 115) passes v1.0 but is quarantined under
v1.1 — `remediate_v1_0.quarantined=13` vs `remediate_v1_1.quarantined=14`.

## 6. Candidate → approved flow

`CandidateRuleGenerator` emits a `DRAFT` RuleSet whose rules are all
`status=CANDIDATE` with `generation={confidence, evidence, generator}`.
Candidates **cannot** be published or executed. A steward reviews and calls
`approve_candidates(candidates, rule_ids, ruleset_id=…, version=…, approved_by=…,
overrides=…)` → a new DRAFT RuleSet with `APPROVED` rules, which is then saved as
YAML, reviewed, and published via `dqctl ruleset publish`.

## 7. CLI cheat-sheet

```bash
dqctl ruleset publish config/rulesets/customers_v1.yaml --activate
dqctl ruleset list
dqctl ruleset diff    customers_silver 1.0 1.1
dqctl ruleset activate customers_silver 1.1
dqctl ruleset rollback customers_silver 1.0 --reason "v1.1 needs review"
dqctl ruleset verify  config/rulesets/customers_v1.yaml   # checksum check
```
