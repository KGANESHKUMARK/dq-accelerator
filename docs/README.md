# DQ Accelerator — Documentation Index

Everything is grouped by audience. Pick your folder, not the whole tree.

| Folder | Who it's for | Contents |
|---|---|---|
| [`guides/`](guides/) | **Users & newcomers** | `SETUP.md` (fresh machine → live app), `PROJECT_GUIDE.md` (master reference), `DEVELOPER_GUIDE.md` (build phases + release), `PIPELINE_ONBOARDING.md` + `databricks_onboarding.md` (existing cluster pipelines), `ui_demo_guide.md` (UI walkthrough) |
| [`architecture/`](architecture/) | **Engineers & reviewers** | `architecture.md` (HLD/LLD diagrams + status), `spark_core_engine.md` (module LLD), `code_flow.md` (request path), `medallion_data_flow.md` (Bronze/Silver/Gold) |
| [`concepts/`](concepts/) | **Engine deep-dives** | `profiling.md`, `rule_management.md`, `remediation.md`, `quarantine.md`, `confidence_policy.md`, `pipeline_adapters.md` |
| [`poc/`](poc/) | **Scope & guardrails** | `POC.md` (use case), `POC_CONTEXT.md` (authoritative priorities — read first) |
| [`presentation/`](presentation/) | **Client-facing** | deck points, runbooks, demo scripts, Q&A prep, walkthroughs |
| [`reference/`](reference/) | **Evidence** | `testing_report.md` (executed test evidence), `sample_pipeline_manifest.yaml` |

Root level: `README.md` (repo overview), `ROADMAP.md` (priorities + gap register), `AGENTS.md` (agent/session memory).

**Rule of thumb:** presenting → `presentation/` + `architecture/architecture.md` ·
setting up → `guides/SETUP.md` · understanding a subsystem → `concepts/` ·
changing code → `guides/DEVELOPER_GUIDE.md` + `architecture/`.
