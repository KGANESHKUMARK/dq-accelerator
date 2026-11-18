"""Seed the NoSQL metadata store.

Two modes, both idempotent:

* default — migrate the file registry (registry/*.json) into the store.
* --from-config — fresh-clone seed: publish every config/rulesets/*.yaml and
  onboard every config/pipelines/*.yaml using the engine's configured backend
  (use this on a new machine where registry/ and data/ are gitignored).

Usage:  .venv/Scripts/python.exe scripts/migrate_registry_to_nosql.py [--db ...] [--from-config]
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "packages" / "dq-core" / "src"))

from dq_core.metadata.store import TinyMetadataStore  # noqa: E402


def seed_from_config() -> tuple[int, int]:
    """Publish rulesets + onboard pipelines straight from config YAMLs."""
    from dq_core.metadata.serialization import load_ruleset
    from dq_core.pipelines.manifest import PipelineManifest
    from dq_core.registries.factory import build_pipeline_registry, build_ruleset_registry
    from dq_core.runtime.config import EngineConfig

    cfg = EngineConfig.from_yaml(ROOT / "config" / "engine.yaml")
    rs_reg = build_ruleset_registry(cfg.registry)
    pp_reg = build_pipeline_registry(cfg.registry)
    n_rs = n_pp = 0
    rdir = ROOT / "config" / "rulesets"
    if rs_reg is not None and rdir.exists():
        for p in sorted(rdir.glob("*.yaml")):
            try:
                rs_reg.publish(load_ruleset(p), published_by="seed", activate=True)
                n_rs += 1
            except Exception as exc:
                print(f"  skip ruleset {p.name}: {exc}")
    mdir = ROOT / "config" / "pipelines"
    if pp_reg is not None and mdir.exists():
        for p in sorted(mdir.glob("*.yaml")):
            try:
                pp_reg.onboard(PipelineManifest.load(p), actor="seed")
                n_pp += 1
            except Exception as exc:
                print(f"  skip manifest {p.name}: {exc}")
    return n_rs, n_pp


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(ROOT / "data" / "metadata.nosql.json"))
    ap.add_argument("--registry", default=str(ROOT / "registry"))
    ap.add_argument("--from-config", action="store_true",
                    help="fresh-clone seed: publish/onboard everything under config/")
    args = ap.parse_args()

    seeded_rs = seeded_pp = 0
    if args.from_config:
        seeded_rs, seeded_pp = seed_from_config()
        print(f"seeded from config: {seeded_rs} rulesets published, {seeded_pp} pipelines onboarded")

    store = TinyMetadataStore.open(args.db)
    reg = Path(args.registry)
    n_rs = n_pp = 0

    # rulesets: <id>/index.json + <id>/versions/<v>.json
    rroot = reg / "rulesets"
    if rroot.exists():
        for d in sorted(rroot.iterdir()):
            idx_path = d / "index.json"
            if not idx_path.exists():
                continue
            index = json.loads(idx_path.read_text(encoding="utf-8"))
            rid = index.get("ruleset_id") or d.name
            store.collection("ruleset_index").replace_one(
                {"ruleset_id": rid}, {"ruleset_id": rid, "index": index}, upsert=True)
            n_rs += 1
            vdir = d / "versions"
            if vdir.exists():
                for vf in vdir.glob("*.json"):
                    payload = json.loads(vf.read_text(encoding="utf-8"))
                    ver = vf.stem
                    store.collection("ruleset_versions").replace_one(
                        {"ruleset_id": rid, "version": ver},
                        {"ruleset_id": rid, "version": ver, "payload": payload}, upsert=True)

    # pipelines: <id>.json or <id>/entry.json — detect layout
    proot = reg / "pipelines"
    if proot.exists():
        for p in sorted(proot.rglob("*.json")):
            if p.name in ("index.json",) or "versions" in p.parts:
                continue
            try:
                entry = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue
            pid = entry.get("pipeline_id") or p.stem
            store.collection("pipelines").replace_one(
                {"pipeline_id": pid}, {"pipeline_id": pid, "entry": entry}, upsert=True)
            n_pp += 1

    # ---- governance definitions: manifests, policies, approvals, rejections ----
    import yaml
    n_man = n_pol = n_app = n_rej = 0
    mdir = ROOT / "config" / "pipelines"
    if mdir.exists():
        for p in sorted(mdir.glob("*.yaml")):
            try:
                doc = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
            except Exception:
                continue
            mf = doc.get("manifest") or doc
            pid = mf.get("pipeline_id") or p.stem
            store.collection("manifests").replace_one(
                {"pipeline_id": pid}, {"pipeline_id": pid, "manifest": mf}, upsert=True)
            n_man += 1
    pdir = ROOT / "config" / "policies"
    if pdir.exists():
        for p in sorted(pdir.glob("*.yaml")):
            try:
                doc = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
            except Exception:
                continue
            pol = doc.get("policy") or doc
            key = str(pol.get("policy_id") or p.stem)
            store.collection("policies").replace_one(
                {"policy_id": key}, {"policy_id": key, "config": pol}, upsert=True)
            n_pol += 1
    adir = ROOT / "config" / "approvals"
    if adir.exists():
        for p in sorted(adir.glob("*.yaml")):
            try:
                doc = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
            except Exception:
                continue
            for item in doc.get("approvals") or []:
                key = {"dataset": item.get("dataset"), "column": item.get("column"),
                       "original_value": item.get("original_value"),
                       "approved_value": item.get("approved_value")}
                if not store.collection("approvals").find_one(key):
                    store.collection("approvals").insert_one(dict(item))
                    n_app += 1
            for item in doc.get("rejections") or []:
                key = {"dataset": item.get("dataset"), "column": item.get("column"),
                       "original_value": item.get("original_value"),
                       "proposed_value": item.get("proposed_value", item.get("corrected_value"))}
                if not store.collection("rejections").find_one(key):
                    store.collection("rejections").insert_one(dict(item))
                    n_rej += 1

    print(f"migrated: {n_rs} ruleset indexes, {n_pp} pipeline entries, "
          f"{n_man} manifests, {n_pol} policies, {n_app} approvals, {n_rej} rejections")
    print("ruleset_index docs:", store.collection("ruleset_index").count())
    print("ruleset_versions docs:", store.collection("ruleset_versions").count())
    print("pipelines docs:", store.collection("pipelines").count())
    print("manifests docs:", store.collection("manifests").count())
    print("policies docs:", store.collection("policies").count())
    print("approvals docs:", store.collection("approvals").count())
    print("rejections docs:", store.collection("rejections").count())


if __name__ == "__main__":
    main()
