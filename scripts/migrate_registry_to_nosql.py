"""One-time migration: file registry -> NoSQL metadata store (tinydb).

Reads every published ruleset version + pipeline entry from the file registry
and upserts them into the document store, preserving checksums/history exactly.
Idempotent — safe to re-run.

Usage:  .venv/Scripts/python.exe scripts/migrate_registry_to_nosql.py [--db data/metadata.nosql.json]
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "packages" / "dq-core" / "src"))

from dq_core.metadata.store import TinyMetadataStore  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(ROOT / "data" / "metadata.nosql.json"))
    ap.add_argument("--registry", default=str(ROOT / "registry"))
    args = ap.parse_args()

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
