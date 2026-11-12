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

    print(f"migrated: {n_rs} ruleset indexes, pipeline entries -> {args.db}")
    print("ruleset_index docs:", store.collection("ruleset_index").count())
    print("ruleset_versions docs:", store.collection("ruleset_versions").count())
    print("pipelines docs:", store.collection("pipelines").count())


if __name__ == "__main__":
    main()
