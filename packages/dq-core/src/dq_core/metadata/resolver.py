"""Metadata resolver — store-first loading with YAML fallback + write-through.

Pattern: NoSQL collections are canonical once populated; YAML files remain the
authoring/seed format. Every loader tries the document store first, falls back
to the YAML path, and lazily upserts the YAML content into the store so the
next read hits the DB. Stores without ``collection`` (file/sqlite backends)
are skipped transparently — YAML stays canonical there.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from dq_core.runtime.utils import utc_now_iso


def doc_store(engine_or_registry: Any) -> Any | None:
    """Return the MetadataStore when the active backend is a document store."""
    store = getattr(engine_or_registry, "store", None)
    return store if store is not None and hasattr(store, "collection") else None


def store_from_engine(engine: Any) -> Any | None:
    return doc_store(getattr(engine, "ruleset_registry", None))


def upsert(store: Any, collection: str, key: str, doc: Mapping[str, Any]) -> None:
    store.collection(collection).replace_one({key: doc.get(key)}, dict(doc), upsert=True)


# ------------------------------------------------------------------ manifests

def manifest_from_store(store: Any, pipeline_id: str) -> dict[str, Any] | None:
    doc = store.collection("manifests").find_one({"pipeline_id": pipeline_id})
    return doc.get("manifest") if doc else None


def save_manifest(store: Any, manifest: Any) -> None:
    raw = manifest.to_dict() if hasattr(manifest, "to_dict") else dict(manifest)
    store.collection("manifests").replace_one(
        {"pipeline_id": manifest.pipeline_id},
        {"pipeline_id": manifest.pipeline_id, "manifest": raw, "updated_at": utc_now_iso()}, upsert=True)


# ------------------------------------------------------------------ policies

def policy_from_store(store: Any, policy_id: str) -> dict[str, Any] | None:
    doc = store.collection("policies").find_one({"policy_id": policy_id})
    return doc.get("config") if doc else None


def save_policy(store: Any, policy_id: str, config: Mapping[str, Any]) -> None:
    store.collection("policies").replace_one(
        {"policy_id": policy_id}, {"policy_id": policy_id, "config": dict(config),
                                   "updated_at": utc_now_iso()}, upsert=True)


# ------------------------------------------------------------------ approvals / rejections

def approval_docs(store: Any) -> list[dict[str, Any]]:
    return store.collection("approvals").find()


def rejection_docs(store: Any) -> list[dict[str, Any]]:
    return store.collection("rejections").find()


def _as_dict(obj: Any) -> dict[str, Any]:
    if isinstance(obj, Mapping):
        return dict(obj)
    if hasattr(obj, "to_dict"):
        return dict(obj.to_dict())
    return dict(obj.__dict__)


def record_approval(store: Any, approval: Any) -> None:
    store.collection("approvals").insert_one({**_as_dict(approval), "recorded_at": utc_now_iso()})


def record_rejection(store: Any, rejection: Any) -> None:
    store.collection("rejections").insert_one({**_as_dict(rejection), "recorded_at": utc_now_iso()})
