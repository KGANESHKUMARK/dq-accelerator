"""NoSQL metadata store — document collections backing all DQ governance metadata.

Collections ("meta tables")::

    ruleset_index      one doc per ruleset_id   {ruleset_id, index:{versions,active_version,history}}
    ruleset_versions   one doc per ruleset@ver  {ruleset_id, version, payload:{ruleset,checksum}}
    pipelines          one doc per pipeline_id   {pipeline_id, entry:{...lifecycle state...}}
    manifests          one doc per pipeline_id   {pipeline_id, manifest:{...PipelineManifest.raw...}}
    policies           one doc per policy_id     {policy_id, version, config:{...}}
    approvals          one doc per decision      {dataset,column,original_value,approved_value,...}
    rejections         one doc per decision      {dataset,column,original_value,proposed_value,...}
    profiles           one doc per dataset+ts    {dataset, profiled_at, profile:{...}}
    candidates         one doc per ruleset draft {ruleset_id, version, rules:[...CANDIDATE...]}
    jobs               one doc per job_id        {job_id, kind, status, ...}

Backends:
  * ``tinydb``  (default) — embedded JSON document store, zero-install, file-backed.
                  Selected via ``registry.backend: nosql`` + ``metadata.db_path``.
  * ``mongodb`` — pymongo (lazy import); production path via ``metadata.uri``.
                  Same collection API — swap is a config change, not a code change.

YAML remains the authoring/seed/export format: ``export_yaml``/``import_yaml``
helpers round-trip any collection for review, diff and git-friendly artifacts.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Iterable, Mapping

from dq_core.runtime.errors import ConfigurationError


class MetadataStore:
    """Collection-oriented document store. Documents are plain JSON-able dicts."""

    def collection(self, name: str) -> "Collection":  # pragma: no cover - interface
        raise NotImplementedError

    def close(self) -> None:  # pragma: no cover - interface
        pass


class Collection:
    """Minimal document-collection API (Mongo-style naming for easy mental mapping)."""

    def insert_one(self, doc: Mapping[str, Any]) -> Any: ...
    def replace_one(self, query: Mapping[str, Any], doc: Mapping[str, Any], upsert: bool = False) -> Any: ...
    def find_one(self, query: Mapping[str, Any]) -> dict[str, Any] | None: ...
    def find(self, query: Mapping[str, Any] | None = None) -> list[dict[str, Any]]: ...
    def delete_one(self, query: Mapping[str, Any]) -> Any: ...
    def count(self, query: Mapping[str, Any] | None = None) -> int: ...


# --------------------------------------------------------------------------- TinyDB backend


class TinyCollection(Collection):
    def __init__(self, table: Any) -> None:
        self._t = table

    @staticmethod
    def _cond(query: Mapping[str, Any]):
        from tinydb import Query
        q = Query()
        cond = None
        for k, v in dict(query).items():
            clause = q[k] == v
            cond = clause if cond is None else (cond & clause)
        return cond if cond is not None else q.anything.exists() if False else None

    def _search(self, query: Mapping[str, Any] | None):
        if not query:
            return self._t.all()
        cond = self._cond(query)
        return self._t.search(cond) if cond is not None else []

    def insert_one(self, doc: Mapping[str, Any]) -> Any:
        return self._t.insert(dict(doc))

    def replace_one(self, query: Mapping[str, Any], doc: Mapping[str, Any], upsert: bool = False) -> Any:
        docs = self._search(query)
        if docs:
            return self._t.update(dict(doc), doc_ids=[d.doc_id for d in docs])
        if upsert:
            merged = {**dict(query), **dict(doc)}
            return self._t.insert(merged)
        return None

    def find_one(self, query: Mapping[str, Any]) -> dict[str, Any] | None:
        docs = self._search(query)
        return dict(docs[0]) if docs else None

    def find(self, query: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        return [dict(d) for d in self._search(query)]

    def delete_one(self, query: Mapping[str, Any]) -> Any:
        docs = self._search(query)
        return self._t.remove(doc_ids=[d.doc_id for d in docs]) if docs else None

    def count(self, query: Mapping[str, Any] | None = None) -> int:
        return len(self.find(query))


class TinyMetadataStore(MetadataStore):
    """Embedded NoSQL document store backed by a single JSON file (tinydb)."""

    _cache: dict[str, "TinyMetadataStore"] = {}
    _cache_lock = threading.Lock()

    @classmethod
    def open(cls, db_path: str | Path) -> "TinyMetadataStore":
        key = str(Path(db_path).resolve())
        with cls._cache_lock:
            if key not in cls._cache:
                Path(key).parent.mkdir(parents=True, exist_ok=True)
                cls._cache[key] = cls(key)
        return cls._cache[key]

    def __init__(self, db_path: str | Path) -> None:
        from tinydb import TinyDB
        from tinydb.storages import JSONStorage
        self.path = str(db_path)
        self._db = TinyDB(self.path, storage=JSONStorage)
        self._tables: dict[str, TinyCollection] = {}

    def collection(self, name: str) -> TinyCollection:
        if name not in self._tables:
            self._tables[name] = TinyCollection(self._db.table(name))
        return self._tables[name]

    def close(self) -> None:
        self._db.close()


# --------------------------------------------------------------------------- MongoDB backend (lazy)


class MongoCollection(Collection):
    """pymongo-backed collection — same API, production swap target."""

    def __init__(self, coll: Any) -> None:
        self._c = coll

    def insert_one(self, doc: Mapping[str, Any]) -> Any:
        return self._c.insert_one(dict(doc)).inserted_id

    def replace_one(self, query: Mapping[str, Any], doc: Mapping[str, Any], upsert: bool = False) -> Any:
        return self._c.replace_one(dict(query), dict(doc), upsert=upsert)

    def find_one(self, query: Mapping[str, Any]) -> dict[str, Any] | None:
        return self._c.find_one(dict(query))

    def find(self, query: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        return [dict(d) for d in self._c.find(dict(query or {}))]

    def delete_one(self, query: Mapping[str, Any]) -> Any:
        return self._c.delete_one(dict(query))

    def count(self, query: Mapping[str, Any] | None = None) -> int:
        return self._c.count_documents(dict(query or {}))


class MongoMetadataStore(MetadataStore):
    """MongoDB document store — requires ``pymongo`` + ``metadata.uri``.

    Same collection semantics as TinyMetadataStore; selected via
    ``registry.backend: nosql`` + ``metadata.engine: mongodb`` + ``metadata.uri``.
    """

    def __init__(self, uri: str, db_name: str = "dq_metadata") -> None:
        try:
            from pymongo import MongoClient
        except ImportError as exc:  # pragma: no cover
            raise ConfigurationError("metadata.engine=mongodb requires pymongo (pip install pymongo)") from exc
        self._client = MongoClient(uri)
        self._db = self._client[db_name]
        self._colls: dict[str, MongoCollection] = {}

    def collection(self, name: str) -> MongoCollection:
        if name not in self._colls:
            self._colls[name] = MongoCollection(self._db[name])
        return self._colls[name]

    def close(self) -> None:
        self._client.close()


# --------------------------------------------------------------------------- factory + YAML round-trip


def open_metadata_store(settings: Any) -> MetadataStore:
    """Build a MetadataStore from engine config ``metadata`` section (or registry fallbacks)."""
    meta = getattr(settings, "metadata", None)
    engine = (getattr(meta, "engine", None) or "tinydb").lower() if meta else "tinydb"
    if engine in ("tinydb", "embedded", "nosql"):
        path = getattr(meta, "db_path", None) if meta else None
        if not path:
            db = getattr(settings, "db_path", None)
            path = str(Path(db).with_suffix(".nosql.json")) if db else "data/metadata.nosql.json"
        return TinyMetadataStore.open(path)
    if engine == "mongodb":
        uri = getattr(meta, "uri", None) if meta else None
        if not uri:
            raise ConfigurationError("metadata.engine=mongodb requires metadata.uri")
        return MongoMetadataStore(uri, getattr(meta, "db_name", "dq_metadata") or "dq_metadata")
    raise ConfigurationError(f"Unsupported metadata.engine '{engine}' (expected tinydb|mongodb)")


def export_collection(store: MetadataStore, name: str) -> list[dict[str, Any]]:
    """Dump a whole collection as plain JSON-able docs (YAML/JSON export source)."""
    return store.collection(name).find()


def import_documents(store: MetadataStore, name: str, docs: Iterable[Mapping[str, Any]]) -> int:
    """Upsert documents into a collection — used by YAML seed/import."""
    coll = store.collection(name)
    n = 0
    for doc in docs:
        coll.replace_one(doc, doc, upsert=True)
        n += 1
    return n


def dump_jsonable(obj: Any) -> str:
    return json.dumps(obj, indent=2, default=str)
