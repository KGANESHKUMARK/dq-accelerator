"""Small shared helpers: identifiers, hashing, timing and path resolution."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

_URI_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*:(//|/)")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat(timespec="seconds").replace("+00:00", "Z")


def new_run_id(prefix: str = "DQRUN") -> str:
    return f"{prefix}-{utc_now().strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def to_jsonable(value: Any) -> Any:
    """Convert dataclasses / enums / decimals / paths into JSON-serialisable structures."""
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return to_jsonable(value.to_dict())
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [to_jsonable(v) for v in value]
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(to_jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_of(value: Any) -> str:
    return sha256_text(canonical_json(value))


def is_uri(path: str) -> bool:
    """True for scheme-qualified locations such as ``dbfs:/``, ``abfss://``, ``s3://``, ``gs://``."""
    return bool(_URI_RE.match(path)) and not re.match(r"^[a-zA-Z]:[\\/]", path)


def resolve_path(path: str | os.PathLike[str] | None, base_dir: str | os.PathLike[str] | None) -> str | None:
    """Resolve a configured path relative to ``base_dir``.

    URIs (dbfs:/, abfss://, s3://, gs://), POSIX absolute paths (/Volumes/..., /dbfs/...) and
    Windows absolute paths are returned unchanged. Environment variables are expanded so that
    platform specific locations can be injected by jobs (e.g. ``${DQ_OUTPUT_ROOT}/silver``).
    """
    if path is None:
        return None
    raw = os.path.expandvars(str(path))
    if is_uri(raw) or raw.startswith(("/", "\\")) or os.path.isabs(raw) or base_dir is None:
        return raw
    return str((Path(base_dir) / raw).resolve())


def join_path(base: str, *parts: str) -> str:
    """Join path segments for both local paths and URIs (always '/' for URIs)."""
    if is_uri(base) or base.startswith("/"):
        return "/".join([base.rstrip("/")] + [p.strip("/") for p in parts])
    return str(Path(base, *parts))


@dataclass
class Stopwatch:
    """Context manager measuring elapsed wall-clock milliseconds."""

    started: float = field(default_factory=time.perf_counter)
    elapsed_ms: float = 0.0

    def __enter__(self) -> "Stopwatch":
        self.started = time.perf_counter()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.elapsed_ms = round((time.perf_counter() - self.started) * 1000, 2)

    def lap_ms(self) -> float:
        return round((time.perf_counter() - self.started) * 1000, 2)
