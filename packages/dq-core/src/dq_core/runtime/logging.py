"""Structured (JSON) logging helpers.

The library never configures the root logger on import (it only attaches a ``NullHandler``);
applications, the CLI and the examples call :func:`configure_logging` explicitly.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, TextIO

LOGGER_NAME = "dq_core"


class JsonFormatter(logging.Formatter):
    """Render log records as single-line JSON documents."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(getattr(record, "dq", None) or {})
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class TextFormatter(logging.Formatter):
    """Human readable formatter that still shows structured fields."""

    def format(self, record: logging.LogRecord) -> str:
        base = f"{datetime.fromtimestamp(record.created).strftime('%H:%M:%S')} {record.levelname:<7} {record.getMessage()}"
        fields = getattr(record, "dq", None) or {}
        extras = {k: v for k, v in fields.items() if k != "event"}
        if extras:
            base += " | " + " ".join(f"{k}={v}" for k, v in extras.items())
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base


def get_logger(name: str = LOGGER_NAME) -> logging.Logger:
    if not name.startswith(LOGGER_NAME):
        name = f"{LOGGER_NAME}.{name}"
    return logging.getLogger(name)


def log_event(logger: logging.Logger, event: str, level: int = logging.INFO, **fields: Any) -> None:
    """Emit a structured event: ``event`` is the message, ``fields`` become JSON attributes."""
    logger.log(level, event, extra={"dq": {"event": event, **fields}})


def configure_logging(
    level: str | int = "INFO",
    json_format: bool = True,
    stream: TextIO | None = None,
    log_file: str | Path | None = None,
) -> None:
    """Configure the ``dq_core`` logger hierarchy (idempotent).

    ``log_file`` may also be supplied indirectly through the ``DQ_LOG_DIR``
    environment variable (the launchers set it to the project ``logs`` folder),
    in which case ``<DQ_LOG_DIR>/engine/dq_core.log`` is used with rotation.
    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level if isinstance(level, int) else level.upper())
    for handler in list(logger.handlers):
        if getattr(handler, "_dq_handler", False):
            logger.removeHandler(handler)
    formatter = JsonFormatter() if json_format else TextFormatter()
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(formatter)
    handler._dq_handler = True  # type: ignore[attr-defined]
    logger.addHandler(handler)
    if log_file is None:
        log_dir = os.environ.get("DQ_LOG_DIR")
        log_file = Path(log_dir) / "engine" / "dq_core.log" if log_dir else None
    if log_file is not None:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(path, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler._dq_handler = True  # type: ignore[attr-defined]
        logger.addHandler(file_handler)
    logger.propagate = False
