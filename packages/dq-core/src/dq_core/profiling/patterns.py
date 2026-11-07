"""Pattern and semantic-type definitions used by the profiler and the rule generator."""

from __future__ import annotations

import re

from pyspark.sql import Column
from pyspark.sql import functions as F

EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
CANDIDATE_DATE_FORMATS = ("yyyy-MM-dd", "dd/MM/yyyy", "MM/dd/yyyy", "yyyy/MM/dd", "dd-MM-yyyy")
SPECIAL_NAME_RE = re.compile(r"[^A-Za-z0-9_]")


def structural_pattern(column: Column) -> Column:
    """Map characters to classes: A=upper, a=lower, 9=digit (other characters kept)."""
    expr = F.regexp_replace(column, "[A-Z]", "A")
    expr = F.regexp_replace(expr, "[a-z]", "a")
    return F.regexp_replace(expr, "[0-9]", "9")


def pattern_to_regex(pattern: str) -> str:
    """Convert a structural pattern such as 'A9999' into an anchored regex '^[A-Z][0-9]{4}$'."""
    if not pattern:
        return "^$"
    classes = {"A": "[A-Z]", "a": "[a-z]", "9": "[0-9]"}
    parts: list[str] = []
    run_char, run_len = pattern[0], 0
    for ch in pattern + "\0":
        if ch == run_char:
            run_len += 1
            continue
        token = classes.get(run_char, re.escape(run_char))
        parts.append(token if run_len == 1 else f"{token}{{{run_len}}}")
        run_char, run_len = ch, 1
    return "^" + "".join(parts) + "$"
