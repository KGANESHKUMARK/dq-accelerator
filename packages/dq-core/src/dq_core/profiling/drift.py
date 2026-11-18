"""Run-over-run profile drift detection.

Compares the current batch's column profiles against a stored baseline
profile (previous onboarded/run snapshot from the ``profiles`` collection)
and emits drift findings — the statistical-drift analogue of DQX's anomaly
engine, kept dependency-free and Spark-agnostic.

Drift metrics per column:

* ``NULL_RATE_DRIFT``   — null_percentage moved beyond ``null_pp`` points
* ``CARDINALITY_DRIFT`` — distinct_count changed beyond ``cardinality_pct``
* ``MEAN_DRIFT``        — mean shifted beyond ``mean_robust_sigma`` robust sigma 
                          (IQR/1.349 — resistant to masking, same maths as
                          the profiler's ROBUST_OUTLIERS)
* ``NEW_CATEGORY_VALUES`` — dominant values in current batch absent from
                          baseline top values (categorical columns)
* ``TYPE_DRIFT``        — inferred_type changed between runs
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class DriftThresholds:
    null_pp: float = 10.0          # percentage points
    cardinality_pct: float = 30.0  # relative % change in distinct_count
    mean_robust_sigma: float = 3.0
    min_baseline_rows: int = 100   # skip drift on tiny baselines


@dataclass(frozen=True)
class DriftFinding:
    column: str
    metric: str
    severity: str
    detail: str
    current: Any = None
    baseline: Any = None

    def to_anomaly(self) -> dict[str, Any]:
        return {"column": self.column, "type": f"DRIFT_{self.metric}",
                "severity": self.severity, "detail": self.detail}


def _get(prof: Any, key: str, default: Any = None) -> Any:
    """Read a field from a ColumnProfile OR its serialized dict form."""
    if isinstance(prof, Mapping):
        return prof.get(key, default)
    return getattr(prof, key, default)


def _f(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def detect_drift(current: Mapping[str, Any], baseline: Mapping[str, Any], *,
                 thresholds: DriftThresholds | None = None) -> list[DriftFinding]:
    """Compare current column profiles to baseline -> drift findings.

    ``current``/``baseline`` map column name -> ColumnProfile or its dict form
    (the ``columns`` member of a ProfileResult / stored profile doc).
    """
    th = thresholds or DriftThresholds()
    findings: list[DriftFinding] = []
    base_rows = _f(_get(baseline.get("__meta__", {}), "row_count")) or 0

    for col, cur in current.items():
        base = baseline.get(col)
        if base is None:
            continue  # new column — schema drift handled elsewhere

        # --- null-rate drift ---
        c_null, b_null = _f(_get(cur, "null_percentage")), _f(_get(base, "null_percentage"))
        if c_null is not None and b_null is not None and abs(c_null - b_null) >= th.null_pp:
            direction = "up" if c_null > b_null else "down"
            findings.append(DriftFinding(
                col, "NULL_RATE", "WARNING" if direction == "up" else "INFO",
                f"null rate drifted {direction}: {b_null:.1f}% -> {c_null:.1f}% "
                f"(delta  {c_null - b_null:+.1f}pp)", c_null, b_null))

        # --- cardinality drift ---
        c_dist, b_dist = _f(_get(cur, "distinct_count")), _f(_get(base, "distinct_count"))
        if (c_dist and b_dist and b_dist > 0
                and abs(c_dist - b_dist) / b_dist * 100 >= th.cardinality_pct):
            direction = "up" if c_dist > b_dist else "down"
            findings.append(DriftFinding(
                col, "CARDINALITY", "WARNING",
                f"distinct values drifted {direction}: {int(b_dist)} -> {int(c_dist)} "
                f"({(c_dist - b_dist) / b_dist * 100:+.0f}%)", int(c_dist), int(b_dist)))

        # --- mean drift via robust sigma (IQR/1.349) ---
        c_mean, b_mean = _f(_get(cur, "mean")), _f(_get(base, "mean"))
        c_p = _get(cur, "percentiles") or {}
        b_p = _get(base, "percentiles") or {}
        c_iqr = (_f(c_p.get("p75")) or 0) - (_f(c_p.get("p25")) or 0)
        b_iqr = (_f(b_p.get("p75")) or 0) - (_f(b_p.get("p25")) or 0)
        if c_mean is not None and b_mean is not None:
            sigma = max(b_iqr / 1.349, 1e-9)
            z = abs(c_mean - b_mean) / sigma
            if z >= th.mean_robust_sigma:
                findings.append(DriftFinding(
                    col, "MEAN", "WARNING",
                    f"mean shifted {z:.1f} robust sigma : {b_mean:.4g} -> {c_mean:.4g} "
                    f"(baseline IQR={b_iqr:.4g})", round(c_mean, 6), round(b_mean, 6)))

        # --- new category values ---
        c_top = {str(t.get("value")) for t in (_get(cur, "top_values") or [])[:10]}
        b_top = {str(t.get("value")) for t in (_get(base, "top_values") or [])[:10]}
        new_vals = sorted(c_top - b_top)[:5]
        if new_vals and len(c_top) >= 3:
            findings.append(DriftFinding(
                col, "NEW_CATEGORY_VALUES", "INFO",
                f"new dominant values vs baseline: {new_vals}", new_vals, sorted(b_top)[:5]))

        # --- inferred-type drift ---
        c_type, b_type = _get(cur, "inferred_type"), _get(base, "inferred_type")
        if c_type and b_type and c_type != b_type:
            findings.append(DriftFinding(
                col, "TYPE", "ERROR",
                f"inferred type changed: '{b_type}' -> '{c_type}'", c_type, b_type))

    return findings
