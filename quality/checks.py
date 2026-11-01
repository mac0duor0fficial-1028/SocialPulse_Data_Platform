"""Procedural data-quality checks: reconciliation, reject-rate breaker, null thresholds."""
from __future__ import annotations

import pandas as pd


class DataQualityError(RuntimeError):
    """A data-quality gate failed; the pipeline must stop."""


def check_row_reconciliation(entity: str, rows_in: int, rows_out: int, rows_rejected: int,
                             source_rows: int | None = None) -> None:
    """Every Bronze row must be accounted for: in == out + rejected, and Bronze == what the
    source reported at ingestion time (row_count metadata)."""
    if source_rows is not None and rows_in != source_rows:
        raise DataQualityError(
            f"{entity}: Bronze rows read ({rows_in}) != rows recorded at ingestion ({source_rows})"
        )
    if rows_in != rows_out + rows_rejected:
        raise DataQualityError(
            f"{entity}: reconciliation failed: in={rows_in} out={rows_out} rejected={rows_rejected}"
        )


def check_reject_rate(entity: str, rows_in: int, rows_rejected: int, max_rate: float) -> None:
    """Circuit breaker: a sudden spike in rejects usually means the *source* broke."""
    if rows_in and rows_rejected / rows_in > max_rate:
        raise DataQualityError(
            f"{entity}: reject rate {rows_rejected / rows_in:.1%} exceeds threshold {max_rate:.1%}"
        )


def check_null_rates(entity: str, df: pd.DataFrame, max_null_rate: dict[str, float] | None = None,
                     default: float = 0.0) -> dict[str, float]:
    """Raise if any column's null share exceeds its threshold (default: no nulls allowed)."""
    max_null_rate = max_null_rate or {}
    rates = df.isna().mean().to_dict() if len(df) else {c: 0.0 for c in df.columns}
    bad = {c: r for c, r in rates.items() if r > max_null_rate.get(c, default)}
    if bad:
        raise DataQualityError(f"{entity}: null-rate threshold exceeded: " +
                               ", ".join(f"{c}={r:.1%}" for c, r in bad.items()))
    return rates


def check_not_empty(name: str, df: pd.DataFrame) -> None:
    if df.empty:
        raise DataQualityError(f"{name}: dataset is empty")
