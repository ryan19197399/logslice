"""Rate-based throttling: keep at most N records per time bucket."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Iterable, Iterator, List

from logslice.parser import LogRecord


@dataclass
class ThrottleResult:
    """Outcome of a throttle pass over a record stream."""

    records: List[LogRecord]
    total_in: int = 0
    total_dropped: int = 0

    @property
    def total_kept(self) -> int:
        return self.total_in - self.total_dropped

    def to_dict(self) -> dict:
        return {
            "total_in": self.total_in,
            "total_kept": self.total_kept,
            "total_dropped": self.total_dropped,
        }


def _bucket_key(ts: datetime, window_seconds: int) -> int:
    """Return an integer bucket index for *ts* given a window size in seconds."""
    return int(ts.timestamp()) // window_seconds


def throttle_records(
    records: Iterable[LogRecord],
    max_per_window: int,
    window_seconds: int = 60,
) -> ThrottleResult:
    """Yield at most *max_per_window* records per time bucket.

    Records whose ``timestamp`` is ``None`` are always kept.

    Args:
        records: Input stream of :class:`~logslice.parser.LogRecord` objects.
        max_per_window: Maximum number of records allowed in each time bucket.
        window_seconds: Width of each bucket in seconds (default: 60).

    Returns:
        A :class:`ThrottleResult` containing kept records and drop statistics.
    """
    if max_per_window < 1:
        raise ValueError("max_per_window must be >= 1")
    if window_seconds < 1:
        raise ValueError("window_seconds must be >= 1")

    kept: List[LogRecord] = []
    bucket_counts: dict[int, int] = {}
    total_in = 0
    total_dropped = 0

    for record in records:
        total_in += 1
        if record.timestamp is None:
            kept.append(record)
            continue

        key = _bucket_key(record.timestamp, window_seconds)
        count = bucket_counts.get(key, 0)
        if count < max_per_window:
            bucket_counts[key] = count + 1
            kept.append(record)
        else:
            total_dropped += 1

    return ThrottleResult(
        records=kept,
        total_in=total_in,
        total_dropped=total_dropped,
    )
