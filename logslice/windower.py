"""Sliding and tumbling window aggregation over log records."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Iterable, Iterator, List, Optional

from logslice.parser import LogRecord


@dataclass
class WindowSummary:
    window_start: datetime
    window_end: datetime
    records: List[LogRecord] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.records)

    @property
    def level_counts(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for r in self.records:
            key = (r.level or "UNKNOWN").upper()
            counts[key] = counts.get(key, 0) + 1
        return counts

    def to_dict(self) -> dict:
        return {
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "count": self.count,
            "level_counts": self.level_counts,
        }


@dataclass
class WindowResult:
    windows: List[WindowSummary] = field(default_factory=list)
    total_records: int = 0
    skipped_no_timestamp: int = 0

    def to_dict(self) -> dict:
        return {
            "total_records": self.total_records,
            "skipped_no_timestamp": self.skipped_no_timestamp,
            "window_count": len(self.windows),
            "windows": [w.to_dict() for w in self.windows],
        }


def window_records(
    records: Iterable[LogRecord],
    window_size: timedelta,
    step: Optional[timedelta] = None,
) -> WindowResult:
    """Group records into tumbling (step=None) or sliding windows.

    Args:
        records: Iterable of LogRecord instances.
        window_size: Duration of each window.
        step: Slide interval. Defaults to window_size (tumbling).

    Returns:
        WindowResult containing all windows with their records.
    """
    if window_size.total_seconds() <= 0:
        raise ValueError("window_size must be positive")
    if step is None:
        step = window_size
    if step.total_seconds() <= 0:
        raise ValueError("step must be positive")

    result = WindowResult()
    timestamped: List[LogRecord] = []

    for record in records:
        result.total_records += 1
        if record.timestamp is None:
            result.skipped_no_timestamp += 1
        else:
            timestamped.append(record)

    if not timestamped:
        return result

    timestamped.sort(key=lambda r: r.timestamp)  # type: ignore[arg-type]
    first_ts: datetime = timestamped[0].timestamp  # type: ignore[assignment]
    last_ts: datetime = timestamped[-1].timestamp  # type: ignore[assignment]

    window_start = first_ts
    while window_start <= last_ts:
        window_end = window_start + window_size
        summary = WindowSummary(window_start=window_start, window_end=window_end)
        for r in timestamped:
            if window_start <= r.timestamp < window_end:  # type: ignore[operator]
                summary.records.append(r)
        result.windows.append(summary)
        window_start += step

    return result
