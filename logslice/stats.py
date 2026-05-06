"""Statistics aggregation for parsed log records."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, Optional

from logslice.parser import LogRecord


@dataclass
class LogStats:
    """Aggregated statistics over a collection of log records."""

    total: int = 0
    by_level: Counter = field(default_factory=Counter)
    first_timestamp: Optional[datetime] = None
    last_timestamp: Optional[datetime] = None
    unparsed_count: int = 0

    @property
    def parsed_count(self) -> int:
        return self.total - self.unparsed_count

    @property
    def time_span_seconds(self) -> Optional[float]:
        if self.first_timestamp is None or self.last_timestamp is None:
            return None
        return (self.last_timestamp - self.first_timestamp).total_seconds()

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "parsed": self.parsed_count,
            "unparsed": self.unparsed_count,
            "by_level": dict(self.by_level),
            "first_timestamp": self.first_timestamp.isoformat() if self.first_timestamp else None,
            "last_timestamp": self.last_timestamp.isoformat() if self.last_timestamp else None,
            "time_span_seconds": self.time_span_seconds,
        }


def compute_stats(records: Iterable[LogRecord]) -> LogStats:
    """Compute statistics from an iterable of LogRecord objects."""
    stats = LogStats()

    for record in records:
        stats.total += 1

        if record.timestamp is None:
            stats.unparsed_count += 1
        else:
            if stats.first_timestamp is None or record.timestamp < stats.first_timestamp:
                stats.first_timestamp = record.timestamp
            if stats.last_timestamp is None or record.timestamp > stats.last_timestamp:
                stats.last_timestamp = record.timestamp

        level_key = (record.level or "UNKNOWN").upper()
        stats.by_level[level_key] += 1

    return stats
