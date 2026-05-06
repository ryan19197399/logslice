"""Aggregates log records by time bucket (minute, hour, day) and level."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Literal, Tuple

from logslice.parser import LogRecord

Bucket = Literal["minute", "hour", "day"]


def _truncate(dt: datetime, bucket: Bucket) -> datetime:
    """Truncate a datetime to the start of the given bucket."""
    if bucket == "minute":
        return dt.replace(second=0, microsecond=0)
    if bucket == "hour":
        return dt.replace(minute=0, second=0, microsecond=0)
    # day
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


@dataclass
class BucketSummary:
    bucket_start: datetime
    total: int = 0
    by_level: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "bucket_start": self.bucket_start.isoformat(),
            "total": self.total,
            "by_level": dict(self.by_level),
        }


@dataclass
class AggregationResult:
    bucket: Bucket
    summaries: List[BucketSummary]

    @property
    def total_records(self) -> int:
        return sum(s.total for s in self.summaries)

    def to_dict(self) -> dict:
        return {
            "bucket": self.bucket,
            "total_records": self.total_records,
            "buckets": [s.to_dict() for s in self.summaries],
        }


def aggregate_records(
    records: Iterable[LogRecord],
    bucket: Bucket = "hour",
) -> AggregationResult:
    """Group records into time buckets and count totals and per-level tallies."""
    buckets: Dict[datetime, BucketSummary] = {}

    for record in records:
        if record.timestamp is None:
            key = datetime.min.replace(tzinfo=timezone.utc)
        else:
            key = _truncate(record.timestamp, bucket)

        if key not in buckets:
            buckets[key] = BucketSummary(bucket_start=key)

        summary = buckets[key]
        summary.total += 1
        level = record.level or "UNKNOWN"
        summary.by_level[level] = summary.by_level.get(level, 0) + 1

    sorted_summaries = [buckets[k] for k in sorted(buckets)]
    return AggregationResult(bucket=bucket, summaries=sorted_summaries)
