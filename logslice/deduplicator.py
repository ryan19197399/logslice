"""Deduplication of log records based on message similarity or exact match."""

from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable, Iterator

from logslice.parser import LogRecord


def _record_key(record: LogRecord, ignore_timestamp: bool = True) -> str:
    """Return a stable hash key for a log record."""
    parts = [record.level or "", record.message or ""]
    if not ignore_timestamp and record.timestamp is not None:
        parts.insert(0, record.timestamp.isoformat())
    raw = "\x00".join(parts)
    return hashlib.md5(raw.encode(), usedforsecurity=False).hexdigest()


@dataclass
class DeduplicationResult:
    """Summary produced by a deduplication pass."""

    records: list[LogRecord]
    duplicate_counts: dict[str, int] = field(default_factory=dict)

    @property
    def total_dropped(self) -> int:
        return sum(self.duplicate_counts.values())

    @property
    def unique_count(self) -> int:
        return len(self.records)


def deduplicate(
    records: Iterable[LogRecord],
    ignore_timestamp: bool = True,
    keep: str = "first",
) -> DeduplicationResult:
    """Remove duplicate log records.

    Args:
        records: Iterable of LogRecord objects.
        ignore_timestamp: When True (default), timestamps are excluded from
            the equality key so records with identical level+message are
            treated as duplicates regardless of when they occurred.
        keep: ``'first'`` (default) keeps the earliest occurrence;
            ``'last'`` keeps the latest occurrence.

    Returns:
        A :class:`DeduplicationResult` containing the deduplicated records
        and a mapping of duplicate keys to the number of extras dropped.
    """
    if keep not in {"first", "last"}:
        raise ValueError(f"keep must be 'first' or 'last', got {keep!r}")

    seen: dict[str, LogRecord] = {}
    duplicate_counts: dict[str, int] = defaultdict(int)

    for record in records:
        key = _record_key(record, ignore_timestamp=ignore_timestamp)
        if key not in seen:
            seen[key] = record
        else:
            if keep == "last":
                seen[key] = record
            duplicate_counts[key] += 1

    return DeduplicationResult(
        records=list(seen.values()),
        duplicate_counts=dict(duplicate_counts),
    )


def iter_deduplicated(
    records: Iterable[LogRecord],
    ignore_timestamp: bool = True,
) -> Iterator[LogRecord]:
    """Streaming deduplication — yields each unique record once (first-seen)."""
    seen: set[str] = set()
    for record in records:
        key = _record_key(record, ignore_timestamp=ignore_timestamp)
        if key not in seen:
            seen.add(key)
            yield record
