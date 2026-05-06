"""Merge multiple sorted log record streams into a single ordered sequence."""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Iterable, Iterator, List, Tuple

from logslice.parser import LogRecord


@dataclass
class MergeResult:
    """Result of merging multiple record streams."""

    records: List[LogRecord] = field(default_factory=list)
    source_counts: dict = field(default_factory=dict)

    @property
    def total(self) -> int:
        return len(self.records)

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "source_counts": self.source_counts,
        }


def _sort_key(record: LogRecord):
    """Return a key suitable for heap ordering; unparsed records sort last."""
    if record.timestamp is None:
        return (1, 0)
    return (0, record.timestamp.timestamp())


def merge_record_streams(
    *streams: Iterable[LogRecord],
    source_names: List[str] | None = None,
) -> MergeResult:
    """Merge multiple iterables of LogRecord into one time-ordered list.

    Each stream is assumed to be individually sorted by timestamp.
    Uses a min-heap for an efficient k-way merge.

    Args:
        *streams: One or more iterables of LogRecord.
        source_names: Optional labels for each stream (used in source_counts).

    Returns:
        MergeResult with merged records and per-source counts.
    """
    if source_names is None:
        source_names = [str(i) for i in range(len(streams))]

    if len(source_names) != len(streams):
        raise ValueError(
            f"source_names length ({len(source_names)}) must match "
            f"number of streams ({len(streams)})"
        )

    source_counts: dict = {name: 0 for name in source_names}
    iterators = [iter(s) for s in streams]

    # heap entries: (sort_key, tie_breaker, source_index, record)
    heap: List[Tuple] = []
    for idx, it in enumerate(iterators):
        try:
            record = next(it)
            heapq.heappush(heap, (_sort_key(record), idx, idx, record))
        except StopIteration:
            pass

    merged: List[LogRecord] = []

    while heap:
        key, tie, src_idx, record = heapq.heappop(heap)
        merged.append(record)
        source_counts[source_names[src_idx]] += 1

        try:
            next_record = next(iterators[src_idx])
            heapq.heappush(
                heap,
                (_sort_key(next_record), len(merged), src_idx, next_record),
            )
        except StopIteration:
            pass

    return MergeResult(records=merged, source_counts=source_counts)
