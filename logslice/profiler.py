"""Profiler: measures processing time and throughput for log record pipelines."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Iterable, Iterator, List

from logslice.parser import LogRecord


@dataclass
class ProfileResult:
    """Holds timing and throughput statistics for a processed record stream."""

    total_records: int
    elapsed_seconds: float
    records_per_second: float
    records: List[LogRecord] = field(repr=False)

    def to_dict(self) -> dict:
        return {
            "total_records": self.total_records,
            "elapsed_seconds": round(self.elapsed_seconds, 6),
            "records_per_second": round(self.records_per_second, 2),
        }


def _iter_with_timing(source: Iterable[LogRecord]) -> Iterator[LogRecord]:
    """Yield records unchanged; used to allow lazy consumption by profile_records."""
    yield from source


def profile_records(
    records: Iterable[LogRecord],
    *,
    clock=time.perf_counter,
) -> ProfileResult:
    """Consume *records*, measure wall-clock time, and return a ProfileResult.

    Parameters
    ----------
    records:
        Any iterable of :class:`LogRecord` objects.
    clock:
        Callable returning the current time in seconds.  Defaults to
        ``time.perf_counter``; injectable for deterministic testing.
    """
    collected: List[LogRecord] = []
    start = clock()
    for record in records:
        collected.append(record)
    elapsed = clock() - start

    total = len(collected)
    rps = total / elapsed if elapsed > 0 else 0.0

    return ProfileResult(
        total_records=total,
        elapsed_seconds=elapsed,
        records_per_second=rps,
        records=collected,
    )
