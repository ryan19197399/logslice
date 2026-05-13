"""Record limiter: cap the number of records emitted from a stream."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Iterator, List

from logslice.parser import LogRecord


@dataclass
class LimitResult:
    """Outcome of a limit operation."""

    records: List[LogRecord] = field(default_factory=list)
    total_seen: int = 0
    limit: int = 0

    @property
    def total_kept(self) -> int:  # noqa: D401
        """Number of records kept (always <= limit)."""
        return len(self.records)

    @property
    def dropped(self) -> int:
        """Number of records dropped because the limit was reached."""
        return max(0, self.total_seen - self.total_kept)

    @property
    def limit_reached(self) -> bool:
        """True when the stream was cut short by the limit."""
        return self.total_seen > self.limit

    def to_dict(self) -> dict:
        return {
            "limit": self.limit,
            "total_seen": self.total_seen,
            "total_kept": self.total_kept,
            "dropped": self.dropped,
            "limit_reached": self.limit_reached,
        }


def _iter_limited(records: Iterable[LogRecord], limit: int) -> Iterator[LogRecord]:
    """Yield up to *limit* records, then stop."""
    if limit <= 0:
        return
    emitted = 0
    for record in records:
        if emitted >= limit:
            break
        yield record
        emitted += 1


def limit_records(
    records: Iterable[LogRecord],
    limit: int,
    *,
    count_all: bool = False,
) -> LimitResult:
    """Return at most *limit* records from *records*.

    Parameters
    ----------
    records:
        Source iterable of :class:`LogRecord` objects.
    limit:
        Maximum number of records to keep.  Must be >= 0.
    count_all:
        When *True* the full source is consumed so that ``total_seen``
        reflects the real stream length.  When *False* (default) iteration
        stops as soon as *limit* is reached, which is more efficient but
        leaves ``total_seen`` equal to ``total_kept``.
    """
    if limit < 0:
        raise ValueError(f"limit must be >= 0, got {limit}")

    kept: List[LogRecord] = []
    total_seen = 0

    for record in records:
        total_seen += 1
        if len(kept) < limit:
            kept.append(record)
        elif not count_all:
            # Fast path: stop early, total_seen will be approximate.
            total_seen = total_seen  # already incremented
            # Drain remaining count if caller asked for accuracy
            break

    if count_all and len(kept) == limit:
        # We already consumed the whole iterator above.
        pass

    return LimitResult(records=kept, total_seen=total_seen, limit=limit)
