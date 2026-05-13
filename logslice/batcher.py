"""Batch log records into fixed-size groups for bulk processing."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Iterator, List

from logslice.parser import LogRecord


@dataclass
class BatchResult:
    """Container returned by :func:`batch_records`."""

    batches: List[List[LogRecord]] = field(default_factory=list)
    total_records: int = 0
    batch_size: int = 0

    @property
    def batch_count(self) -> int:
        """Number of batches produced."""
        return len(self.batches)

    @property
    def last_batch_size(self) -> int:
        """Size of the final (possibly partial) batch."""
        if not self.batches:
            return 0
        return len(self.batches[-1])

    def to_dict(self) -> dict:
        return {
            "batch_count": self.batch_count,
            "batch_size": self.batch_size,
            "total_records": self.total_records,
            "last_batch_size": self.last_batch_size,
        }


def _iter_batches(
    records: Iterable[LogRecord], size: int
) -> Iterator[List[LogRecord]]:
    """Yield successive *size*-length lists from *records*."""
    batch: List[LogRecord] = []
    for record in records:
        batch.append(record)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def batch_records(
    records: Iterable[LogRecord],
    batch_size: int,
) -> BatchResult:
    """Split *records* into batches of at most *batch_size* entries.

    Parameters
    ----------
    records:
        Any iterable of :class:`~logslice.parser.LogRecord` objects.
    batch_size:
        Maximum number of records per batch.  Must be a positive integer.

    Returns
    -------
    BatchResult
        Aggregated result containing all batches and summary statistics.
    """
    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")

    result = BatchResult(batch_size=batch_size)
    for batch in _iter_batches(records, batch_size):
        result.batches.append(batch)
        result.total_records += len(batch)
    return result
