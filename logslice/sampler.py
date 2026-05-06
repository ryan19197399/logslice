"""Record sampler: reduce log volume by keeping every Nth record or a
random fraction of records from a sequence."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Iterable, Iterator, List, Optional

from logslice.parser import LogRecord


@dataclass
class SampleResult:
    """Container returned by :func:`sample_records`."""

    records: List[LogRecord]
    total_seen: int = 0
    total_kept: int = 0

    @property
    def drop_rate(self) -> float:
        """Fraction of records that were dropped (0.0 – 1.0)."""
        if self.total_seen == 0:
            return 0.0
        return 1.0 - self.total_kept / self.total_seen


def _nth_sampler(
    records: Iterable[LogRecord], n: int
) -> Iterator[LogRecord]:
    """Yield every *n*-th record (1-based index)."""
    if n < 1:
        raise ValueError(f"n must be >= 1, got {n}")
    for idx, record in enumerate(records, start=1):
        if idx % n == 0:
            yield record


def _fraction_sampler(
    records: Iterable[LogRecord],
    fraction: float,
    seed: Optional[int] = None,
) -> Iterator[LogRecord]:
    """Yield each record with probability *fraction*."""
    if not 0.0 < fraction <= 1.0:
        raise ValueError(f"fraction must be in (0, 1], got {fraction}")
    rng = random.Random(seed)
    for record in records:
        if rng.random() < fraction:
            yield record


def sample_records(
    records: Iterable[LogRecord],
    *,
    every_nth: Optional[int] = None,
    fraction: Optional[float] = None,
    seed: Optional[int] = None,
) -> SampleResult:
    """Sample *records* and return a :class:`SampleResult`.

    Exactly one of *every_nth* or *fraction* must be supplied.

    Args:
        records: Source iterable of :class:`~logslice.parser.LogRecord`.
        every_nth: Keep every N-th record (e.g. ``10`` keeps 1 in 10).
        fraction: Keep each record with this probability (e.g. ``0.1``).
        seed: Optional RNG seed for reproducible fraction sampling.

    Returns:
        A :class:`SampleResult` with the sampled records and counters.
    """
    if (every_nth is None) == (fraction is None):
        raise ValueError("Specify exactly one of 'every_nth' or 'fraction'.")

    all_records: List[LogRecord] = list(records)
    total_seen = len(all_records)

    if every_nth is not None:
        kept = list(_nth_sampler(all_records, every_nth))
    else:
        kept = list(_fraction_sampler(all_records, fraction, seed=seed))  # type: ignore[arg-type]

    return SampleResult(records=kept, total_seen=total_seen, total_kept=len(kept))
