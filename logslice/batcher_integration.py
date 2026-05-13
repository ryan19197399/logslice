"""Integration helper that wires :mod:`logslice.batcher` into a pipeline.

Typical usage::

    config = BatcherConfig(batch_size=100, level_filter="ERROR")
    config.validate()
    result = run_batcher(records, config)
    for batch in result.batches:
        process(batch)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional

from logslice.batcher import BatchResult, batch_records
from logslice.filter import LogFilter
from logslice.parser import LogRecord


@dataclass
class BatcherConfig:
    """Configuration for the batcher integration."""

    batch_size: int = 100
    level_filter: Optional[str] = None
    pattern: Optional[str] = None

    def validate(self) -> None:
        if self.batch_size < 1:
            raise ValueError("batch_size must be >= 1")

    def build_filter(self) -> LogFilter:
        return LogFilter(
            level=self.level_filter,
            pattern=self.pattern,
        )


def run_batcher(
    records: Iterable[LogRecord],
    config: BatcherConfig,
) -> BatchResult:
    """Filter *records* according to *config* then batch them.

    Parameters
    ----------
    records:
        Source records to process.
    config:
        :class:`BatcherConfig` controlling filtering and batch size.

    Returns
    -------
    BatchResult
        Batched output after optional filtering.
    """
    log_filter = config.build_filter()
    filtered: Iterable[LogRecord] = (
        log_filter.apply(records)
        if not log_filter.is_empty()
        else records
    )
    return batch_records(filtered, batch_size=config.batch_size)
