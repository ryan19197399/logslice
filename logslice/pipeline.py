"""High-level pipeline that chains filter → deduplicate → sort → truncate."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional

from logslice.deduplicator import DeduplicationResult, deduplicate
from logslice.filter import LogFilter, filter_records
from logslice.parser import LogRecord
from logslice.sorter import SortKey, SortOrder, SortResult, sort_records
from logslice.truncator import TruncationResult, truncate_records


@dataclass
class PipelineConfig:
    """Configuration for a single pipeline run."""

    log_filter: Optional[LogFilter] = None
    deduplicate: bool = False
    sort_key: Optional[SortKey] = None
    sort_order: SortOrder = "asc"
    max_message_length: Optional[int] = None
    ellipsis: str = "..."


@dataclass
class PipelineResult:
    """Aggregated outcome of running the pipeline."""

    records: List[LogRecord]
    filtered_count: int
    dedup_result: Optional[DeduplicationResult] = None
    sort_result: Optional[SortResult] = None
    truncation_result: Optional[TruncationResult] = None

    @property
    def count(self) -> int:
        return len(self.records)


def run_pipeline(
    records: Iterable[LogRecord],
    config: PipelineConfig,
) -> PipelineResult:
    """Run *records* through the configured pipeline stages.

    Stages (in order):
    1. **Filter** — apply time-range / pattern filter if configured.
    2. **Deduplicate** — drop exact duplicates if enabled.
    3. **Sort** — reorder by key/direction if a sort key is given.
    4. **Truncate** — shorten long messages if a max length is set.
    """
    items: List[LogRecord] = list(records)
    original_count = len(items)

    # Stage 1: filter
    if config.log_filter is not None:
        items = list(filter_records(items, config.log_filter))
    filtered_count = original_count - len(items)

    # Stage 2: deduplicate
    dedup_result: Optional[DeduplicationResult] = None
    if config.deduplicate:
        dedup_result = deduplicate(items)
        items = list(dedup_result)

    # Stage 3: sort
    sort_result: Optional[SortResult] = None
    if config.sort_key is not None:
        sort_result = sort_records(items, key=config.sort_key, order=config.sort_order)
        items = sort_result.records

    # Stage 4: truncate
    trunc_result: Optional[TruncationResult] = None
    if config.max_message_length is not None:
        trunc_result = truncate_records(
            items,
            max_length=config.max_message_length,
            ellipsis=config.ellipsis,
        )
        items = trunc_result.records

    return PipelineResult(
        records=items,
        filtered_count=filtered_count,
        dedup_result=dedup_result,
        sort_result=sort_result,
        truncation_result=trunc_result,
    )
