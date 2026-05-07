"""End-to-end pipeline that chains all processing stages."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, List, Optional

from logslice.parser import LogRecord
from logslice.filter import LogFilter, filter_records
from logslice.deduplicator import deduplicate
from logslice.normalizer import normalise_records
from logslice.redactor import RedactionRule, redact_records
from logslice.tagger import TagRule, tag_records
from logslice.sorter import sort_records
from logslice.truncator import truncate_records


@dataclass
class PipelineConfig:
    start: Optional[datetime] = None
    end: Optional[datetime] = None
    patterns: List[str] = field(default_factory=list)
    deduplicate: bool = False
    normalise_levels: bool = True
    redaction_rules: List[RedactionRule] = field(default_factory=list)
    tag_rules: List[TagRule] = field(default_factory=list)
    sort_by: str = "timestamp"
    ascending: bool = True
    max_message_length: Optional[int] = None


@dataclass
class PipelineResult:
    records: List[LogRecord] = field(default_factory=list)
    stats: dict = field(default_factory=dict)

    @property
    def count(self) -> int:
        return len(self.records)


def run_pipeline(
    records: Iterable[LogRecord],
    config: PipelineConfig,
) -> PipelineResult:
    """Run *records* through every configured processing stage."""
    items: List[LogRecord] = list(records)
    stats: dict = {"input_count": len(items)}

    # Normalise levels first so filters work on canonical values
    if config.normalise_levels:
        norm = normalise_records(items)
        items = norm.records
        stats["normalised"] = norm.changed_count

    # Filter by time range and patterns
    log_filter = LogFilter(
        start=config.start,
        end=config.end,
        patterns=config.patterns or None,
    )
    items = list(filter_records(items, log_filter))
    stats["after_filter"] = len(items)

    # Deduplication
    if config.deduplicate:
        dedup = deduplicate(items)
        items = dedup.records
        stats["duplicates_dropped"] = dedup.total_dropped

    # Redaction
    if config.redaction_rules:
        red = redact_records(items, config.redaction_rules)
        items = red.records
        stats["redacted"] = red.redacted_count

    # Tagging
    if config.tag_rules:
        tagged = tag_records(items, config.tag_rules)
        items = tagged.records
        stats["tag_counts"] = tagged.tag_counts

    # Truncation
    if config.max_message_length is not None:
        trunc = truncate_records(items, config.max_message_length)
        items = trunc.records
        stats["truncated"] = trunc.truncated_count

    # Sort
    sorted_result = sort_records(items, by=config.sort_by, ascending=config.ascending)
    items = list(sorted_result)

    stats["output_count"] = len(items)
    return PipelineResult(records=items, stats=stats)
