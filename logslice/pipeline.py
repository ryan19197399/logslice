"""Pipeline module: orchestrate the full logslice processing chain."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional

from logslice.annotator import AnnotationRule, annotate_records
from logslice.deduplicator import deduplicate
from logslice.filter import LogFilter, filter_records
from logslice.parser import LogRecord
from logslice.redactor import RedactionRule, redact_records
from logslice.sampler import SampleResult, sample_records
from logslice.sorter import SortResult, sort_records
from logslice.truncator import truncate_records


@dataclass
class PipelineConfig:
    """Configuration for a single pipeline run."""

    log_filter: Optional[LogFilter] = None
    deduplicate: bool = False
    sort_by: Optional[str] = None          # "timestamp" | "level"
    sort_ascending: bool = True
    sample_nth: Optional[int] = None
    sample_fraction: Optional[float] = None
    max_message_length: Optional[int] = None
    redaction_rules: List[RedactionRule] = field(default_factory=list)
    annotation_rules: List[AnnotationRule] = field(default_factory=list)


@dataclass
class PipelineResult:
    """Result returned after running the pipeline."""

    records: List[LogRecord] = field(default_factory=list)
    dropped_duplicates: int = 0
    dropped_samples: int = 0
    redacted_count: int = 0
    annotated_count: int = 0

    @property
    def count(self) -> int:
        return len(self.records)


def run_pipeline(
    records: Iterable[LogRecord],
    config: PipelineConfig,
) -> PipelineResult:
    """Run *records* through the processing stages defined in *config*."""
    result = PipelineResult()

    record_list: List[LogRecord] = list(records)

    # 1. Filter by time range / pattern
    if config.log_filter is not None:
        record_list = list(filter_records(record_list, config.log_filter))

    # 2. Deduplicate
    if config.deduplicate:
        dedup = deduplicate(record_list)
        record_list = list(dedup.records)
        result.dropped_duplicates = dedup.total_dropped

    # 3. Redact sensitive data
    if config.redaction_rules:
        redacted = redact_records(record_list, config.redaction_rules)
        record_list = list(redacted.records)
        result.redacted_count = redacted.redacted_count

    # 4. Annotate with tags
    if config.annotation_rules:
        annotated = annotate_records(record_list, config.annotation_rules)
        record_list = annotated.records
        result.annotated_count = annotated.annotated_count

    # 5. Truncate long messages
    if config.max_message_length is not None:
        trunc = truncate_records(record_list, config.max_message_length)
        record_list = list(trunc.records)

    # 6. Sort
    if config.sort_by is not None:
        sorted_result: SortResult = sort_records(
            record_list,
            by=config.sort_by,
            ascending=config.sort_ascending,
        )
        record_list = list(sorted_result)

    # 7. Sample
    if config.sample_nth is not None or config.sample_fraction is not None:
        sampled: SampleResult = sample_records(
            record_list,
            every_nth=config.sample_nth,
            fraction=config.sample_fraction,
        )
        result.dropped_samples = sampled.dropped
        record_list = list(sampled.records)

    result.records = record_list
    return result
