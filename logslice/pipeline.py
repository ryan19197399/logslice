"""End-to-end processing pipeline for log records."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional

from logslice.annotator import AnnotationRule, annotate_records
from logslice.deduplicator import deduplicate
from logslice.filter import LogFilter, filter_records
from logslice.parser import LogRecord
from logslice.redactor import RedactionRule, redact_records
from logslice.sampler import sample_records
from logslice.sorter import sort_records
from logslice.truncator import truncate_records


@dataclass
class PipelineConfig:
    """Configuration controlling which pipeline stages are active."""

    log_filter: Optional[LogFilter] = None
    deduplicate: bool = False
    sort_by: Optional[str] = None          # "timestamp" | "level"
    sort_ascending: bool = True
    sample_nth: Optional[int] = None
    sample_fraction: Optional[float] = None
    truncate_length: Optional[int] = None
    redaction_rules: List[RedactionRule] = field(default_factory=list)
    annotation_rules: List[AnnotationRule] = field(default_factory=list)


@dataclass
class PipelineResult:
    """Holds the records produced by the pipeline plus basic metadata."""

    records: List[LogRecord] = field(default_factory=list)
    stages_applied: List[str] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.records)


def run_pipeline(
    records: Iterable[LogRecord],
    config: PipelineConfig,
) -> PipelineResult:
    """Run *records* through the stages described by *config*.

    Stages are applied in a fixed, deterministic order:
    filter -> deduplicate -> redact -> annotate -> sort -> sample -> truncate.
    """
    result = PipelineResult()
    current: Iterable[LogRecord] = records

    if config.log_filter is not None:
        current = filter_records(current, config.log_filter)
        result.stages_applied.append("filter")

    if config.deduplicate:
        dedup = deduplicate(current)
        current = iter(dedup.records)
        result.stages_applied.append("deduplicate")

    if config.redaction_rules:
        redacted = redact_records(list(current), config.redaction_rules)
        current = iter(redacted.records)
        result.stages_applied.append("redact")

    if config.annotation_rules:
        annotated = annotate_records(list(current), config.annotation_rules)
        current = iter(annotated.records)
        result.stages_applied.append("annotate")

    if config.sort_by is not None:
        sorted_result = sort_records(
            list(current),
            by=config.sort_by,
            ascending=config.sort_ascending,
        )
        current = iter(sorted_result)
        result.stages_applied.append("sort")

    if config.sample_nth is not None or config.sample_fraction is not None:
        sampled = sample_records(
            list(current),
            nth=config.sample_nth,
            fraction=config.sample_fraction,
        )
        current = iter(sampled.records)
        result.stages_applied.append("sample")

    if config.truncate_length is not None:
        truncated = truncate_records(list(current), max_length=config.truncate_length)
        current = iter(truncated.records)
        result.stages_applied.append("truncate")

    result.records = list(current)
    return result
