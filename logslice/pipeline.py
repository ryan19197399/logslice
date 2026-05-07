"""End-to-end processing pipeline for log records."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional

from logslice.annotator import AnnotationRule, annotate_records
from logslice.classifier import ClassificationResult, ClassificationRule, classify_records
from logslice.deduplicator import deduplicate
from logslice.filter import LogFilter
from logslice.normalizer import normalise_records
from logslice.parser import LogRecord
from logslice.redactor import RedactionRule, redact_records
from logslice.sampler import sample_records
from logslice.sorter import SortField, SortOrder, sort_records
from logslice.truncator import truncate_records


@dataclass
class PipelineConfig:
    """Configuration driving :func:`run_pipeline`."""

    log_filter: Optional[LogFilter] = None
    deduplicate: bool = False
    normalise_levels: bool = False
    redaction_rules: List[RedactionRule] = field(default_factory=list)
    annotation_rules: List[AnnotationRule] = field(default_factory=list)
    classification_rules: List[ClassificationRule] = field(default_factory=list)
    multi_label_classification: bool = False
    truncate_at: Optional[int] = None
    sample_nth: Optional[int] = None
    sample_fraction: Optional[float] = None
    sort_field: Optional[SortField] = None
    sort_order: SortOrder = SortOrder.ASC


@dataclass
class PipelineResult:
    """Aggregated output produced by :func:`run_pipeline`."""

    records: List[LogRecord] = field(default_factory=list)
    classification: Optional[ClassificationResult] = None

    @property
    def count(self) -> int:
        return len(self.records)


def run_pipeline(
    records: Iterable[LogRecord],
    config: PipelineConfig,
) -> PipelineResult:
    """Apply the processing steps described by *config* to *records*.

    Steps are applied in a fixed, deterministic order:
    filter → deduplicate → normalise → redact → annotate → truncate
    → sample → sort → classify.
    """
    items: List[LogRecord] = list(records)

    if config.log_filter and not config.log_filter.is_empty():
        items = list(config.log_filter.apply(items))

    if config.deduplicate:
        items = list(deduplicate(items).records)

    if config.normalise_levels:
        items = list(normalise_records(items).records)

    if config.redaction_rules:
        items = list(redact_records(items, config.redaction_rules).records)

    if config.annotation_rules:
        items = list(annotate_records(items, config.annotation_rules).records)

    if config.truncate_at is not None:
        items = list(truncate_records(items, config.truncate_at).records)

    if config.sample_nth is not None or config.sample_fraction is not None:
        items = list(
            sample_records(
                items,
                nth=config.sample_nth,
                fraction=config.sample_fraction,
            ).records
        )

    if config.sort_field is not None:
        items = list(
            sort_records(items, by=config.sort_field, order=config.sort_order).records
        )

    classification: Optional[ClassificationResult] = None
    if config.classification_rules:
        classification = classify_records(
            items,
            config.classification_rules,
            multi_label=config.multi_label_classification,
        )

    return PipelineResult(records=items, classification=classification)
