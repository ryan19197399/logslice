"""Labeler: attach free-form key/value labels to log records based on pattern rules."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

from logslice.parser import LogRecord


@dataclass
class LabelRule:
    key: str
    value: str
    pattern: str
    case_sensitive: bool = False

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("LabelRule.key must not be empty")
        if not self.value:
            raise ValueError("LabelRule.value must not be empty")
        if not self.pattern:
            raise ValueError("LabelRule.pattern must not be empty")

    @property
    def regex(self) -> re.Pattern:
        flags = 0 if self.case_sensitive else re.IGNORECASE
        return re.compile(self.pattern, flags)

    def matches(self, record: LogRecord) -> bool:
        return bool(self.regex.search(record.message or ""))


@dataclass
class LabeledRecord:
    record: LogRecord
    labels: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "timestamp": record.timestamp.isoformat() if (record := self.record).timestamp else None,
            "level": record.level,
            "message": record.message,
            "labels": self.labels,
        }


@dataclass
class LabelingResult:
    records: List[LabeledRecord] = field(default_factory=list)
    total: int = 0
    labeled_count: int = 0

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "labeled_count": self.labeled_count,
            "unlabeled_count": self.total - self.labeled_count,
        }


def label_records(
    records: Iterable[LogRecord],
    rules: List[LabelRule],
    *,
    default_labels: Optional[Dict[str, str]] = None,
) -> LabelingResult:
    """Apply label rules to each record; a record may receive labels from multiple rules."""
    result = LabelingResult()
    for record in records:
        result.total += 1
        labels: Dict[str, str] = dict(default_labels or {})
        for rule in rules:
            if rule.matches(record):
                labels[rule.key] = rule.value
        labeled = LabeledRecord(record=record, labels=labels)
        if labels and labels != (default_labels or {}):
            result.labeled_count += 1
        result.records.append(labeled)
    return result
