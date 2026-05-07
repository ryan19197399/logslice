"""Classify log records into named categories based on pattern rules."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

from logslice.parser import LogRecord


@dataclass
class ClassificationRule:
    """A named rule that assigns a category when a pattern matches the message."""

    name: str
    pattern: str
    case_sensitive: bool = False

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("ClassificationRule.name must not be empty")
        if not self.pattern:
            raise ValueError("ClassificationRule.pattern must not be empty")

    @property
    def regex(self) -> re.Pattern:
        flags = 0 if self.case_sensitive else re.IGNORECASE
        return re.compile(self.pattern, flags)

    def matches(self, record: LogRecord) -> bool:
        return bool(self.regex.search(record.message))


@dataclass
class ClassificationResult:
    """Outcome of classifying a sequence of records."""

    records: List[LogRecord] = field(default_factory=list)
    categories: Dict[str, List[LogRecord]] = field(default_factory=dict)
    unclassified: List[LogRecord] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.records)

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "unclassified": len(self.unclassified),
            "categories": {k: len(v) for k, v in self.categories.items()},
        }


def classify_records(
    records: Iterable[LogRecord],
    rules: List[ClassificationRule],
    *,
    multi_label: bool = False,
) -> ClassificationResult:
    """Classify *records* using *rules*.

    Parameters
    ----------
    records:
        Iterable of :class:`LogRecord` objects to classify.
    rules:
        Ordered list of :class:`ClassificationRule` objects.  When
        *multi_label* is ``False`` (default) only the **first** matching
        rule assigns a category; when ``True`` every matching rule does.
    multi_label:
        Allow a record to belong to more than one category.
    """
    result = ClassificationResult()
    for record in records:
        result.records.append(record)
        matched: List[str] = []
        for rule in rules:
            if rule.matches(record):
                matched.append(rule.name)
                if not multi_label:
                    break
        if matched:
            for cat in matched:
                result.categories.setdefault(cat, []).append(record)
        else:
            result.unclassified.append(record)
    return result
