"""Annotator module: attach extra metadata fields to log records."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, Iterator, List, Optional

from logslice.parser import LogRecord


@dataclass
class AnnotationRule:
    """A rule that adds a tag to records whose message matches a pattern."""

    tag: str
    pattern: str
    case_sensitive: bool = False

    def __post_init__(self) -> None:
        if not self.tag:
            raise ValueError("tag must not be empty")
        flags = 0 if self.case_sensitive else re.IGNORECASE
        self._regex = re.compile(self.pattern, flags)

    @property
    def regex(self) -> re.Pattern:  # type: ignore[type-arg]
        return self._regex

    def matches(self, record: LogRecord) -> bool:
        """Return True when the record message matches this rule."""
        return bool(self._regex.search(record.message or ""))


@dataclass
class AnnotationResult:
    """Container returned by :func:`annotate_records`."""

    records: List[LogRecord] = field(default_factory=list)
    annotated_count: int = 0

    @property
    def total(self) -> int:
        return len(self.records)


def annotate_records(
    records: Iterable[LogRecord],
    rules: List[AnnotationRule],
    tag_field: str = "tags",
) -> AnnotationResult:
    """Apply *rules* to each record and attach matching tags as extra metadata.

    Tags are stored in ``record.extra[tag_field]`` as a sorted list of strings.
    A record is counted as annotated when at least one rule matches.
    """
    result_records: List[LogRecord] = []
    annotated_count = 0

    for record in records:
        matched_tags = [
            rule.tag for rule in rules if rule.matches(record)
        ]
        if matched_tags:
            extra = dict(record.extra) if record.extra else {}
            existing: List[str] = list(extra.get(tag_field, []))
            merged = sorted(set(existing) | set(matched_tags))
            extra[tag_field] = merged
            record = LogRecord(
                timestamp=record.timestamp,
                level=record.level,
                message=record.message,
                raw=record.raw,
                extra=extra,
            )
            annotated_count += 1
        result_records.append(record)

    return AnnotationResult(records=result_records, annotated_count=annotated_count)
