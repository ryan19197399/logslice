"""Tag log records by applying pattern-based rules to their messages."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, Iterator, List, Optional

from logslice.parser import LogRecord


@dataclass
class TagRule:
    """A single pattern-to-tag mapping."""

    pattern: str
    tag: str
    case_sensitive: bool = False

    def __post_init__(self) -> None:
        if not self.tag or not self.tag.strip():
            raise ValueError("tag must be a non-empty string")
        if not self.pattern:
            raise ValueError("pattern must be a non-empty string")

    @property
    def regex(self) -> re.Pattern[str]:
        flags = 0 if self.case_sensitive else re.IGNORECASE
        return re.compile(self.pattern, flags)

    def matches(self, record: LogRecord) -> bool:
        return bool(self.regex.search(record.message or ""))


@dataclass
class TaggingResult:
    """Outcome of a tagging pass over a sequence of records."""

    records: List[LogRecord] = field(default_factory=list)
    tag_counts: dict[str, int] = field(default_factory=dict)

    @property
    def total(self) -> int:
        return len(self.records)

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "tag_counts": dict(self.tag_counts),
        }


def _apply_rules(record: LogRecord, rules: List[TagRule]) -> LogRecord:
    """Return a copy of *record* with matched tags merged into its extras."""
    matched_tags: List[str] = [rule.tag for rule in rules if rule.matches(record)]
    if not matched_tags:
        return record
    existing: List[str] = list((record.extras or {}).get("tags", []))
    merged = existing + [t for t in matched_tags if t not in existing]
    new_extras = dict(record.extras or {})
    new_extras["tags"] = merged
    return LogRecord(
        raw=record.raw,
        timestamp=record.timestamp,
        level=record.level,
        message=record.message,
        extras=new_extras,
    )


def tag_records(
    records: Iterable[LogRecord],
    rules: List[TagRule],
) -> TaggingResult:
    """Apply *rules* to each record and collect tagging statistics."""
    result = TaggingResult()
    for record in records:
        tagged = _apply_rules(record, rules)
        result.records.append(tagged)
        for tag in (tagged.extras or {}).get("tags", []):
            result.tag_counts[tag] = result.tag_counts.get(tag, 0) + 1
    return result
