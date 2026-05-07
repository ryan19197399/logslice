"""Enricher: attach derived fields to log records."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, Iterable, List, Optional

from logslice.parser import LogRecord


@dataclass
class EnrichmentRule:
    """A single enrichment rule that adds a key/value pair to a record's extra dict."""

    key: str
    pattern: str
    value_group: int = 0  # regex group index; 0 means the full match
    default: Optional[str] = None
    case_sensitive: bool = False

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("EnrichmentRule.key must not be empty")
        if not self.pattern:
            raise ValueError("EnrichmentRule.pattern must not be empty")
        flags = 0 if self.case_sensitive else re.IGNORECASE
        self._regex: re.Pattern = re.compile(self.pattern, flags)

    def extract(self, text: str) -> Optional[str]:
        """Return the extracted value from *text*, or *default* if no match."""
        m = self._regex.search(text)
        if m is None:
            return self.default
        try:
            return m.group(self.value_group)
        except IndexError:
            return self.default


@dataclass
class EnrichmentResult:
    records: List[LogRecord]
    enriched_count: int = 0
    total: int = 0

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "enriched_count": self.enriched_count,
            "unchanged_count": self.total - self.enriched_count,
        }


def enrich_records(
    records: Iterable[LogRecord],
    rules: List[EnrichmentRule],
) -> EnrichmentResult:
    """Apply *rules* to each record, storing extracted values in ``record.extra``.

    A record is counted as *enriched* when at least one rule produces a
    non-``None`` value that is written into ``extra``.
    """
    out: List[LogRecord] = []
    enriched_count = 0
    total = 0

    for record in records:
        total += 1
        touched = False
        for rule in rules:
            value = rule.extract(record.message)
            if value is not None:
                record.extra[rule.key] = value
                touched = True
        if touched:
            enriched_count += 1
        out.append(record)

    return EnrichmentResult(records=out, enriched_count=enriched_count, total=total)
