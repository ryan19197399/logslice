"""Split a stream of log records into named buckets by field value or pattern."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

from logslice.parser import LogRecord


@dataclass
class SplitResult:
    """Holds the bucketed output from a split operation."""

    buckets: Dict[str, List[LogRecord]] = field(default_factory=dict)
    unmatched: List[LogRecord] = field(default_factory=list)

    @property
    def total(self) -> int:
        return sum(len(v) for v in self.buckets.values()) + len(self.unmatched)

    def bucket_names(self) -> List[str]:
        return list(self.buckets.keys())

    def to_dict(self) -> dict:
        return {
            "buckets": {k: len(v) for k, v in self.buckets.items()},
            "unmatched": len(self.unmatched),
            "total": self.total,
        }


def split_by_level(records: Iterable[LogRecord]) -> SplitResult:
    """Split records into buckets keyed by normalised log level."""
    result = SplitResult()
    for record in records:
        key = (record.level or "UNKNOWN").upper()
        result.buckets.setdefault(key, []).append(record)
    return result


def split_by_pattern(
    records: Iterable[LogRecord],
    patterns: Dict[str, str],
    *,
    case_sensitive: bool = False,
) -> SplitResult:
    """Split records into named buckets based on regex patterns matched against the message.

    Each record is placed in the *first* bucket whose pattern matches.  Records
    that match no pattern are collected in ``unmatched``.

    Args:
        records: Iterable of :class:`LogRecord` objects.
        patterns: Mapping of bucket name -> regex pattern string.
        case_sensitive: When *False* (default) patterns are compiled with
            ``re.IGNORECASE``.
    """
    flags = 0 if case_sensitive else re.IGNORECASE
    compiled = {name: re.compile(pat, flags) for name, pat in patterns.items()}

    result = SplitResult()
    for record in records:
        message = record.message or ""
        matched = False
        for name, regex in compiled.items():
            if regex.search(message):
                result.buckets.setdefault(name, []).append(record)
                matched = True
                break
        if not matched:
            result.unmatched.append(record)
    return result


def split_by_field(
    records: Iterable[LogRecord],
    field_name: str,
    default_bucket: str = "OTHER",
) -> SplitResult:
    """Split records by the value of an extra metadata field.

    The field is looked up in ``record.extra`` (a dict).  Records that lack
    the field are placed in *default_bucket*.
    """
    result = SplitResult()
    for record in records:
        extra: Optional[dict] = getattr(record, "extra", None) or {}
        key = str(extra.get(field_name, default_bucket))
        result.buckets.setdefault(key, []).append(record)
    return result
