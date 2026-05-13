"""logslice.masker — Mask sensitive fields in log messages using named rules."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, Iterator, List

from logslice.parser import LogRecord


@dataclass
class MaskRule:
    """A single masking rule: replaces pattern matches with a fixed placeholder."""

    name: str
    pattern: str
    placeholder: str = "***"
    case_sensitive: bool = False

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("MaskRule.name must not be empty")
        if not self.pattern:
            raise ValueError("MaskRule.pattern must not be empty")
        if not self.placeholder:
            raise ValueError("MaskRule.placeholder must not be empty")
        flags = 0 if self.case_sensitive else re.IGNORECASE
        self._regex = re.compile(self.pattern, flags)

    @property
    def regex(self) -> re.Pattern:  # type: ignore[type-arg]
        return self._regex

    def apply(self, text: str) -> str:
        """Return *text* with all matches replaced by the placeholder."""
        return self._regex.sub(self.placeholder, text)


@dataclass
class MaskingResult:
    """Outcome of a masking pass over a stream of records."""

    records: List[LogRecord] = field(default_factory=list)
    total: int = 0
    masked_count: int = 0

    def to_dict(self) -> dict:  # type: ignore[type-arg]
        return {
            "total": self.total,
            "masked_count": self.masked_count,
            "unchanged_count": self.total - self.masked_count,
        }


def _apply_rules(message: str, rules: List[MaskRule]) -> str:
    for rule in rules:
        message = rule.apply(message)
    return message


def mask_records(
    records: Iterable[LogRecord],
    rules: List[MaskRule],
) -> MaskingResult:
    """Apply *rules* to every record's message, returning a :class:`MaskingResult`."""
    result = MaskingResult()
    if not rules:
        result.records = list(records)
        result.total = len(result.records)
        return result

    out: List[LogRecord] = []
    for record in records:
        result.total += 1
        original = record.message
        masked = _apply_rules(original, rules)
        if masked != original:
            result.masked_count += 1
            record = LogRecord(
                timestamp=record.timestamp,
                level=record.level,
                message=masked,
                raw=record.raw,
            )
        out.append(record)
    result.records = out
    return result
