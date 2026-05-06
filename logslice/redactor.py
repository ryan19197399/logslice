"""Redact sensitive patterns from log record messages."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, Iterator, List, Optional, Tuple

from logslice.parser import LogRecord

# Common built-in patterns (name -> raw regex)
BUILTIN_PATTERNS: dict[str, str] = {
    "ipv4": r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
    "email": r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    "jwt": r"eyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+",
    "credit_card": r"\b(?:\d[ \-]?){13,16}\b",
}

PLACEHOLDER = "[REDACTED]"


@dataclass
class RedactionRule:
    name: str
    pattern: str
    replacement: str = PLACEHOLDER
    _compiled: re.Pattern = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._compiled = re.compile(self.pattern)

    def apply(self, text: str) -> Tuple[str, int]:
        """Return (redacted_text, match_count)."""
        result, count = self._compiled.subn(self.replacement, text)
        return result, count


@dataclass
class RedactionResult:
    records: List[LogRecord]
    total_redactions: int
    affected_records: int


def _build_rules(
    builtin_names: Optional[Iterable[str]] = None,
    custom_patterns: Optional[Iterable[Tuple[str, str]]] = None,
) -> List[RedactionRule]:
    rules: List[RedactionRule] = []
    for name in builtin_names or []:
        if name not in BUILTIN_PATTERNS:
            raise ValueError(f"Unknown built-in redaction pattern: {name!r}")
        rules.append(RedactionRule(name=name, pattern=BUILTIN_PATTERNS[name]))
    for name, pattern in custom_patterns or []:
        rules.append(RedactionRule(name=name, pattern=pattern))
    return rules


def redact_records(
    records: Iterable[LogRecord],
    builtin_names: Optional[Iterable[str]] = None,
    custom_patterns: Optional[Iterable[Tuple[str, str]]] = None,
) -> RedactionResult:
    """Apply redaction rules to each record's message in-place (new record objects)."""
    rules = _build_rules(builtin_names, custom_patterns)
    if not rules:
        items = list(records)
        return RedactionResult(records=items, total_redactions=0, affected_records=0)

    out: List[LogRecord] = []
    total_redactions = 0
    affected_records = 0

    for record in records:
        message = record.message or ""
        record_count = 0
        for rule in rules:
            message, n = rule.apply(message)
            record_count += n
        if record_count:
            record = LogRecord(
                timestamp=record.timestamp,
                level=record.level,
                message=message,
                raw=record.raw,
            )
            total_redactions += record_count
            affected_records += 1
        out.append(record)

    return RedactionResult(
        records=out,
        total_redactions=total_redactions,
        affected_records=affected_records,
    )
