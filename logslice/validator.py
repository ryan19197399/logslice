"""Validator: check log records against user-defined rules and report violations."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, List, Optional

from logslice.parser import LogRecord


@dataclass
class ValidationRule:
    """A rule that a log record's message must satisfy."""

    name: str
    pattern: str
    must_match: bool = True  # True → message must match; False → must NOT match
    case_sensitive: bool = False

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("ValidationRule.name must not be empty")
        if not self.pattern:
            raise ValueError("ValidationRule.pattern must not be empty")
        flags = 0 if self.case_sensitive else re.IGNORECASE
        self._regex: re.Pattern = re.compile(self.pattern, flags)

    def check(self, record: LogRecord) -> Optional[str]:
        """Return a violation message, or *None* if the record passes."""
        matched = bool(self._regex.search(record.message))
        if self.must_match and not matched:
            return f"Rule '{self.name}': expected pattern not found in message"
        if not self.must_match and matched:
            return f"Rule '{self.name}': forbidden pattern found in message"
        return None


@dataclass
class Violation:
    record: LogRecord
    rule_name: str
    reason: str


@dataclass
class ValidationResult:
    valid: List[LogRecord] = field(default_factory=list)
    violations: List[Violation] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.valid) + len(self.violations)

    @property
    def violation_count(self) -> int:
        return len(self.violations)

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "valid_count": len(self.valid),
            "violation_count": self.violation_count,
        }


def validate_records(
    records: Iterable[LogRecord],
    rules: List[ValidationRule],
) -> ValidationResult:
    """Run *rules* over every record; collect passing records and violations."""
    result = ValidationResult()
    for record in records:
        record_violations: List[Violation] = []
        for rule in rules:
            reason = rule.check(record)
            if reason is not None:
                record_violations.append(Violation(record=record, rule_name=rule.name, reason=reason))
        if record_violations:
            result.violations.extend(record_violations)
        else:
            result.valid.append(record)
    return result
