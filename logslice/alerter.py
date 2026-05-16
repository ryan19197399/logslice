"""Alert rule evaluation: flag records that match threshold conditions."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, List, Optional

from logslice.parser import LogRecord


@dataclass
class AlertRule:
    name: str
    pattern: str
    level_filter: Optional[str] = None  # e.g. "ERROR", case-insensitive
    case_sensitive: bool = False

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("AlertRule name must not be empty")
        if not self.pattern.strip():
            raise ValueError("AlertRule pattern must not be empty")

    @property
    def regex(self) -> re.Pattern:
        flags = 0 if self.case_sensitive else re.IGNORECASE
        return re.compile(self.pattern, flags)

    def matches(self, record: LogRecord) -> bool:
        if self.level_filter and (record.level or "").upper() != self.level_filter.upper():
            return False
        return bool(self.regex.search(record.message))


@dataclass
class AlertResult:
    rules: List[AlertRule]
    triggered: List[tuple]  # (rule_name, record)
    total_evaluated: int

    @property
    def triggered_count(self) -> int:
        return len(self.triggered)

    @property
    def alert_names(self) -> List[str]:
        return sorted({rule_name for rule_name, _ in self.triggered})

    def to_dict(self) -> dict:
        return {
            "total_evaluated": self.total_evaluated,
            "triggered_count": self.triggered_count,
            "alert_names": self.alert_names,
            "triggered": [
                {"rule": rule_name, "message": record.message}
                for rule_name, record in self.triggered
            ],
        }


def evaluate_alerts(
    records: Iterable[LogRecord],
    rules: List[AlertRule],
) -> AlertResult:
    """Evaluate all records against all alert rules."""
    triggered: List[tuple] = []
    total = 0
    for record in records:
        total += 1
        for rule in rules:
            if rule.matches(record):
                triggered.append((rule.name, record))
    return AlertResult(rules=rules, triggered=triggered, total_evaluated=total)
