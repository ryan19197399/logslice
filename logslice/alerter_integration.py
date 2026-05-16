"""Integration config and runner for the alerter module."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional

from logslice.alerter import AlertRule, AlertResult, evaluate_alerts
from logslice.parser import LogRecord


@dataclass
class AlerterConfig:
    rules: List[dict] = field(default_factory=list)  # list of rule dicts

    def validate(self) -> None:
        if not self.rules:
            raise ValueError("AlerterConfig must contain at least one rule")
        for i, r in enumerate(self.rules):
            if "name" not in r:
                raise ValueError(f"Rule at index {i} missing 'name'")
            if "pattern" not in r:
                raise ValueError(f"Rule at index {i} missing 'pattern'")

    def build_rules(self) -> List[AlertRule]:
        return [
            AlertRule(
                name=r["name"],
                pattern=r["pattern"],
                level_filter=r.get("level_filter"),
                case_sensitive=r.get("case_sensitive", False),
            )
            for r in self.rules
        ]


def run_alerter(
    records: Iterable[LogRecord],
    config: AlerterConfig,
) -> AlertResult:
    """Validate config, build rules, and evaluate records."""
    config.validate()
    rules = config.build_rules()
    return evaluate_alerts(records, rules)
