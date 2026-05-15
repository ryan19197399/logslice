"""Integration helpers: build and run a labeling pass from a config dict."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

from logslice.parser import LogRecord
from logslice.labeler import LabelRule, LabelingResult, label_records


@dataclass
class LabelerConfig:
    rules: List[Dict] = field(default_factory=list)
    default_labels: Optional[Dict[str, str]] = None

    def validate(self) -> None:
        if not isinstance(self.rules, list):
            raise TypeError("rules must be a list")
        for idx, r in enumerate(self.rules):
            for attr in ("key", "value", "pattern"):
                if not r.get(attr):
                    raise ValueError(f"Rule[{idx}] missing required field '{attr}'")

    def build_rules(self) -> List[LabelRule]:
        return [
            LabelRule(
                key=r["key"],
                value=r["value"],
                pattern=r["pattern"],
                case_sensitive=r.get("case_sensitive", False),
            )
            for r in self.rules
        ]


def run_labeler(
    records: Iterable[LogRecord],
    config: LabelerConfig,
) -> LabelingResult:
    """Validate config, build rules, and label all records."""
    config.validate()
    rules = config.build_rules()
    return label_records(records, rules, default_labels=config.default_labels)
