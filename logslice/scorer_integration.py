"""Integration helpers — run the scorer as a pipeline step with config-driven rules."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

from logslice.parser import LogRecord
from logslice.scorer import ScoringResult, ScoringRule, score_records


@dataclass
class ScorerConfig:
    """Declarative configuration for the scoring step."""
    rules: List[Dict] = field(default_factory=list)
    threshold: Optional[float] = None
    top_n: Optional[int] = None

    def build_rules(self) -> List[ScoringRule]:
        built = []
        for entry in self.rules:
            built.append(
                ScoringRule(
                    pattern=entry["pattern"],
                    weight=float(entry.get("weight", 1.0)),
                    case_sensitive=bool(entry.get("case_sensitive", False)),
                )
            )
        return built

    def validate(self) -> None:
        if not self.rules:
            raise ValueError("ScorerConfig must contain at least one rule")
        if self.threshold is not None and self.threshold < 0:
            raise ValueError("threshold must be non-negative")
        if self.top_n is not None and self.top_n < 1:
            raise ValueError("top_n must be at least 1")


def run_scorer(
    records: Iterable[LogRecord],
    config: ScorerConfig,
) -> ScoringResult:
    """Validate config, build rules, score records, apply top_n if requested."""
    config.validate()
    rules = config.build_rules()
    result = score_records(records, rules=rules, threshold=config.threshold)
    if config.top_n is not None:
        top = result.top_n(config.top_n)
        from logslice.scorer import ScoringResult as SR
        return SR(scored=top)
    return result
