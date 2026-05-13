"""Relevance scorer — assigns a numeric score to each log record based on weighted pattern matches."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, Iterator, List, Optional, Tuple

from logslice.parser import LogRecord


@dataclass
class ScoringRule:
    pattern: str
    weight: float = 1.0
    case_sensitive: bool = False

    def __post_init__(self) -> None:
        if not self.pattern:
            raise ValueError("pattern must not be empty")
        if self.weight <= 0:
            raise ValueError("weight must be positive")

    @property
    def regex(self) -> re.Pattern:
        flags = 0 if self.case_sensitive else re.IGNORECASE
        return re.compile(self.pattern, flags)

    def score(self, text: str) -> float:
        """Return weight if pattern matches text, else 0.0."""
        return self.weight if self.regex.search(text) else 0.0


@dataclass
class ScoredRecord:
    record: LogRecord
    score: float

    def to_dict(self) -> dict:
        return {
            "score": self.score,
            "timestamp": self.record.timestamp.isoformat() if self.record.timestamp else None,
            "level": self.record.level,
            "message": self.record.message,
        }


@dataclass
class ScoringResult:
    scored: List[ScoredRecord] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.scored)

    def above_threshold(self, threshold: float) -> List[ScoredRecord]:
        return [s for s in self.scored if s.score >= threshold]

    def top_n(self, n: int) -> List[ScoredRecord]:
        return sorted(self.scored, key=lambda s: s.score, reverse=True)[:n]

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "scored": [s.to_dict() for s in self.scored],
        }


def score_records(
    records: Iterable[LogRecord],
    rules: List[ScoringRule],
    threshold: Optional[float] = None,
) -> ScoringResult:
    """Score each record against all rules; optionally drop records below threshold."""
    result = ScoringResult()
    for record in records:
        text = record.message or ""
        total_score = sum(rule.score(text) for rule in rules)
        if threshold is None or total_score >= threshold:
            result.scored.append(ScoredRecord(record=record, score=total_score))
    return result
