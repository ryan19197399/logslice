"""Tests for logslice.scorer."""
from __future__ import annotations

import pytest
from datetime import datetime

from logslice.parser import LogRecord
from logslice.scorer import (
    ScoringRule,
    ScoredRecord,
    ScoringResult,
    score_records,
)


def make_record(message: str, level: str = "INFO") -> LogRecord:
    return LogRecord(
        raw=message,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        level=level,
        message=message,
    )


class TestScoringRule:
    def test_empty_pattern_raises(self):
        with pytest.raises(ValueError, match="pattern"):
            ScoringRule(pattern="")

    def test_non_positive_weight_raises(self):
        with pytest.raises(ValueError, match="weight"):
            ScoringRule(pattern="error", weight=0)

    def test_negative_weight_raises(self):
        with pytest.raises(ValueError, match="weight"):
            ScoringRule(pattern="error", weight=-1.0)

    def test_match_returns_weight(self):
        rule = ScoringRule(pattern="error", weight=3.0)
        assert rule.score("an error occurred") == 3.0

    def test_no_match_returns_zero(self):
        rule = ScoringRule(pattern="critical", weight=5.0)
        assert rule.score("everything is fine") == 0.0

    def test_case_insensitive_by_default(self):
        rule = ScoringRule(pattern="ERROR")
        assert rule.score("an error occurred") == 1.0

    def test_case_sensitive_no_match(self):
        rule = ScoringRule(pattern="ERROR", case_sensitive=True)
        assert rule.score("an error occurred") == 0.0

    def test_case_sensitive_match(self):
        rule = ScoringRule(pattern="ERROR", case_sensitive=True)
        assert rule.score("an ERROR occurred") == 1.0


class TestScoreRecords:
    def test_empty_records_returns_empty_result(self):
        result = score_records([], rules=[ScoringRule(pattern="error")])
        assert result.total == 0
        assert result.scored == []

    def test_no_rules_scores_all_zero(self):
        records = [make_record("something happened")]
        result = score_records(records, rules=[])
        assert result.total == 1
        assert result.scored[0].score == 0.0

    def test_single_rule_scores_match(self):
        records = [make_record("disk error detected")]
        result = score_records(records, rules=[ScoringRule(pattern="error", weight=2.0)])
        assert result.scored[0].score == 2.0

    def test_multiple_rules_accumulate_scores(self):
        records = [make_record("critical disk error")]
        rules = [ScoringRule(pattern="error", weight=2.0), ScoringRule(pattern="critical", weight=5.0)]
        result = score_records(records, rules=rules)
        assert result.scored[0].score == 7.0

    def test_threshold_excludes_low_scores(self):
        records = [make_record("minor warning"), make_record("critical error")]
        rules = [ScoringRule(pattern="critical", weight=10.0)]
        result = score_records(records, rules=rules, threshold=5.0)
        assert result.total == 1
        assert result.scored[0].record.message == "critical error"

    def test_threshold_none_keeps_all(self):
        records = [make_record("info message"), make_record("error message")]
        rules = [ScoringRule(pattern="error", weight=1.0)]
        result = score_records(records, rules=rules, threshold=None)
        assert result.total == 2

    def test_top_n_returns_highest_scored(self):
        records = [
            make_record("critical error disk"),
            make_record("minor info"),
            make_record("error found"),
        ]
        rules = [ScoringRule(pattern="error", weight=2.0), ScoringRule(pattern="critical", weight=5.0)]
        result = score_records(records, rules=rules)
        top = result.top_n(1)
        assert len(top) == 1
        assert top[0].record.message == "critical error disk"

    def test_above_threshold_filters_correctly(self):
        records = [make_record("error"), make_record("info"), make_record("critical error")]
        rules = [ScoringRule(pattern="error", weight=2.0), ScoringRule(pattern="critical", weight=3.0)]
        result = score_records(records, rules=rules)
        above = result.above_threshold(4.0)
        assert len(above) == 1
        assert above[0].score == 5.0

    def test_to_dict_structure(self):
        records = [make_record("error occurred")]
        result = score_records(records, rules=[ScoringRule(pattern="error", weight=1.0)])
        d = result.to_dict()
        assert "total" in d
        assert "scored" in d
        assert d["total"] == 1
        assert d["scored"][0]["score"] == 1.0
