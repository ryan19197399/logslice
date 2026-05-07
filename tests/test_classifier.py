"""Tests for logslice.classifier."""
from __future__ import annotations

import pytest

from logslice.classifier import (
    ClassificationRule,
    ClassificationResult,
    classify_records,
)
from logslice.parser import LogRecord


def make_record(message: str, level: str = "INFO") -> LogRecord:
    return LogRecord(raw=message, timestamp=None, level=level, message=message)


# ---------------------------------------------------------------------------
# ClassificationRule
# ---------------------------------------------------------------------------

class TestClassificationRule:
    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="name"):
            ClassificationRule(name="", pattern="error")

    def test_empty_pattern_raises(self):
        with pytest.raises(ValueError, match="pattern"):
            ClassificationRule(name="errors", pattern="")

    def test_matches_case_insensitive_by_default(self):
        rule = ClassificationRule(name="errors", pattern="error")
        assert rule.matches(make_record("An ERROR occurred"))

    def test_no_match_returns_false(self):
        rule = ClassificationRule(name="errors", pattern="error")
        assert not rule.matches(make_record("All systems nominal"))

    def test_case_sensitive_mode(self):
        rule = ClassificationRule(name="errors", pattern="error", case_sensitive=True)
        assert not rule.matches(make_record("An ERROR occurred"))
        assert rule.matches(make_record("An error occurred"))


# ---------------------------------------------------------------------------
# classify_records — basic behaviour
# ---------------------------------------------------------------------------

class TestClassifyRecords:
    def _rules(self):
        return [
            ClassificationRule(name="errors", pattern=r"error"),
            ClassificationRule(name="timeouts", pattern=r"timeout"),
        ]

    def test_empty_input_returns_empty_result(self):
        result = classify_records([], self._rules())
        assert result.total == 0
        assert result.unclassified == []
        assert result.categories == {}

    def test_matching_record_placed_in_category(self):
        records = [make_record("connection error")]
        result = classify_records(records, self._rules())
        assert "errors" in result.categories
        assert len(result.categories["errors"]) == 1
        assert result.unclassified == []

    def test_unmatched_record_goes_to_unclassified(self):
        records = [make_record("all good")]
        result = classify_records(records, self._rules())
        assert len(result.unclassified) == 1
        assert result.categories == {}

    def test_first_rule_wins_by_default(self):
        records = [make_record("error timeout")]
        result = classify_records(records, self._rules())
        assert "errors" in result.categories
        assert "timeouts" not in result.categories

    def test_multi_label_assigns_all_matching_categories(self):
        records = [make_record("error timeout")]
        result = classify_records(records, self._rules(), multi_label=True)
        assert "errors" in result.categories
        assert "timeouts" in result.categories
        assert result.unclassified == []

    def test_total_counts_all_records(self):
        records = [make_record("error"), make_record("ok"), make_record("timeout")]
        result = classify_records(records, self._rules())
        assert result.total == 3

    def test_to_dict_structure(self):
        records = [make_record("error"), make_record("ok")]
        result = classify_records(records, self._rules())
        d = result.to_dict()
        assert d["total"] == 2
        assert d["unclassified"] == 1
        assert d["categories"]["errors"] == 1

    def test_no_rules_all_unclassified(self):
        records = [make_record("error"), make_record("info")]
        result = classify_records(records, [])
        assert len(result.unclassified) == 2
        assert result.categories == {}
