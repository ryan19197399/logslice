"""Tests for logslice.labeler."""
from __future__ import annotations

import pytest
from datetime import datetime

from logslice.parser import LogRecord
from logslice.labeler import LabelRule, LabeledRecord, LabelingResult, label_records


def make_record(message: str, level: str = "INFO") -> LogRecord:
    return LogRecord(
        raw=message,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        level=level,
        message=message,
    )


class TestLabelRule:
    def test_empty_key_raises(self):
        with pytest.raises(ValueError, match="key"):
            LabelRule(key="", value="v", pattern="x")

    def test_empty_value_raises(self):
        with pytest.raises(ValueError, match="value"):
            LabelRule(key="k", value="", pattern="x")

    def test_empty_pattern_raises(self):
        with pytest.raises(ValueError, match="pattern"):
            LabelRule(key="k", value="v", pattern="")

    def test_matches_case_insensitive_by_default(self):
        rule = LabelRule(key="env", value="prod", pattern="PRODUCTION")
        assert rule.matches(make_record("production server started"))

    def test_no_match_returns_false(self):
        rule = LabelRule(key="env", value="prod", pattern="production")
        assert not rule.matches(make_record("staging server started"))

    def test_case_sensitive_mode(self):
        rule = LabelRule(key="env", value="prod", pattern="PRODUCTION", case_sensitive=True)
        assert not rule.matches(make_record("production server started"))
        assert rule.matches(make_record("PRODUCTION server started"))


class TestLabelRecords:
    def test_empty_input_returns_empty_result(self):
        result = label_records([], [])
        assert result.total == 0
        assert result.labeled_count == 0
        assert result.records == []

    def test_no_rules_no_labels(self):
        records = [make_record("hello world")]
        result = label_records(records, [])
        assert result.total == 1
        assert result.labeled_count == 0
        assert result.records[0].labels == {}

    def test_matching_rule_attaches_label(self):
        rule = LabelRule(key="env", value="prod", pattern="production")
        result = label_records([make_record("production deploy")], [rule])
        assert result.labeled_count == 1
        assert result.records[0].labels == {"env": "prod"}

    def test_multiple_rules_can_match(self):
        rules = [
            LabelRule(key="env", value="prod", pattern="production"),
            LabelRule(key="action", value="deploy", pattern="deploy"),
        ]
        result = label_records([make_record("production deploy")], rules)
        assert result.records[0].labels == {"env": "prod", "action": "deploy"}

    def test_default_labels_applied_to_all(self):
        result = label_records(
            [make_record("anything")],
            [],
            default_labels={"source": "syslog"},
        )
        assert result.records[0].labels["source"] == "syslog"

    def test_rule_label_overrides_default(self):
        rule = LabelRule(key="source", value="app", pattern="app")
        result = label_records(
            [make_record("app started")],
            [rule],
            default_labels={"source": "syslog"},
        )
        assert result.records[0].labels["source"] == "app"

    def test_to_dict_structure(self):
        result = label_records([make_record("x")], [])
        d = result.to_dict()
        assert set(d.keys()) == {"total", "labeled_count", "unlabeled_count"}
        assert d["total"] == 1
        assert d["unlabeled_count"] == 1

    def test_labeled_record_to_dict(self):
        rule = LabelRule(key="k", value="v", pattern="msg")
        result = label_records([make_record("msg here")], [rule])
        d = result.records[0].to_dict()
        assert d["labels"] == {"k": "v"}
        assert d["message"] == "msg here"
