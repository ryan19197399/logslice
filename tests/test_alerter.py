"""Tests for logslice.alerter."""
from __future__ import annotations

import pytest
from datetime import datetime

from logslice.alerter import AlertRule, AlertResult, evaluate_alerts
from logslice.parser import LogRecord


def make_record(message: str, level: str = "INFO") -> LogRecord:
    return LogRecord(
        raw=message,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        level=level,
        message=message,
    )


class TestAlertRule:
    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="name"):
            AlertRule(name="", pattern="error")

    def test_empty_pattern_raises(self):
        with pytest.raises(ValueError, match="pattern"):
            AlertRule(name="my-alert", pattern="")

    def test_matches_case_insensitive_by_default(self):
        rule = AlertRule(name="r", pattern="TIMEOUT")
        assert rule.matches(make_record("connection timeout occurred"))

    def test_no_match_returns_false(self):
        rule = AlertRule(name="r", pattern="critical")
        assert not rule.matches(make_record("everything is fine"))

    def test_level_filter_excludes_wrong_level(self):
        rule = AlertRule(name="r", pattern="fail", level_filter="ERROR")
        assert not rule.matches(make_record("fail", level="INFO"))

    def test_level_filter_includes_correct_level(self):
        rule = AlertRule(name="r", pattern="fail", level_filter="ERROR")
        assert rule.matches(make_record("fail", level="ERROR"))

    def test_case_sensitive_no_match(self):
        rule = AlertRule(name="r", pattern="TIMEOUT", case_sensitive=True)
        assert not rule.matches(make_record("timeout"))

    def test_case_sensitive_match(self):
        rule = AlertRule(name="r", pattern="TIMEOUT", case_sensitive=True)
        assert rule.matches(make_record("TIMEOUT error"))


class TestEvaluateAlerts:
    def test_empty_records_returns_zero(self):
        rules = [AlertRule(name="r", pattern="error")]
        result = evaluate_alerts([], rules)
        assert result.total_evaluated == 0
        assert result.triggered_count == 0

    def test_no_matching_records(self):
        rules = [AlertRule(name="r", pattern="critical")]
        records = [make_record("all good"), make_record("info only")]
        result = evaluate_alerts(records, rules)
        assert result.triggered_count == 0
        assert result.total_evaluated == 2

    def test_matching_record_triggers_alert(self):
        rules = [AlertRule(name="disk-full", pattern="disk full")]
        records = [make_record("disk full error"), make_record("ok")]
        result = evaluate_alerts(records, rules)
        assert result.triggered_count == 1
        assert result.alert_names == ["disk-full"]

    def test_multiple_rules_multiple_triggers(self):
        rules = [
            AlertRule(name="a1", pattern="timeout"),
            AlertRule(name="a2", pattern="refused"),
        ]
        records = [
            make_record("connection timeout"),
            make_record("connection refused"),
            make_record("all ok"),
        ]
        result = evaluate_alerts(records, rules)
        assert result.triggered_count == 2
        assert set(result.alert_names) == {"a1", "a2"}

    def test_to_dict_keys(self):
        rules = [AlertRule(name="r", pattern="err")]
        result = evaluate_alerts([make_record("err")], rules)
        d = result.to_dict()
        assert "total_evaluated" in d
        assert "triggered_count" in d
        assert "alert_names" in d
        assert "triggered" in d

    def test_triggered_entry_has_rule_and_message(self):
        rules = [AlertRule(name="my-rule", pattern="boom")]
        result = evaluate_alerts([make_record("boom!")], rules)
        entry = result.to_dict()["triggered"][0]
        assert entry["rule"] == "my-rule"
        assert "boom" in entry["message"]
