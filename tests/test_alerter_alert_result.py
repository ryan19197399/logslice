"""Tests focused on AlertResult properties and to_dict."""
from __future__ import annotations

from datetime import datetime

from logslice.alerter import AlertRule, AlertResult, evaluate_alerts
from logslice.parser import LogRecord


def make_record(message: str, level: str = "WARN") -> LogRecord:
    return LogRecord(
        raw=message,
        timestamp=datetime(2024, 3, 15, 8, 30, 0),
        level=level,
        message=message,
    )


class TestAlertResult:
    def _make_result(self) -> AlertResult:
        rules = [
            AlertRule(name="oom", pattern="out of memory"),
            AlertRule(name="timeout", pattern="timed? ?out"),
        ]
        records = [
            make_record("out of memory error"),
            make_record("request timed out"),
            make_record("all fine"),
        ]
        return evaluate_alerts(records, rules)

    def test_triggered_count(self):
        result = self._make_result()
        assert result.triggered_count == 2

    def test_total_evaluated(self):
        result = self._make_result()
        assert result.total_evaluated == 3

    def test_alert_names_sorted(self):
        result = self._make_result()
        assert result.alert_names == ["oom", "timeout"]

    def test_to_dict_triggered_list_length(self):
        result = self._make_result()
        assert len(result.to_dict()["triggered"]) == 2

    def test_no_triggers_gives_empty_alert_names(self):
        rules = [AlertRule(name="r", pattern="never")]
        result = evaluate_alerts([make_record("fine")], rules)
        assert result.alert_names == []

    def test_same_rule_multiple_matches(self):
        rules = [AlertRule(name="err", pattern="error")]
        records = [
            make_record("error one"),
            make_record("error two"),
        ]
        result = evaluate_alerts(records, rules)
        assert result.triggered_count == 2
        assert result.alert_names == ["err"]
