"""Tests for logslice.alerter_integration."""
from __future__ import annotations

import pytest
from datetime import datetime

from logslice.alerter_integration import AlerterConfig, run_alerter
from logslice.parser import LogRecord


def make_record(message: str, level: str = "ERROR") -> LogRecord:
    return LogRecord(
        raw=message,
        timestamp=datetime(2024, 6, 1, 9, 0, 0),
        level=level,
        message=message,
    )


class TestAlerterConfig:
    def test_empty_rules_raises_on_validate(self):
        cfg = AlerterConfig(rules=[])
        with pytest.raises(ValueError, match="at least one rule"):
            cfg.validate()

    def test_missing_name_raises(self):
        cfg = AlerterConfig(rules=[{"pattern": "err"}])
        with pytest.raises(ValueError, match="name"):
            cfg.validate()

    def test_missing_pattern_raises(self):
        cfg = AlerterConfig(rules=[{"name": "r"}])
        with pytest.raises(ValueError, match="pattern"):
            cfg.validate()

    def test_build_rules_creates_alert_rules(self):
        cfg = AlerterConfig(rules=[{"name": "r", "pattern": "fail"}])
        cfg.validate()
        rules = cfg.build_rules()
        assert len(rules) == 1
        assert rules[0].name == "r"

    def test_level_filter_passed_through(self):
        cfg = AlerterConfig(
            rules=[{"name": "r", "pattern": "fail", "level_filter": "ERROR"}]
        )
        rules = cfg.build_rules()
        assert rules[0].level_filter == "ERROR"

    def test_run_alerter_returns_result(self):
        cfg = AlerterConfig(rules=[{"name": "r", "pattern": "crash"}])
        records = [make_record("system crash detected")]
        result = run_alerter(records, cfg)
        assert result.triggered_count == 1

    def test_run_alerter_no_match(self):
        cfg = AlerterConfig(rules=[{"name": "r", "pattern": "nuclear"}])
        records = [make_record("all systems nominal")]
        result = run_alerter(records, cfg)
        assert result.triggered_count == 0
        assert result.total_evaluated == 1
