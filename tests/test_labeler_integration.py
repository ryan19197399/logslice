"""Integration tests for labeler_integration."""
from __future__ import annotations

import pytest
from datetime import datetime

from logslice.parser import LogRecord
from logslice.labeler_integration import LabelerConfig, run_labeler


def make_record(message: str, level: str = "INFO") -> LogRecord:
    return LogRecord(
        raw=message,
        timestamp=datetime(2024, 6, 1, 8, 0, 0),
        level=level,
        message=message,
    )


class TestLabelerConfig:
    def test_missing_key_raises(self):
        cfg = LabelerConfig(rules=[{"value": "v", "pattern": "p"}])
        with pytest.raises(ValueError, match="key"):
            cfg.validate()

    def test_missing_value_raises(self):
        cfg = LabelerConfig(rules=[{"key": "k", "pattern": "p"}])
        with pytest.raises(ValueError, match="value"):
            cfg.validate()

    def test_missing_pattern_raises(self):
        cfg = LabelerConfig(rules=[{"key": "k", "value": "v"}])
        with pytest.raises(ValueError, match="pattern"):
            cfg.validate()

    def test_valid_config_passes(self):
        cfg = LabelerConfig(rules=[{"key": "k", "value": "v", "pattern": "p"}])
        cfg.validate()  # should not raise

    def test_empty_rules_passes(self):
        cfg = LabelerConfig(rules=[])
        cfg.validate()

    def test_build_rules_returns_label_rules(self):
        from logslice.labeler import LabelRule
        cfg = LabelerConfig(rules=[{"key": "env", "value": "prod", "pattern": "prod"}])
        rules = cfg.build_rules()
        assert len(rules) == 1
        assert isinstance(rules[0], LabelRule)


class TestRunLabeler:
    def test_end_to_end_labels_matching_records(self):
        cfg = LabelerConfig(
            rules=[{"key": "severity", "value": "high", "pattern": "critical"}]
        )
        records = [
            make_record("critical disk failure"),
            make_record("routine health check"),
        ]
        result = run_labeler(records, cfg)
        assert result.total == 2
        assert result.labeled_count == 1
        assert result.records[0].labels == {"severity": "high"}
        assert result.records[1].labels == {}

    def test_default_labels_propagated(self):
        cfg = LabelerConfig(rules=[], default_labels={"host": "web-01"})
        result = run_labeler([make_record("startup")], cfg)
        assert result.records[0].labels["host"] == "web-01"

    def test_invalid_config_raises_before_processing(self):
        cfg = LabelerConfig(rules=[{"key": "", "value": "v", "pattern": "p"}])
        with pytest.raises(ValueError):
            run_labeler([make_record("x")], cfg)
