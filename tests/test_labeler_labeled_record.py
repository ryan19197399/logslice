"""Focused tests for LabeledRecord behaviour."""
from __future__ import annotations

from datetime import datetime

from logslice.parser import LogRecord
from logslice.labeler import LabeledRecord


def make_record(message: str = "test", level: str = "DEBUG") -> LogRecord:
    return LogRecord(
        raw=message,
        timestamp=datetime(2024, 3, 15, 10, 30, 0),
        level=level,
        message=message,
    )


class TestLabeledRecord:
    def test_default_labels_empty(self):
        lr = LabeledRecord(record=make_record())
        assert lr.labels == {}

    def test_to_dict_contains_timestamp_iso(self):
        lr = LabeledRecord(record=make_record("hello"), labels={"k": "v"})
        d = lr.to_dict()
        assert d["timestamp"] == "2024-03-15T10:30:00"

    def test_to_dict_none_timestamp(self):
        rec = LogRecord(raw="x", timestamp=None, level="INFO", message="x")
        lr = LabeledRecord(record=rec, labels={})
        assert lr.to_dict()["timestamp"] is None

    def test_to_dict_level_and_message(self):
        lr = LabeledRecord(record=make_record("boom", level="ERROR"), labels={"sev": "high"})
        d = lr.to_dict()
        assert d["level"] == "ERROR"
        assert d["message"] == "boom"

    def test_to_dict_labels_present(self):
        lr = LabeledRecord(record=make_record(), labels={"a": "1", "b": "2"})
        assert lr.to_dict()["labels"] == {"a": "1", "b": "2"}

    def test_multiple_labels_stored(self):
        labels = {"env": "prod", "team": "platform", "tier": "backend"}
        lr = LabeledRecord(record=make_record(), labels=labels)
        assert lr.labels == labels
