"""Tests for logslice.truncator."""
from __future__ import annotations

import pytest

from logslice.parser import LogRecord
from logslice.truncator import (
    TruncationResult,
    truncate_message,
    truncate_records,
)


def make_record(message: str | None = "hello") -> LogRecord:
    return LogRecord(timestamp=None, level="INFO", message=message, raw=message or "")


# ---------------------------------------------------------------------------
# truncate_message
# ---------------------------------------------------------------------------

class TestTruncateMessage:
    def test_short_message_unchanged(self):
        assert truncate_message("short", max_length=20) == "short"

    def test_exact_length_unchanged(self):
        msg = "a" * 20
        assert truncate_message(msg, max_length=20) == msg

    def test_long_message_truncated(self):
        msg = "a" * 50
        result = truncate_message(msg, max_length=10)
        assert len(result) == 10
        assert result.endswith("...")

    def test_truncated_prefix_preserved(self):
        msg = "Hello, world! This is a long message."
        result = truncate_message(msg, max_length=10)
        assert result == "Hello, ."
        # prefix length = max_length - len("...") = 7
        assert result == msg[:7] + "..."

    def test_default_max_length_applied(self):
        long_msg = "x" * 300
        result = truncate_message(long_msg)
        assert len(result) == 200
        assert result.endswith("...")

    def test_invalid_max_length_raises(self):
        with pytest.raises(ValueError, match="max_length"):
            truncate_message("hi", max_length=2)

    def test_empty_string_unchanged(self):
        assert truncate_message("", max_length=10) == ""


# ---------------------------------------------------------------------------
# truncate_records
# ---------------------------------------------------------------------------

class TestTruncateRecords:
    def test_empty_iterable_returns_empty_result(self):
        result = truncate_records([])
        assert isinstance(result, TruncationResult)
        assert result.records == []
        assert result.total_input == 0
        assert result.truncated_count == 0

    def test_no_truncation_needed(self):
        records = [make_record("short"), make_record("also short")]
        result = truncate_records(records, max_length=50)
        assert result.truncated_count == 0
        assert result.unchanged_count == 2
        assert result.records[0].message == "short"

    def test_truncation_applied_to_long_messages(self):
        long_msg = "y" * 300
        records = [make_record(long_msg)]
        result = truncate_records(records, max_length=20)
        assert result.truncated_count == 1
        assert len(result.records[0].message) == 20
        assert result.records[0].message.endswith("...")

    def test_original_record_fields_preserved(self):
        record = LogRecord(timestamp=None, level="ERROR", message="x" * 50, raw="raw")
        result = truncate_records([record], max_length=10)
        out = result.records[0]
        assert out.level == "ERROR"
        assert out.raw == "raw"
        assert out.timestamp is None

    def test_none_message_passes_through(self):
        record = make_record(None)
        result = truncate_records([record], max_length=10)
        assert result.truncated_count == 0
        assert result.records[0].message is None

    def test_mixed_records_counted_correctly(self):
        records = [
            make_record("short"),
            make_record("x" * 300),
            make_record("also short"),
            make_record("y" * 300),
        ]
        result = truncate_records(records, max_length=50)
        assert result.total_input == 4
        assert result.truncated_count == 2
        assert result.unchanged_count == 2
