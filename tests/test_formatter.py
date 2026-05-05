"""Tests for logslice.formatter."""
from datetime import datetime, timezone

import pytest

from logslice.formatter import format_records, format_records_list, _record_to_text
from logslice.highlighter import COLOURS, make_highlighter
from logslice.parser import LogRecord


def make_record(
    message: str = "hello",
    level: str = "INFO",
    ts: str = "2024-01-15T10:00:00+00:00",
    raw: str = "",
) -> LogRecord:
    timestamp = datetime.fromisoformat(ts) if ts else None
    return LogRecord(
        timestamp=timestamp,
        level=level,
        message=message,
        raw=raw or f"{ts} [{level}] {message}",
    )


class TestRecordToText:
    def test_includes_timestamp(self):
        r = make_record()
        assert "2024-01-15" in _record_to_text(r)

    def test_includes_level(self):
        r = make_record(level="ERROR")
        assert "ERROR" in _record_to_text(r)

    def test_includes_message(self):
        r = make_record(message="disk full")
        assert "disk full" in _record_to_text(r)

    def test_no_timestamp_falls_back_to_raw(self):
        r = LogRecord(timestamp=None, level=None, message="", raw="raw line")
        assert _record_to_text(r) == "raw line"


class TestFormatRecords:
    def test_yields_one_line_per_record(self):
        records = [make_record(message=f"msg {i}") for i in range(3)]
        lines = list(format_records(records))
        assert len(lines) == 3

    def test_no_highlighter_plain_text(self):
        records = [make_record(message="error here")]
        line = list(format_records(records))[0]
        assert COLOURS["yellow"] not in line

    def test_with_highlighter_adds_colour(self):
        h = make_highlighter(["error"], colour="red")
        records = [make_record(message="an error occurred")]
        line = list(format_records(records, highlighter=h))[0]
        assert COLOURS["red"] in line

    def test_custom_template(self):
        records = [make_record(message="ping", level="DEBUG")]
        line = list(format_records(records, template="{level}: {message}"))[0]
        assert line == "DEBUG: ping"

    def test_template_missing_timestamp_is_empty_string(self):
        r = LogRecord(timestamp=None, level="WARN", message="low disk", raw="")
        line = list(format_records([r], template="{timestamp}|{level}|{message}"))[0]
        assert line.startswith("|WARN|")


class TestFormatRecordsList:
    def test_returns_list(self):
        records = [make_record()]
        result = format_records_list(records)
        assert isinstance(result, list)

    def test_patterns_applied(self):
        records = [make_record(message="critical failure")]
        result = format_records_list(records, patterns=["critical"], colour="magenta")
        assert COLOURS["magenta"] in result[0]

    def test_highlight_false_suppresses_colour(self):
        records = [make_record(message="error")]
        result = format_records_list(records, patterns=["error"], highlight=False)
        assert COLOURS["yellow"] not in result[0]
