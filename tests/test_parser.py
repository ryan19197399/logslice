"""Tests for logslice.parser module."""

import pytest
from datetime import datetime
from logslice.parser import parse_line, parse_timestamp, LogRecord


class TestParseTimestamp:
    def test_iso8601_with_z(self):
        dt = parse_timestamp("2024-01-15T13:45:00Z")
        assert dt == datetime(2024, 1, 15, 13, 45, 0)

    def test_iso8601_with_microseconds(self):
        dt = parse_timestamp("2024-01-15T13:45:00.123456Z")
        assert dt == datetime(2024, 1, 15, 13, 45, 0, 123456)

    def test_space_separated(self):
        dt = parse_timestamp("2024-01-15 13:45:00")
        assert dt == datetime(2024, 1, 15, 13, 45, 0)

    def test_space_separated_with_ms(self):
        dt = parse_timestamp("2024-01-15 13:45:00.500")
        assert dt is not None
        assert dt.second == 0

    def test_invalid_returns_none(self):
        assert parse_timestamp("not-a-date") is None

    def test_empty_string_returns_none(self):
        assert parse_timestamp("") is None


class TestParseLine:
    def test_iso8601_log_line(self):
        line = "2024-01-15T13:45:00Z ERROR Database connection failed"
        record = parse_line(line)
        assert record.is_parsed
        assert record.level == "ERROR"
        assert record.message == "Database connection failed"
        assert record.timestamp == datetime(2024, 1, 15, 13, 45, 0)

    def test_bracketed_level_format(self):
        line = "2024-01-15 13:45:00 [WARN] Disk usage above 80%"
        record = parse_line(line)
        assert record.is_parsed
        assert record.level == "WARN"
        assert "Disk usage" in record.message

    def test_unrecognized_line_returns_raw(self):
        line = "some random text without timestamp"
        record = parse_line(line)
        assert not record.is_parsed
        assert record.raw == line
        assert record.message == line
        assert record.level is None

    def test_raw_preserved(self):
        line = "2024-01-15T13:45:00Z INFO Starting up"
        record = parse_line(line)
        assert record.raw == line

    def test_newline_stripped_from_raw(self):
        line = "2024-01-15T13:45:00Z INFO msg\n"
        record = parse_line(line)
        assert not record.raw.endswith("\n")

    def test_custom_pattern(self):
        pattern = r"(?P<timestamp>\d{4}/\d{2}/\d{2})\s+(?P<level>\w+)\s+(?P<message>.*)"
        line = "2024/01/15 DEBUG custom format line"
        record = parse_line(line, patterns=[pattern])
        assert record.level == "DEBUG"
        assert record.message == "custom format line"

    def test_log_record_is_parsed_false_by_default(self):
        record = LogRecord(raw="bare line")
        assert not record.is_parsed
