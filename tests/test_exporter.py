"""Tests for logslice.exporter."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from io import StringIO

import pytest

from logslice.exporter import export, export_csv, export_json, export_text, export_to_string
from logslice.parser import LogRecord


DT = datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc)


def make_record(
    message: str = "hello world",
    level: str = "INFO",
    timestamp: datetime | None = DT,
    raw: str | None = None,
) -> LogRecord:
    raw_line = raw or f"2024-03-15T12:00:00Z [{level}] {message}\n"
    return LogRecord(timestamp=timestamp, level=level, message=message, raw=raw_line)


class TestExportJson:
    def test_produces_valid_json(self):
        records = [make_record("msg one"), make_record("msg two", level="ERROR")]
        result = export_to_string(records, fmt="json")
        data = json.loads(result)
        assert isinstance(data, list)
        assert len(data) == 2

    def test_timestamp_serialised_as_iso(self):
        result = export_to_string([make_record()], fmt="json")
        data = json.loads(result)
        assert data[0]["timestamp"] == DT.isoformat()

    def test_none_timestamp(self):
        record = make_record(timestamp=None)
        result = export_to_string([record], fmt="json")
        data = json.loads(result)
        assert data[0]["timestamp"] is None

    def test_fields_present(self):
        result = export_to_string([make_record()], fmt="json")
        data = json.loads(result)
        assert set(data[0].keys()) == {"timestamp", "level", "message", "raw"}


class TestExportCsv:
    def _parse(self, text: str) -> list[dict]:
        return list(csv.DictReader(StringIO(text)))

    def test_has_header(self):
        result = export_to_string([make_record()], fmt="csv")
        assert result.startswith("timestamp,level,message,raw")

    def test_row_count(self):
        records = [make_record(), make_record("second")]
        rows = self._parse(export_to_string(records, fmt="csv"))
        assert len(rows) == 2

    def test_level_column(self):
        rows = self._parse(export_to_string([make_record(level="WARN")], fmt="csv"))
        assert rows[0]["level"] == "WARN"


class TestExportText:
    def test_raw_lines_preserved(self):
        r = make_record(raw="raw line content\n")
        result = export_to_string([r], fmt="text")
        assert "raw line content" in result

    def test_newline_appended_if_missing(self):
        r = make_record(raw="no newline")
        result = export_to_string([r], fmt="text")
        assert result.endswith("\n")

    def test_multiple_records(self):
        records = [make_record(raw=f"line {i}\n") for i in range(3)]
        result = export_to_string(records, fmt="text")
        assert result.count("\n") == 3


class TestExportDispatch:
    def test_default_format_is_text(self):
        buf = StringIO()
        export([make_record(raw="hello\n")], output=buf)
        assert "hello" in buf.getvalue()

    def test_unknown_format_falls_back_to_text(self):
        buf = StringIO()
        export([make_record(raw="fallback\n")], fmt="text", output=buf)
        assert "fallback" in buf.getvalue()
