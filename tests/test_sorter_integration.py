"""Integration tests: sorter working with real parsed records."""

from __future__ import annotations

from datetime import timezone

from logslice.parser import parse_line
from logslice.sorter import sort_records


RAW_LINES = [
    "2024-01-01T12:30:00Z ERROR Something went wrong",
    "2024-01-01T08:00:00Z DEBUG Starting up",
    "2024-01-01T10:15:00Z INFO Processing request",
    "2024-01-01T09:45:00Z WARNING Disk space low",
]


def parsed_records():
    return [parse_line(line) for line in RAW_LINES]


class TestSorterWithParsedRecords:
    def test_sort_by_timestamp_asc(self):
        records = parsed_records()
        result = sort_records(records, key="timestamp", order="asc")
        hours = [r.timestamp.hour for r in result if r.timestamp]
        assert hours == sorted(hours)

    def test_sort_by_timestamp_desc(self):
        records = parsed_records()
        result = sort_records(records, key="timestamp", order="desc")
        hours = [r.timestamp.hour for r in result if r.timestamp]
        assert hours == sorted(hours, reverse=True)

    def test_sort_by_level_asc_severity(self):
        records = parsed_records()
        result = sort_records(records, key="level", order="asc")
        levels = [r.level.upper() for r in result if r.level]
        expected_order = ["DEBUG", "INFO", "WARNING", "ERROR"]
        assert levels == expected_order

    def test_sort_by_message_asc(self):
        records = parsed_records()
        result = sort_records(records, key="message", order="asc")
        messages = [r.message.lower() for r in result if r.message]
        assert messages == sorted(messages)

    def test_stable_on_equal_keys(self):
        """Records with the same level should not raise and produce a list."""
        from logslice.parser import LogRecord
        from datetime import datetime

        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        records = [
            LogRecord(timestamp=ts, level="INFO", message="first", raw="first"),
            LogRecord(timestamp=ts, level="INFO", message="second", raw="second"),
        ]
        result = sort_records(records, key="level", order="asc")
        assert result.count == 2
