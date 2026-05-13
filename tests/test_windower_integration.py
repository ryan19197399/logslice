"""Integration tests for windower using records produced by the real parser."""
from datetime import timedelta

import pytest

from logslice.parser import parse_line
from logslice.windower import window_records

RAW_LINES = [
    "2024-01-01T10:00:00Z INFO  service started",
    "2024-01-01T10:04:00Z DEBUG checking config",
    "2024-01-01T10:09:59Z INFO  ready",
    "2024-01-01T10:10:00Z ERROR connection refused",
    "2024-01-01T10:15:00Z WARN  retrying",
    "not a timestamped line at all",
]


@pytest.fixture()
def parsed_records():
    return [parse_line(line) for line in RAW_LINES]


class TestWindowerWithParsedRecords:
    def test_tumbling_10min_creates_two_windows(self, parsed_records):
        result = window_records(parsed_records, timedelta(minutes=10))
        # [10:00-10:10) has 3 records, [10:10-10:20) has 2 records
        assert len(result.windows) == 2

    def test_first_window_count(self, parsed_records):
        result = window_records(parsed_records, timedelta(minutes=10))
        assert result.windows[0].count == 3

    def test_second_window_count(self, parsed_records):
        result = window_records(parsed_records, timedelta(minutes=10))
        assert result.windows[1].count == 2

    def test_skipped_no_timestamp_counted(self, parsed_records):
        result = window_records(parsed_records, timedelta(minutes=10))
        assert result.skipped_no_timestamp == 1

    def test_total_records_includes_all(self, parsed_records):
        result = window_records(parsed_records, timedelta(minutes=10))
        assert result.total_records == len(RAW_LINES)

    def test_error_level_in_second_window(self, parsed_records):
        result = window_records(parsed_records, timedelta(minutes=10))
        lc = result.windows[1].level_counts
        assert "ERROR" in lc
