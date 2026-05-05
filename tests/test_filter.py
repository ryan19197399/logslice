"""Tests for logslice.filter module."""

from datetime import datetime, timezone

import pytest

from logslice.filter import LogFilter, filter_records
from logslice.parser import LogRecord


def make_record(raw: str, timestamp=None, level=None) -> LogRecord:
    return LogRecord(raw=raw, timestamp=timestamp, level=level, message=raw)


TS_EARLY = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
TS_MID = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
TS_LATE = datetime(2024, 1, 1, 14, 0, 0, tzinfo=timezone.utc)


class TestLogFilterTimeRange:
    def test_start_filter_excludes_early(self):
        f = LogFilter(start=TS_MID)
        record = make_record("early", timestamp=TS_EARLY)
        assert not f.matches(record)

    def test_start_filter_includes_equal(self):
        f = LogFilter(start=TS_MID)
        record = make_record("mid", timestamp=TS_MID)
        assert f.matches(record)

    def test_end_filter_excludes_late(self):
        f = LogFilter(end=TS_MID)
        record = make_record("late", timestamp=TS_LATE)
        assert not f.matches(record)

    def test_range_includes_within(self):
        f = LogFilter(start=TS_EARLY, end=TS_LATE)
        record = make_record("mid", timestamp=TS_MID)
        assert f.matches(record)

    def test_no_timestamp_passes_time_filter(self):
        f = LogFilter(start=TS_EARLY, end=TS_LATE)
        record = make_record("no ts", timestamp=None)
        assert f.matches(record)


class TestLogFilterPattern:
    def test_pattern_match(self):
        f = LogFilter(pattern=r"ERROR")
        assert f.matches(make_record("2024-01-01 ERROR something broke"))

    def test_pattern_no_match(self):
        f = LogFilter(pattern=r"ERROR")
        assert not f.matches(make_record("2024-01-01 INFO all good"))

    def test_pattern_regex(self):
        f = LogFilter(pattern=r"user_\d+")
        assert f.matches(make_record("login user_42 succeeded"))
        assert not f.matches(make_record("login admin succeeded"))


class TestLogFilterLevel:
    def test_level_match(self):
        f = LogFilter(level="ERROR")
        assert f.matches(make_record("msg", level="ERROR"))

    def test_level_case_insensitive(self):
        f = LogFilter(level="error")
        assert f.matches(make_record("msg", level="ERROR"))

    def test_level_no_match(self):
        f = LogFilter(level="ERROR")
        assert not f.matches(make_record("msg", level="INFO"))

    def test_no_level_on_record_passes(self):
        f = LogFilter(level="ERROR")
        assert not f.matches(make_record("msg", level=None))


class TestFilterRecords:
    def test_filter_records_convenience(self):
        records = [
            make_record("a ERROR", timestamp=TS_EARLY, level="ERROR"),
            make_record("b INFO", timestamp=TS_MID, level="INFO"),
            make_record("c ERROR", timestamp=TS_LATE, level="ERROR"),
        ]
        result = list(filter_records(records, start=TS_MID, level="ERROR"))
        assert len(result) == 1
        assert result[0].raw == "c ERROR"
