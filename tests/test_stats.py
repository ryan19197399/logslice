"""Tests for logslice.stats module."""
from datetime import datetime, timezone

import pytest

from logslice.parser import LogRecord
from logslice.stats import LogStats, compute_stats


def make_record(
    message: str = "msg",
    level: str = "INFO",
    timestamp: datetime | None = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
) -> LogRecord:
    return LogRecord(raw=message, timestamp=timestamp, level=level, message=message)


class TestComputeStats:
    def test_empty_iterable_returns_zero_totals(self):
        stats = compute_stats([])
        assert stats.total == 0
        assert stats.parsed_count == 0
        assert stats.unparsed_count == 0

    def test_counts_total_records(self):
        records = [make_record() for _ in range(5)]
        stats = compute_stats(records)
        assert stats.total == 5

    def test_counts_by_level(self):
        records = [
            make_record(level="INFO"),
            make_record(level="INFO"),
            make_record(level="ERROR"),
        ]
        stats = compute_stats(records)
        assert stats.by_level["INFO"] == 2
        assert stats.by_level["ERROR"] == 1

    def test_level_normalised_to_uppercase(self):
        records = [make_record(level="warning")]
        stats = compute_stats(records)
        assert stats.by_level["WARNING"] == 1

    def test_none_level_counted_as_unknown(self):
        records = [make_record(level=None)]
        stats = compute_stats(records)
        assert stats.by_level["UNKNOWN"] == 1

    def test_unparsed_counted_when_no_timestamp(self):
        records = [make_record(timestamp=None)]
        stats = compute_stats(records)
        assert stats.unparsed_count == 1
        assert stats.parsed_count == 0

    def test_first_and_last_timestamp(self):
        t1 = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t3 = datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        records = [make_record(timestamp=t) for t in [t1, t2, t3]]
        stats = compute_stats(records)
        assert stats.first_timestamp == t1
        assert stats.last_timestamp == t2

    def test_time_span_seconds(self):
        t1 = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 1, 10, 0, 30, tzinfo=timezone.utc)
        records = [make_record(timestamp=t1), make_record(timestamp=t2)]
        stats = compute_stats(records)
        assert stats.time_span_seconds == 30.0

    def test_time_span_none_when_no_timestamps(self):
        records = [make_record(timestamp=None)]
        stats = compute_stats(records)
        assert stats.time_span_seconds is None


class TestLogStatsToDIct:
    def test_to_dict_keys(self):
        stats = LogStats(total=3, unparsed_count=1)
        d = stats.to_dict()
        assert set(d.keys()) == {
            "total", "parsed", "unparsed", "by_level",
            "first_timestamp", "last_timestamp", "time_span_seconds",
        }

    def test_to_dict_timestamps_as_iso(self):
        ts = datetime(2024, 6, 15, 9, 30, 0, tzinfo=timezone.utc)
        stats = LogStats(total=1, first_timestamp=ts, last_timestamp=ts)
        d = stats.to_dict()
        assert d["first_timestamp"] == "2024-06-15T09:30:00+00:00"

    def test_to_dict_none_timestamps(self):
        stats = LogStats()
        d = stats.to_dict()
        assert d["first_timestamp"] is None
        assert d["last_timestamp"] is None
        assert d["time_span_seconds"] is None
