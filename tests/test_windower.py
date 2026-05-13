"""Tests for logslice.windower."""
from datetime import datetime, timedelta, timezone

import pytest

from logslice.parser import LogRecord
from logslice.windower import WindowResult, WindowSummary, window_records


def make_record(
    message: str = "msg",
    level: str = "INFO",
    ts: datetime | None = None,
) -> LogRecord:
    return LogRecord(raw=message, timestamp=ts, level=level, message=message)


def dt(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2024, 1, 1, hour, minute, second, tzinfo=timezone.utc)


SIZE = timedelta(minutes=10)


class TestWindowRecords:
    def test_empty_input_returns_empty_result(self):
        result = window_records([], SIZE)
        assert result.total_records == 0
        assert result.windows == []
        assert result.skipped_no_timestamp == 0

    def test_records_without_timestamp_are_skipped(self):
        records = [make_record(ts=None), make_record(ts=None)]
        result = window_records(records, SIZE)
        assert result.skipped_no_timestamp == 2
        assert result.total_records == 2
        assert result.windows == []

    def test_single_record_creates_one_window(self):
        records = [make_record(ts=dt(10, 5))]
        result = window_records(records, SIZE)
        assert len(result.windows) == 1
        assert result.windows[0].count == 1

    def test_tumbling_window_groups_correctly(self):
        records = [
            make_record(ts=dt(10, 0)),
            make_record(ts=dt(10, 5)),
            make_record(ts=dt(10, 11)),
        ]
        result = window_records(records, SIZE)
        assert len(result.windows) == 2
        assert result.windows[0].count == 2
        assert result.windows[1].count == 1

    def test_window_boundaries_are_half_open(self):
        # Record exactly at window_end should fall into next window.
        records = [
            make_record(ts=dt(10, 0)),
            make_record(ts=dt(10, 10)),  # exactly at boundary
        ]
        result = window_records(records, SIZE)
        assert result.windows[0].count == 1
        assert result.windows[1].count == 1

    def test_sliding_window_overlaps(self):
        records = [
            make_record(ts=dt(10, 0)),
            make_record(ts=dt(10, 3)),
            make_record(ts=dt(10, 8)),
        ]
        result = window_records(records, SIZE, step=timedelta(minutes=5))
        # Windows: [10:00-10:10], [10:05-10:15]
        assert len(result.windows) == 2
        assert result.windows[0].count == 3  # all three in first window
        assert result.windows[1].count == 1  # only 10:08 in second

    def test_level_counts_in_summary(self):
        records = [
            make_record(level="INFO", ts=dt(10, 1)),
            make_record(level="ERROR", ts=dt(10, 2)),
            make_record(level="info", ts=dt(10, 3)),
        ]
        result = window_records(records, SIZE)
        lc = result.windows[0].level_counts
        assert lc["INFO"] == 2
        assert lc["ERROR"] == 1

    def test_to_dict_structure(self):
        records = [make_record(ts=dt(10, 0))]
        result = window_records(records, SIZE)
        d = result.to_dict()
        assert "total_records" in d
        assert "window_count" in d
        assert "windows" in d
        assert isinstance(d["windows"], list)

    def test_invalid_window_size_raises(self):
        with pytest.raises(ValueError, match="window_size"):
            window_records([], timedelta(seconds=0))

    def test_invalid_step_raises(self):
        with pytest.raises(ValueError, match="step"):
            window_records([], SIZE, step=timedelta(seconds=-1))

    def test_total_records_includes_skipped(self):
        records = [make_record(ts=dt(10, 0)), make_record(ts=None)]
        result = window_records(records, SIZE)
        assert result.total_records == 2
        assert result.skipped_no_timestamp == 1
