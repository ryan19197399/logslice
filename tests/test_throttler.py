"""Tests for logslice.throttler."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from logslice.parser import LogRecord
from logslice.throttler import ThrottleResult, throttle_records


def make_record(ts: datetime | None = None, message: str = "msg") -> LogRecord:
    return LogRecord(raw=message, timestamp=ts, level="INFO", message=message)


def dt(second: int = 0, minute: int = 0) -> datetime:
    return datetime(2024, 1, 1, 0, minute, second, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# ThrottleResult helpers
# ---------------------------------------------------------------------------

class TestThrottleResult:
    def test_total_kept_derived(self):
        r = ThrottleResult(records=[], total_in=10, total_dropped=3)
        assert r.total_kept == 7

    def test_to_dict_keys(self):
        r = ThrottleResult(records=[], total_in=5, total_dropped=2)
        d = r.to_dict()
        assert set(d) == {"total_in", "total_kept", "total_dropped"}
        assert d["total_kept"] == 3


# ---------------------------------------------------------------------------
# throttle_records
# ---------------------------------------------------------------------------

class TestThrottleRecords:
    def test_empty_input_returns_empty_result(self):
        result = throttle_records([], max_per_window=5)
        assert result.records == []
        assert result.total_in == 0
        assert result.total_dropped == 0

    def test_all_kept_when_under_limit(self):
        records = [make_record(dt(second=i)) for i in range(5)]
        result = throttle_records(records, max_per_window=10, window_seconds=60)
        assert result.total_kept == 5
        assert result.total_dropped == 0

    def test_excess_records_dropped_within_window(self):
        # 6 records all within the same 60-second bucket, limit = 3
        records = [make_record(dt(second=i)) for i in range(6)]
        result = throttle_records(records, max_per_window=3, window_seconds=60)
        assert result.total_kept == 3
        assert result.total_dropped == 3

    def test_records_in_different_buckets_all_kept(self):
        # One record per minute — each lands in its own 60-second bucket
        records = [make_record(dt(minute=i)) for i in range(5)]
        result = throttle_records(records, max_per_window=1, window_seconds=60)
        assert result.total_kept == 5
        assert result.total_dropped == 0

    def test_none_timestamp_always_kept(self):
        records = [make_record(ts=None) for _ in range(10)]
        result = throttle_records(records, max_per_window=2, window_seconds=60)
        assert result.total_kept == 10
        assert result.total_dropped == 0

    def test_total_in_counts_all_records(self):
        records = [make_record(dt(second=0)) for _ in range(8)]
        result = throttle_records(records, max_per_window=3, window_seconds=60)
        assert result.total_in == 8

    def test_invalid_max_per_window_raises(self):
        with pytest.raises(ValueError, match="max_per_window"):
            throttle_records([], max_per_window=0)

    def test_invalid_window_seconds_raises(self):
        with pytest.raises(ValueError, match="window_seconds"):
            throttle_records([], max_per_window=1, window_seconds=0)

    def test_kept_records_are_first_n_in_bucket(self):
        records = [make_record(dt(second=i), message=f"m{i}") for i in range(4)]
        result = throttle_records(records, max_per_window=2, window_seconds=60)
        assert [r.message for r in result.records] == ["m0", "m1"]
