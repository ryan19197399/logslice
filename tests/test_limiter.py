"""Tests for logslice.limiter."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from logslice.parser import LogRecord
from logslice.limiter import LimitResult, limit_records


def make_record(msg: str = "hello", level: str = "INFO") -> LogRecord:
    return LogRecord(
        raw=msg,
        timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        level=level,
        message=msg,
    )


def make_records(n: int) -> List[LogRecord]:
    return [make_record(f"msg-{i}") for i in range(n)]


# ---------------------------------------------------------------------------
# LimitResult properties
# ---------------------------------------------------------------------------

class TestLimitResult:
    def test_total_kept_equals_records_length(self):
        records = make_records(3)
        result = LimitResult(records=records, total_seen=5, limit=3)
        assert result.total_kept == 3

    def test_dropped_calculated_correctly(self):
        records = make_records(3)
        result = LimitResult(records=records, total_seen=7, limit=3)
        assert result.dropped == 4

    def test_limit_reached_true_when_seen_exceeds_limit(self):
        result = LimitResult(records=make_records(2), total_seen=5, limit=2)
        assert result.limit_reached is True

    def test_limit_reached_false_when_seen_equals_limit(self):
        result = LimitResult(records=make_records(2), total_seen=2, limit=2)
        assert result.limit_reached is False

    def test_to_dict_contains_expected_keys(self):
        result = LimitResult(records=make_records(1), total_seen=3, limit=1)
        d = result.to_dict()
        assert set(d) == {"limit", "total_seen", "total_kept", "dropped", "limit_reached"}


# ---------------------------------------------------------------------------
# limit_records behaviour
# ---------------------------------------------------------------------------

class TestLimitRecords:
    def test_empty_input_returns_empty_result(self):
        result = limit_records([], limit=10)
        assert result.records == []
        assert result.total_seen == 0

    def test_limit_zero_returns_no_records(self):
        result = limit_records(make_records(5), limit=0)
        assert result.total_kept == 0

    def test_negative_limit_raises(self):
        with pytest.raises(ValueError, match="limit must be >= 0"):
            limit_records(make_records(3), limit=-1)

    def test_fewer_records_than_limit_returns_all(self):
        records = make_records(3)
        result = limit_records(records, limit=10)
        assert result.total_kept == 3
        assert result.limit_reached is False

    def test_exactly_limit_records_returned(self):
        records = make_records(5)
        result = limit_records(records, limit=5)
        assert result.total_kept == 5

    def test_excess_records_are_dropped(self):
        records = make_records(10)
        result = limit_records(records, limit=4)
        assert result.total_kept == 4
        assert result.records[0].message == "msg-0"
        assert result.records[3].message == "msg-3"

    def test_limit_reached_set_when_stream_cut(self):
        result = limit_records(make_records(10), limit=3)
        assert result.limit_reached is True

    def test_count_all_gives_accurate_total_seen(self):
        result = limit_records(make_records(10), limit=3, count_all=True)
        assert result.total_kept == 3
        assert result.total_seen == 10
        assert result.dropped == 7

    def test_result_limit_field_matches_argument(self):
        result = limit_records(make_records(5), limit=2)
        assert result.limit == 2
