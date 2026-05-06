"""Tests for logslice.deduplicator."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from logslice.deduplicator import (
    DeduplicationResult,
    deduplicate,
    iter_deduplicated,
)
from logslice.parser import LogRecord


def make_record(
    message: str,
    level: str = "INFO",
    ts: datetime | None = None,
    raw: str = "",
) -> LogRecord:
    return LogRecord(
        timestamp=ts,
        level=level,
        message=message,
        raw=raw or f"[{level}] {message}",
    )


T1 = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
T2 = datetime(2024, 1, 1, 10, 0, 5, tzinfo=timezone.utc)


class TestDeduplicate:
    def test_empty_input_returns_empty_result(self):
        result = deduplicate([])
        assert result.records == []
        assert result.total_dropped == 0

    def test_no_duplicates_unchanged(self):
        records = [
            make_record("alpha"),
            make_record("beta"),
            make_record("gamma"),
        ]
        result = deduplicate(records)
        assert result.unique_count == 3
        assert result.total_dropped == 0

    def test_exact_duplicates_dropped(self):
        r = make_record("disk full", level="ERROR")
        result = deduplicate([r, r, r])
        assert result.unique_count == 1
        assert result.total_dropped == 2

    def test_different_timestamps_same_message_deduped_by_default(self):
        r1 = make_record("heartbeat", ts=T1)
        r2 = make_record("heartbeat", ts=T2)
        result = deduplicate([r1, r2], ignore_timestamp=True)
        assert result.unique_count == 1

    def test_different_timestamps_not_deduped_when_timestamp_included(self):
        r1 = make_record("heartbeat", ts=T1)
        r2 = make_record("heartbeat", ts=T2)
        result = deduplicate([r1, r2], ignore_timestamp=False)
        assert result.unique_count == 2

    def test_keep_first_retains_earliest(self):
        r1 = make_record("msg", ts=T1)
        r2 = make_record("msg", ts=T2)
        result = deduplicate([r1, r2], keep="first")
        assert result.records[0].timestamp == T1

    def test_keep_last_retains_latest(self):
        r1 = make_record("msg", ts=T1)
        r2 = make_record("msg", ts=T2)
        result = deduplicate([r1, r2], keep="last")
        assert result.records[0].timestamp == T2

    def test_invalid_keep_raises(self):
        with pytest.raises(ValueError, match="keep must be"):
            deduplicate([], keep="random")

    def test_duplicate_counts_correct(self):
        r = make_record("loop")
        result = deduplicate([r, r, r, r])
        assert result.total_dropped == 3

    def test_result_unique_count_property(self):
        records = [make_record("a"), make_record("b"), make_record("a")]
        result = deduplicate(records)
        assert result.unique_count == 2


class TestIterDeduplicated:
    def test_yields_unique_records(self):
        records = [
            make_record("x"),
            make_record("y"),
            make_record("x"),
        ]
        out = list(iter_deduplicated(records))
        assert len(out) == 2
        assert out[0].message == "x"
        assert out[1].message == "y"

    def test_streaming_generator(self):
        records = (make_record(f"msg{i % 3}") for i in range(9))
        out = list(iter_deduplicated(records))
        assert len(out) == 3
