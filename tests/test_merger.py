"""Tests for logslice.merger."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from logslice.parser import LogRecord
from logslice.merger import MergeResult, merge_record_streams


def make_record(ts: datetime | None, message: str = "msg") -> LogRecord:
    return LogRecord(timestamp=ts, level="INFO", message=message, raw=message)


def dt(hour: int, minute: int = 0) -> datetime:
    return datetime(2024, 1, 1, hour, minute, tzinfo=timezone.utc)


class TestMergeRecordStreams:
    def test_empty_streams_return_empty_result(self):
        result = merge_record_streams([], [])
        assert result.total == 0
        assert result.records == []

    def test_single_stream_passthrough(self):
        records = [make_record(dt(1)), make_record(dt(2)), make_record(dt(3))]
        result = merge_record_streams(records)
        assert result.total == 3
        assert result.records == records

    def test_two_streams_merged_in_order(self):
        a = [make_record(dt(1)), make_record(dt(3)), make_record(dt(5))]
        b = [make_record(dt(2)), make_record(dt(4)), make_record(dt(6))]
        result = merge_record_streams(a, b)
        timestamps = [r.timestamp for r in result.records]
        assert timestamps == sorted(timestamps)
        assert result.total == 6

    def test_source_counts_tracked(self):
        a = [make_record(dt(1)), make_record(dt(3))]
        b = [make_record(dt(2))]
        result = merge_record_streams(a, b, source_names=["app", "system"])
        assert result.source_counts["app"] == 2
        assert result.source_counts["system"] == 1

    def test_source_names_default_to_indices(self):
        result = merge_record_streams([], [], [])
        assert set(result.source_counts.keys()) == {"0", "1", "2"}

    def test_mismatched_source_names_raises(self):
        with pytest.raises(ValueError, match="source_names length"):
            merge_record_streams([], [], source_names=["only_one"])

    def test_records_with_none_timestamp_sort_last(self):
        a = [make_record(dt(1)), make_record(None, "no-ts")]
        b = [make_record(dt(2))]
        result = merge_record_streams(a, b)
        assert result.records[-1].message == "no-ts"

    def test_three_streams_fully_interleaved(self):
        a = [make_record(dt(1)), make_record(dt(4))]
        b = [make_record(dt(2)), make_record(dt(5))]
        c = [make_record(dt(3)), make_record(dt(6))]
        result = merge_record_streams(a, b, c)
        hours = [r.timestamp.hour for r in result.records]
        assert hours == [1, 2, 3, 4, 5, 6]

    def test_to_dict_structure(self):
        a = [make_record(dt(1))]
        result = merge_record_streams(a, source_names=["src"])
        d = result.to_dict()
        assert d["total"] == 1
        assert "source_counts" in d
        assert d["source_counts"]["src"] == 1

    def test_equal_timestamps_all_included(self):
        ts = dt(10)
        a = [make_record(ts, "a1"), make_record(ts, "a2")]
        b = [make_record(ts, "b1")]
        result = merge_record_streams(a, b)
        assert result.total == 3
