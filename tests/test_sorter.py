"""Tests for logslice.sorter."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from logslice.parser import LogRecord
from logslice.sorter import SortResult, sort_records


def make_record(
    message: str = "msg",
    level: str | None = "INFO",
    ts: datetime | None = None,
) -> LogRecord:
    return LogRecord(timestamp=ts, level=level, message=message, raw=message)


def dt(hour: int) -> datetime:
    return datetime(2024, 1, 1, hour, 0, 0, tzinfo=timezone.utc)


class TestSortByTimestamp:
    def test_ascending_order(self):
        records = [make_record(ts=dt(3)), make_record(ts=dt(1)), make_record(ts=dt(2))]
        result = sort_records(records, key="timestamp", order="asc")
        timestamps = [r.timestamp for r in result]
        assert timestamps == [dt(1), dt(2), dt(3)]

    def test_descending_order(self):
        records = [make_record(ts=dt(1)), make_record(ts=dt(3)), make_record(ts=dt(2))]
        result = sort_records(records, key="timestamp", order="desc")
        timestamps = [r.timestamp for r in result]
        assert timestamps == [dt(3), dt(2), dt(1)]

    def test_none_timestamp_placed_last(self):
        records = [make_record(ts=None), make_record(ts=dt(1))]
        result = sort_records(records, key="timestamp", order="asc", nulls_last=True)
        assert result.records[0].timestamp == dt(1)
        assert result.records[1].timestamp is None

    def test_returns_sort_result(self):
        result = sort_records([], key="timestamp", order="asc")
        assert isinstance(result, SortResult)
        assert result.sort_key == "timestamp"
        assert result.order == "asc"


class TestSortByLevel:
    def test_ascending_severity(self):
        records = [
            make_record(level="ERROR"),
            make_record(level="DEBUG"),
            make_record(level="INFO"),
        ]
        result = sort_records(records, key="level", order="asc")
        levels = [r.level for r in result]
        assert levels == ["DEBUG", "INFO", "ERROR"]

    def test_descending_severity(self):
        records = [
            make_record(level="DEBUG"),
            make_record(level="CRITICAL"),
            make_record(level="WARNING"),
        ]
        result = sort_records(records, key="level", order="desc")
        levels = [r.level for r in result]
        assert levels == ["CRITICAL", "WARNING", "DEBUG"]

    def test_unknown_level_placed_last_when_nulls_last(self):
        records = [make_record(level="UNKNOWN"), make_record(level="INFO")]
        result = sort_records(records, key="level", order="asc", nulls_last=True)
        assert result.records[-1].level == "UNKNOWN"


class TestSortByMessage:
    def test_alphabetical_ascending(self):
        records = [make_record(message="zebra"), make_record(message="apple"), make_record(message="mango")]
        result = sort_records(records, key="message", order="asc")
        messages = [r.message for r in result]
        assert messages == ["apple", "mango", "zebra"]

    def test_case_insensitive(self):
        records = [make_record(message="Banana"), make_record(message="apple")]
        result = sort_records(records, key="message", order="asc")
        assert result.records[0].message == "apple"


class TestSortValidation:
    def test_invalid_key_raises(self):
        with pytest.raises(ValueError, match="Invalid sort key"):
            sort_records([], key="unknown")  # type: ignore[arg-type]

    def test_invalid_order_raises(self):
        with pytest.raises(ValueError, match="Invalid sort order"):
            sort_records([], order="random")  # type: ignore[arg-type]

    def test_empty_input_returns_empty_result(self):
        result = sort_records([])
        assert result.count == 0
        assert result.records == []

    def test_count_property(self):
        records = [make_record(), make_record(), make_record()]
        result = sort_records(records)
        assert result.count == 3

    def test_iter_protocol(self):
        records = [make_record(message="a"), make_record(message="b")]
        result = sort_records(records, key="message")
        assert list(result) == result.records
