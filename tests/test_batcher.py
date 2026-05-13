"""Tests for logslice.batcher."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from logslice.batcher import BatchResult, batch_records
from logslice.parser import LogRecord


def make_record(msg: str = "hello", level: str = "INFO") -> LogRecord:
    return LogRecord(
        raw=msg,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        level=level,
        message=msg,
    )


def make_records(n: int) -> List[LogRecord]:
    return [make_record(f"msg-{i}") for i in range(n)]


class TestBatchRecords:
    def test_empty_input_returns_empty_result(self):
        result = batch_records([], batch_size=5)
        assert result.batches == []
        assert result.total_records == 0
        assert result.batch_count == 0

    def test_single_batch_when_records_lt_size(self):
        records = make_records(3)
        result = batch_records(records, batch_size=10)
        assert result.batch_count == 1
        assert result.total_records == 3
        assert len(result.batches[0]) == 3

    def test_exact_multiple_produces_full_batches(self):
        records = make_records(9)
        result = batch_records(records, batch_size=3)
        assert result.batch_count == 3
        for batch in result.batches:
            assert len(batch) == 3

    def test_remainder_forms_partial_last_batch(self):
        records = make_records(10)
        result = batch_records(records, batch_size=3)
        assert result.batch_count == 4
        assert result.last_batch_size == 1

    def test_total_records_matches_input_length(self):
        records = make_records(17)
        result = batch_records(records, batch_size=5)
        assert result.total_records == 17

    def test_batch_size_one_each_record_own_batch(self):
        records = make_records(5)
        result = batch_records(records, batch_size=1)
        assert result.batch_count == 5
        for batch in result.batches:
            assert len(batch) == 1

    def test_batch_size_equals_total(self):
        records = make_records(6)
        result = batch_records(records, batch_size=6)
        assert result.batch_count == 1
        assert result.last_batch_size == 6

    def test_invalid_batch_size_raises(self):
        with pytest.raises(ValueError, match="batch_size must be >= 1"):
            batch_records([], batch_size=0)

    def test_negative_batch_size_raises(self):
        with pytest.raises(ValueError):
            batch_records(make_records(3), batch_size=-1)

    def test_to_dict_keys_present(self):
        result = batch_records(make_records(5), batch_size=2)
        d = result.to_dict()
        assert set(d.keys()) == {
            "batch_count",
            "batch_size",
            "total_records",
            "last_batch_size",
        }

    def test_records_preserved_in_order(self):
        records = make_records(6)
        result = batch_records(records, batch_size=2)
        flat = [r for batch in result.batches for r in batch]
        assert flat == records

    def test_last_batch_size_zero_for_empty(self):
        result = batch_records([], batch_size=4)
        assert result.last_batch_size == 0
