"""Tests for logslice.compressor."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from logslice.compressor import compress_records, CompressedRecord, CompressionResult
from logslice.parser import LogRecord


def make_record(message: str, level: str = "INFO", ts: datetime | None = None) -> LogRecord:
    return LogRecord(
        raw=f"{level} {message}",
        timestamp=ts,
        level=level,
        message=message,
    )


class TestCompressRecords:
    def test_empty_input_returns_empty_result(self):
        result = compress_records([])
        assert isinstance(result, CompressionResult)
        assert result.records == []
        assert result.total_input == 0
        assert result.total_output == 0

    def test_single_record_run_length_one(self):
        result = compress_records([make_record("hello")])
        assert len(result.records) == 1
        assert result.records[0].run_length == 1
        assert result.records[0].is_repeated is False

    def test_two_identical_consecutive_collapsed(self):
        records = [make_record("same"), make_record("same")]
        result = compress_records(records)
        assert len(result.records) == 1
        assert result.records[0].run_length == 2
        assert result.records[0].is_repeated is True

    def test_non_consecutive_duplicates_not_collapsed(self):
        records = [make_record("a"), make_record("b"), make_record("a")]
        result = compress_records(records)
        assert len(result.records) == 3

    def test_run_length_counted_correctly(self):
        records = [make_record("x")] * 5
        result = compress_records(records)
        assert result.records[0].run_length == 5

    def test_different_levels_not_collapsed(self):
        records = [make_record("msg", "INFO"), make_record("msg", "ERROR")]
        result = compress_records(records)
        assert len(result.records) == 2

    def test_total_input_equals_original_count(self):
        records = [make_record("a"), make_record("a"), make_record("b")]
        result = compress_records(records)
        assert result.total_input == 3

    def test_dropped_reflects_collapsed_records(self):
        records = [make_record("a"), make_record("a"), make_record("b")]
        result = compress_records(records)
        assert result.dropped == 1  # 3 input - 2 output

    def test_to_dict_structure(self):
        records = [make_record("hello")]
        result = compress_records(records)
        d = result.to_dict()
        assert d["total_input"] == 1
        assert d["total_output"] == 1
        assert d["dropped"] == 0
        assert isinstance(d["records"], list)

    def test_compressed_record_to_dict_includes_timestamp(self):
        ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = compress_records([make_record("hi", ts=ts)])
        d = result.records[0].to_dict()
        assert "timestamp" in d
        assert "2024-01-01" in d["timestamp"]

    def test_compressed_record_to_dict_no_timestamp(self):
        result = compress_records([make_record("hi")])
        d = result.records[0].to_dict()
        assert "timestamp" not in d

    def test_mixed_runs(self):
        records = (
            [make_record("a")] * 3
            + [make_record("b")] * 2
            + [make_record("a")] * 1
        )
        result = compress_records(records)
        assert len(result.records) == 3
        assert result.records[0].run_length == 3
        assert result.records[1].run_length == 2
        assert result.records[2].run_length == 1
        assert result.total_input == 6
