"""Tests for logslice.sampler."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from logslice.parser import LogRecord
from logslice.sampler import SampleResult, sample_records


def make_record(msg: str = "test", level: str = "INFO") -> LogRecord:
    return LogRecord(
        timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        level=level,
        message=msg,
        raw=f"2024-01-01T12:00:00Z {level} {msg}",
    )


def make_records(n: int) -> List[LogRecord]:
    return [make_record(f"msg-{i}") for i in range(n)]


class TestSampleRecordsNth:
    def test_every_nth_keeps_correct_count(self):
        records = make_records(10)
        result = sample_records(records, every_nth=2)
        assert result.total_kept == 5
        assert len(result.records) == 5

    def test_every_nth_selects_correct_indices(self):
        records = make_records(6)
        result = sample_records(records, every_nth=3)
        # 1-based: indices 3 and 6 → messages msg-2 and msg-5
        assert [r.message for r in result.records] == ["msg-2", "msg-5"]

    def test_every_nth_one_keeps_all(self):
        records = make_records(5)
        result = sample_records(records, every_nth=1)
        assert result.total_kept == 5

    def test_every_nth_larger_than_input(self):
        records = make_records(3)
        result = sample_records(records, every_nth=10)
        assert result.total_kept == 0
        assert result.records == []

    def test_invalid_nth_raises(self):
        with pytest.raises(ValueError, match="n must be"):
            sample_records(make_records(5), every_nth=0)


class TestSampleRecordsFraction:
    def test_fraction_one_keeps_all(self):
        records = make_records(20)
        result = sample_records(records, fraction=1.0, seed=0)
        assert result.total_kept == 20

    def test_fraction_roughly_correct(self):
        records = make_records(1000)
        result = sample_records(records, fraction=0.1, seed=42)
        assert 50 <= result.total_kept <= 150

    def test_fraction_zero_raises(self):
        with pytest.raises(ValueError, match="fraction must be"):
            sample_records(make_records(5), fraction=0.0)

    def test_fraction_above_one_raises(self):
        with pytest.raises(ValueError, match="fraction must be"):
            sample_records(make_records(5), fraction=1.5)

    def test_seed_reproducible(self):
        records = make_records(100)
        r1 = sample_records(records, fraction=0.3, seed=7)
        r2 = sample_records(records, fraction=0.3, seed=7)
        assert [r.message for r in r1.records] == [r.message for r in r2.records]


class TestSampleResult:
    def test_drop_rate_empty(self):
        result = SampleResult(records=[], total_seen=0, total_kept=0)
        assert result.drop_rate == 0.0

    def test_drop_rate_half(self):
        result = SampleResult(records=[], total_seen=10, total_kept=5)
        assert result.drop_rate == pytest.approx(0.5)

    def test_both_params_raises(self):
        with pytest.raises(ValueError, match="exactly one"):
            sample_records(make_records(5), every_nth=2, fraction=0.5)

    def test_neither_param_raises(self):
        with pytest.raises(ValueError, match="exactly one"):
            sample_records(make_records(5))
