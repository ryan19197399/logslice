"""Tests for logslice.profiler."""
from __future__ import annotations

import itertools
from datetime import datetime, timezone
from typing import Iterator

import pytest

from logslice.parser import LogRecord
from logslice.profiler import ProfileResult, profile_records


def make_record(msg: str = "hello", level: str = "INFO") -> LogRecord:
    return LogRecord(
        raw=f"2024-01-01T00:00:00Z {level} {msg}",
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        level=level,
        message=msg,
    )


def make_records(n: int) -> list[LogRecord]:
    return [make_record(f"msg {i}") for i in range(n)]


# ---------------------------------------------------------------------------
# Monotonically increasing fake clock
# ---------------------------------------------------------------------------

def _fake_clock(start: float = 0.0, step: float = 0.001):
    """Return a callable that advances by *step* each call."""
    t = [start]

    def _clock() -> float:
        val = t[0]
        t[0] += step
        return val

    return _clock


class TestProfileResult:
    def test_to_dict_keys(self):
        result = ProfileResult(
            total_records=10,
            elapsed_seconds=1.0,
            records_per_second=10.0,
            records=[],
        )
        d = result.to_dict()
        assert set(d.keys()) == {"total_records", "elapsed_seconds", "records_per_second"}

    def test_to_dict_values_rounded(self):
        result = ProfileResult(
            total_records=3,
            elapsed_seconds=1.123456789,
            records_per_second=2.666666,
            records=[],
        )
        d = result.to_dict()
        assert d["elapsed_seconds"] == round(1.123456789, 6)
        assert d["records_per_second"] == round(2.666666, 2)


class TestProfileRecords:
    def test_empty_input_returns_zero_totals(self):
        clock = _fake_clock(step=0.5)
        result = profile_records([], clock=clock)
        assert result.total_records == 0
        assert result.records == []

    def test_zero_elapsed_gives_zero_rps(self):
        # If clock always returns the same value, elapsed == 0
        result = profile_records(make_records(5), clock=lambda: 0.0)
        assert result.records_per_second == 0.0

    def test_counts_all_records(self):
        clock = _fake_clock(start=0.0, step=0.1)
        records = make_records(7)
        result = profile_records(records, clock=clock)
        assert result.total_records == 7

    def test_records_list_preserved(self):
        records = make_records(3)
        result = profile_records(records, clock=_fake_clock())
        assert result.records == records

    def test_elapsed_positive(self):
        clock = _fake_clock(start=0.0, step=0.05)
        result = profile_records(make_records(4), clock=clock)
        assert result.elapsed_seconds > 0

    def test_rps_calculation(self):
        # clock returns 0.0 then 2.0 → elapsed == 2.0; 10 records → 5 rps
        values = iter([0.0, 2.0])
        result = profile_records(make_records(10), clock=lambda: next(values))
        assert result.records_per_second == pytest.approx(5.0)

    def test_accepts_generator(self):
        def gen() -> Iterator[LogRecord]:
            for i in range(5):
                yield make_record(f"gen {i}")

        result = profile_records(gen(), clock=_fake_clock())
        assert result.total_records == 5
