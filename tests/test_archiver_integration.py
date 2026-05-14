"""Integration tests for logslice.archiver_integration."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from logslice.archiver_integration import ArchiverConfig, run_archiver
from logslice.parser import LogRecord


def make_record(
    message: str = "msg",
    level: str = "INFO",
    timestamp: datetime | None = None,
) -> LogRecord:
    return LogRecord(raw=message, message=message, level=level, timestamp=timestamp)


def dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)


class TestArchiverConfig:
    def test_default_mode_is_date(self):
        cfg = ArchiverConfig()
        assert cfg.mode == "date"

    def test_invalid_mode_raises_on_validate(self):
        cfg = ArchiverConfig(mode="minute")
        with pytest.raises(ValueError, match="mode must be one of"):
            cfg.validate()

    def test_valid_modes_do_not_raise(self):
        for mode in ("date", "level"):
            ArchiverConfig(mode=mode).validate()


class TestRunArchiver:
    def _records(self):
        return [
            make_record("a", "INFO", dt("2024-06-01T08:00:00")),
            make_record("b", "ERROR", dt("2024-06-01T09:00:00")),
            make_record("c", "DEBUG", dt("2024-06-02T10:00:00")),
            make_record("d", "INFO", dt("2024-06-02T11:00:00")),
        ]

    def test_date_mode_creates_correct_buckets(self):
        result = run_archiver(self._records(), ArchiverConfig(mode="date"))
        assert set(result.bucket_names) == {"2024-06-01", "2024-06-02"}

    def test_level_mode_creates_correct_buckets(self):
        result = run_archiver(self._records(), ArchiverConfig(mode="level"))
        assert "INFO" in result.bucket_names
        assert "ERROR" in result.bucket_names

    def test_level_filter_reduces_records(self):
        cfg = ArchiverConfig(mode="level", levels=["ERROR"])
        result = run_archiver(self._records(), cfg)
        assert result.total == 1
        assert "ERROR" in result.entries

    def test_time_range_filter_applied(self):
        cfg = ArchiverConfig(
            mode="date",
            start="2024-06-02T00:00:00",
        )
        result = run_archiver(self._records(), cfg)
        assert result.total == 2
        assert "2024-06-01" not in result.bucket_names

    def test_empty_records_returns_empty_result(self):
        result = run_archiver([], ArchiverConfig())
        assert result.total == 0
        assert result.entries == {}
