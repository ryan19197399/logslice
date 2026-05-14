"""Tests for logslice.archiver."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from logslice.archiver import ArchiveEntry, ArchiveResult, archive_records
from logslice.parser import LogRecord


def make_record(
    message: str = "msg",
    level: str = "INFO",
    timestamp: datetime | None = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
) -> LogRecord:
    return LogRecord(raw=message, message=message, level=level, timestamp=timestamp)


# ---------------------------------------------------------------------------
# ArchiveEntry
# ---------------------------------------------------------------------------

class TestArchiveEntry:
    def test_count_reflects_records(self):
        entry = ArchiveEntry(bucket="2024-06-01")
        entry.records.append(make_record())
        entry.records.append(make_record())
        assert entry.count == 2

    def test_to_dict_keys(self):
        entry = ArchiveEntry(bucket="ERROR")
        d = entry.to_dict()
        assert d["bucket"] == "ERROR"
        assert "count" in d


# ---------------------------------------------------------------------------
# ArchiveResult
# ---------------------------------------------------------------------------

class TestArchiveResult:
    def test_bucket_names_sorted(self):
        result = ArchiveResult()
        result.entries["2024-06-03"] = ArchiveEntry(bucket="2024-06-03")
        result.entries["2024-06-01"] = ArchiveEntry(bucket="2024-06-01")
        assert result.bucket_names == ["2024-06-01", "2024-06-03"]

    def test_to_dict_structure(self):
        result = ArchiveResult(total=5, unarchived=[make_record()])
        d = result.to_dict()
        assert d["total"] == 5
        assert d["unarchived"] == 1
        assert "buckets" in d


# ---------------------------------------------------------------------------
# archive_records — date mode
# ---------------------------------------------------------------------------

class TestArchiveRecordsByDate:
    def test_empty_input_returns_empty_result(self):
        result = archive_records([], mode="date")
        assert result.total == 0
        assert result.entries == {}
        assert result.unarchived == []

    def test_single_record_creates_one_bucket(self):
        result = archive_records([make_record()], mode="date")
        assert result.total == 1
        assert "2024-06-01" in result.entries

    def test_records_grouped_by_date(self):
        r1 = make_record(timestamp=datetime(2024, 6, 1, 10, 0, tzinfo=timezone.utc))
        r2 = make_record(timestamp=datetime(2024, 6, 1, 22, 0, tzinfo=timezone.utc))
        r3 = make_record(timestamp=datetime(2024, 6, 2, 8, 0, tzinfo=timezone.utc))
        result = archive_records([r1, r2, r3], mode="date")
        assert result.entries["2024-06-01"].count == 2
        assert result.entries["2024-06-02"].count == 1

    def test_no_timestamp_goes_to_unarchived(self):
        result = archive_records([make_record(timestamp=None)], mode="date")
        assert len(result.unarchived) == 1
        assert result.entries == {}


# ---------------------------------------------------------------------------
# archive_records — level mode
# ---------------------------------------------------------------------------

class TestArchiveRecordsByLevel:
    def test_records_grouped_by_level(self):
        records = [
            make_record(level="INFO"),
            make_record(level="info"),
            make_record(level="ERROR"),
        ]
        result = archive_records(records, mode="level")
        assert result.entries["INFO"].count == 2
        assert result.entries["ERROR"].count == 1

    def test_no_level_goes_to_unarchived(self):
        r = LogRecord(raw="msg", message="msg", level=None, timestamp=None)
        result = archive_records([r], mode="level")
        assert len(result.unarchived) == 1

    def test_invalid_mode_raises(self):
        with pytest.raises(ValueError, match="Unknown archive mode"):
            archive_records([], mode="hour")

    def test_total_counts_all_records(self):
        records = [make_record(level="DEBUG") for _ in range(7)]
        result = archive_records(records, mode="level")
        assert result.total == 7
