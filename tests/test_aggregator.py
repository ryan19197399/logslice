"""Tests for logslice.aggregator."""
from datetime import datetime, timezone

import pytest

from logslice.aggregator import aggregate_records, BucketSummary, AggregationResult
from logslice.parser import LogRecord


def make_record(ts: datetime | None, level: str = "INFO", msg: str = "test") -> LogRecord:
    return LogRecord(timestamp=ts, level=level, message=msg, raw=msg)


DT = datetime(2024, 6, 15, 10, 23, 45, tzinfo=timezone.utc)


class TestAggregateRecords:
    def test_empty_returns_empty_summaries(self):
        result = aggregate_records([])
        assert result.summaries == []
        assert result.total_records == 0

    def test_single_record_creates_one_bucket(self):
        result = aggregate_records([make_record(DT)], bucket="hour")
        assert len(result.summaries) == 1
        assert result.summaries[0].total == 1

    def test_hour_bucket_truncates_minutes(self):
        r1 = make_record(datetime(2024, 6, 15, 10, 5, tzinfo=timezone.utc))
        r2 = make_record(datetime(2024, 6, 15, 10, 55, tzinfo=timezone.utc))
        result = aggregate_records([r1, r2], bucket="hour")
        assert len(result.summaries) == 1
        assert result.summaries[0].total == 2
        assert result.summaries[0].bucket_start.minute == 0

    def test_minute_bucket_separates_different_minutes(self):
        r1 = make_record(datetime(2024, 6, 15, 10, 1, 0, tzinfo=timezone.utc))
        r2 = make_record(datetime(2024, 6, 15, 10, 2, 0, tzinfo=timezone.utc))
        result = aggregate_records([r1, r2], bucket="minute")
        assert len(result.summaries) == 2

    def test_day_bucket_groups_whole_day(self):
        records = [
            make_record(datetime(2024, 6, 15, h, 0, tzinfo=timezone.utc))
            for h in range(5)
        ]
        result = aggregate_records(records, bucket="day")
        assert len(result.summaries) == 1
        assert result.summaries[0].total == 5

    def test_by_level_counts_correctly(self):
        records = [
            make_record(DT, level="INFO"),
            make_record(DT, level="ERROR"),
            make_record(DT, level="INFO"),
        ]
        result = aggregate_records(records, bucket="hour")
        by_level = result.summaries[0].by_level
        assert by_level["INFO"] == 2
        assert by_level["ERROR"] == 1

    def test_none_timestamp_grouped_separately(self):
        r_none = make_record(None)
        r_ts = make_record(DT)
        result = aggregate_records([r_none, r_ts], bucket="hour")
        assert len(result.summaries) == 2

    def test_buckets_sorted_chronologically(self):
        r1 = make_record(datetime(2024, 6, 15, 12, 0, tzinfo=timezone.utc))
        r2 = make_record(datetime(2024, 6, 15, 8, 0, tzinfo=timezone.utc))
        result = aggregate_records([r1, r2], bucket="hour")
        starts = [s.bucket_start for s in result.summaries]
        assert starts == sorted(starts)

    def test_total_records_sums_all_buckets(self):
        records = [make_record(DT)] * 7
        result = aggregate_records(records, bucket="hour")
        assert result.total_records == 7

    def test_to_dict_contains_expected_keys(self):
        result = aggregate_records([make_record(DT)], bucket="hour")
        d = result.to_dict()
        assert "bucket" in d
        assert "total_records" in d
        assert "buckets" in d
        assert d["buckets"][0]["bucket_start"] == "2024-06-15T10:00:00+00:00"

    def test_unknown_level_for_none_level(self):
        r = LogRecord(timestamp=DT, level=None, message="msg", raw="msg")
        result = aggregate_records([r], bucket="hour")
        assert "UNKNOWN" in result.summaries[0].by_level
