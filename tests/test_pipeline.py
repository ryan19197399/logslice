"""Tests for logslice.pipeline."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from logslice.filter import LogFilter
from logslice.parser import LogRecord
from logslice.pipeline import PipelineConfig, PipelineResult, run_pipeline


def dt(hour: int) -> datetime:
    return datetime(2024, 6, 1, hour, 0, 0, tzinfo=timezone.utc)


def make_record(
    message: str = "msg",
    level: str = "INFO",
    ts: datetime | None = None,
) -> LogRecord:
    return LogRecord(timestamp=ts, level=level, message=message, raw=message)


class TestRunPipelineBasic:
    def test_returns_pipeline_result(self):
        result = run_pipeline([], PipelineConfig())
        assert isinstance(result, PipelineResult)

    def test_empty_input_empty_output(self):
        result = run_pipeline([], PipelineConfig())
        assert result.count == 0
        assert result.records == []

    def test_passthrough_with_no_stages(self):
        records = [make_record("a"), make_record("b")]
        result = run_pipeline(records, PipelineConfig())
        assert result.count == 2
        assert result.filtered_count == 0


class TestPipelineFilter:
    def test_filter_removes_records_outside_range(self):
        records = [
            make_record(ts=dt(8)),
            make_record(ts=dt(10)),
            make_record(ts=dt(12)),
        ]
        log_filter = LogFilter(start=dt(9), end=dt(11))
        config = PipelineConfig(log_filter=log_filter)
        result = run_pipeline(records, config)
        assert result.count == 1
        assert result.filtered_count == 2

    def test_no_filter_sets_filtered_count_zero(self):
        records = [make_record(), make_record()]
        result = run_pipeline(records, PipelineConfig())
        assert result.filtered_count == 0


class TestPipelineDedup:
    def test_dedup_removes_duplicates(self):
        r = make_record(message="same", level="INFO", ts=dt(1))
        records = [r, r, make_record(message="other")]
        config = PipelineConfig(deduplicate=True)
        result = run_pipeline(records, config)
        assert result.count == 2
        assert result.dedup_result is not None

    def test_dedup_disabled_keeps_duplicates(self):
        r = make_record(message="same", ts=dt(1))
        result = run_pipeline([r, r], PipelineConfig(deduplicate=False))
        assert result.count == 2
        assert result.dedup_result is None


class TestPipelineSort:
    def test_sort_orders_by_timestamp(self):
        records = [make_record(ts=dt(5)), make_record(ts=dt(2)), make_record(ts=dt(9))]
        config = PipelineConfig(sort_key="timestamp", sort_order="asc")
        result = run_pipeline(records, config)
        timestamps = [r.timestamp for r in result.records]
        assert timestamps == [dt(2), dt(5), dt(9)]

    def test_no_sort_key_skips_stage(self):
        records = [make_record(message="z"), make_record(message="a")]
        result = run_pipeline(records, PipelineConfig())
        assert result.sort_result is None
        assert result.records[0].message == "z"


class TestPipelineTruncate:
    def test_truncates_long_messages(self):
        records = [make_record(message="hello world this is long")]
        config = PipelineConfig(max_message_length=10, ellipsis="...")
        result = run_pipeline(records, config)
        assert len(result.records[0].message) <= 10 + len("...")
        assert result.truncation_result is not None

    def test_no_max_length_skips_truncation(self):
        records = [make_record(message="hello world this is long")]
        result = run_pipeline(records, PipelineConfig())
        assert result.truncation_result is None
        assert result.records[0].message == "hello world this is long"


class TestPipelineChained:
    def test_all_stages_together(self):
        records = [
            make_record(message="duplicate long message here", level="DEBUG", ts=dt(7)),
            make_record(message="duplicate long message here", level="DEBUG", ts=dt(7)),
            make_record(message="keep this one", level="INFO", ts=dt(10)),
            make_record(message="too early", level="INFO", ts=dt(3)),
        ]
        config = PipelineConfig(
            log_filter=LogFilter(start=dt(6)),
            deduplicate=True,
            sort_key="timestamp",
            sort_order="asc",
            max_message_length=20,
        )
        result = run_pipeline(records, config)
        assert result.filtered_count == 1
        assert result.count == 2
        for r in result.records:
            assert len(r.message) <= 23  # 20 + len("...")
