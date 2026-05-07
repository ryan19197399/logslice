"""Tests for logslice.splitter."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest

from logslice.parser import LogRecord
from logslice.splitter import (
    SplitResult,
    split_by_field,
    split_by_level,
    split_by_pattern,
)


def make_record(
    message: str = "test message",
    level: Optional[str] = "INFO",
    ts: Optional[datetime] = None,
    extra: Optional[dict] = None,
) -> LogRecord:
    return LogRecord(
        timestamp=ts or datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        level=level,
        message=message,
        raw=message,
        extra=extra or {},
    )


class TestSplitResult:
    def test_total_counts_all_buckets_and_unmatched(self):
        result = SplitResult(
            buckets={"A": [make_record(), make_record()], "B": [make_record()]},
            unmatched=[make_record()],
        )
        assert result.total == 4

    def test_bucket_names_returns_keys(self):
        result = SplitResult(buckets={"X": [], "Y": []})
        assert set(result.bucket_names()) == {"X", "Y"}

    def test_to_dict_structure(self):
        result = SplitResult(
            buckets={"ERR": [make_record()]},
            unmatched=[make_record(), make_record()],
        )
        d = result.to_dict()
        assert d["buckets"] == {"ERR": 1}
        assert d["unmatched"] == 2
        assert d["total"] == 3


class TestSplitByLevel:
    def test_groups_by_level(self):
        records = [make_record(level="INFO"), make_record(level="ERROR"), make_record(level="info")]
        result = split_by_level(records)
        assert len(result.buckets["INFO"]) == 2
        assert len(result.buckets["ERROR"]) == 1

    def test_none_level_goes_to_unknown(self):
        result = split_by_level([make_record(level=None)])
        assert "UNKNOWN" in result.buckets

    def test_empty_input_returns_empty_result(self):
        result = split_by_level([])
        assert result.total == 0
        assert result.buckets == {}


class TestSplitByPattern:
    def test_first_matching_pattern_wins(self):
        records = [make_record(message="connection timeout error")]
        result = split_by_pattern(records, {"timeout": r"timeout", "error": r"error"})
        assert len(result.buckets.get("timeout", [])) == 1
        assert "error" not in result.buckets

    def test_unmatched_collected(self):
        records = [make_record(message="nothing special here")]
        result = split_by_pattern(records, {"error": r"error"})
        assert len(result.unmatched) == 1
        assert result.buckets == {}

    def test_case_insensitive_by_default(self):
        records = [make_record(message="FATAL: disk full")]
        result = split_by_pattern(records, {"fatal": r"fatal"})
        assert len(result.buckets["fatal"]) == 1

    def test_case_sensitive_mode(self):
        records = [make_record(message="FATAL: disk full")]
        result = split_by_pattern(records, {"fatal": r"fatal"}, case_sensitive=True)
        assert len(result.unmatched) == 1

    def test_multiple_patterns_multiple_buckets(self):
        records = [
            make_record(message="disk error"),
            make_record(message="network timeout"),
            make_record(message="all fine"),
        ]
        result = split_by_pattern(records, {"disk": r"disk", "network": r"network"})
        assert len(result.buckets["disk"]) == 1
        assert len(result.buckets["network"]) == 1
        assert len(result.unmatched) == 1


class TestSplitByField:
    def test_splits_by_extra_field(self):
        records = [
            make_record(extra={"service": "auth"}),
            make_record(extra={"service": "api"}),
            make_record(extra={"service": "auth"}),
        ]
        result = split_by_field(records, "service")
        assert len(result.buckets["auth"]) == 2
        assert len(result.buckets["api"]) == 1

    def test_missing_field_uses_default_bucket(self):
        records = [make_record(extra={})]
        result = split_by_field(records, "service")
        assert "OTHER" in result.buckets

    def test_custom_default_bucket_name(self):
        records = [make_record(extra={})]
        result = split_by_field(records, "service", default_bucket="UNKNOWN_SERVICE")
        assert "UNKNOWN_SERVICE" in result.buckets
