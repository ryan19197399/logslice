"""Tests for logslice.normalizer."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest

from logslice.normalizer import NormalisationResult, normalise_level, normalise_records
from logslice.parser import LogRecord


def make_record(level: Optional[str], message: str = "msg") -> LogRecord:
    return LogRecord(
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        level=level,
        message=message,
        raw=f"{level} {message}",
    )


# ---------------------------------------------------------------------------
# normalise_level
# ---------------------------------------------------------------------------

class TestNormaliseLevel:
    def test_info_variants(self):
        assert normalise_level("info") == "INFO"
        assert normalise_level("information") == "INFO"
        assert normalise_level("notice") == "INFO"

    def test_warning_variants(self):
        assert normalise_level("warn") == "WARNING"
        assert normalise_level("WARNING") == "WARNING"

    def test_error_variants(self):
        assert normalise_level("err") == "ERROR"
        assert normalise_level("ERROR") == "ERROR"

    def test_critical_variants(self):
        assert normalise_level("fatal") == "CRITICAL"
        assert normalise_level("emergency") == "CRITICAL"
        assert normalise_level("crit") == "CRITICAL"

    def test_debug_variants(self):
        assert normalise_level("dbg") == "DEBUG"
        assert normalise_level("debug") == "DEBUG"

    def test_trace_variant(self):
        assert normalise_level("trace") == "TRACE"

    def test_unknown_returned_as_is(self):
        assert normalise_level("VERBOSE") == "VERBOSE"

    def test_none_returns_none(self):
        assert normalise_level(None) is None

    def test_strips_whitespace(self):
        assert normalise_level("  warn  ") == "WARNING"


# ---------------------------------------------------------------------------
# normalise_records
# ---------------------------------------------------------------------------

class TestNormaliseRecords:
    def test_empty_input_returns_empty_result(self):
        result = normalise_records([])
        assert isinstance(result, NormalisationResult)
        assert result.total == 0
        assert result.records == []

    def test_known_level_is_canonicalised(self):
        records = [make_record("warn"), make_record("fatal")]
        result = normalise_records(records)
        assert result.records[0].level == "WARNING"
        assert result.records[1].level == "CRITICAL"

    def test_changed_count_incremented(self):
        records = [make_record("warn"), make_record("INFO")]
        result = normalise_records(records)
        assert result.changed_count == 1
        assert result.unchanged_count == 1

    def test_unknown_kept_without_fallback(self):
        records = [make_record("VERBOSE")]
        result = normalise_records(records)
        assert result.records[0].level == "VERBOSE"
        assert result.unchanged_count == 1

    def test_unknown_replaced_with_fallback(self):
        records = [make_record("VERBOSE")]
        result = normalise_records(records, unknown_fallback="UNKNOWN")
        assert result.records[0].level == "UNKNOWN"
        assert result.changed_count == 1

    def test_none_level_with_fallback(self):
        records = [make_record(None)]
        result = normalise_records(records, unknown_fallback="UNKNOWN")
        assert result.records[0].level == "UNKNOWN"

    def test_to_dict_structure(self):
        result = normalise_records([make_record("warn"), make_record("INFO")])
        d = result.to_dict()
        assert d["total"] == 2
        assert d["changed"] == 1
        assert d["unchanged"] == 1

    def test_original_records_not_mutated(self):
        original = make_record("warn")
        normalise_records([original])
        assert original.level == "warn"
