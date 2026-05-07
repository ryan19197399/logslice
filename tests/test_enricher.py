"""Tests for logslice.enricher."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from logslice.parser import LogRecord
from logslice.enricher import EnrichmentRule, enrich_records


def make_record(message: str, level: str = "INFO") -> LogRecord:
    return LogRecord(
        raw=message,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        level=level,
        message=message,
        extra={},
    )


# ---------------------------------------------------------------------------
# EnrichmentRule
# ---------------------------------------------------------------------------

class TestEnrichmentRule:
    def test_empty_key_raises(self):
        with pytest.raises(ValueError, match="key"):
            EnrichmentRule(key="", pattern=r"\d+")

    def test_empty_pattern_raises(self):
        with pytest.raises(ValueError, match="pattern"):
            EnrichmentRule(key="num", pattern="")

    def test_full_match_group_zero(self):
        rule = EnrichmentRule(key="code", pattern=r"ERR-\d+", value_group=0)
        assert rule.extract("got ERR-404 here") == "ERR-404"

    def test_capture_group(self):
        rule = EnrichmentRule(key="code", pattern=r"ERR-(\d+)", value_group=1)
        assert rule.extract("got ERR-404 here") == "404"

    def test_no_match_returns_default(self):
        rule = EnrichmentRule(key="code", pattern=r"ERR-\d+", default="NONE")
        assert rule.extract("everything is fine") == "NONE"

    def test_no_match_returns_none_when_no_default(self):
        rule = EnrichmentRule(key="code", pattern=r"ERR-\d+")
        assert rule.extract("everything is fine") is None

    def test_case_insensitive_by_default(self):
        rule = EnrichmentRule(key="word", pattern=r"error")
        assert rule.extract("An ERROR occurred") is not None

    def test_case_sensitive_flag(self):
        rule = EnrichmentRule(key="word", pattern=r"error", case_sensitive=True)
        assert rule.extract("An ERROR occurred") is None
        assert rule.extract("An error occurred") == "error"

    def test_invalid_group_returns_default(self):
        rule = EnrichmentRule(key="x", pattern=r"(\d+)", value_group=99, default="?")
        assert rule.extract("42") == "?"


# ---------------------------------------------------------------------------
# enrich_records
# ---------------------------------------------------------------------------

class TestEnrichRecords:
    def test_empty_input_returns_empty_result(self):
        result = enrich_records([], [])
        assert result.records == []
        assert result.total == 0
        assert result.enriched_count == 0

    def test_matching_rule_writes_to_extra(self):
        rec = make_record("Request took 123ms")
        rule = EnrichmentRule(key="duration_ms", pattern=r"(\d+)ms", value_group=1)
        result = enrich_records([rec], [rule])
        assert result.records[0].extra["duration_ms"] == "123"

    def test_non_matching_rule_leaves_extra_empty(self):
        rec = make_record("All good")
        rule = EnrichmentRule(key="duration_ms", pattern=r"(\d+)ms", value_group=1)
        result = enrich_records([rec], [rule])
        assert "duration_ms" not in result.records[0].extra

    def test_enriched_count_tracks_touched_records(self):
        records = [make_record("took 10ms"), make_record("ok"), make_record("took 5ms")]
        rule = EnrichmentRule(key="ms", pattern=r"(\d+)ms", value_group=1)
        result = enrich_records(records, [rule])
        assert result.enriched_count == 2
        assert result.total == 3

    def test_multiple_rules_applied(self):
        rec = make_record("user=alice status=200")
        rules = [
            EnrichmentRule(key="user", pattern=r"user=(\w+)", value_group=1),
            EnrichmentRule(key="status", pattern=r"status=(\d+)", value_group=1),
        ]
        result = enrich_records([rec], rules)
        assert result.records[0].extra["user"] == "alice"
        assert result.records[0].extra["status"] == "200"

    def test_to_dict_structure(self):
        records = [make_record("took 10ms"), make_record("ok")]
        rule = EnrichmentRule(key="ms", pattern=r"(\d+)ms", value_group=1)
        result = enrich_records(records, [rule])
        d = result.to_dict()
        assert d["total"] == 2
        assert d["enriched_count"] == 1
        assert d["unchanged_count"] == 1
