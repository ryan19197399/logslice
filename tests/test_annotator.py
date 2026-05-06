"""Tests for logslice.annotator."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest

from logslice.annotator import AnnotationRule, annotate_records
from logslice.parser import LogRecord


def make_record(
    message: str = "hello world",
    level: str = "INFO",
    extra: Optional[dict] = None,
) -> LogRecord:
    return LogRecord(
        timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        level=level,
        message=message,
        raw=message,
        extra=extra or {},
    )


class TestAnnotationRule:
    def test_empty_tag_raises(self):
        with pytest.raises(ValueError, match="tag"):
            AnnotationRule(tag="", pattern="error")

    def test_matches_case_insensitive_by_default(self):
        rule = AnnotationRule(tag="error", pattern="ERROR")
        record = make_record(message="An error occurred")
        assert rule.matches(record)

    def test_no_match_returns_false(self):
        rule = AnnotationRule(tag="critical", pattern="critical")
        record = make_record(message="everything is fine")
        assert not rule.matches(record)

    def test_case_sensitive_flag_respected(self):
        rule = AnnotationRule(tag="upper", pattern="ERROR", case_sensitive=True)
        assert rule.matches(make_record(message="ERROR here"))
        assert not rule.matches(make_record(message="error here"))


class TestAnnotateRecords:
    def test_empty_input_returns_empty_result(self):
        result = annotate_records([], [])
        assert result.records == []
        assert result.annotated_count == 0
        assert result.total == 0

    def test_no_rules_leaves_records_unchanged(self):
        records = [make_record(), make_record(message="boom")]
        result = annotate_records(records, [])
        assert result.annotated_count == 0
        assert result.total == 2

    def test_matching_rule_adds_tag(self):
        rule = AnnotationRule(tag="error", pattern="error")
        record = make_record(message="An error occurred")
        result = annotate_records([record], [rule])
        assert result.annotated_count == 1
        assert "error" in result.records[0].extra["tags"]

    def test_non_matching_record_has_no_tags(self):
        rule = AnnotationRule(tag="error", pattern="error")
        record = make_record(message="all good")
        result = annotate_records([record], [rule])
        assert result.annotated_count == 0
        assert "tags" not in result.records[0].extra

    def test_multiple_rules_multiple_tags(self):
        rules = [
            AnnotationRule(tag="error", pattern="error"),
            AnnotationRule(tag="network", pattern="timeout"),
        ]
        record = make_record(message="network error timeout")
        result = annotate_records([record], rules)
        tags = result.records[0].extra["tags"]
        assert "error" in tags
        assert "network" in tags

    def test_existing_tags_are_preserved(self):
        rule = AnnotationRule(tag="new", pattern="new")
        record = make_record(message="new message", extra={"tags": ["existing"]})
        result = annotate_records([record], [rule])
        tags = result.records[0].extra["tags"]
        assert "existing" in tags
        assert "new" in tags

    def test_duplicate_tags_deduplicated(self):
        rule = AnnotationRule(tag="error", pattern="error")
        record = make_record(message="error again", extra={"tags": ["error"]})
        result = annotate_records([record], [rule])
        assert result.records[0].extra["tags"].count("error") == 1

    def test_custom_tag_field(self):
        rule = AnnotationRule(tag="warn", pattern="warn")
        record = make_record(message="warning issued")
        result = annotate_records([record], [rule], tag_field="labels")
        assert "warn" in result.records[0].extra["labels"]
        assert "tags" not in result.records[0].extra
