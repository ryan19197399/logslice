"""Tests for logslice.tagger."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from logslice.parser import LogRecord
from logslice.tagger import TagRule, TaggingResult, tag_records


def make_record(message: str, extras: dict | None = None) -> LogRecord:
    return LogRecord(
        raw=message,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        level="INFO",
        message=message,
        extras=extras,
    )


class TestTagRule:
    def test_empty_tag_raises(self):
        with pytest.raises(ValueError, match="tag"):
            TagRule(pattern="error", tag="")

    def test_empty_pattern_raises(self):
        with pytest.raises(ValueError, match="pattern"):
            TagRule(pattern="", tag="important")

    def test_matches_case_insensitive_by_default(self):
        rule = TagRule(pattern="ERROR", tag="err")
        assert rule.matches(make_record("an error occurred"))

    def test_no_match_returns_false(self):
        rule = TagRule(pattern="timeout", tag="slow")
        assert not rule.matches(make_record("everything is fine"))

    def test_case_sensitive_flag_respected(self):
        rule = TagRule(pattern="ERROR", tag="err", case_sensitive=True)
        assert not rule.matches(make_record("an error occurred"))
        assert rule.matches(make_record("an ERROR occurred"))


class TestTagRecords:
    def test_empty_input_returns_empty_result(self):
        result = tag_records([], [TagRule(pattern="x", tag="t")])
        assert result.total == 0
        assert result.tag_counts == {}

    def test_no_rules_records_unchanged(self):
        records = [make_record("hello"), make_record("world")]
        result = tag_records(records, [])
        assert result.total == 2
        assert result.tag_counts == {}

    def test_matching_rule_adds_tag(self):
        rule = TagRule(pattern="timeout", tag="slow")
        result = tag_records([make_record("connection timeout")], [rule])
        assert result.records[0].extras["tags"] == ["slow"]

    def test_non_matching_record_has_no_tags(self):
        rule = TagRule(pattern="timeout", tag="slow")
        result = tag_records([make_record("all good")], [rule])
        assert "tags" not in (result.records[0].extras or {})

    def test_multiple_rules_can_match_same_record(self):
        rules = [
            TagRule(pattern="error", tag="err"),
            TagRule(pattern="critical", tag="crit"),
        ]
        result = tag_records([make_record("critical error detected")], rules)
        tags = result.records[0].extras["tags"]
        assert "err" in tags
        assert "crit" in tags

    def test_tag_counts_aggregated_correctly(self):
        rule = TagRule(pattern="fail", tag="failure")
        records = [make_record("fail 1"), make_record("ok"), make_record("fail 2")]
        result = tag_records(records, [rule])
        assert result.tag_counts["failure"] == 2

    def test_existing_tags_preserved(self):
        rule = TagRule(pattern="new", tag="new-tag")
        rec = make_record("something new", extras={"tags": ["existing"]})
        result = tag_records([rec], [rule])
        assert "existing" in result.records[0].extras["tags"]
        assert "new-tag" in result.records[0].extras["tags"]

    def test_duplicate_tags_not_added_twice(self):
        rule = TagRule(pattern="warn", tag="warning")
        rec = make_record("warn", extras={"tags": ["warning"]})
        result = tag_records([rec], [rule])
        assert result.records[0].extras["tags"].count("warning") == 1

    def test_to_dict_structure(self):
        rule = TagRule(pattern="ok", tag="healthy")
        result = tag_records([make_record("ok")], [rule])
        d = result.to_dict()
        assert "total" in d
        assert "tag_counts" in d
        assert d["tag_counts"]["healthy"] == 1
