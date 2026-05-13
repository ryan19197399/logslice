"""Tests for logslice.masker."""
from __future__ import annotations

import pytest

from logslice.masker import MaskRule, MaskingResult, mask_records
from logslice.parser import LogRecord


def make_record(message: str, level: str = "INFO") -> LogRecord:
    return LogRecord(timestamp=None, level=level, message=message, raw=message)


# ---------------------------------------------------------------------------
# MaskRule unit tests
# ---------------------------------------------------------------------------

class TestMaskRule:
    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="name"):
            MaskRule(name="", pattern=r"\d+")

    def test_empty_pattern_raises(self):
        with pytest.raises(ValueError, match="pattern"):
            MaskRule(name="digits", pattern="")

    def test_empty_placeholder_raises(self):
        with pytest.raises(ValueError, match="placeholder"):
            MaskRule(name="digits", pattern=r"\d+", placeholder="")

    def test_replaces_match(self):
        rule = MaskRule(name="ip", pattern=r"\d{1,3}(\.\d{1,3}){3}", placeholder="<IP>")
        assert rule.apply("connect from 192.168.1.1") == "connect from <IP>"

    def test_no_match_returns_original(self):
        rule = MaskRule(name="email", pattern=r"[\w.]+@[\w.]+")
        assert rule.apply("no email here") == "no email here"

    def test_case_insensitive_by_default(self):
        rule = MaskRule(name="token", pattern=r"token=\S+")
        result = rule.apply("TOKEN=abc123")
        assert result == "***"

    def test_case_sensitive_mode(self):
        rule = MaskRule(name="token", pattern=r"token=\S+", case_sensitive=True)
        # uppercase should NOT match in case-sensitive mode
        assert rule.apply("TOKEN=abc123") == "TOKEN=abc123"
        assert rule.apply("token=abc123") == "***"

    def test_multiple_matches_all_replaced(self):
        rule = MaskRule(name="digits", pattern=r"\d+", placeholder="#")
        assert rule.apply("user 42 has 7 items") == "user # has # items"

    def test_custom_placeholder(self):
        rule = MaskRule(name="pw", pattern=r"password=\S+", placeholder="[REDACTED]")
        assert rule.apply("password=secret") == "[REDACTED]"


# ---------------------------------------------------------------------------
# mask_records tests
# ---------------------------------------------------------------------------

class TestMaskRecords:
    def test_empty_input_returns_empty_result(self):
        result = mask_records([], rules=[MaskRule(name="r", pattern=r"\d+")])
        assert result.records == []
        assert result.total == 0
        assert result.masked_count == 0

    def test_no_rules_returns_all_records_unchanged(self):
        records = [make_record("hello 123"), make_record("world")]
        result = mask_records(records, rules=[])
        assert result.total == 2
        assert result.masked_count == 0
        assert result.records[0].message == "hello 123"

    def test_masked_count_incremented_only_for_changed(self):
        rules = [MaskRule(name="num", pattern=r"\d+", placeholder="#")]
        records = [make_record("abc"), make_record("abc 99"), make_record("xyz")]
        result = mask_records(records, rules=rules)
        assert result.total == 3
        assert result.masked_count == 1

    def test_message_is_replaced_in_output_record(self):
        rules = [MaskRule(name="email", pattern=r"[\w.]+@[\w.]+", placeholder="<EMAIL>")]
        records = [make_record("sent to user@example.com today")]
        result = mask_records(records, rules=rules)
        assert result.records[0].message == "sent to <EMAIL> today"

    def test_original_record_raw_preserved(self):
        rules = [MaskRule(name="num", pattern=r"\d+", placeholder="#")]
        rec = make_record("value=42")
        result = mask_records([rec], rules=rules)
        assert result.records[0].raw == "value=42"

    def test_to_dict_keys(self):
        result = MaskingResult(total=10, masked_count=3)
        d = result.to_dict()
        assert set(d.keys()) == {"total", "masked_count", "unchanged_count"}
        assert d["unchanged_count"] == 7

    def test_multiple_rules_applied_in_order(self):
        rules = [
            MaskRule(name="ip", pattern=r"\d{1,3}(\.\d{1,3}){3}", placeholder="<IP>"),
            MaskRule(name="port", pattern=r"port \d+", placeholder="port <PORT>"),
        ]
        records = [make_record("connect 10.0.0.1 port 8080")]
        result = mask_records(records, rules=rules)
        assert "<IP>" in result.records[0].message
        assert "<PORT>" in result.records[0].message
