"""Tests for logslice.redactor."""
from __future__ import annotations

import pytest

from logslice.parser import LogRecord
from logslice.redactor import (
    BUILTIN_PATTERNS,
    RedactionRule,
    redact_records,
)


def make_record(message: str, level: str = "INFO") -> LogRecord:
    return LogRecord(timestamp=None, level=level, message=message, raw=message)


# ---------------------------------------------------------------------------
# RedactionRule unit tests
# ---------------------------------------------------------------------------

class TestRedactionRule:
    def test_replaces_match(self):
        rule = RedactionRule(name="ip", pattern=r"\d+\.\d+\.\d+\.\d+")
        result, count = rule.apply("Connected from 192.168.1.1")
        assert "[REDACTED]" in result
        assert count == 1

    def test_no_match_returns_original(self):
        rule = RedactionRule(name="ip", pattern=r"\d+\.\d+\.\d+\.\d+")
        result, count = rule.apply("No IP here")
        assert result == "No IP here"
        assert count == 0

    def test_custom_replacement(self):
        rule = RedactionRule(name="x", pattern=r"secret", replacement="***")
        result, count = rule.apply("my secret token")
        assert result == "my *** token"
        assert count == 1

    def test_multiple_matches_counted(self):
        rule = RedactionRule(name="digit", pattern=r"\d+")
        _, count = rule.apply("123 and 456 and 789")
        assert count == 3


# ---------------------------------------------------------------------------
# redact_records tests
# ---------------------------------------------------------------------------

class TestRedactRecords:
    def test_empty_input_returns_empty(self):
        result = redact_records([], builtin_names=["ipv4"])
        assert result.records == []
        assert result.total_redactions == 0
        assert result.affected_records == 0

    def test_no_rules_returns_unchanged(self):
        records = [make_record("hello world")]
        result = redact_records(records)
        assert result.records[0].message == "hello world"
        assert result.total_redactions == 0

    def test_builtin_ipv4_redacted(self):
        records = [make_record("Request from 10.0.0.1 received")]
        result = redact_records(records, builtin_names=["ipv4"])
        assert "[REDACTED]" in result.records[0].message
        assert result.total_redactions == 1
        assert result.affected_records == 1

    def test_builtin_email_redacted(self):
        records = [make_record("User user@example.com logged in")]
        result = redact_records(records, builtin_names=["email"])
        assert "[REDACTED]" in result.records[0].message
        assert result.total_redactions >= 1

    def test_unaffected_records_not_counted(self):
        records = [
            make_record("clean message"),
            make_record("ip: 1.2.3.4"),
        ]
        result = redact_records(records, builtin_names=["ipv4"])
        assert result.affected_records == 1
        assert result.records[0].message == "clean message"

    def test_custom_pattern_applied(self):
        records = [make_record("token=abc123xyz")]
        result = redact_records(
            records, custom_patterns=[("token", r"token=[A-Za-z0-9]+")]
        )
        assert "[REDACTED]" in result.records[0].message
        assert result.total_redactions == 1

    def test_multiple_rules_combined(self):
        records = [make_record("ip 1.2.3.4 email foo@bar.com")]
        result = redact_records(records, builtin_names=["ipv4", "email"])
        assert result.total_redactions == 2

    def test_unknown_builtin_raises(self):
        with pytest.raises(ValueError, match="Unknown built-in"):
            redact_records([], builtin_names=["nonexistent"])

    def test_original_record_not_mutated(self):
        original = make_record("ip 9.9.9.9")
        redact_records([original], builtin_names=["ipv4"])
        assert original.message == "ip 9.9.9.9"
