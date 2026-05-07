"""Tests for logslice.validator."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from logslice.parser import LogRecord
from logslice.validator import ValidationRule, Violation, validate_records


def make_record(message: str, level: str = "INFO") -> LogRecord:
    return LogRecord(
        raw=message,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        level=level,
        message=message,
        extra={},
    )


# ---------------------------------------------------------------------------
# ValidationRule
# ---------------------------------------------------------------------------

class TestValidationRule:
    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="name"):
            ValidationRule(name="", pattern=r"ok")

    def test_empty_pattern_raises(self):
        with pytest.raises(ValueError, match="pattern"):
            ValidationRule(name="r", pattern="")

    def test_must_match_passes_when_found(self):
        rule = ValidationRule(name="has_id", pattern=r"id=\d+", must_match=True)
        rec = make_record("id=42 processed")
        assert rule.check(rec) is None

    def test_must_match_fails_when_not_found(self):
        rule = ValidationRule(name="has_id", pattern=r"id=\d+", must_match=True)
        rec = make_record("no identifier here")
        assert rule.check(rec) is not None

    def test_must_not_match_passes_when_absent(self):
        rule = ValidationRule(name="no_error", pattern=r"error", must_match=False)
        rec = make_record("everything is fine")
        assert rule.check(rec) is None

    def test_must_not_match_fails_when_present(self):
        rule = ValidationRule(name="no_error", pattern=r"error", must_match=False)
        rec = make_record("an error occurred")
        assert rule.check(rec) is not None

    def test_case_insensitive_by_default(self):
        rule = ValidationRule(name="r", pattern=r"error", must_match=True)
        assert rule.check(make_record("ERROR happened")) is None

    def test_case_sensitive_flag(self):
        rule = ValidationRule(name="r", pattern=r"error", must_match=True, case_sensitive=True)
        assert rule.check(make_record("ERROR happened")) is not None
        assert rule.check(make_record("error happened")) is None

    def test_violation_message_contains_rule_name(self):
        rule = ValidationRule(name="must_have_id", pattern=r"id=\d+", must_match=True)
        msg = rule.check(make_record("no id"))
        assert "must_have_id" in msg


# ---------------------------------------------------------------------------
# validate_records
# ---------------------------------------------------------------------------

class TestValidateRecords:
    def test_empty_input_returns_empty_result(self):
        result = validate_records([], [])
        assert result.total == 0
        assert result.violation_count == 0

    def test_no_rules_all_records_valid(self):
        records = [make_record("a"), make_record("b")]
        result = validate_records(records, [])
        assert len(result.valid) == 2
        assert result.violation_count == 0

    def test_passing_records_go_to_valid(self):
        rule = ValidationRule(name="has_ok", pattern=r"ok", must_match=True)
        records = [make_record("ok done"), make_record("ok great")]
        result = validate_records(records, [rule])
        assert len(result.valid) == 2
        assert result.violation_count == 0

    def test_failing_records_go_to_violations(self):
        rule = ValidationRule(name="has_ok", pattern=r"ok", must_match=True)
        records = [make_record("ok done"), make_record("failed badly")]
        result = validate_records(records, [rule])
        assert len(result.valid) == 1
        assert result.violation_count == 1

    def test_violation_carries_record_and_rule_name(self):
        rule = ValidationRule(name="has_ok", pattern=r"ok", must_match=True)
        rec = make_record("failed")
        result = validate_records([rec], [rule])
        v = result.violations[0]
        assert v.rule_name == "has_ok"
        assert v.record is rec

    def test_multiple_rules_one_failing_marks_violation(self):
        rules = [
            ValidationRule(name="has_id", pattern=r"id=\d+", must_match=True),
            ValidationRule(name="no_error", pattern=r"error", must_match=False),
        ]
        rec = make_record("id=1 but error here")
        result = validate_records([rec], rules)
        assert result.violation_count == 1
        assert len(result.valid) == 0

    def test_to_dict_structure(self):
        rule = ValidationRule(name="has_ok", pattern=r"ok", must_match=True)
        records = [make_record("ok"), make_record("fail")]
        result = validate_records(records, [rule])
        d = result.to_dict()
        assert d["total"] == 2
        assert d["valid_count"] == 1
        assert d["violation_count"] == 1
