"""Tests for logslice.router."""
from __future__ import annotations

import pytest

from logslice.parser import LogRecord
from logslice.router import RouteRule, RoutingResult, route_records


def make_record(message: str, level: str = "INFO") -> LogRecord:
    return LogRecord(raw=message, timestamp=None, level=level, message=message)


# ---------------------------------------------------------------------------
# RouteRule
# ---------------------------------------------------------------------------

class TestRouteRule:
    def test_empty_channel_raises(self):
        with pytest.raises(ValueError, match="channel"):
            RouteRule(channel="", pattern="error")

    def test_empty_pattern_raises(self):
        with pytest.raises(ValueError, match="pattern"):
            RouteRule(channel="errors", pattern="")

    def test_matches_case_insensitive_by_default(self):
        rule = RouteRule(channel="errors", pattern="ERROR")
        assert rule.matches(make_record("An error occurred"))

    def test_no_match_returns_false(self):
        rule = RouteRule(channel="errors", pattern="critical")
        assert not rule.matches(make_record("everything is fine"))

    def test_case_sensitive_flag(self):
        rule = RouteRule(channel="errors", pattern="ERROR", case_sensitive=True)
        assert not rule.matches(make_record("an error happened"))
        assert rule.matches(make_record("an ERROR happened"))


# ---------------------------------------------------------------------------
# RoutingResult helpers
# ---------------------------------------------------------------------------

class TestRoutingResult:
    def test_total_counts_all(self):
        r = RoutingResult(
            channels={"a": [make_record("x"), make_record("y")], "b": [make_record("z")]},
            unrouted=[make_record("u")],
        )
        assert r.total == 4

    def test_channel_names(self):
        r = RoutingResult(channels={"alpha": [], "beta": []}, unrouted=[])
        assert set(r.channel_names()) == {"alpha", "beta"}

    def test_to_dict_structure(self):
        r = RoutingResult(
            channels={"errors": [make_record("e")]},
            unrouted=[make_record("u"), make_record("v")],
        )
        d = r.to_dict()
        assert d["channels"] == {"errors": 1}
        assert d["unrouted"] == 2
        assert d["total"] == 3


# ---------------------------------------------------------------------------
# route_records
# ---------------------------------------------------------------------------

class TestRouteRecords:
    def test_empty_input_returns_empty_result(self):
        result = route_records([], [RouteRule(channel="errors", pattern="error")])
        assert result.total == 0
        assert result.unrouted == []

    def test_matching_record_placed_in_channel(self):
        rules = [RouteRule(channel="errors", pattern="error")]
        records = [make_record("disk error detected")]
        result = route_records(records, rules)
        assert len(result.channels["errors"]) == 1
        assert result.unrouted == []

    def test_non_matching_record_goes_to_unrouted(self):
        rules = [RouteRule(channel="errors", pattern="error")]
        records = [make_record("all good")]
        result = route_records(records, rules)
        assert len(result.unrouted) == 1
        assert result.channels["errors"] == []

    def test_first_matching_rule_wins(self):
        rules = [
            RouteRule(channel="critical", pattern="critical"),
            RouteRule(channel="errors", pattern="error"),
        ]
        records = [make_record("critical error")]
        result = route_records(records, rules)
        assert len(result.channels["critical"]) == 1
        assert result.channels["errors"] == []

    def test_default_channel_captures_unmatched(self):
        rules = [RouteRule(channel="errors", pattern="error")]
        records = [make_record("info: started")]
        result = route_records(records, rules, default_channel="misc")
        assert len(result.channels["misc"]) == 1
        assert result.unrouted == []

    def test_multiple_records_split_across_channels(self):
        rules = [
            RouteRule(channel="errors", pattern="error"),
            RouteRule(channel="warnings", pattern="warn"),
        ]
        records = [
            make_record("error: disk full"),
            make_record("warn: low memory"),
            make_record("info: ok"),
        ]
        result = route_records(records, rules)
        assert len(result.channels["errors"]) == 1
        assert len(result.channels["warnings"]) == 1
        assert len(result.unrouted) == 1
