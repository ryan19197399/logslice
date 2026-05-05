"""Tests for logslice.highlighter."""
import re

import pytest

from logslice.highlighter import (
    COLOURS,
    HighlightRule,
    Highlighter,
    make_highlighter,
)


class TestHighlightRule:
    def test_valid_colour_accepted(self):
        rule = HighlightRule(pattern="error", colour="red")
        assert rule.colour == "red"

    def test_invalid_colour_raises(self):
        with pytest.raises(ValueError, match="Unknown colour"):
            HighlightRule(pattern="error", colour="ultraviolet")

    def test_regex_compiled(self):
        rule = HighlightRule(pattern=r"\d+", colour="cyan")
        assert rule.regex.search("line 42")

    def test_case_insensitive_by_default(self):
        rule = HighlightRule(pattern="error")
        assert rule.regex.search("ERROR")


class TestHighlighter:
    def test_no_rules_returns_original(self):
        h = Highlighter()
        assert h.highlight("hello world") == "hello world"

    def test_single_rule_wraps_match(self):
        h = Highlighter()
        h.add_rule("error", "red")
        result = h.highlight("an error occurred")
        assert COLOURS["red"] in result
        assert COLOURS["reset"] in result
        assert "error" in result

    def test_disabled_returns_plain_text(self):
        h = Highlighter(enabled=False)
        h.add_rule("error", "red")
        result = h.highlight("an error occurred")
        assert COLOURS["red"] not in result

    def test_multiple_rules_applied(self):
        h = Highlighter()
        h.add_rule("error", "red")
        h.add_rule(r"\d+", "cyan")
        result = h.highlight("error on line 42")
        assert COLOURS["red"] in result
        assert COLOURS["cyan"] in result

    def test_highlight_lines(self):
        h = Highlighter()
        h.add_rule("warn", "yellow")
        lines = ["info ok", "warn something", "debug ok"]
        out = h.highlight_lines(lines)
        assert COLOURS["yellow"] not in out[0]
        assert COLOURS["yellow"] in out[1]
        assert COLOURS["yellow"] not in out[2]

    def test_no_match_unchanged(self):
        h = Highlighter()
        h.add_rule("error")
        result = h.highlight("everything is fine")
        assert result == "everything is fine"


class TestMakeHighlighter:
    def test_returns_highlighter(self):
        h = make_highlighter(["error"])
        assert isinstance(h, Highlighter)

    def test_empty_patterns(self):
        h = make_highlighter()
        assert h.rules == []

    def test_enabled_false(self):
        h = make_highlighter(["error"], enabled=False)
        assert not h.enabled

    def test_custom_colour(self):
        h = make_highlighter(["warn"], colour="magenta")
        assert h.rules[0].colour == "magenta"
