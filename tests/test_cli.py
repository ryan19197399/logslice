"""Tests for logslice.cli."""

from __future__ import annotations

import json
from io import StringIO
from unittest.mock import patch

import pytest

from logslice.cli import build_parser, main, parse_dt


SAMPLE_LOG = (
    "2024-03-15T10:00:00Z [INFO] startup complete\n"
    "2024-03-15T11:00:00Z [WARN] disk usage high\n"
    "2024-03-15T12:00:00Z [ERROR] connection refused\n"
    "2024-03-15T13:00:00Z [INFO] shutdown\n"
)


class TestParseDt:
    def test_date_only(self):
        dt = parse_dt("2024-03-15")
        assert dt.year == 2024 and dt.month == 3 and dt.day == 15

    def test_datetime_t_sep(self):
        dt = parse_dt("2024-03-15T12:00:00")
        assert dt.hour == 12

    def test_datetime_space_sep(self):
        dt = parse_dt("2024-03-15 08:30:00")
        assert dt.hour == 8

    def test_invalid_raises(self):
        import argparse
        with pytest.raises(argparse.ArgumentTypeError):
            parse_dt("not-a-date")


class TestBuildParser:
    def test_defaults(self):
        args = build_parser().parse_args([])
        assert args.fmt == "text"
        assert args.start is None
        assert args.end is None
        assert args.pattern is None
        assert args.count is False

    def test_format_choices(self):
        for fmt in ("text", "json", "csv"):
            args = build_parser().parse_args(["--format", fmt])
            assert args.fmt == fmt


class TestMain:
    def _run(self, argv, stdin_text=SAMPLE_LOG):
        with patch("logslice.cli.sys.stdin", StringIO(stdin_text)):
            buf = StringIO()
            with patch("logslice.cli.sys.stdout", buf):
                code = main(argv)
        return code, buf.getvalue()

    def test_no_args_returns_all_lines(self):
        code, out = self._run([])
        assert code == 0
        assert "startup complete" in out
        assert "shutdown" in out

    def test_start_filter(self):
        _, out = self._run(["--start", "2024-03-15T11:30:00"])
        assert "startup complete" not in out
        assert "connection refused" in out

    def test_end_filter(self):
        _, out = self._run(["--end", "2024-03-15T11:00:00"])
        assert "connection refused" not in out
        assert "disk usage high" in out

    def test_pattern_filter(self):
        _, out = self._run(["--pattern", "ERROR"])
        assert "connection refused" in out
        assert "startup complete" not in out

    def test_json_format(self):
        _, out = self._run(["--format", "json"])
        data = json.loads(out)
        assert isinstance(data, list)
        assert len(data) == 4

    def test_count_flag(self):
        _, out = self._run(["--count"])
        assert out.strip() == "4"

    def test_count_with_filter(self):
        _, out = self._run(["--count", "--pattern", "INFO"])
        assert out.strip() == "2"
