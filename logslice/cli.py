"""Command-line interface for logslice."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from typing import Optional

from logslice.exporter import OutputFormat, export
from logslice.filter import LogFilter
from logslice.slicer import iter_records


def parse_dt(value: str) -> datetime:
    """Parse an ISO-8601 datetime string, attaching UTC if no tzinfo."""
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"Cannot parse datetime: {value!r}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="logslice",
        description="Filter and export structured log output by time range or pattern.",
    )
    p.add_argument("file", nargs="?", help="Log file to read (default: stdin)")
    p.add_argument("--start", metavar="DATETIME", type=parse_dt, help="Include records at or after this time")
    p.add_argument("--end", metavar="DATETIME", type=parse_dt, help="Include records at or before this time")
    p.add_argument("--pattern", metavar="REGEX", help="Only include lines matching this pattern")
    p.add_argument(
        "--format",
        dest="fmt",
        choices=("text", "json", "csv"),
        default="text",
        help="Output format (default: text)",
    )
    p.add_argument("--count", action="store_true", help="Print match count instead of records")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    log_filter = LogFilter(
        start=args.start,
        end=args.end,
        pattern=args.pattern,
    )

    if args.file:
        source = open(args.file, "r", encoding="utf-8", errors="replace")
    else:
        source = sys.stdin

    try:
        records = list(log_filter.apply(iter_records(source)))
        if args.count:
            print(len(records))
        else:
            export(records, fmt=args.fmt, output=sys.stdout)
    finally:
        if args.file:
            source.close()

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
