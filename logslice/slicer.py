"""High-level API: parse a log file and return filtered records."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional, Union

from logslice.filter import LogFilter
from logslice.parser import LogRecord, parse_line


def iter_records(source: Union[str, Path]) -> Iterator[LogRecord]:
    """Parse a log file line by line, yielding LogRecord objects."""
    path = Path(source)
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if line:
                yield parse_line(line)


def slice_log(
    source: Union[str, Path],
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    pattern: Optional[str] = None,
    level: Optional[str] = None,
) -> Iterator[LogRecord]:
    """Parse *source* and yield only records that pass the given filters.

    Parameters
    ----------
    source:
        Path to the log file.
    start:
        Inclusive lower bound on record timestamps.
    end:
        Inclusive upper bound on record timestamps.
    pattern:
        Regular expression that must match the raw log line.
    level:
        Log level string (e.g. ``"ERROR"``) to filter on.
    """
    log_filter = LogFilter(start=start, end=end, pattern=pattern, level=level)
    return log_filter.apply(iter_records(source))


def count_matches(
    source: Union[str, Path],
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    pattern: Optional[str] = None,
    level: Optional[str] = None,
) -> int:
    """Return the number of records in *source* that pass the given filters."""
    return sum(1 for _ in slice_log(source, start=start, end=end, pattern=pattern, level=level))
