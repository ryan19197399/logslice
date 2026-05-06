"""Filter log records by time range or pattern."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Iterable, Iterator, Optional

from logslice.parser import LogRecord


class LogFilter:
    """Filter LogRecord objects by time range and/or regex pattern."""

    def __init__(
        self,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        pattern: Optional[str] = None,
        level: Optional[str] = None,
    ) -> None:
        self.start = start
        self.end = end
        try:
            self.pattern = re.compile(pattern) if pattern else None
        except re.error as exc:
            raise ValueError(f"Invalid regex pattern {pattern!r}: {exc}") from exc
        self.level = level.upper() if level else None

    def matches(self, record: LogRecord) -> bool:
        """Return True if the record passes all active filters."""
        if self.start and record.timestamp and record.timestamp < self.start:
            return False
        if self.end and record.timestamp and record.timestamp > self.end:
            return False
        if self.level and record.level and record.level.upper() != self.level:
            return False
        if self.pattern and not self.pattern.search(record.raw):
            return False
        return True

    def apply(self, records: Iterable[LogRecord]) -> Iterator[LogRecord]:
        """Yield records that match all filters."""
        for record in records:
            if self.matches(record):
                yield record


def filter_records(
    records: Iterable[LogRecord],
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    pattern: Optional[str] = None,
    level: Optional[str] = None,
) -> Iterator[LogRecord]:
    """Convenience function to filter an iterable of LogRecords."""
    f = LogFilter(start=start, end=end, pattern=pattern, level=level)
    return f.apply(records)
