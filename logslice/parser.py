"""Core log line parser for logslice.

Parses individual log lines into structured records, supporting common
log formats with timestamp, level, and message extraction.
"""

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

# Common log format patterns
DEFAULT_PATTERNS = [
    # ISO 8601: 2024-01-15T13:45:00.123Z ERROR Some message
    r"(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\s+(?P<level>\w+)\s+(?P<message>.*)",
    # Common log: 2024-01-15 13:45:00 [ERROR] Some message
    r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)\s+\[?(?P<level>\w+)\]?\s+(?P<message>.*)",
    # Syslog-like: Jan 15 13:45:00 hostname process[pid]: message
    r"(?P<timestamp>[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+\S+\s+\S+:\s+(?P<message>.*)",
]

TIMESTAMP_FORMATS = [
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%b %d %H:%M:%S",
]


@dataclass
class LogRecord:
    """A parsed log line with structured fields."""
    raw: str
    timestamp: Optional[datetime] = None
    level: Optional[str] = None
    message: Optional[str] = None
    extra: dict = field(default_factory=dict)

    @property
    def is_parsed(self) -> bool:
        return self.timestamp is not None


def parse_timestamp(ts_str: str) -> Optional[datetime]:
    """Attempt to parse a timestamp string into a datetime object."""
    ts_str = ts_str.strip()
    for fmt in TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    return None


def parse_line(line: str, patterns: Optional[list] = None) -> LogRecord:
    """Parse a single log line into a LogRecord.

    Args:
        line: Raw log line string.
        patterns: Optional list of regex pattern strings to try.

    Returns:
        A LogRecord with extracted fields where possible.
    """
    patterns = patterns or DEFAULT_PATTERNS
    stripped = line.rstrip("\n")

    for pattern in patterns:
        match = re.match(pattern, stripped)
        if match:
            groups = match.groupdict()
            timestamp = parse_timestamp(groups.get("timestamp", "")) if groups.get("timestamp") else None
            return LogRecord(
                raw=stripped,
                timestamp=timestamp,
                level=groups.get("level"),
                message=groups.get("message", stripped),
            )

    return LogRecord(raw=stripped, message=stripped)
