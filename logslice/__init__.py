"""logslice — Fast log file parser that filters and exports structured output.

Usage example::

    from logslice.parser import parse_line

    with open("app.log") as f:
        for line in f:
            record = parse_line(line)
            if record.is_parsed:
                print(record.timestamp, record.level, record.message)
"""

__version__ = "0.1.0"
__author__ = "logslice contributors"

from logslice.parser import LogRecord, parse_line, parse_timestamp

__all__ = ["LogRecord", "parse_line", "parse_timestamp"]
