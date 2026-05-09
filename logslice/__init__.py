"""logslice — Fast log file parser that filters and exports structured output."""

from logslice.parser import LogRecord, parse_line, parse_timestamp
from logslice.filter import LogFilter
from logslice.slicer import iter_records, slice_log, count_matches
from logslice.exporter import export
from logslice.router import RouteRule, RoutingResult, route_records

__all__ = [
    "LogRecord",
    "parse_line",
    "parse_timestamp",
    "LogFilter",
    "iter_records",
    "slice_log",
    "count_matches",
    "export",
    "RouteRule",
    "RoutingResult",
    "route_records",
]
