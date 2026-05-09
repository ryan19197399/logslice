"""Route log records to named output channels based on pattern rules."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

from logslice.parser import LogRecord


@dataclass
class RouteRule:
    channel: str
    pattern: str
    case_sensitive: bool = False

    def __post_init__(self) -> None:
        if not self.channel:
            raise ValueError("channel must not be empty")
        if not self.pattern:
            raise ValueError("pattern must not be empty")

    @property
    def regex(self) -> re.Pattern:
        flags = 0 if self.case_sensitive else re.IGNORECASE
        return re.compile(self.pattern, flags)

    def matches(self, record: LogRecord) -> bool:
        return bool(self.regex.search(record.message))


@dataclass
class RoutingResult:
    channels: Dict[str, List[LogRecord]] = field(default_factory=dict)
    unrouted: List[LogRecord] = field(default_factory=list)

    @property
    def total(self) -> int:
        return sum(len(v) for v in self.channels.values()) + len(self.unrouted)

    def channel_names(self) -> List[str]:
        return list(self.channels.keys())

    def to_dict(self) -> dict:
        return {
            "channels": {k: len(v) for k, v in self.channels.items()},
            "unrouted": len(self.unrouted),
            "total": self.total,
        }


def route_records(
    records: Iterable[LogRecord],
    rules: List[RouteRule],
    *,
    default_channel: Optional[str] = None,
) -> RoutingResult:
    """Route each record to the first matching channel.

    If no rule matches and *default_channel* is given, the record is placed
    in that channel; otherwise it lands in ``unrouted``.
    """
    result = RoutingResult()
    for rule in rules:
        result.channels.setdefault(rule.channel, [])
    if default_channel:
        result.channels.setdefault(default_channel, [])

    for record in records:
        matched = False
        for rule in rules:
            if rule.matches(record):
                result.channels[rule.channel].append(record)
                matched = True
                break
        if not matched:
            if default_channel:
                result.channels[default_channel].append(record)
            else:
                result.unrouted.append(record)

    return result
