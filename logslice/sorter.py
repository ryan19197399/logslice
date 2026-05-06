"""Sorting utilities for log records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Iterator, List, Literal

from logslice.parser import LogRecord

SortKey = Literal["timestamp", "level", "message"]
SortOrder = Literal["asc", "desc"]


@dataclass(frozen=True)
class SortResult:
    records: List[LogRecord]
    sort_key: SortKey
    order: SortOrder

    @property
    def count(self) -> int:
        return len(self.records)

    def __iter__(self) -> Iterator[LogRecord]:
        return iter(self.records)


_LEVEL_ORDER = {
    "debug": 0,
    "info": 1,
    "warning": 2,
    "warn": 2,
    "error": 3,
    "critical": 4,
    "fatal": 4,
}


def _level_key(record: LogRecord) -> int:
    """Return a numeric sort key for a log level string."""
    raw = (record.level or "").lower().strip()
    return _LEVEL_ORDER.get(raw, -1)


def sort_records(
    records: Iterable[LogRecord],
    key: SortKey = "timestamp",
    order: SortOrder = "asc",
    nulls_last: bool = True,
) -> SortResult:
    """Sort *records* by *key* in the given *order*.

    Parameters
    ----------
    records:
        Iterable of :class:`~logslice.parser.LogRecord` objects.
    key:
        Field to sort by: ``"timestamp"``, ``"level"``, or ``"message"``.
    order:
        ``"asc"`` for ascending, ``"desc"`` for descending.
    nulls_last:
        When ``True`` records with a ``None`` sort value are placed at the
        end regardless of *order*.
    """
    if key not in ("timestamp", "level", "message"):
        raise ValueError(f"Invalid sort key: {key!r}")
    if order not in ("asc", "desc"):
        raise ValueError(f"Invalid sort order: {order!r}")

    items: List[LogRecord] = list(records)
    reverse = order == "desc"

    if key == "timestamp":
        def sort_fn(r: LogRecord):
            if r.timestamp is None:
                return (1, None) if nulls_last else (0, None)
            return (0, r.timestamp) if not reverse else (0, r.timestamp)
    elif key == "level":
        def sort_fn(r: LogRecord):
            v = _level_key(r)
            if v == -1:
                return (1, v) if nulls_last else (0, v)
            return (0, v)
    else:  # message
        def sort_fn(r: LogRecord):
            msg = r.message or ""
            return (0, msg.lower())

    items.sort(key=sort_fn, reverse=reverse)
    return SortResult(records=items, sort_key=key, order=order)
