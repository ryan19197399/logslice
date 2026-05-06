"""Message truncation utilities for log records."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Iterator

from logslice.parser import LogRecord

_DEFAULT_MAX_LENGTH = 200
_ELLIPSIS = "..."


@dataclass(frozen=True)
class TruncationResult:
    """Container returned by :func:`truncate_records`."""

    records: list[LogRecord]
    total_input: int
    truncated_count: int

    @property
    def unchanged_count(self) -> int:
        return self.total_input - self.truncated_count


def truncate_message(message: str, max_length: int = _DEFAULT_MAX_LENGTH) -> str:
    """Return *message* trimmed to *max_length* characters.

    If the message is already within the limit it is returned unchanged.
    Otherwise the tail is replaced with an ellipsis so the total length
    equals *max_length*.

    Args:
        message: The raw log message string.
        max_length: Maximum allowed character count (must be >= 4).

    Returns:
        Possibly-truncated message string.

    Raises:
        ValueError: If *max_length* is less than ``len(_ELLIPSIS) + 1``.
    """
    if max_length < len(_ELLIPSIS) + 1:
        raise ValueError(
            f"max_length must be at least {len(_ELLIPSIS) + 1}, got {max_length}"
        )
    if len(message) <= max_length:
        return message
    return message[: max_length - len(_ELLIPSIS)] + _ELLIPSIS


def truncate_records(
    records: Iterable[LogRecord],
    max_length: int = _DEFAULT_MAX_LENGTH,
) -> TruncationResult:
    """Apply :func:`truncate_message` to every record in *records*.

    Records whose message is ``None`` are passed through unchanged.

    Args:
        records: Iterable of :class:`~logslice.parser.LogRecord` objects.
        max_length: Forwarded to :func:`truncate_message`.

    Returns:
        A :class:`TruncationResult` with the processed records and counts.
    """
    out: list[LogRecord] = []
    total = 0
    truncated = 0

    for record in records:
        total += 1
        if record.message is not None and len(record.message) > max_length:
            new_record = LogRecord(
                timestamp=record.timestamp,
                level=record.level,
                message=truncate_message(record.message, max_length),
                raw=record.raw,
            )
            out.append(new_record)
            truncated += 1
        else:
            out.append(record)

    return TruncationResult(records=out, total_input=total, truncated_count=truncated)
