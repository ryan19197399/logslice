"""Compressor: groups consecutive repeated log records into compressed summaries."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Iterator, List

from logslice.parser import LogRecord


def _message_key(record: LogRecord) -> str:
    """Return the key used to detect consecutive duplicates."""
    return (record.level or "", record.message)


@dataclass
class CompressedRecord:
    """A single entry in a compressed stream."""

    record: LogRecord
    run_length: int = 1

    @property
    def is_repeated(self) -> bool:
        return self.run_length > 1

    def to_dict(self) -> dict:
        base = {
            "run_length": self.run_length,
            "message": self.record.message,
            "level": self.record.level,
        }
        if self.record.timestamp is not None:
            base["timestamp"] = self.record.timestamp.isoformat()
        return base


@dataclass
class CompressionResult:
    """Result returned by :func:`compress_records`."""

    records: List[CompressedRecord] = field(default_factory=list)

    @property
    def total_input(self) -> int:
        return sum(cr.run_length for cr in self.records)

    @property
    def total_output(self) -> int:
        return len(self.records)

    @property
    def dropped(self) -> int:
        return self.total_input - self.total_output

    def to_dict(self) -> dict:
        return {
            "total_input": self.total_input,
            "total_output": self.total_output,
            "dropped": self.dropped,
            "records": [cr.to_dict() for cr in self.records],
        }


def _iter_compressed(records: Iterable[LogRecord]) -> Iterator[CompressedRecord]:
    current: CompressedRecord | None = None
    for record in records:
        key = _message_key(record)
        if current is None:
            current = CompressedRecord(record=record, run_length=1)
            current_key = key
        elif key == current_key:
            current.run_length += 1
        else:
            yield current
            current = CompressedRecord(record=record, run_length=1)
            current_key = key
    if current is not None:
        yield current


def compress_records(records: Iterable[LogRecord]) -> CompressionResult:
    """Collapse consecutive duplicate records into single :class:`CompressedRecord` entries."""
    return CompressionResult(records=list(_iter_compressed(records)))
