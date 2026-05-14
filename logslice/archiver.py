"""Archive log records into named time-based or level-based archive buckets."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

from logslice.parser import LogRecord


@dataclass
class ArchiveEntry:
    bucket: str
    records: List[LogRecord] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.records)

    def to_dict(self) -> dict:
        return {"bucket": self.bucket, "count": self.count}


@dataclass
class ArchiveResult:
    entries: Dict[str, ArchiveEntry] = field(default_factory=dict)
    total: int = 0
    unarchived: List[LogRecord] = field(default_factory=list)

    @property
    def bucket_names(self) -> List[str]:
        return sorted(self.entries.keys())

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "unarchived": len(self.unarchived),
            "buckets": {k: v.to_dict() for k, v in self.entries.items()},
        }


def _date_bucket(record: LogRecord) -> Optional[str]:
    if record.timestamp is None:
        return None
    return record.timestamp.strftime("%Y-%m-%d")


def _level_bucket(record: LogRecord) -> Optional[str]:
    if not record.level:
        return None
    return record.level.upper()


def archive_records(
    records: Iterable[LogRecord],
    mode: str = "date",
) -> ArchiveResult:
    """Archive records into buckets by *mode* ('date' or 'level').

    Records that cannot be bucketed are placed in *unarchived*.
    """
    if mode not in ("date", "level"):
        raise ValueError(f"Unknown archive mode: {mode!r}. Use 'date' or 'level'.")

    key_fn = _date_bucket if mode == "date" else _level_bucket
    result = ArchiveResult()

    for record in records:
        result.total += 1
        bucket = key_fn(record)
        if bucket is None:
            result.unarchived.append(record)
        else:
            if bucket not in result.entries:
                result.entries[bucket] = ArchiveEntry(bucket=bucket)
            result.entries[bucket].records.append(record)

    return result
