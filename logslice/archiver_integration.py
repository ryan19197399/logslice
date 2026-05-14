"""High-level integration helper for the archiver module."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from logslice.archiver import ArchiveResult, archive_records
from logslice.filter import LogFilter
from logslice.parser import LogRecord

_VALID_MODES = ("date", "level")


@dataclass
class ArchiverConfig:
    mode: str = "date"
    start: Optional[str] = None
    end: Optional[str] = None
    levels: List[str] = field(default_factory=list)

    def validate(self) -> None:
        if self.mode not in _VALID_MODES:
            raise ValueError(
                f"mode must be one of {_VALID_MODES}, got {self.mode!r}"
            )


def _build_filter(cfg: ArchiverConfig) -> LogFilter:
    from datetime import datetime

    start_dt: Optional[datetime] = None
    end_dt: Optional[datetime] = None

    if cfg.start:
        start_dt = datetime.fromisoformat(cfg.start)
    if cfg.end:
        end_dt = datetime.fromisoformat(cfg.end)

    return LogFilter(
        start=start_dt,
        end=end_dt,
        levels=cfg.levels if cfg.levels else None,
    )


def run_archiver(
    records: List[LogRecord],
    cfg: ArchiverConfig,
) -> ArchiveResult:
    """Filter *records* according to *cfg* then archive them."""
    cfg.validate()
    log_filter = _build_filter(cfg)
    filtered = list(log_filter.apply(iter(records)))
    return archive_records(filtered, mode=cfg.mode)
