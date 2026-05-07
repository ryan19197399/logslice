"""Normalise log levels across different naming conventions."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, Iterator, List, Optional

from logslice.parser import LogRecord

# Mapping from raw level strings to a canonical form.
_CANONICAL: Dict[str, str] = {
    "trace": "TRACE",
    "debug": "DEBUG",
    "dbg": "DEBUG",
    "info": "INFO",
    "information": "INFO",
    "notice": "INFO",
    "warn": "WARNING",
    "warning": "WARNING",
    "error": "ERROR",
    "err": "ERROR",
    "critical": "CRITICAL",
    "crit": "CRITICAL",
    "fatal": "CRITICAL",
    "emergency": "CRITICAL",
}


@dataclass
class NormalisationResult:
    records: List[LogRecord]
    changed_count: int = 0
    unchanged_count: int = 0

    @property
    def total(self) -> int:
        return self.changed_count + self.unchanged_count

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "changed": self.changed_count,
            "unchanged": self.unchanged_count,
        }


def normalise_level(raw: Optional[str]) -> Optional[str]:
    """Return the canonical level string, or *raw* if not recognised."""
    if raw is None:
        return None
    return _CANONICAL.get(raw.strip().lower(), raw)


def normalise_records(
    records: Iterable[LogRecord],
    unknown_fallback: Optional[str] = None,
) -> NormalisationResult:
    """Normalise the *level* field of every record.

    Parameters
    ----------
    records:
        Input records to process.
    unknown_fallback:
        If provided, replace unrecognised level strings with this value.
        When *None* (default) unrecognised strings are kept as-is.
    """
    out: List[LogRecord] = []
    changed = 0
    unchanged = 0

    for rec in records:
        canonical = normalise_level(rec.level)
        if canonical is None and unknown_fallback is not None:
            canonical = unknown_fallback
        if canonical != rec.level:
            rec = LogRecord(
                timestamp=rec.timestamp,
                level=canonical,
                message=rec.message,
                raw=rec.raw,
                extra=rec.extra,
            )
            changed += 1
        else:
            unchanged += 1
        out.append(rec)

    return NormalisationResult(records=out, changed_count=changed, unchanged_count=unchanged)
