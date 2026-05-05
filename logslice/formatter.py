"""Output formatters that optionally apply highlighting to text export."""
from __future__ import annotations

from typing import Iterable, Iterator, List, Optional

from logslice.highlighter import Highlighter, make_highlighter
from logslice.parser import LogRecord


def _record_to_text(record: LogRecord) -> str:
    """Render a single LogRecord as a plain text line."""
    ts = record.timestamp.isoformat() if record.timestamp else ""
    level = f" [{record.level}]" if record.level else ""
    parts = [p for p in (ts, level, record.message) if p]
    return " ".join(parts) if parts else record.raw


def format_records(
    records: Iterable[LogRecord],
    highlighter: Optional[Highlighter] = None,
    template: Optional[str] = None,
) -> Iterator[str]:
    """Yield formatted (and optionally highlighted) text lines.

    Args:
        records:     Iterable of LogRecord objects.
        highlighter: Optional Highlighter instance; skipped when *None*.
        template:    Optional Python format string with keys
                     ``{timestamp}``, ``{level}``, ``{message}``, ``{raw}``.
    """
    for record in records:
        if template:
            line = template.format(
                timestamp=record.timestamp.isoformat() if record.timestamp else "",
                level=record.level or "",
                message=record.message,
                raw=record.raw,
            )
        else:
            line = _record_to_text(record)

        if highlighter:
            line = highlighter.highlight(line)

        yield line


def format_records_list(
    records: Iterable[LogRecord],
    patterns: Optional[List[str]] = None,
    colour: str = "yellow",
    template: Optional[str] = None,
    highlight: bool = True,
) -> List[str]:
    """Convenience wrapper returning a list of formatted lines."""
    h = make_highlighter(patterns, colour=colour, enabled=highlight) if patterns else None
    return list(format_records(records, highlighter=h, template=template))
