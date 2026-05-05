"""Export filtered log records to structured output formats."""

from __future__ import annotations

import csv
import json
import sys
from io import StringIO
from typing import Iterable, List, Literal, TextIO

from logslice.parser import LogRecord

OutputFormat = Literal["json", "csv", "text"]


def records_to_dicts(records: Iterable[LogRecord]) -> List[dict]:
    """Convert LogRecord objects to plain dictionaries."""
    return [
        {
            "timestamp": r.timestamp.isoformat() if r.timestamp else None,
            "level": r.level,
            "message": r.message,
            "raw": r.raw,
        }
        for r in records
    ]


def export_json(
    records: Iterable[LogRecord],
    output: TextIO = sys.stdout,
    indent: int = 2,
) -> None:
    """Write records as a JSON array to *output*."""
    data = records_to_dicts(records)
    json.dump(data, output, indent=indent, default=str)
    output.write("\n")


def export_csv(
    records: Iterable[LogRecord],
    output: TextIO = sys.stdout,
) -> None:
    """Write records as CSV rows to *output*."""
    fieldnames = ["timestamp", "level", "message", "raw"]
    writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in records_to_dicts(records):
        writer.writerow(row)


def export_text(
    records: Iterable[LogRecord],
    output: TextIO = sys.stdout,
) -> None:
    """Write raw log lines to *output*."""
    for record in records:
        output.write(record.raw)
        if not record.raw.endswith("\n"):
            output.write("\n")


def export(
    records: Iterable[LogRecord],
    fmt: OutputFormat = "text",
    output: TextIO = sys.stdout,
) -> None:
    """Dispatch export to the requested format handler."""
    if fmt == "json":
        export_json(records, output)
    elif fmt == "csv":
        export_csv(records, output)
    else:
        export_text(records, output)


def export_to_string(
    records: Iterable[LogRecord],
    fmt: OutputFormat = "text",
) -> str:
    """Return exported content as a string instead of writing to a stream."""
    buf = StringIO()
    export(list(records), fmt=fmt, output=buf)
    return buf.getvalue()
