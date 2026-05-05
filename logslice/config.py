"""Lightweight configuration dataclass for a logslice run."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class LogSliceConfig:
    """Holds all runtime parameters for a single logslice invocation."""

    # Input
    input_file: str = "-"  # '-' means stdin

    # Time filtering
    start: Optional[datetime] = None
    end: Optional[datetime] = None

    # Pattern filtering
    patterns: List[str] = field(default_factory=list)
    exclude_patterns: List[str] = field(default_factory=list)
    case_sensitive: bool = False

    # Output
    output_format: str = "text"  # text | json | csv
    output_file: Optional[str] = None  # None → stdout
    template: Optional[str] = None

    # Highlighting (text mode only)
    highlight: bool = True
    highlight_colour: str = "yellow"
    highlight_patterns: List[str] = field(default_factory=list)

    # Misc
    max_records: Optional[int] = None
    verbose: bool = False

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self) -> None:
        """Raise *ValueError* if the configuration is inconsistent."""
        valid_formats = {"text", "json", "csv"}
        if self.output_format not in valid_formats:
            raise ValueError(
                f"Invalid output_format '{self.output_format}'. "
                f"Choose from: {sorted(valid_formats)}"
            )

        if self.start and self.end and self.start > self.end:
            raise ValueError(
                f"start ({self.start}) must not be later than end ({self.end})"
            )

        if self.max_records is not None and self.max_records < 1:
            raise ValueError("max_records must be a positive integer")

        valid_colours = {
            "red", "green", "yellow", "blue", "magenta", "cyan", "bold"
        }
        if self.highlight_colour not in valid_colours:
            raise ValueError(
                f"Invalid highlight_colour '{self.highlight_colour}'. "
                f"Choose from: {sorted(valid_colours)}"
            )

    @property
    def effective_highlight_patterns(self) -> List[str]:
        """Patterns to highlight: explicit list or fall back to filter patterns."""
        return self.highlight_patterns if self.highlight_patterns else self.patterns
