"""Pattern-based text highlighting for log output."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional

# ANSI colour codes
COLOURS = {
    "red": "\033[31m",
    "green": "\033[32m",
    "yellow": "\033[33m",
    "blue": "\033[34m",
    "magenta": "\033[35m",
    "cyan": "\033[36m",
    "bold": "\033[1m",
    "reset": "\033[0m",
}


@dataclass
class HighlightRule:
    """A single pattern-to-colour mapping."""

    pattern: str
    colour: str = "yellow"
    flags: int = re.IGNORECASE

    def __post_init__(self) -> None:
        if self.colour not in COLOURS:
            raise ValueError(f"Unknown colour '{self.colour}'. Choose from: {list(COLOURS)}")
        self._regex = re.compile(self.pattern, self.flags)

    @property
    def regex(self) -> re.Pattern:
        return self._regex


@dataclass
class Highlighter:
    """Applies a list of HighlightRules to text lines."""

    rules: List[HighlightRule] = field(default_factory=list)
    enabled: bool = True

    def add_rule(self, pattern: str, colour: str = "yellow") -> None:
        self.rules.append(HighlightRule(pattern=pattern, colour=colour))

    def highlight(self, text: str) -> str:
        """Return *text* with all matching patterns wrapped in ANSI codes."""
        if not self.enabled or not self.rules:
            return text
        for rule in self.rules:
            replacement = (
                f"{COLOURS[rule.colour]}\\g<0>{COLOURS['reset']}"
            )
            text = rule.regex.sub(replacement, text)
        return text

    def highlight_lines(self, lines: List[str]) -> List[str]:
        return [self.highlight(line) for line in lines]


def make_highlighter(
    patterns: Optional[List[str]] = None,
    colour: str = "yellow",
    enabled: bool = True,
) -> Highlighter:
    """Convenience factory used by the CLI."""
    h = Highlighter(enabled=enabled)
    for pattern in (patterns or []):
        h.add_rule(pattern, colour)
    return h
