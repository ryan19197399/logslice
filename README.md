# logslice

Fast log file parser that filters and exports structured output by time range or pattern.

---

## Installation

```bash
pip install logslice
```

Or install from source:

```bash
git clone https://github.com/yourname/logslice.git
cd logslice && pip install .
```

---

## Usage

```python
from logslice import LogParser

parser = LogParser("app.log")

# Filter by time range
results = parser.slice(start="2024-01-15 08:00:00", end="2024-01-15 09:00:00")

# Filter by pattern
results = parser.filter(pattern=r"ERROR|CRITICAL")

# Export structured output
parser.export(results, format="json", output="errors.json")
```

**CLI usage:**

```bash
logslice app.log --start "2024-01-15 08:00" --end "2024-01-15 09:00" --out results.json
logslice app.log --pattern "ERROR" --format csv
```

---

## Features

- Filter log entries by time range or regex pattern
- Export to JSON, CSV, or plain text
- Supports common log formats (Apache, syslog, custom)
- Fast parsing with minimal memory footprint

---

## License

This project is licensed under the [MIT License](LICENSE).