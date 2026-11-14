"""
GDELT Mentions TSV parser.
Parses tab-separated GDELT 2.0 Mentions files (.mentions.CSV.zip).
Returns list of dicts compatible with GDELTMention Pydantic model.
"""
from __future__ import annotations

import csv
import io
import logging
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# 20-column Mentions schema
MENTIONS_COLUMNS: List[str] = [
    "GlobalEventID",
    "EventTimeDate",
    "MentionTimeDate",
    "MentionType",
    "MentionSourceName",
    "MentionIdentifier",
    "SentenceID",
    "Actor1CharOffset",
    "Actor2CharOffset",
    "ActionCharOffset",
    "InRawText",
    "Confidence",
    "MentionDocLen",
    "MentionDocTone",
    "MentionDocTranslationInfo",
    "Extras",
    # Pad to 20 for safety
    "_col17", "_col18", "_col19", "_col20",
]

# Try to load from phase01 schema module
try:
    from src.common.gdelt_schema import MENTIONS_COLUMNS as _MC  # type: ignore[import]
    MENTIONS_COLUMNS = _MC
except ImportError:
    pass

INT_COLS = {
    "GlobalEventID", "EventTimeDate", "MentionTimeDate",
    "MentionType", "SentenceID", "Actor1CharOffset",
    "Actor2CharOffset", "ActionCharOffset", "InRawText",
    "Confidence", "MentionDocLen",
}

FLOAT_COLS = {"MentionDocTone"}


def _safe_int(v: str) -> Optional[int]:
    try:
        return int(v) if v.strip() else None
    except (ValueError, TypeError):
        return None


def _safe_float(v: str) -> Optional[float]:
    try:
        return float(v) if v.strip() else None
    except (ValueError, TypeError):
        return None


def _cast_row(raw: Dict[str, str]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for col, val in raw.items():
        if col.startswith("_col"):
            continue
        if col in INT_COLS:
            result[col] = _safe_int(val)
        elif col in FLOAT_COLS:
            result[col] = _safe_float(val)
        else:
            result[col] = val.strip() or None
    return result


class MentionsParser:
    """Parse a GDELT Mentions .mentions.CSV.zip or raw TSV content."""

    def __init__(self, dead_letter_dir: Optional[str | Path] = None) -> None:
        self.dead_letter_dir = Path(dead_letter_dir) if dead_letter_dir else None
        if self.dead_letter_dir:
            self.dead_letter_dir.mkdir(parents=True, exist_ok=True)

    def parse_zip(self, zip_path: str | Path) -> Tuple[List[Dict], List[Dict]]:
        zip_path = Path(zip_path)
        logger.info("Parsing mentions ZIP: %s", zip_path)
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = [n for n in zf.namelist() if "mention" in n.lower() or n.endswith(".CSV")]
            if not names:
                names = zf.namelist()
            with zf.open(names[0]) as fh:
                content = fh.read().decode("utf-8", errors="replace")
        return self.parse_tsv(content, source=str(zip_path))

    def parse_tsv(self, content: str, source: str = "") -> Tuple[List[Dict], List[Dict]]:
        accepted: List[Dict] = []
        rejected: List[Dict] = []

        reader = csv.reader(io.StringIO(content), delimiter="\t", quoting=csv.QUOTE_NONE)
        for line_num, fields in enumerate(reader, start=1):
            if not any(fields):
                continue
            if len(fields) < len(MENTIONS_COLUMNS):
                fields.extend([""] * (len(MENTIONS_COLUMNS) - len(fields)))
            elif len(fields) > len(MENTIONS_COLUMNS):
                fields = fields[: len(MENTIONS_COLUMNS)]

            raw = dict(zip(MENTIONS_COLUMNS, fields))
            try:
                row = _cast_row(raw)
                if not row.get("GlobalEventID") or not row.get("MentionIdentifier"):
                    err = "Missing GlobalEventID or MentionIdentifier"
                    self._quarantine(raw, err, source, line_num)
                    rejected.append({"_error": err, "_line": line_num, "_source": source})
                else:
                    row["_source_file"] = source
                    row["_line_num"] = line_num
                    accepted.append(row)
            except Exception as exc:
                err = str(exc)
                self._quarantine(raw, err, source, line_num)
                rejected.append({"_error": err, "_line": line_num, "_source": source})

        logger.info("Mentions parse: %d accepted, %d rejected", len(accepted), len(rejected))
        return accepted, rejected

    def _quarantine(self, raw: Dict, error: str, source: str, line_num: int) -> None:
        if not self.dead_letter_dir:
            return
        import json
        entry = {"source": source, "line_num": line_num, "error": error, "raw": raw}
        dl_file = self.dead_letter_dir / "mentions_dead_letter.jsonl"
        with open(dl_file, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")
