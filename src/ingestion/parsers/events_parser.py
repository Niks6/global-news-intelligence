"""
GDELT Events TSV parser.
Parses tab-separated GDELT 2.0 Events export CSV files.
Returns validated GDELTEvent Pydantic models.
Quarantines malformed rows to the dead-letter topic / local file.
"""
from __future__ import annotations

import csv
import io
import logging
import zipfile
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column definitions (inline fallback; will be overridden if gdelt_schema
# is importable from src.common after phase01 completes)
# ---------------------------------------------------------------------------
EVENTS_COLUMNS: List[str] = [
    "GlobalEventID", "SQLDATE", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode",
    "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code",
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
    "QuadClass", "GoldsteinScale", "NumMentions", "NumSources",
    "NumArticles", "AvgTone",
    "Actor1Geo_Type", "Actor1Geo_FullName", "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code", "Actor1Geo_Lat",
    "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code", "Actor2Geo_Lat",
    "Actor2Geo_Long", "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code", "ActionGeo_ADM2Code", "ActionGeo_Lat",
    "ActionGeo_Long", "ActionGeo_FeatureID",
    "DATEADDED", "SOURCEURL",
]

# Try to import from phase01-generated schema module
try:
    from src.common.gdelt_schema import EVENTS_COLUMNS as _EC  # type: ignore[import]
    EVENTS_COLUMNS = _EC
    logger.debug("Loaded EVENTS_COLUMNS from src.common.gdelt_schema")
except ImportError:
    logger.debug("src.common.gdelt_schema not yet available; using inline column list")

# Fields that must be non-empty for a row to be valid
REQUIRED_FIELDS = {
    "GlobalEventID",
    "SQLDATE",
    "EventCode",
    "ActionGeo_CountryCode",
}

# Numeric columns (int)
INT_COLS = {
    "GlobalEventID", "SQLDATE", "MonthYear", "Year",
    "IsRootEvent", "QuadClass", "NumMentions", "NumSources",
    "NumArticles", "DATEADDED",
    "Actor1Geo_Type", "Actor2Geo_Type", "ActionGeo_Type",
}

# Numeric columns (float)
FLOAT_COLS = {
    "FractionDate", "GoldsteinScale", "AvgTone",
    "Actor1Geo_Lat", "Actor1Geo_Long",
    "Actor2Geo_Lat", "Actor2Geo_Long",
    "ActionGeo_Lat", "ActionGeo_Long",
}


def _safe_int(value: str) -> Optional[int]:
    try:
        return int(value) if value.strip() else None
    except (ValueError, TypeError):
        return None


def _safe_float(value: str) -> Optional[float]:
    try:
        return float(value) if value.strip() else None
    except (ValueError, TypeError):
        return None


def _parse_sqldate(value: str) -> Optional[str]:
    """Convert YYYYMMDD integer string to ISO date string."""
    try:
        s = value.strip()
        if len(s) == 8:
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        return s or None
    except Exception:
        return None


def _cast_row(raw: Dict[str, str]) -> Dict[str, Any]:
    """Cast string values to appropriate Python types."""
    result: Dict[str, Any] = {}
    for col, val in raw.items():
        if col in INT_COLS:
            result[col] = _safe_int(val)
        elif col in FLOAT_COLS:
            result[col] = _safe_float(val)
        else:
            result[col] = val.strip() or None
    # Reformat SQLDATE as ISO string
    if result.get("SQLDATE") is not None:
        result["SQLDATE_ISO"] = _parse_sqldate(raw.get("SQLDATE", ""))
    return result


def _validate_row(row: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Return (is_valid, error_message).
    A row is valid if all REQUIRED_FIELDS are non-null.
    """
    missing = [f for f in REQUIRED_FIELDS if not row.get(f)]
    if missing:
        return False, f"Missing required fields: {missing}"
    return True, ""


class EventsParser:
    """Parse a GDELT Events .export.CSV.zip or raw TSV file."""

    def __init__(
        self,
        dead_letter_dir: Optional[str | Path] = None,
        dead_letter_topic: str = "gdelt.dead-letter",
    ) -> None:
        self.dead_letter_dir = Path(dead_letter_dir) if dead_letter_dir else None
        self.dead_letter_topic = dead_letter_topic
        if self.dead_letter_dir:
            self.dead_letter_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def parse_lines(self, lines: List[str], source: str = "") -> Tuple[List[Any], List[Dict]]:
        """
        Parse a list of TSV line strings into validated GDELTEvent models.

        Returns:
            (accepted_models, rejected_dicts)
        """
        content = "\n".join(lines)
        accepted_dicts, rejected = self.parse_tsv(content, source=source)
        models = []
        extra_rejected = []
        for d in accepted_dicts:
            try:
                from src.common.schemas import GDELTEvent
                # Filter out internal fields before model construction
                model_data = {k: v for k, v in d.items() if not k.startswith("_")}
                model = GDELTEvent.model_validate(model_data)
                models.append(model)
            except Exception as exc:
                extra_rejected.append({
                    "_source": d.get("_source_file", source),
                    "_line": d.get("_line_num", 0),
                    "_error": f"Model validation: {exc}",
                    "_raw": d,
                })
        return models, rejected + extra_rejected

    def parse_zip(self, zip_path: str | Path) -> Tuple[List[Dict], List[Dict]]:
        """
        Parse a .export.CSV.zip file.

        Returns:
            (accepted, rejected) where each element is a list of dicts.
        """
        zip_path = Path(zip_path)
        logger.info("Parsing events ZIP: %s", zip_path)
        with zipfile.ZipFile(zip_path, "r") as zf:
            # The archive typically contains exactly one .CSV file
            csv_names = [n for n in zf.namelist() if n.upper().endswith(".CSV") or n.upper().endswith(".TSV") or "export" in n.lower()]
            if not csv_names:
                csv_names = zf.namelist()
            if not csv_names:
                raise ValueError(f"No files found in ZIP {zip_path}")
            csv_name = csv_names[0]
            with zf.open(csv_name) as fh:
                content = fh.read().decode("utf-8", errors="replace")
        return self.parse_tsv(content, source=str(zip_path))

    def parse_tsv(
        self, content: str, source: str = ""
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Parse raw TSV content.

        Returns:
            (accepted, rejected)
        """
        accepted: List[Dict] = []
        rejected: List[Dict] = []

        reader = csv.reader(io.StringIO(content), delimiter="\t", quoting=csv.QUOTE_NONE)
        for line_num, fields in enumerate(reader, start=1):
            if not any(fields):
                continue  # skip blank lines

            # Handle column count mismatches gracefully
            if len(fields) < len(EVENTS_COLUMNS):
                fields.extend([""] * (len(EVENTS_COLUMNS) - len(fields)))
            elif len(fields) > len(EVENTS_COLUMNS):
                fields = fields[: len(EVENTS_COLUMNS)]

            raw = dict(zip(EVENTS_COLUMNS, fields))
            try:
                row = _cast_row(raw)
                valid, err = _validate_row(row)
                if valid:
                    row["_source_file"] = source
                    row["_line_num"] = line_num
                    accepted.append(row)
                else:
                    self._quarantine(raw, err, source, line_num)
                    rejected.append({"_source": source, "_line": line_num, "_error": err, "_raw": raw})
            except Exception as exc:
                err = str(exc)
                self._quarantine(raw, err, source, line_num)
                rejected.append({"_source": source, "_line": line_num, "_error": err, "_raw": raw})

        logger.info(
            "Events parse complete: %d accepted, %d rejected (source=%s)",
            len(accepted), len(rejected), source,
        )
        return accepted, rejected

    # ------------------------------------------------------------------
    # Dead-letter handling
    # ------------------------------------------------------------------

    def _quarantine(
        self, raw: Dict, error: str, source: str, line_num: int
    ) -> None:
        if self.dead_letter_dir is None:
            return
        import json
        entry = {
            "source": source,
            "line_num": line_num,
            "error": error,
            "raw": raw,
        }
        dl_file = self.dead_letter_dir / "events_dead_letter.jsonl"
        with open(dl_file, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")
