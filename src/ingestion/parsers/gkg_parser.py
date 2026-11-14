"""
GDELT GKG (Global Knowledge Graph) parser.
GKG uses a complex format with semicolon/hash/comma delimiters in multi-value fields.
Parses .gkg.csv.zip files and returns list of dicts.
"""
from __future__ import annotations

import csv
import io
import logging
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Main GKG v2 column names (tab-separated at top level)
GKG_COLUMNS: List[str] = [
    "GKGRecordID",
    "DATE",
    "SourceCollectionIdentifier",
    "SourceCommonName",
    "DocumentIdentifier",
    "Counts",
    "V2Counts",
    "Themes",
    "V2Themes",
    "Locations",
    "V2Locations",
    "Persons",
    "V2Persons",
    "Organizations",
    "V2Organizations",
    "Tone",
    "Dates",
    "GCAM",
    "SharingImage",
    "RelatedImages",
    "SocialImageEmbeds",
    "SocialVideoEmbeds",
    "Quotations",
    "AllNames",
    "Amounts",
    "TranslationInfo",
    "Extras",
]

# Try to import from phase01 schema
try:
    from src.common.gdelt_schema import GKG_COLUMNS as _GC  # type: ignore[import]
    GKG_COLUMNS = _GC
except ImportError:
    pass


def _parse_v2themes(raw: str) -> List[str]:
    """V2Themes: semicolon-separated theme names (each may have ;score suffix)."""
    if not raw or not raw.strip():
        return []
    parts = [p.strip() for p in raw.split(";") if p.strip()]
    # Each part is "THEME_NAME,score" or just "THEME_NAME"
    themes = []
    for p in parts:
        name = p.split(",")[0].strip()
        if name:
            themes.append(name)
    return themes


def _parse_v2locations(raw: str) -> List[Dict[str, Any]]:
    """V2Locations: semicolon-separated location blocks, each field is #-delimited."""
    if not raw or not raw.strip():
        return []
    locations = []
    for block in raw.split(";"):
        block = block.strip()
        if not block:
            continue
        parts = block.split("#")
        try:
            loc = {
                "type": int(parts[0]) if len(parts) > 0 and parts[0] else None,
                "fullname": parts[1] if len(parts) > 1 else None,
                "country_code": parts[2] if len(parts) > 2 else None,
                "adm1_code": parts[3] if len(parts) > 3 else None,
                "adm2_code": parts[4] if len(parts) > 4 else None,
                "lat": float(parts[5]) if len(parts) > 5 and parts[5] else None,
                "lon": float(parts[6]) if len(parts) > 6 and parts[6] else None,
                "feature_id": parts[7] if len(parts) > 7 else None,
            }
            locations.append(loc)
        except (ValueError, IndexError):
            pass
    return locations


def _parse_v2persons(raw: str) -> List[str]:
    """V2Persons: semicolon-separated 'name,offset' pairs."""
    if not raw or not raw.strip():
        return []
    persons = []
    for block in raw.split(";"):
        name = block.split(",")[0].strip()
        if name:
            persons.append(name)
    return persons


def _parse_v2organizations(raw: str) -> List[str]:
    """V2Organizations: semicolon-separated 'name,offset' pairs."""
    if not raw or not raw.strip():
        return []
    orgs = []
    for block in raw.split(";"):
        name = block.split(",")[0].strip()
        if name:
            orgs.append(name)
    return orgs


def _parse_tone(raw: str) -> Dict[str, Optional[float]]:
    """
    Tone: comma-separated values:
    tone, positive_score, negative_score, polarity,
    activity_ref_density, self_ref_density, word_count
    """
    defaults: Dict[str, Optional[float]] = {
        "tone": None,
        "positive_score": None,
        "negative_score": None,
        "polarity": None,
        "activity_ref_density": None,
        "self_ref_density": None,
        "word_count": None,
    }
    if not raw or not raw.strip():
        return defaults
    parts = raw.split(",")
    keys = list(defaults.keys())
    for i, key in enumerate(keys):
        try:
            defaults[key] = float(parts[i]) if i < len(parts) and parts[i].strip() else None
        except (ValueError, IndexError):
            pass
    return defaults


def _parse_gcam(raw: str) -> Dict[str, float]:
    """GCAM: comma-separated 'dimension:score' pairs."""
    if not raw or not raw.strip():
        return {}
    result: Dict[str, float] = {}
    for part in raw.split(","):
        part = part.strip()
        if ":" in part:
            dim, score_str = part.split(":", 1)
            try:
                result[dim.strip()] = float(score_str.strip())
            except ValueError:
                pass
    return result


class GKGParser:
    """Parse a GDELT GKG .gkg.csv.zip or raw tab-separated content."""

    def __init__(self, dead_letter_dir: Optional[str | Path] = None) -> None:
        self.dead_letter_dir = Path(dead_letter_dir) if dead_letter_dir else None
        if self.dead_letter_dir:
            self.dead_letter_dir.mkdir(parents=True, exist_ok=True)

    def parse_zip(self, zip_path: str | Path) -> Tuple[List[Dict], List[Dict]]:
        zip_path = Path(zip_path)
        logger.info("Parsing GKG ZIP: %s", zip_path)
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = [n for n in zf.namelist() if "gkg" in n.lower() or n.endswith(".csv")]
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
            if len(fields) < len(GKG_COLUMNS):
                fields.extend([""] * (len(GKG_COLUMNS) - len(fields)))
            elif len(fields) > len(GKG_COLUMNS):
                fields = fields[: len(GKG_COLUMNS)]

            raw = dict(zip(GKG_COLUMNS, fields))
            try:
                row = self._parse_row(raw)
                if not row.get("GKGRecordID"):
                    err = "Missing GKGRecordID"
                    self._quarantine(raw, err, source, line_num)
                    rejected.append({"_error": err, "_line": line_num})
                else:
                    row["_source_file"] = source
                    row["_line_num"] = line_num
                    accepted.append(row)
            except Exception as exc:
                err = str(exc)
                self._quarantine(raw, err, source, line_num)
                rejected.append({"_error": err, "_line": line_num})

        logger.info("GKG parse: %d accepted, %d rejected", len(accepted), len(rejected))
        return accepted, rejected

    def _parse_row(self, raw: Dict[str, str]) -> Dict[str, Any]:
        """Transform raw string dict into structured dict with parsed sub-fields."""
        tone_parsed = _parse_tone(raw.get("Tone", ""))
        return {
            "GKGRecordID": raw.get("GKGRecordID", "").strip() or None,
            "DATE": raw.get("DATE", "").strip() or None,
            "SourceCollectionIdentifier": _safe_int_str(raw.get("SourceCollectionIdentifier", "")),
            "SourceCommonName": raw.get("SourceCommonName", "").strip() or None,
            "DocumentIdentifier": raw.get("DocumentIdentifier", "").strip() or None,
            # Parsed multi-value fields
            "Themes": _parse_v2themes(raw.get("V2Themes", "") or raw.get("Themes", "")),
            "Locations": _parse_v2locations(raw.get("V2Locations", "") or raw.get("Locations", "")),
            "Persons": _parse_v2persons(raw.get("V2Persons", "") or raw.get("Persons", "")),
            "Organizations": _parse_v2organizations(raw.get("V2Organizations", "") or raw.get("Organizations", "")),
            # Tone values
            "Tone": tone_parsed["tone"],
            "TonePositive": tone_parsed["positive_score"],
            "ToneNegative": tone_parsed["negative_score"],
            "TonePolarity": tone_parsed["polarity"],
            "ToneActivityDensity": tone_parsed["activity_ref_density"],
            "ToneSelfRefDensity": tone_parsed["self_ref_density"],
            "ToneWordCount": tone_parsed["word_count"],
            # Raw fields kept for downstream
            "GCAM": _parse_gcam(raw.get("GCAM", "")),
            "SharingImage": raw.get("SharingImage", "").strip() or None,
            "TranslationInfo": raw.get("TranslationInfo", "").strip() or None,
        }

    def _quarantine(self, raw: Dict, error: str, source: str, line_num: int) -> None:
        if not self.dead_letter_dir:
            return
        import json
        entry = {"source": source, "line_num": line_num, "error": error, "raw": {k: raw.get(k, "") for k in list(raw.keys())[:5]}}
        dl_file = self.dead_letter_dir / "gkg_dead_letter.jsonl"
        with open(dl_file, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")


def _safe_int_str(v: str) -> Optional[int]:
    try:
        return int(v.strip()) if v.strip() else None
    except (ValueError, TypeError):
        return None
