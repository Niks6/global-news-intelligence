"""
Geographic helper service: country lists, ADM1 lookup, geocoding cache.
Uses Natural Earth public-domain data for country names.
"""
from __future__ import annotations
import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Optional

from src.common.cameo_mapping import FIPS_TO_ISO2

# Static country name lookup (FIPS code → display name)
FIPS_COUNTRY_NAMES: Dict[str, str] = {
    "US": "United States", "UK": "United Kingdom", "FR": "France",
    "GM": "Germany", "RS": "Russia", "CH": "China", "JA": "Japan",
    "IN": "India", "BR": "Brazil", "MX": "Mexico", "AU": "Australia",
    "CA": "Canada", "IT": "Italy", "SP": "Spain", "NL": "Netherlands",
    "SW": "Sweden", "NO": "Norway", "DA": "Denmark", "FI": "Finland",
    "PL": "Poland", "EZ": "Czech Republic", "HU": "Hungary", "RO": "Romania",
    "BU": "Bulgaria", "GR": "Greece", "TU": "Turkey", "IR": "Iran",
    "IZ": "Iraq", "SY": "Syria", "SA": "Saudi Arabia", "EG": "Egypt",
    "LY": "Libya", "MO": "Morocco", "AL": "Algeria", "TN": "Tunisia",
    "SF": "South Africa", "NI": "Nigeria", "GH": "Ghana", "ET": "Ethiopia",
    "KE": "Kenya", "ID": "Indonesia", "TH": "Thailand", "VM": "Vietnam",
    "PH": "Philippines", "MY": "Malaysia", "SN": "Singapore", "PK": "Pakistan",
    "BD": "Bangladesh", "AF": "Afghanistan", "UP": "Ukraine", "PO": "Portugal",
    "BO": "Bolivia", "CI": "Colombia", "AR": "Argentina", "VE": "Venezuela",
    "PE": "Peru", "CL": "Chile", "IS": "Israel", "JO": "Jordan",
    "LE": "Lebanon", "MP": "Nepal", "MG": "Mongolia", "KZ": "Kazakhstan",
    "UZ": "Uzbekistan", "BE": "Belgium", "SZ": "Switzerland", "AT": "Austria",
    "SE": "Serbia", "HR": "Croatia", "IO": "Indonesia",
}


def get_country_list() -> List[Dict]:
    return [
        {"code": code, "name": name, "iso2": FIPS_TO_ISO2.get(code, code)}
        for code, name in sorted(FIPS_COUNTRY_NAMES.items(), key=lambda x: x[1])
    ]


def get_country_name(fips_code: str) -> str:
    return FIPS_COUNTRY_NAMES.get(fips_code, fips_code)


class GeocodingCache:
    """SQLite-backed cache for reverse geocoding to avoid API rate limits."""

    def __init__(self, db_path: str = "data/cache/geocode_cache.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS geocode_cache (
                    lat REAL, lon REAL,
                    country_code TEXT, region_name TEXT,
                    city TEXT, resolution TEXT,
                    created_at TEXT,
                    PRIMARY KEY (lat, lon)
                )
            """)

    def get(self, lat: float, lon: float) -> Optional[Dict]:
        lat_r, lon_r = round(lat, 3), round(lon, 3)
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT country_code, region_name, city, resolution FROM geocode_cache WHERE lat=? AND lon=?",
                (lat_r, lon_r),
            ).fetchone()
        if row:
            return {"country_code": row[0], "region_name": row[1], "city": row[2], "resolution": row[3]}
        return None

    def put(self, lat: float, lon: float, data: Dict):
        lat_r, lon_r = round(lat, 3), round(lon, 3)
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO geocode_cache VALUES (?,?,?,?,?,?,?)",
                (lat_r, lon_r, data.get("country_code"), data.get("region_name"),
                 data.get("city"), data.get("resolution", "APPROXIMATE"), now),
            )


def geo_resolution_label(geo_type: Optional[int]) -> str:
    """Map GDELT GeoType to a human-readable resolution label."""
    if geo_type is None:
        return "UNKNOWN"
    return {1: "COUNTRY", 2: "APPROXIMATE", 3: "EXACT", 4: "EXACT", 5: "APPROXIMATE"}.get(geo_type, "UNKNOWN")

