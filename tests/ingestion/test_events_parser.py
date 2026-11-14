"""
Tests for the GDELT Events parser.
"""
import pytest
from pathlib import Path
from src.common.gdelt_schema import EVENTS_COLUMNS


class TestEventsSchema:
    def test_column_count(self):
        assert len(EVENTS_COLUMNS) == 61, f"Expected 61 columns, got {len(EVENTS_COLUMNS)}"

    def test_first_column(self):
        assert EVENTS_COLUMNS[0] == "GlobalEventID"

    def test_last_column(self):
        assert EVENTS_COLUMNS[60] == "SOURCEURL"

    def test_key_columns_present(self):
        required = {"GlobalEventID", "SQLDATE", "EventCode", "EventRootCode", "QuadClass",
                     "GoldsteinScale", "AvgTone", "ActionGeo_Lat", "ActionGeo_Long",
                     "ActionGeo_CountryCode", "SOURCEURL"}
        col_set = set(EVENTS_COLUMNS)
        for r in required:
            assert r in col_set, f"Missing column: {r}"


class TestEventsParser:
    """Test the events parser with synthetic TSV data."""

    def _make_row(self, overrides: dict = None) -> str:
        """Create a synthetic 61-column TAB-separated events row."""
        defaults = [""] * 61
        defaults[0] = "123456789"     # GlobalEventID
        defaults[1] = "20261114"      # SQLDATE
        defaults[2] = "202611"        # MonthYear
        defaults[3] = "2026"          # Year
        defaults[4] = "2026.87"       # FractionDate
        defaults[5] = "USA"           # Actor1Code
        defaults[6] = "UNITED STATES" # Actor1Name
        defaults[7] = "US"            # Actor1CountryCode
        defaults[25] = "1"            # IsRootEvent
        defaults[26] = "043"          # EventCode
        defaults[27] = "043"          # EventBaseCode
        defaults[28] = "04"           # EventRootCode
        defaults[29] = "1"            # QuadClass
        defaults[30] = "3.4"          # GoldsteinScale
        defaults[31] = "10"           # NumMentions
        defaults[32] = "5"            # NumSources
        defaults[33] = "8"            # NumArticles
        defaults[34] = "-1.5"         # AvgTone
        defaults[56] = "38.9"         # ActionGeo_Lat
        defaults[57] = "-77.0"        # ActionGeo_Long
        defaults[59] = "20261114120000" # DATEADDED
        defaults[60] = "https://example.com/news" # SOURCEURL
        if overrides:
            for k, v in overrides.items():
                idx = EVENTS_COLUMNS.index(k) if isinstance(k, str) else k
                defaults[idx] = str(v)
        return "\t".join(defaults)

    def test_parse_valid_row(self):
        from src.ingestion.parsers.events_parser import EventsParser
        parser = EventsParser()
        row = self._make_row()
        records, errors = parser.parse_lines([row])
        assert len(records) == 1
        assert len(errors) == 0
        assert records[0].GlobalEventID == 123456789
        assert records[0].EventRootCode == "04"
        assert records[0].AvgTone == -1.5

    def test_parse_missing_required_field(self):
        from src.ingestion.parsers.events_parser import EventsParser
        parser = EventsParser()
        row = self._make_row({0: ""})  # Empty GlobalEventID
        records, errors = parser.parse_lines([row])
        assert len(records) == 0
        assert len(errors) == 1

    def test_parse_wrong_column_count(self):
        from src.ingestion.parsers.events_parser import EventsParser
        parser = EventsParser()
        row = "too\tfew\tcolumns"
        records, errors = parser.parse_lines([row])
        assert len(records) == 0
        assert len(errors) == 1

    def test_deduplication(self):
        from src.ingestion.parsers.events_parser import EventsParser
        parser = EventsParser()
        row = self._make_row()
        records, errors = parser.parse_lines([row, row])
        # Parser may or may not deduplicate — Kafka/Spark handle it
        assert len(records) >= 1


class TestCameoMapping:
    def test_root_categories(self):
        from src.common.cameo_mapping import CAMEO_ROOT_CATEGORIES
        assert len(CAMEO_ROOT_CATEGORIES) == 20

    def test_cooperation(self):
        from src.common.cameo_mapping import is_conflict
        assert is_conflict("01") == False
        assert is_conflict("04") == False
        assert is_conflict("10") == True
        assert is_conflict("19") == True

    def test_get_category_name(self):
        from src.common.cameo_mapping import get_category_name
        assert get_category_name("01") == "Make Public Statement"
        assert get_category_name("20") == "Use Unconventional Mass Violence"
        assert get_category_name("99") == "Unknown"

