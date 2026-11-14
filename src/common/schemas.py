"""
Pydantic v2 schemas for all data models in the pipeline.
"""
from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# GDELT Raw Models (from parsers → Kafka)
# ---------------------------------------------------------------------------

class GDELTEvent(BaseModel):
    GlobalEventID: int
    SQLDATE: int  # YYYYMMDD
    MonthYear: int
    Year: int
    FractionDate: Optional[float] = None
    Actor1Code: Optional[str] = None
    Actor1Name: Optional[str] = None
    Actor1CountryCode: Optional[str] = None
    Actor1KnownGroupCode: Optional[str] = None
    Actor1EthnicCode: Optional[str] = None
    Actor1Religion1Code: Optional[str] = None
    Actor1Religion2Code: Optional[str] = None
    Actor1Type1Code: Optional[str] = None
    Actor1Type2Code: Optional[str] = None
    Actor1Type3Code: Optional[str] = None
    Actor2Code: Optional[str] = None
    Actor2Name: Optional[str] = None
    Actor2CountryCode: Optional[str] = None
    Actor2KnownGroupCode: Optional[str] = None
    Actor2EthnicCode: Optional[str] = None
    Actor2Religion1Code: Optional[str] = None
    Actor2Religion2Code: Optional[str] = None
    Actor2Type1Code: Optional[str] = None
    Actor2Type2Code: Optional[str] = None
    Actor2Type3Code: Optional[str] = None
    IsRootEvent: Optional[int] = None
    EventCode: str
    EventBaseCode: Optional[str] = None
    EventRootCode: str
    QuadClass: int
    GoldsteinScale: Optional[float] = None
    NumMentions: Optional[int] = None
    NumSources: Optional[int] = None
    NumArticles: Optional[int] = None
    AvgTone: Optional[float] = None
    Actor1Geo_Type: Optional[int] = None
    Actor1Geo_FullName: Optional[str] = None
    Actor1Geo_CountryCode: Optional[str] = None
    Actor1Geo_ADM1Code: Optional[str] = None
    Actor1Geo_ADM2Code: Optional[str] = None
    Actor1Geo_Lat: Optional[float] = None
    Actor1Geo_Long: Optional[float] = None
    Actor1Geo_FeatureID: Optional[str] = None
    Actor2Geo_Type: Optional[int] = None
    Actor2Geo_FullName: Optional[str] = None
    Actor2Geo_CountryCode: Optional[str] = None
    Actor2Geo_ADM1Code: Optional[str] = None
    Actor2Geo_ADM2Code: Optional[str] = None
    Actor2Geo_Lat: Optional[float] = None
    Actor2Geo_Long: Optional[float] = None
    Actor2Geo_FeatureID: Optional[str] = None
    ActionGeo_Type: Optional[int] = None
    ActionGeo_FullName: Optional[str] = None
    ActionGeo_CountryCode: Optional[str] = None
    ActionGeo_ADM1Code: Optional[str] = None
    ActionGeo_ADM2Code: Optional[str] = None
    ActionGeo_Lat: Optional[float] = None
    ActionGeo_Long: Optional[float] = None
    ActionGeo_FeatureID: Optional[str] = None
    DATEADDED: Optional[int] = None
    SOURCEURL: Optional[str] = None

    @property
    def event_date(self) -> str:
        s = str(self.SQLDATE)
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}" if len(s) == 8 else s

    @property
    def has_coordinates(self) -> bool:
        return self.ActionGeo_Lat is not None and self.ActionGeo_Long is not None


class GDELTMention(BaseModel):
    GlobalEventID: int
    EventTimeDate: Optional[int] = None
    MentionTimeDate: Optional[int] = None
    MentionType: Optional[int] = None
    MentionSourceName: Optional[str] = None
    MentionIdentifier: str  # URL or doc ID
    SentenceID: Optional[int] = None
    Actor1CharOffset: Optional[int] = None
    Actor2CharOffset: Optional[int] = None
    ActionCharOffset: Optional[int] = None
    InRawText: Optional[int] = None
    Confidence: Optional[int] = None
    MentionDocLen: Optional[int] = None
    MentionDocTone: Optional[float] = None
    MentionDocTranslationInfo: Optional[str] = None
    Extras: Optional[str] = None


class GDELTGKGRecord(BaseModel):
    GKGRecordID: str
    DATE: Optional[int] = None
    SourceCollectionIdentifier: Optional[int] = None
    SourceCommonName: Optional[str] = None
    DocumentIdentifier: Optional[str] = None
    Themes: List[str] = Field(default_factory=list)
    EnhancedThemes: List[str] = Field(default_factory=list)
    Locations: List[str] = Field(default_factory=list)
    Persons: List[str] = Field(default_factory=list)
    Organizations: List[str] = Field(default_factory=list)
    Tone: Optional[float] = None
    PositiveScore: Optional[float] = None
    NegativeScore: Optional[float] = None
    Polarity: Optional[float] = None
    WordCount: Optional[int] = None
    RawV15Tone: Optional[str] = None


# ---------------------------------------------------------------------------
# Normalized / Enriched Event (output of Spark processing)
# ---------------------------------------------------------------------------

class NormalizedEvent(BaseModel):
    GlobalEventID: int
    EventDate: str  # YYYY-MM-DD
    EventDateTime: Optional[str] = None  # ISO 8601
    Actor1Name: Optional[str] = None
    Actor1CountryCode: Optional[str] = None
    Actor2Name: Optional[str] = None
    Actor2CountryCode: Optional[str] = None
    ActionCountryCode: Optional[str] = None  # ActionGeo_CountryCode
    ActionADM1Code: Optional[str] = None
    EventCode: str
    EventBaseCode: Optional[str] = None
    EventRootCode: str
    EventCategoryName: str  # derived from EventRootCode
    QuadClass: int
    QuadClassName: str  # "Verbal Cooperation" etc.
    IsConflict: bool
    GoldsteinScale: Optional[float] = None
    NumMentions: Optional[int] = None
    NumArticles: Optional[int] = None
    AvgTone: Optional[float] = None
    ActionGeo_Lat: Optional[float] = None
    ActionGeo_Long: Optional[float] = None
    ActionGeo_Type: Optional[int] = None
    ActionGeo_FullName: Optional[str] = None
    GeoResolution: str = "UNKNOWN"  # EXACT, APPROXIMATE, COUNTRY, UNKNOWN
    SOURCEURL: Optional[str] = None


# ---------------------------------------------------------------------------
# Analytics / Aggregate Models
# ---------------------------------------------------------------------------

class CountryMetric(BaseModel):
    country_code: str
    date: str  # YYYY-MM-DD
    hour: Optional[int] = None
    event_count: int = 0
    mention_count: int = 0
    article_count: int = 0
    avg_tone: Optional[float] = None
    avg_goldstein: Optional[float] = None
    cooperation_count: int = 0
    conflict_count: int = 0
    top_categories: Dict[str, int] = Field(default_factory=dict)


class TopicMetric(BaseModel):
    topic: str
    date: str
    hour: Optional[int] = None
    count: int = 0
    article_count: int = 0
    avg_tone: Optional[float] = None
    country_distribution: Dict[str, int] = Field(default_factory=dict)


class CategoryMetric(BaseModel):
    root_code: str
    category_name: str
    date: str
    count: int = 0
    avg_goldstein: Optional[float] = None
    avg_tone: Optional[float] = None
    is_conflict: bool = False


class AnomalyMetric(BaseModel):
    anomaly_id: str
    anomaly_type: str  # topic | country | category
    label: str
    country_code: Optional[str] = None
    topic: Optional[str] = None
    detected_at: str  # ISO 8601
    baseline_avg: float
    current_count: float
    pct_change: float
    z_score: float
    severity: str  # critical | high | medium
    spike_started: Optional[str] = None


# ---------------------------------------------------------------------------
# Ingestion Metadata
# ---------------------------------------------------------------------------

class IngestionMetadata(BaseModel):
    file_url: str
    file_type: str  # events | mentions | gkg
    file_hash: Optional[str] = None
    file_size: Optional[int] = None
    download_path: Optional[str] = None
    status: str = "pending"  # pending | downloaded | processed | failed
    discovered_at: Optional[str] = None
    downloaded_at: Optional[str] = None
    processed_at: Optional[str] = None
    kafka_published_at: Optional[str] = None
    error_msg: Optional[str] = None
    retry_count: int = 0
    records_accepted: int = 0
    records_rejected: int = 0

