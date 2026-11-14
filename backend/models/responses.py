"""
Response Pydantic models for the Global News Intelligence API.
All models include an optional 'message' field for empty/insufficient-data states.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Shared / utility
# ---------------------------------------------------------------------------

class EmptyResponse(BaseModel):
    data: List = Field(default_factory=list)
    total: int = 0
    message: str = "No data available for the selected time range"


class PaginationMeta(BaseModel):
    page: int
    limit: int
    total: int
    total_pages: int


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    version: str = "1.0.0"


class ComponentHealth(BaseModel):
    name: str
    healthy: bool
    details: Optional[str] = None


class PipelineHealthResponse(BaseModel):
    status: str  # "healthy" | "degraded" | "unavailable"
    hbase_connected: bool
    last_ingestion_time: Optional[datetime] = None
    files_processed_24h: int = 0
    records_accepted_24h: int = 0
    records_rejected_24h: int = 0
    pipeline_lag_minutes: Optional[float] = None
    components: List[ComponentHealth] = Field(default_factory=list)
    message: Optional[str] = None


# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------

class TimeSeriesPoint(BaseModel):
    timestamp: str  # ISO date string YYYY-MM-DD
    value: float


class TopicItem(BaseModel):
    topic: str
    count: int
    trend: Optional[float] = None  # % change


class CountryItem(BaseModel):
    country_code: str
    country_name: str
    count: int


class CategoryItem(BaseModel):
    category_code: str
    category_name: str
    count: int
    percentage: float


class CoopConflictPoint(BaseModel):
    timestamp: str
    cooperation: int
    conflict: int


class MapSummaryItem(BaseModel):
    country_code: str
    country_name: str
    lat: float
    lon: float
    event_count: int
    avg_tone: Optional[float] = None


class RecentEventItem(BaseModel):
    global_event_id: int
    sql_date: str
    actor1_name: Optional[str] = None
    actor2_name: Optional[str] = None
    event_code: Optional[str] = None
    event_description: Optional[str] = None
    goldstein_scale: Optional[float] = None
    source_url: Optional[str] = None
    action_geo_country_code: Optional[str] = None
    action_geo_country_name: Optional[str] = None


class OverviewResponse(BaseModel):
    total_events: int = 0
    total_articles: int = 0
    total_mentions: int = 0
    events_over_time: List[TimeSeriesPoint] = Field(default_factory=list)
    top_topics: List[TopicItem] = Field(default_factory=list)
    top_countries: List[CountryItem] = Field(default_factory=list)
    category_distribution: List[CategoryItem] = Field(default_factory=list)
    cooperation_conflict: List[CoopConflictPoint] = Field(default_factory=list)
    avg_tone_trend: List[TimeSeriesPoint] = Field(default_factory=list)
    recent_events: List[RecentEventItem] = Field(default_factory=list)
    map_summary: List[MapSummaryItem] = Field(default_factory=list)
    last_ingestion_time: Optional[datetime] = None
    message: Optional[str] = None


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

class EventResponse(BaseModel):
    """Full GDELT event record — all 61 columns plus derived fields."""
    # Core identifiers
    global_event_id: int
    sql_date: str
    month_year: Optional[int] = None
    year: Optional[int] = None
    fraction_date: Optional[float] = None

    # Actor 1
    actor1_code: Optional[str] = None
    actor1_name: Optional[str] = None
    actor1_country_code: Optional[str] = None
    actor1_known_group_code: Optional[str] = None
    actor1_ethnic_code: Optional[str] = None
    actor1_religion1_code: Optional[str] = None
    actor1_religion2_code: Optional[str] = None
    actor1_type1_code: Optional[str] = None
    actor1_type2_code: Optional[str] = None
    actor1_type3_code: Optional[str] = None

    # Actor 2
    actor2_code: Optional[str] = None
    actor2_name: Optional[str] = None
    actor2_country_code: Optional[str] = None
    actor2_known_group_code: Optional[str] = None
    actor2_ethnic_code: Optional[str] = None
    actor2_religion1_code: Optional[str] = None
    actor2_religion2_code: Optional[str] = None
    actor2_type1_code: Optional[str] = None
    actor2_type2_code: Optional[str] = None
    actor2_type3_code: Optional[str] = None

    # Event codes
    is_root_event: Optional[bool] = None
    event_code: Optional[str] = None
    event_base_code: Optional[str] = None
    event_root_code: Optional[str] = None
    quad_class: Optional[int] = None
    goldstein_scale: Optional[float] = None

    # Mention stats
    num_mentions: Optional[int] = None
    num_sources: Optional[int] = None
    num_articles: Optional[int] = None
    avg_tone: Optional[float] = None

    # Actor1Geo
    actor1_geo_type: Optional[int] = None
    actor1_geo_fullname: Optional[str] = None
    actor1_geo_country_code: Optional[str] = None
    actor1_geo_adm1_code: Optional[str] = None
    actor1_geo_lat: Optional[float] = None
    actor1_geo_long: Optional[float] = None
    actor1_geo_feature_id: Optional[str] = None

    # Actor2Geo
    actor2_geo_type: Optional[int] = None
    actor2_geo_fullname: Optional[str] = None
    actor2_geo_country_code: Optional[str] = None
    actor2_geo_adm1_code: Optional[str] = None
    actor2_geo_lat: Optional[float] = None
    actor2_geo_long: Optional[float] = None
    actor2_geo_feature_id: Optional[str] = None

    # ActionGeo (physical location of event)
    action_geo_type: Optional[int] = None
    action_geo_fullname: Optional[str] = None
    action_geo_country_code: Optional[str] = None
    action_geo_adm1_code: Optional[str] = None
    action_geo_lat: Optional[float] = None
    action_geo_long: Optional[float] = None
    action_geo_feature_id: Optional[str] = None

    # Date added / source
    date_added: Optional[str] = None
    source_url: Optional[str] = None

    # Derived / enriched
    event_description: Optional[str] = None
    action_geo_country_name: Optional[str] = None
    action_geo_adm1_name: Optional[str] = None
    geo_precision: Optional[str] = None  # "exact" | "approximate" | "country-level" | "unknown"
    is_cooperation: Optional[bool] = None


class EventListResponse(BaseModel):
    data: List[EventResponse] = Field(default_factory=list)
    pagination: PaginationMeta
    message: Optional[str] = None


class MentionItem(BaseModel):
    global_event_id: int
    event_time_date: Optional[str] = None
    mention_time_date: Optional[str] = None
    mention_type: Optional[int] = None
    mention_source_name: Optional[str] = None
    mention_identifier: Optional[str] = None
    sentence_id: Optional[int] = None
    actor1_char_offset: Optional[int] = None
    actor2_char_offset: Optional[int] = None
    action_char_offset: Optional[int] = None
    in_raw_text: Optional[int] = None
    confidence: Optional[int] = None
    mention_doc_len: Optional[int] = None
    mention_doc_tone: Optional[float] = None


class MentionListResponse(BaseModel):
    data: List[MentionItem] = Field(default_factory=list)
    total: int = 0
    message: Optional[str] = None


# ---------------------------------------------------------------------------
# Topics
# ---------------------------------------------------------------------------

class TopicResponse(BaseModel):
    data: List[TopicItem] = Field(default_factory=list)
    total: int = 0
    message: Optional[str] = None


class TopicTrendPoint(BaseModel):
    timestamp: str  # ISO date YYYY-MM-DD
    count: int
    avg_tone: Optional[float] = None


class TopicTrendResponse(BaseModel):
    topic: str
    data: List[TopicTrendPoint] = Field(default_factory=list)
    message: Optional[str] = None


class TopicCountryItem(BaseModel):
    country_code: str
    country_name: str
    count: int
    percentage: float


class TopicCountryResponse(BaseModel):
    topic: str
    data: List[TopicCountryItem] = Field(default_factory=list)
    message: Optional[str] = None


# ---------------------------------------------------------------------------
# Countries
# ---------------------------------------------------------------------------

class CountryListItem(BaseModel):
    code: str
    name: str


class CountryListResponse(BaseModel):
    data: List[CountryListItem] = Field(default_factory=list)
    total: int = 0


class CountryOverviewResponse(BaseModel):
    country_code: str
    country_name: str
    total_events: int = 0
    total_articles: int = 0
    total_mentions: int = 0
    top_topics: List[TopicItem] = Field(default_factory=list)
    category_distribution: List[CategoryItem] = Field(default_factory=list)
    cooperation_conflict: List[CoopConflictPoint] = Field(default_factory=list)
    avg_tone_trend: List[TimeSeriesPoint] = Field(default_factory=list)
    events_over_time: List[TimeSeriesPoint] = Field(default_factory=list)
    message: Optional[str] = None


class CountryEventsResponse(BaseModel):
    country_code: str
    data: List[EventResponse] = Field(default_factory=list)
    pagination: PaginationMeta
    message: Optional[str] = None


class RegionItem(BaseModel):
    adm1_code: str
    adm1_name: str
    event_count: int
    avg_tone: Optional[float] = None


class CountryTopicsResponse(BaseModel):
    country_code: str
    data: List[TopicItem] = Field(default_factory=list)
    message: Optional[str] = None


class CountryRegionsResponse(BaseModel):
    country_code: str
    data: List[RegionItem] = Field(default_factory=list)
    message: Optional[str] = None


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------

class CountryMetricSnapshot(BaseModel):
    country_code: str
    country_name: str
    total_events: int = 0
    total_articles: int = 0
    avg_tone: Optional[float] = None
    top_topics: List[TopicItem] = Field(default_factory=list)
    category_distribution: List[CategoryItem] = Field(default_factory=list)
    events_over_time: List[TimeSeriesPoint] = Field(default_factory=list)


class ComparisonResponse(BaseModel):
    countries: List[CountryMetricSnapshot] = Field(default_factory=list)
    message: Optional[str] = None


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

class CategoryResponse(BaseModel):
    data: List[CategoryItem] = Field(default_factory=list)
    total_events: int = 0
    message: Optional[str] = None


class ToneDataPoint(BaseModel):
    timestamp: str
    avg_tone: float
    min_tone: Optional[float] = None
    max_tone: Optional[float] = None
    sample_size: int = 0


class ToneResponse(BaseModel):
    data: List[ToneDataPoint] = Field(default_factory=list)
    overall_avg_tone: Optional[float] = None
    message: Optional[str] = None


class CooperationConflictResponse(BaseModel):
    data: List[CoopConflictPoint] = Field(default_factory=list)
    total_cooperation: int = 0
    total_conflict: int = 0
    message: Optional[str] = None


class AnomalyDataPoint(BaseModel):
    timestamp: str
    country_code: Optional[str] = None
    topic: Optional[str] = None
    observed_count: int
    expected_count: float
    z_score: float
    is_anomaly: bool
    description: Optional[str] = None


class AnomalyResponse(BaseModel):
    data: List[AnomalyDataPoint] = Field(default_factory=list)
    total_anomalies: int = 0
    message: Optional[str] = None


# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------

class MapEventPoint(BaseModel):
    global_event_id: int
    lat: float
    lon: float
    event_code: Optional[str] = None
    event_description: Optional[str] = None
    goldstein_scale: Optional[float] = None
    avg_tone: Optional[float] = None
    actor1_name: Optional[str] = None
    actor2_name: Optional[str] = None
    action_geo_fullname: Optional[str] = None
    sql_date: Optional[str] = None
    source_url: Optional[str] = None
    is_cluster: bool = False
    cluster_count: Optional[int] = None


class MapEventResponse(BaseModel):
    data: List[MapEventPoint] = Field(default_factory=list)
    total_events: int = 0
    returned_points: int = 0
    is_clustered: bool = False
    message: Optional[str] = None


class MapCountryPoint(BaseModel):
    country_code: str
    country_name: str
    lat: float
    lon: float
    event_count: int
    avg_tone: Optional[float] = None
    intensity_score: float = 0.0  # normalized 0-1


class MapCountryResponse(BaseModel):
    data: List[MapCountryPoint] = Field(default_factory=list)
    message: Optional[str] = None


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

class SearchResultItem(BaseModel):
    global_event_id: int
    sql_date: str
    actor1_name: Optional[str] = None
    actor2_name: Optional[str] = None
    event_code: Optional[str] = None
    event_description: Optional[str] = None
    goldstein_scale: Optional[float] = None
    avg_tone: Optional[float] = None
    source_url: Optional[str] = None
    action_geo_fullname: Optional[str] = None
    action_geo_country_code: Optional[str] = None
    action_geo_country_name: Optional[str] = None
    highlight: Optional[str] = None  # matched snippet


class SearchResponse(BaseModel):
    data: List[SearchResultItem] = Field(default_factory=list)
    pagination: PaginationMeta
    query: str = ""
    message: Optional[str] = None


# ---------------------------------------------------------------------------
# Regions
# ---------------------------------------------------------------------------

class RegionOverviewResponse(BaseModel):
    region_id: str
    region_name: str
    country_code: str
    country_name: str
    total_events: int = 0
    total_articles: int = 0
    avg_tone: Optional[float] = None
    top_topics: List[TopicItem] = Field(default_factory=list)
    events_over_time: List[TimeSeriesPoint] = Field(default_factory=list)
    message: Optional[str] = None
