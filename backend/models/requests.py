"""
Request parameter models for the Global News Intelligence API.
All query-parameter models use Pydantic v2 with FastAPI's Query/Depends pattern.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import List, Optional

from fastapi import Query
from pydantic import BaseModel, field_validator, model_validator


class EventFilterParams(BaseModel):
    """Query parameters for /api/events endpoint."""

    from_dt: Optional[datetime] = None
    to_dt: Optional[datetime] = None
    country: Optional[str] = Query(None, max_length=2, description="ISO 2-letter country code")
    topic: Optional[str] = Query(None, max_length=100)
    category: Optional[str] = Query(None, max_length=10, description="CAMEO root code e.g. '01'")
    tone_min: Optional[float] = Query(None, ge=-100.0, le=100.0)
    tone_max: Optional[float] = Query(None, ge=-100.0, le=100.0)
    page: int = Query(1, ge=1)
    limit: int = Query(50, ge=1, le=500)

    model_config = {"arbitrary_types_allowed": True}

    @model_validator(mode="after")
    def validate_tone_range(self) -> "EventFilterParams":
        if self.tone_min is not None and self.tone_max is not None:
            if self.tone_min > self.tone_max:
                raise ValueError("tone_min must be <= tone_max")
        return self


class TopicFilterParams(BaseModel):
    """Query parameters for /api/topics endpoints."""

    from_dt: Optional[datetime] = None
    to_dt: Optional[datetime] = None
    country: Optional[str] = Query(None, max_length=2)
    limit: int = Query(20, ge=1, le=200)


class CountryFilterParams(BaseModel):
    """Query parameters for country-scoped endpoints."""

    from_dt: Optional[datetime] = None
    to_dt: Optional[datetime] = None
    page: int = Query(1, ge=1)
    limit: int = Query(50, ge=1, le=500)


class AnalyticsFilterParams(BaseModel):
    """Query parameters for /api/analytics/* endpoints."""

    from_dt: Optional[datetime] = None
    to_dt: Optional[datetime] = None
    country: Optional[str] = Query(None, max_length=2)
    topic: Optional[str] = Query(None, max_length=100)


class MapFilterParams(BaseModel):
    """Query parameters for /api/map/* endpoints."""

    from_dt: Optional[datetime] = None
    to_dt: Optional[datetime] = None
    country: Optional[str] = Query(None, max_length=2)
    topic: Optional[str] = Query(None, max_length=100)
    category: Optional[str] = Query(None, max_length=10)


class SearchParams(BaseModel):
    """Query parameters for /api/search endpoint."""

    q: str = Query(..., min_length=1, max_length=200, description="Search keyword")
    from_dt: Optional[datetime] = None
    to_dt: Optional[datetime] = None
    page: int = Query(1, ge=1)
    limit: int = Query(50, ge=1, le=500)


class CompareParams(BaseModel):
    """Query parameters for /api/compare/countries endpoint."""

    codes: str = Query(
        ...,
        description="Comma-separated list of ISO 2-letter country codes, e.g. US,GB,FR",
        max_length=100,
    )
    from_dt: Optional[datetime] = None
    to_dt: Optional[datetime] = None

    @property
    def country_codes(self) -> List[str]:
        return [c.strip().upper() for c in self.codes.split(",") if c.strip()]

    @model_validator(mode="after")
    def validate_codes(self) -> "CompareParams":
        codes = self.country_codes
        if len(codes) < 1:
            raise ValueError("At least one country code is required")
        if len(codes) > 10:
            raise ValueError("Maximum 10 country codes for comparison")
        return self
