"""
FastAPI shared dependencies: pagination, time-range, HBase client.
"""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Query, HTTPException
from pydantic import BaseModel, model_validator

from src.common.config import get_settings

settings = get_settings()


class PageParams(BaseModel):
    page: int = Query(1, ge=1, description="Page number (1-indexed)")
    limit: int = Query(50, ge=1, le=500, description="Results per page")

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.limit


class DateRangeParams(BaseModel):
    from_dt: datetime
    to_dt: datetime

    @model_validator(mode="after")
    def validate_range(self) -> "DateRangeParams":
        if self.from_dt >= self.to_dt:
            raise ValueError("from_dt must be before to_dt")
        delta = self.to_dt - self.from_dt
        max_days = settings.api_max_time_range_days
        if delta.days > max_days:
            raise ValueError(f"Time range cannot exceed {max_days} days")
        return self


def get_date_range(
    from_dt: Optional[str] = Query(None, description="ISO 8601 start datetime (default: 7 days ago)"),
    to_dt: Optional[str] = Query(None, description="ISO 8601 end datetime (default: now)"),
) -> DateRangeParams:
    now = datetime.now(timezone.utc)
    default_days = settings.api_default_time_range_days
    try:
        to = datetime.fromisoformat(to_dt.replace("Z", "+00:00")) if to_dt else now
        fr = datetime.fromisoformat(from_dt.replace("Z", "+00:00")) if from_dt else now - timedelta(days=default_days)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=422, detail="Invalid datetime format. Use ISO 8601 (e.g. 2026-01-01T00:00:00Z)")
    try:
        return DateRangeParams(from_dt=fr, to_dt=to)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
