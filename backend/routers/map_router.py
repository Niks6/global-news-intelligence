"""Map data endpoints."""
from __future__ import annotations
from typing import Optional
from fastapi import APIRouter, Depends, Query
from backend.dependencies import get_date_range, DateRangeParams
from backend.services.hbase_service import HBaseService

router = APIRouter()


@router.get("/map/events")
def map_events(
    date_range: DateRangeParams = Depends(get_date_range),
    country: Optional[str] = Query(None),
    topic: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    max_points: int = Query(2000, ge=100, le=5000),
):
    svc = HBaseService()
    return svc.get_map_events(
        date_range.from_dt, date_range.to_dt,
        country=country, topic=topic, category=category,
        max_points=max_points,
    )


@router.get("/map/countries")
def map_countries(date_range: DateRangeParams = Depends(get_date_range)):
    svc = HBaseService()
    return svc.get_map_countries(date_range.from_dt, date_range.to_dt)

