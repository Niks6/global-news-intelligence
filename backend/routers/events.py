"""Events endpoints."""
from __future__ import annotations
from typing import Optional
from fastapi import APIRouter, Depends, Query
from backend.dependencies import get_date_range, DateRangeParams
from backend.services.hbase_service import HBaseService

router = APIRouter()


@router.get("/events")
def list_events(
    date_range: DateRangeParams = Depends(get_date_range),
    country: Optional[str] = Query(None, description="FIPS country code"),
    topic: Optional[str] = Query(None),
    category: Optional[str] = Query(None, description="CAMEO root code 01-20"),
    tone_min: Optional[float] = Query(None, ge=-100, le=100),
    tone_max: Optional[float] = Query(None, ge=-100, le=100),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=500),
):
    svc = HBaseService()
    return svc.get_events(
        from_dt=date_range.from_dt, to_dt=date_range.to_dt,
        country=country, topic=topic, category=category,
        tone_min=tone_min, tone_max=tone_max, page=page, limit=limit,
    )


@router.get("/events/{global_event_id}")
def get_event(global_event_id: int):
    svc = HBaseService()
    return svc.get_event_by_id(global_event_id)


@router.get("/events/{global_event_id}/mentions")
def get_event_mentions(global_event_id: int, limit: int = Query(50, ge=1, le=200)):
    svc = HBaseService()
    return svc.get_event_mentions(global_event_id, limit=limit)

