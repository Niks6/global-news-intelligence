"""Topics endpoints."""
from __future__ import annotations
from typing import Optional
from fastapi import APIRouter, Depends, Query
from backend.dependencies import get_date_range, DateRangeParams
from backend.services.hbase_service import HBaseService

router = APIRouter()


@router.get("/topics")
def list_topics(
    date_range: DateRangeParams = Depends(get_date_range),
    country: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
):
    svc = HBaseService()
    return svc.get_topics(date_range.from_dt, date_range.to_dt, country=country, limit=limit)


@router.get("/topics/{topic}/trend")
def topic_trend(topic: str, date_range: DateRangeParams = Depends(get_date_range)):
    svc = HBaseService()
    return svc.get_topic_trend(topic, date_range.from_dt, date_range.to_dt)


@router.get("/topics/{topic}/countries")
def topic_countries(topic: str, date_range: DateRangeParams = Depends(get_date_range)):
    svc = HBaseService()
    return svc.get_topic_countries(topic, date_range.from_dt, date_range.to_dt)

