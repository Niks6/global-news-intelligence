"""Analytics endpoints: categories, tone, cooperation/conflict, anomalies."""
from __future__ import annotations
from typing import Optional
from fastapi import APIRouter, Depends, Query
from backend.dependencies import get_date_range, DateRangeParams
from backend.services.hbase_service import HBaseService

router = APIRouter()


@router.get("/analytics/categories")
def get_categories(
    date_range: DateRangeParams = Depends(get_date_range),
    country: Optional[str] = Query(None),
):
    svc = HBaseService()
    return svc.get_category_metrics(date_range.from_dt, date_range.to_dt, country=country)


@router.get("/analytics/tone")
def get_tone(
    date_range: DateRangeParams = Depends(get_date_range),
    country: Optional[str] = Query(None),
    topic: Optional[str] = Query(None),
):
    svc = HBaseService()
    return svc.get_tone_metrics(date_range.from_dt, date_range.to_dt, country=country, topic=topic)


@router.get("/analytics/cooperation-conflict")
def get_cooperation_conflict(
    date_range: DateRangeParams = Depends(get_date_range),
    country: Optional[str] = Query(None),
):
    svc = HBaseService()
    return svc.get_cooperation_conflict(date_range.from_dt, date_range.to_dt, country=country)


@router.get("/analytics/anomalies")
def get_anomalies(
    date_range: DateRangeParams = Depends(get_date_range),
    country: Optional[str] = Query(None),
    min_z_score: float = Query(2.5, ge=1.0, le=10.0),
):
    svc = HBaseService()
    return svc.get_anomalies(date_range.from_dt, date_range.to_dt, country=country, min_z_score=min_z_score)

