"""Countries endpoints."""
from __future__ import annotations
from fastapi import APIRouter, Depends, Query
from backend.dependencies import get_date_range, DateRangeParams
from backend.services.hbase_service import HBaseService
from backend.services.geography_service import get_country_list

router = APIRouter()


@router.get("/countries")
def list_countries():
    return {"countries": get_country_list()}


@router.get("/countries/{country_code}/overview")
def country_overview(country_code: str, date_range: DateRangeParams = Depends(get_date_range)):
    svc = HBaseService()
    return svc.get_country_overview(country_code.upper(), date_range.from_dt, date_range.to_dt)


@router.get("/countries/{country_code}/events")
def country_events(
    country_code: str,
    date_range: DateRangeParams = Depends(get_date_range),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=500),
):
    svc = HBaseService()
    return svc.get_events(
        from_dt=date_range.from_dt, to_dt=date_range.to_dt,
        country=country_code.upper(), page=page, limit=limit,
    )


@router.get("/countries/{country_code}/topics")
def country_topics(country_code: str, date_range: DateRangeParams = Depends(get_date_range)):
    svc = HBaseService()
    return svc.get_topics(date_range.from_dt, date_range.to_dt, country=country_code.upper())


@router.get("/countries/{country_code}/regions")
def country_regions(country_code: str, date_range: DateRangeParams = Depends(get_date_range)):
    svc = HBaseService()
    return svc.get_country_regions(country_code.upper(), date_range.from_dt, date_range.to_dt)

