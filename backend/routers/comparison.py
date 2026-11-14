"""Country comparison endpoint."""
from __future__ import annotations
from typing import List
from fastapi import APIRouter, Depends, Query, HTTPException
from backend.dependencies import get_date_range, DateRangeParams
from backend.services.hbase_service import HBaseService

router = APIRouter()


@router.get("/compare/countries")
def compare_countries(
    codes: str = Query(..., description="Comma-separated FIPS country codes, e.g. US,UK,FR"),
    date_range: DateRangeParams = Depends(get_date_range),
):
    country_list: List[str] = [c.strip().upper() for c in codes.split(",") if c.strip()]
    if len(country_list) < 2:
        raise HTTPException(status_code=422, detail="Provide at least 2 country codes")
    if len(country_list) > 6:
        raise HTTPException(status_code=422, detail="Maximum 6 countries for comparison")
    svc = HBaseService()
    return svc.compare_countries(country_list, date_range.from_dt, date_range.to_dt)

