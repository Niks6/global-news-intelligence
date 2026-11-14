"""Region-level endpoints."""
from __future__ import annotations
from fastapi import APIRouter, Depends
from backend.dependencies import get_date_range, DateRangeParams
from backend.services.hbase_service import HBaseService

router = APIRouter()


@router.get("/regions/{region_id}/overview")
def region_overview(region_id: str, date_range: DateRangeParams = Depends(get_date_range)):
    svc = HBaseService()
    return svc.get_region_overview(region_id.upper(), date_range.from_dt, date_range.to_dt)

