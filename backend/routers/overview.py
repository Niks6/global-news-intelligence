"""Overview dashboard endpoint."""
from __future__ import annotations
from fastapi import APIRouter, Depends
from backend.dependencies import get_date_range, DateRangeParams
from backend.services.hbase_service import HBaseService

router = APIRouter()


@router.get("/overview")
def get_overview(date_range: DateRangeParams = Depends(get_date_range)):
    svc = HBaseService()
    return svc.get_overview(date_range.from_dt, date_range.to_dt)

