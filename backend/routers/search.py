"""Search endpoint."""
from __future__ import annotations
from fastapi import APIRouter, Depends, Query
from backend.dependencies import get_date_range, DateRangeParams
from backend.services.hbase_service import HBaseService

router = APIRouter()


@router.get("/search")
def search(
    q: str = Query(..., min_length=2, max_length=200, description="Search query"),
    date_range: DateRangeParams = Depends(get_date_range),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
):
    svc = HBaseService()
    return svc.search_events(q, date_range.from_dt, date_range.to_dt, page=page, limit=limit)

