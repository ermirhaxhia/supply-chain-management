# ============================================================
# api/routers/sales.py
# API 1 — Sales Endpoints
# ============================================================

import logging
from fastapi import APIRouter, Depends, Query
from typing import Optional
from config.settings import supabase
from api.auth import verify_sales_key

logger = logging.getLogger("sales_router")
router = APIRouter(prefix="/api/v1/sales", tags=["Sales"])

# ============================================================
# GET /transactions
# ============================================================
@router.get("/transactions", dependencies=[Depends(verify_sales_key)])
async def get_transactions(
    store_id:   Optional[str] = Query(None, description="Filter by store"),
    product_id: Optional[str] = Query(None, description="Filter by product"),
    limit:      int           = Query(100,  description="Max rows", le=1000),
    offset:     int           = Query(0,    description="Pagination offset"),
):
    """Kthen transaksionet e fundit."""
    try:
        query = supabase.table("transactions").select("*")
        if store_id:
            query = query.eq("store_id", store_id)
        if product_id:
            query = query.eq("product_id", product_id)

        response = (
            query
            .order("timestamp", desc=True)
            .range(offset, offset + limit - 1)
            .execute()
        )
        return {
            "status": "ok",
            "count":  len(response.data),
            "data":   response.data
        }
    except Exception as e:
        logger.error(f"❌ GET /transactions: {e}")
        return {"status": "error", "message": str(e)}

# ============================================================
# GET /transactions/store/{store_id}
# ============================================================
@router.get("/transactions/store/{store_id}", dependencies=[Depends(verify_sales_key)])
async def get_transactions_by_store(
    store_id: str,
    limit:    int = Query(100, le=1000),
):
    """Kthen transaksionet për 1 store të caktuar."""
    try:
        response = (
            supabase.table("transactions")
            .select("*")
            .eq("store_id", store_id)
            .order("timestamp", desc=True)
            .limit(limit)
            .execute()
        )
        return {
            "status":   "ok",
            "store_id": store_id,
            "count":    len(response.data),
            "data":     response.data
        }
    except Exception as e:
        logger.error(f"❌ GET /transactions/store/{store_id}: {e}")
        return {"status": "error", "message": str(e)}

# ============================================================
# GET /summary/hourly
# ============================================================
@router.get("/summary/hourly", dependencies=[Depends(verify_sales_key)])
async def get_sales_hourly(
    store_id: Optional[str] = Query(None),
    date:     Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit:    int           = Query(100, le=1000),
):
    """Kthen agregimin orësh të shitjeve."""
    try:
        query = supabase.table("sales_hourly").select("*")
        if store_id:
            query = query.eq("store_id", store_id)
        if date:
            query = query.eq("date", date)

        response = query.order("date", desc=True).limit(limit).execute()
        return {
            "status": "ok",
            "count":  len(response.data),
            "data":   response.data
        }
    except Exception as e:
        logger.error(f"❌ GET /summary/hourly: {e}")
        return {"status": "error", "message": str(e)}

# ============================================================
# GET /summary/daily
# ============================================================
@router.get("/summary/daily", dependencies=[Depends(verify_sales_key)])
async def get_sales_daily(
    store_id: Optional[str] = Query(None),
    limit:    int           = Query(30, le=365),
):
    """Kthen agregimin ditor të shitjeve."""
    try:
        query = supabase.table("sales_daily").select("*")
        if store_id:
            query = query.eq("store_id", store_id)

        response = query.order("date", desc=True).limit(limit).execute()
        return {
            "status": "ok",
            "count":  len(response.data),
            "data":   response.data
        }
    except Exception as e:
        logger.error(f"❌ GET /summary/daily: {e}")
        return {"status": "error", "message": str(e)}

# ============================================================
# GET /summary/monthly
# ============================================================
@router.get("/summary/monthly", dependencies=[Depends(verify_sales_key)])
async def get_sales_monthly(
    store_id: Optional[str] = Query(None),
    limit:    int           = Query(12, le=60),
):
    """Kthen agregimin mujor të shitjeve."""
    try:
        query = supabase.table("sales_monthly").select("*")
        if store_id:
            query = query.eq("store_id", store_id)

        response = (
            query
            .order("year",  desc=True)
            .order("month", desc=True)
            .limit(limit)
            .execute()
        )
        return {
            "status": "ok",
            "count":  len(response.data),
            "data":   response.data
        }
    except Exception as e:
        logger.error(f"❌ GET /summary/monthly: {e}")
        return {"status": "error", "message": str(e)}

# ============================================================
# GET /kpi
# ============================================================
@router.get("/kpi", dependencies=[Depends(verify_sales_key)])
async def get_sales_kpi(
    store_id: Optional[str] = Query(None),
):
    """Kthen KPI-të kryesore të shitjeve."""
    try:
        query = supabase.table("kpi_monthly").select("*")
        if store_id:
            query = query.eq("store_id", store_id)

        response = (
            query
            .order("year",  desc=True)
            .order("month", desc=True)
            .limit(12)
            .execute()
        )
        return {
            "status": "ok",
            "count":  len(response.data),
            "data":   response.data
        }
    except Exception as e:
        logger.error(f"❌ GET /kpi: {e}")
        return {"status": "error", "message": str(e)}