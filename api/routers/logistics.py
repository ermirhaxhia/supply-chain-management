# ============================================================
# api/routers/logistics.py
# API 3 — Logistics Endpoints
# ============================================================

import logging
from fastapi import APIRouter, Depends, Query
from typing import Optional
from config.settings import supabase
from api.auth import verify_logistics_key

logger = logging.getLogger("logistics_router")
router = APIRouter(prefix="/api/v1/logistics", tags=["Logistics"])

# ============================================================
# GET /shipments
# ============================================================
@router.get("/shipments", dependencies=[Depends(verify_logistics_key)])
async def get_shipments(
    status: Optional[str] = Query(None, description="In Transit/Delivered/Delayed"),
    limit:  int           = Query(100, le=1000),
):
    """Kthen të gjitha dërgesat."""
    try:
        query = supabase.table("shipments").select("*")
        if status:
            query = query.eq("status", status)

        response = (
            query
            .order("departure_time", desc=True)
            .limit(limit)
            .execute()
        )
        return {
            "status": "ok",
            "count":  len(response.data),
            "data":   response.data
        }
    except Exception as e:
        logger.error(f"❌ GET /shipments: {e}")
        return {"status": "error", "message": str(e)}

# ============================================================
# GET /shipments/active
# ============================================================
@router.get("/shipments/active", dependencies=[Depends(verify_logistics_key)])
async def get_active_shipments():
    """Kthen dërgesat aktive (In Transit)."""
    try:
        response = (
            supabase.table("shipments")
            .select("*")
            .eq("status", "In Transit")
            .order("departure_time", desc=True)
            .execute()
        )
        return {
            "status": "ok",
            "count":  len(response.data),
            "data":   response.data
        }
    except Exception as e:
        logger.error(f"❌ GET /shipments/active: {e}")
        return {"status": "error", "message": str(e)}

# ============================================================
# GET /warehouse/snapshot
# ============================================================
@router.get("/warehouse/snapshot", dependencies=[Depends(verify_logistics_key)])
async def get_warehouse_snapshots(
    warehouse_id: Optional[str] = Query(None),
    limit:        int           = Query(50, le=500),
):
    """Kthen snapshot-et e magazinave."""
    try:
        query = supabase.table("warehouse_snapshot").select("*")
        if warehouse_id:
            query = query.eq("warehouse_id", warehouse_id)

        response = (
            query
            .order("timestamp", desc=True)
            .limit(limit)
            .execute()
        )
        return {
            "status": "ok",
            "count":  len(response.data),
            "data":   response.data
        }
    except Exception as e:
        logger.error(f"❌ GET /warehouse/snapshot: {e}")
        return {"status": "error", "message": str(e)}

# ============================================================
# GET /routes/performance
# ============================================================
@router.get("/routes/performance", dependencies=[Depends(verify_logistics_key)])
async def get_route_performance(
    limit: int = Query(30, le=365),
):
    """Kthen performancën ditore të rrugëve."""
    try:
        response = (
            supabase.table("transport_daily")
            .select("*")
            .order("date", desc=True)
            .limit(limit)
            .execute()
        )
        return {
            "status": "ok",
            "count":  len(response.data),
            "data":   response.data
        }
    except Exception as e:
        logger.error(f"❌ GET /routes/performance: {e}")
        return {"status": "error", "message": str(e)}

# ============================================================
# GET /kpi
# ============================================================
@router.get("/kpi", dependencies=[Depends(verify_logistics_key)])
async def get_logistics_kpi():
    """Kthen KPI-të kryesore të logjistikës."""
    try:
        response = (
            supabase.table("shipments")
            .select("delay_minutes, transport_cost, status, fuel_consumed")
            .order("departure_time", desc=True)
            .limit(500)
            .execute()
        )
        data = response.data
        if not data:
            return {"status": "ok", "data": {}}

        total       = len(data)
        on_time     = sum(1 for s in data if s["delay_minutes"] == 0)
        avg_delay   = sum(s["delay_minutes"] for s in data) / total
        total_cost  = sum(s["transport_cost"] for s in data)
        total_fuel  = sum(s["fuel_consumed"] for s in data)

        return {
            "status": "ok",
            "data": {
                "total_shipments":  total,
                "otd_pct":          round(on_time / total * 100, 1),
                "avg_delay_min":    round(avg_delay, 1),
                "total_cost_lek":   round(total_cost, 0),
                "total_fuel_liters":round(total_fuel, 1),
            }
        }
    except Exception as e:
        logger.error(f"❌ GET /logistics/kpi: {e}")
        return {"status": "error", "message": str(e)}