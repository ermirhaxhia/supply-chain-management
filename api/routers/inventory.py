# ============================================================
# api/routers/inventory.py
# API 2 — Inventory Endpoints
# ============================================================

import logging
from fastapi import APIRouter, Depends, Query
from typing import Optional
from config.settings import supabase
from api.auth import verify_inventory_key

logger = logging.getLogger("inventory_router")
router = APIRouter(prefix="/api/v1/inventory", tags=["Inventory"])

# ============================================================
# GET /stock/{store_id}
# ============================================================
@router.get("/stock/{store_id}", dependencies=[Depends(verify_inventory_key)])
async def get_stock(store_id: str):
    """Kthen gjendjen aktuale të stokut për 1 store."""
    try:
        response = (
            supabase.table("inventory_log")
            .select("product_id, stock_after, timestamp, change_reason")
            .eq("store_id", store_id)
            .order("timestamp", desc=True)
            .limit(500)
            .execute()
        )
        return {
            "status":   "ok",
            "store_id": store_id,
            "count":    len(response.data),
            "data":     response.data
        }
    except Exception as e:
        logger.error(f"❌ GET /stock/{store_id}: {e}")
        return {"status": "error", "message": str(e)}

# ============================================================
# GET /log
# ============================================================
@router.get("/log", dependencies=[Depends(verify_inventory_key)])
async def get_inventory_log(
    store_id:   Optional[str] = Query(None),
    product_id: Optional[str] = Query(None),
    reason:     Optional[str] = Query(None, description="Sale/Restock/Expired/Shrinkage"),
    limit:      int           = Query(100, le=1000),
):
    """Kthen log-un e lëvizjeve të stokut."""
    try:
        query = supabase.table("inventory_log").select("*")
        if store_id:
            query = query.eq("store_id", store_id)
        if product_id:
            query = query.eq("product_id", product_id)
        if reason:
            query = query.eq("change_reason", reason)

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
        logger.error(f"❌ GET /log: {e}")
        return {"status": "error", "message": str(e)}

# ============================================================
# GET /alerts
# ============================================================
@router.get("/alerts", dependencies=[Depends(verify_inventory_key)])
async def get_stock_alerts(
    store_id: Optional[str] = Query(None),
):
    """Kthen produktet nën reorder point — alerts."""
    try:
        # Merr inventory log + products për krahasim
        log_resp = (
            supabase.table("inventory_log")
            .select("store_id, product_id, stock_after")
            .order("timestamp", desc=True)
            .limit(1000)
            .execute()
        )
        prod_resp = supabase.table("products").select(
            "product_id, product_name, reorder_point, min_stock"
        ).execute()

        products_map = {p["product_id"]: p for p in prod_resp.data}

        # Gjej produktet me stok të ulët
        seen    = set()
        alerts  = []
        for row in log_resp.data:
            key = (row["store_id"], row["product_id"])
            if key in seen:
                continue
            seen.add(key)

            if store_id and row["store_id"] != store_id:
                continue

            product       = products_map.get(row["product_id"], {})
            reorder_point = product.get("reorder_point", 20)
            stock_level   = row["stock_after"]

            if stock_level <= reorder_point:
                alerts.append({
                    "store_id":     row["store_id"],
                    "product_id":   row["product_id"],
                    "product_name": product.get("product_name", "—"),
                    "stock_level":  stock_level,
                    "reorder_point":reorder_point,
                    "status":       "CRITICAL" if stock_level == 0 else "LOW",
                })

        return {
            "status": "ok",
            "count":  len(alerts),
            "data":   alerts
        }
    except Exception as e:
        logger.error(f"❌ GET /alerts: {e}")
        return {"status": "error", "message": str(e)}

# ============================================================
# GET /daily
# ============================================================
@router.get("/daily", dependencies=[Depends(verify_inventory_key)])
async def get_inventory_daily(
    store_id: Optional[str] = Query(None),
    limit:    int           = Query(30, le=365),
):
    """Kthen agregimin ditor të stokut."""
    try:
        query = supabase.table("inventory_daily").select("*")
        if store_id:
            query = query.eq("store_id", store_id)

        response = query.order("date", desc=True).limit(limit).execute()
        return {
            "status": "ok",
            "count":  len(response.data),
            "data":   response.data
        }
    except Exception as e:
        logger.error(f"❌ GET /daily: {e}")
        return {"status": "error", "message": str(e)}