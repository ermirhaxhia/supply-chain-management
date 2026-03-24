# ============================================================
# api/routers/procurement.py
# API 4 — Procurement & Marketing Endpoints
# ============================================================

import logging
from fastapi import APIRouter, Depends, Query
from typing import Optional
from config.settings import supabase
from api.auth import verify_procurement_key

logger = logging.getLogger("procurement_router")
router = APIRouter(prefix="/api/v1/procurement", tags=["Procurement"])

# ============================================================
# GET /orders
# ============================================================
@router.get("/orders", dependencies=[Depends(verify_procurement_key)])
async def get_orders(
    status:   Optional[str] = Query(None, description="Pending/Delivered/Late"),
    supplier: Optional[str] = Query(None),
    limit:    int           = Query(100, le=1000),
):
    """Kthen të gjitha purchase orders."""
    try:
        query = supabase.table("purchase_orders").select("*")
        if status:
            query = query.eq("status", status)
        if supplier:
            query = query.eq("supplier_id", supplier)

        response = (
            query
            .order("order_date", desc=True)
            .limit(limit)
            .execute()
        )
        return {
            "status": "ok",
            "count":  len(response.data),
            "data":   response.data
        }
    except Exception as e:
        logger.error(f"❌ GET /orders: {e}")
        return {"status": "error", "message": str(e)}

# ============================================================
# GET /orders/pending
# ============================================================
@router.get("/orders/pending", dependencies=[Depends(verify_procurement_key)])
async def get_pending_orders():
    """Kthen porositë aktive (Pending)."""
    try:
        response = (
            supabase.table("purchase_orders")
            .select("*")
            .eq("status", "Pending")
            .order("expected_date")
            .execute()
        )
        return {
            "status": "ok",
            "count":  len(response.data),
            "data":   response.data
        }
    except Exception as e:
        logger.error(f"❌ GET /orders/pending: {e}")
        return {"status": "error", "message": str(e)}

# ============================================================
# GET /suppliers/scorecard
# ============================================================
@router.get("/suppliers/scorecard", dependencies=[Depends(verify_procurement_key)])
async def get_supplier_scorecard():
    """Kthen scorecard-in e furnizuesve."""
    try:
        suppliers_resp = supabase.table("suppliers").select("*").execute()
        orders_resp    = (
            supabase.table("purchase_orders")
            .select("supplier_id, status, total_cost")
            .execute()
        )

        suppliers   = suppliers_resp.data
        orders      = orders_resp.data

        scorecard = []
        for sup in suppliers:
            sup_orders = [o for o in orders if o["supplier_id"] == sup["supplier_id"]]
            total      = len(sup_orders)
            delivered  = sum(1 for o in sup_orders if o["status"] == "Delivered")
            total_cost = sum(o["total_cost"] for o in sup_orders)

            scorecard.append({
                "supplier_id":      sup["supplier_id"],
                "supplier_name":    sup["supplier_name"],
                "city":             sup["city"],
                "total_orders":     total,
                "delivered_orders": delivered,
                "on_time_rate":     round(delivered / total * 100, 1) if total > 0 else 0,
                "total_cost_lek":   round(total_cost, 0),
                "reliability_score":sup.get("reliability_score", 0),
            })

        return {
            "status": "ok",
            "count":  len(scorecard),
            "data":   scorecard
        }
    except Exception as e:
        logger.error(f"❌ GET /suppliers/scorecard: {e}")
        return {"status": "error", "message": str(e)}

# ============================================================
# GET /campaigns
# ============================================================
@router.get("/campaigns", dependencies=[Depends(verify_procurement_key)])
async def get_campaigns(
    active_only: bool = Query(False, description="Vetëm kampanjat aktive"),
):
    """Kthen kampanjat marketing."""
    try:
        from datetime import date
        query = supabase.table("campaigns").select("*")

        if active_only:
            today = date.today().isoformat()
            query = query.lte("start_date", today).gte("end_date", today)

        response = query.order("start_date", desc=True).execute()
        return {
            "status": "ok",
            "count":  len(response.data),
            "data":   response.data
        }
    except Exception as e:
        logger.error(f"❌ GET /campaigns: {e}")
        return {"status": "error", "message": str(e)}

# ============================================================
# GET /campaigns/roi
# ============================================================
@router.get("/campaigns/roi", dependencies=[Depends(verify_procurement_key)])
async def get_campaigns_roi():
    """Kthen ROI të kampanjave."""
    try:
        response = (
            supabase.table("campaigns")
            .select("campaign_id, campaign_name, type, cost, revenue_lift_pct, discount_pct")
            .execute()
        )
        data = response.data
        for c in data:
            cost     = c.get("cost", 0)
            lift_pct = c.get("revenue_lift_pct", 0) or 0
            c["roi_estimate"] = round(lift_pct - (cost / 10000), 2)

        return {
            "status": "ok",
            "count":  len(data),
            "data":   data
        }
    except Exception as e:
        logger.error(f"❌ GET /campaigns/roi: {e}")
        return {"status": "error", "message": str(e)}