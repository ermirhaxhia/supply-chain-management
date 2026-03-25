# ============================================================
# aggregation/daily_aggregator.py
# Agregon të dhënat çdo natë → sales_daily + inventory_daily
# ============================================================

import sys
import os
import logging
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import supabase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("daily_aggregator")

def run_daily_aggregation(dt: datetime = None):
    """
    Agregon të dhënat e ditës në:
    - sales_daily
    - inventory_daily
    Ekzekutohet çdo natë ora 23:00.
    """
    if dt is None:
        dt = datetime.now()

    today = dt.date().isoformat()
    logger.info(f"📅 Daily Aggregation | {today}")

    try:
        # ── SALES DAILY ───────────────────────────────────
        txn_resp = (
            supabase.table("transactions")
            .select("store_id, product_id, quantity, total, discount_pct")
            .gte("timestamp", f"{today}T00:00:00")
            .lte("timestamp", f"{today}T23:59:59")
            .execute()
        )
        transactions = txn_resp.data or []

        groups: dict = {}
        for txn in transactions:
            key = (txn["store_id"], txn["product_id"])
            if key not in groups:
                groups[key] = {
                    "transactions":  0,
                    "units_sold":    0,
                    "revenue":       0.0,
                    "discount_total":0.0,
                }
            groups[key]["transactions"]   += 1
            groups[key]["units_sold"]     += txn["quantity"]
            groups[key]["revenue"]        += txn["total"]
            groups[key]["discount_total"] += txn["discount_pct"]

        sales_rows = []
        for (store_id, product_id), vals in groups.items():
            txn_count = vals["transactions"]
            sales_rows.append({
                "store_id":      store_id,
                "product_id":    product_id,
                "date":          today,
                "transactions":  txn_count,
                "units_sold":    vals["units_sold"],
                "revenue":       round(vals["revenue"], 2),
                "avg_basket":    round(vals["revenue"] / txn_count, 2),
                "discount_total":round(vals["discount_total"], 2),
                "stockout_flag": False,
            })

        if sales_rows:
            supabase.table("sales_daily").insert(sales_rows).execute()
            logger.info(f"✅ Sales Daily: {len(sales_rows)} grupe")

        # ── INVENTORY DAILY ───────────────────────────────
        inv_resp = (
            supabase.table("inventory_log")
            .select("store_id, product_id, stock_after, change_reason")
            .gte("timestamp", f"{today}T00:00:00")
            .lte("timestamp", f"{today}T23:59:59")
            .execute()
        )
        inv_logs = inv_resp.data or []

        inv_groups: dict = {}
        for log in inv_logs:
            key = (log["store_id"], log["product_id"])
            if key not in inv_groups:
                inv_groups[key] = {
                    "stock_levels":  [],
                    "expired_units": 0,
                    "restock_count": 0,
                    "stockout_hours":0,
                }
            inv_groups[key]["stock_levels"].append(log["stock_after"])
            if log["change_reason"] == "Expired":
                inv_groups[key]["expired_units"] += 1
            if log["change_reason"] == "Restock":
                inv_groups[key]["restock_count"] += 1
            if log["stock_after"] == 0:
                inv_groups[key]["stockout_hours"] += 1

        inv_rows = []
        for (store_id, product_id), vals in inv_groups.items():
            levels = vals["stock_levels"]
            inv_rows.append({
                "store_id":       store_id,
                "product_id":     product_id,
                "date":           today,
                "avg_stock_level":round(sum(levels) / len(levels), 1),
                "min_stock_level":min(levels),
                "max_stock_level":max(levels),
                "stockout_hours": vals["stockout_hours"],
                "expired_units":  vals["expired_units"],
                "restock_count":  vals["restock_count"],
            })

        if inv_rows:
            supabase.table("inventory_daily").insert(inv_rows).execute()
            logger.info(f"✅ Inventory Daily: {len(inv_rows)} grupe")

        # ── FSHI RAW DATA > 30 DITË ───────────────────────
        cutoff = (dt - timedelta(days=30)).isoformat()
        supabase.table("transactions").delete().lt("timestamp", cutoff).execute()
        supabase.table("inventory_log").delete().lt("timestamp", cutoff).execute()
        logger.info(f"🗑️  Raw data > 30 ditë u fshi")

        return len(sales_rows), len(inv_rows)

    except Exception as e:
        logger.error(f"❌ ERROR daily aggregation: {e}")
        return 0, 0

if __name__ == "__main__":
    run_daily_aggregation()