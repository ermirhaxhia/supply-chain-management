# ============================================================
# aggregation/hourly_aggregator.py
# Agregon të dhënat çdo orë → sales_hourly
# ============================================================

import sys
import os
import logging
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import supabase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("hourly_aggregator")

def run_hourly_aggregation(dt: datetime = None):
    """
    Agregon transaksionet e orës së fundit në sales_hourly.
    Ekzekutohet në minutën :55 të çdo ore.
    """
    if dt is None:
        dt = datetime.now().replace(minute=0, second=0, microsecond=0)

    logger.info(f"⏱️  Hourly Aggregation | {dt.strftime('%Y-%m-%d %H:%M')}")

    try:
        # Ngarko transaksionet e kësaj ore
        hour_start = dt.replace(minute=0, second=0).isoformat()
        hour_end   = dt.replace(minute=59, second=59).isoformat()

        txn_resp = (
            supabase.table("transactions")
            .select("store_id, product_id, quantity, total, discount_pct")
            .gte("timestamp", hour_start)
            .lte("timestamp", hour_end)
            .execute()
        )
        transactions = txn_resp.data or []

        if not transactions:
            logger.info("⚠️  Nuk ka transaksione për agregim")
            return 0

        # Grupo sipas store_id + product_id
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

        # Ndrto rreshtat e agregimit
        rows = []
        for (store_id, product_id), vals in groups.items():
            txn_count = vals["transactions"]
            rows.append({
                "store_id":      store_id,
                "product_id":    product_id,
                "date":          dt.date().isoformat(),
                "hour":          dt.hour,
                "transactions":  txn_count,
                "units_sold":    vals["units_sold"],
                "revenue":       round(vals["revenue"], 2),
                "avg_basket":    round(vals["revenue"] / txn_count, 2),
                "discount_total":round(vals["discount_total"], 2),
            })

        # Batch INSERT
        if rows:
            supabase.table("sales_hourly").insert(rows).execute()
            logger.info(f"✅ Hourly: {len(rows)} grupe të agreguara")

        return len(rows)

    except Exception as e:
        logger.error(f"❌ ERROR hourly aggregation: {e}")
        return 0

if __name__ == "__main__":
    run_hourly_aggregation()