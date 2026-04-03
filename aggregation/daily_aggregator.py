# ============================================================
# aggregation/daily_aggregator.py
# Agregon të dhënat orare -> sales_daily
# ============================================================

import sys
import os
import logging
from datetime import datetime, timedelta

# Fix path për të gjetur config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import supabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("daily_aggregator")

def run_daily_aggregation(dt: datetime = None):
    """Mbledh të dhënat nga sales_hourly dhe i fut në sales_daily."""
    if dt is None:
        dt = datetime.now()
    
    date_str = dt.date().isoformat()
    logger.info(f"📅 Duke nisur agregimin ditor për datën: {date_str}")

    try:
        # 1. Merr të dhënat orare të ditës
        resp = supabase.table("sales_hourly").select("*").eq("date", date_str).execute()
        hourly_data = resp.data or []

        if not hourly_data:
            logger.warning(f"⚠️ Nuk u gjetën të dhëna orare për datën {date_str}")
            return

        # 2. Logjika e grupimit (Store + Product)
        summary = {}
        for row in hourly_data:
            key = (row["store_id"], row["product_id"])
            if key not in summary:
                summary[key] = {
                    "units_sold": 0, "revenue": 0.0, "cogs": 0.0, 
                    "gross_profit": 0.0, "net_revenue": 0.0, 
                    "discount_amount": 0.0, "transactions_count": 0
                }
            
            summary[key]["units_sold"] += row["units_sold"]
            summary[key]["revenue"] += row["revenue"]
            summary[key]["cogs"] += row["cogs"]
            summary[key]["gross_profit"] += row["gross_profit"]
            summary[key]["net_revenue"] += row["net_revenue"]
            summary[key]["discount_amount"] += row["discount_amount"]
            summary[key]["transactions_count"] += row["transactions_count"]

        # 3. Përgatitja e rreshtave për INSERT
        daily_rows = []
        for (sid, pid), v in summary.items():
            daily_rows.append({
                "date": date_str,
                "store_id": sid,
                "product_id": pid,
                "units_sold": v["units_sold"],
                "revenue": round(v["revenue"], 2),
                "cogs": round(v["cogs"], 2),
                "gross_profit": round(v["gross_profit"], 2),
                "net_revenue": round(v["net_revenue"], 2),
                "discount_amount": round(v["discount_amount"], 2),
                "transactions_count": v["transactions_count"]
            })

        # 4. Dërgimi në Supabase
        if daily_rows:
            supabase.table("sales_daily").insert(daily_rows).execute()
            logger.info(f"✅ Sukses: U shtuan {len(daily_rows)} rreshtat në sales_daily.")

    except Exception as e:
        logger.error(f"❌ Gabim gjatë agregimit ditor: {e}")
        raise e

if __name__ == "__main__":
    run_daily_aggregation()