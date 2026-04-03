# ============================================================
# aggregation/daily_aggregator.py
# Agregon sales_hourly -> sales_daily
# ============================================================

import sys
import os
import logging
from datetime import datetime

# Sigurohemi që Python gjen modulin config
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from config.settings import supabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("daily_aggregator")

def run_daily_aggregation(dt: datetime = None):
    """
    Merr të dhënat nga sales_hourly për një datë specifike 
    dhe i agregon ato në tabelën sales_daily.
    """
    if dt is None:
        dt = datetime.now()
    
    date_str = dt.date().isoformat()
    logger.info(f"📅 Duke nisur agregimin ditor për datën: {date_str}")

    try:
        # 1. Lexo të dhënat orare nga Supabase
        resp = supabase.table("sales_hourly").select("*").eq("date", date_str).execute()
        hourly_data = resp.data or []

        if not hourly_data:
            logger.warning(f"⚠️ Nuk u gjetën të dhëna në sales_hourly për datën {date_str}")
            return

        # 2. Grupimi i të dhënave (Store + Product)
        summary = {}
        for row in hourly_data:
            key = (row["store_id"], row["product_id"])
            if key not in summary:
                summary[key] = {
                    "units_sold": 0, "revenue": 0.0, "cogs": 0.0, 
                    "gross_profit": 0.0, "net_revenue": 0.0, 
                    "discount_amount": 0.0, "transactions_count": 0
                }
            
            summary[key]["units_sold"] += row.get("units_sold", 0)
            summary[key]["revenue"] += row.get("revenue", 0.0)
            summary[key]["cogs"] += row.get("cogs", 0.0)
            summary[key]["gross_profit"] += row.get("gross_profit", 0.0)
            summary[key]["net_revenue"] += row.get("net_revenue", 0.0)
            summary[key]["discount_amount"] += row.get("discount_amount", 0.0)
            summary[key]["transactions_count"] += row.get("transactions_count", 0)

        # 3. Përgatitja e rreshtave për sales_daily
        daily_rows = []
        for (sid, pid), v in summary.items():
            # LLOGARITJA E AVG_UNIT_PRICE (Që të mos kemi error 400)
            avg_price = v["revenue"] / v["units_sold"] if v["units_sold"] > 0 else 0
            
            daily_rows.append({
                "date": date_str,
                "store_id": sid,
                "product_id": pid,
                "units_sold": v["units_sold"],
                "avg_unit_price": round(avg_price, 2), # SHTUAR KJO LINJË
                "revenue": round(v["revenue"], 2),
                "cogs": round(v["cogs"], 2),
                "gross_profit": round(v["gross_profit"], 2),
                "net_revenue": round(v["net_revenue"], 2),
                "discount_amount": round(v["discount_amount"], 2),
                "transactions_count": v["transactions_count"]
            })

        # 4. Insertimi në DB
        if daily_rows:
            supabase.table("sales_daily").insert(daily_rows).execute()
            logger.info(f"✅ Sukses: U shtuan {len(daily_rows)} rreshta në sales_daily.")

    except Exception as e:
        logger.error(f"❌ Gabim kritik në daily_aggregator: {e}")
        raise e

if __name__ == "__main__":
    run_daily_aggregation()