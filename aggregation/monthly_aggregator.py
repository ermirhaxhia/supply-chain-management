# ============================================================
# aggregation/monthly_aggregator.py
# Agregon sales_daily -> sales_monthly + kpi_monthly
# ============================================================

import sys
import os
import logging
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from config.settings import supabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("monthly_aggregator")

def run_monthly_aggregation(dt: datetime = None):
    if dt is None:
        dt = datetime.now()

    # Llogaritja e muajit të kaluar
    if dt.month == 1:
        year, month = dt.year - 1, 12
    else:
        year, month = dt.year, dt.month - 1

    month_start = f"{year}-{month:02d}-01"
    month_end   = f"{year}-{month:02d}-31"

    logger.info(f"📊 Duke nisur agregimin mujor për: {year}-{month:02d}")

    try:
        # --- Hapi 1: Leximi i sales_daily ---
        daily_resp = supabase.table("sales_daily").select("*").gte("date", month_start).lte("date", month_end).execute()
        daily_data = daily_resp.data or []

        if not daily_data:
            logger.warning(f"⚠️ Nuk u gjetën të dhëna në sales_daily për periudhën {month_start} deri {month_end}")
            return

        # --- Hapi 2: Agregimi për sales_monthly ---
        groups = {}
        for row in daily_data:
            key = (row["store_id"], row["product_id"])
            if key not in groups:
                groups[key] = {
                    "units": 0, "rev": 0.0, "disc": 0.0, "net": 0.0, "cogs": 0.0, "profit": 0.0, "txns": 0
                }
            groups[key]["units"]  += row["units_sold"]
            groups[key]["rev"]    += row["revenue"]
            groups[key]["disc"]   += row["discount_amount"]
            groups[key]["net"]    += row["net_revenue"]
            groups[key]["cogs"]   += row["cogs"]
            groups[key]["profit"] += row["gross_profit"]
            groups[key]["txns"]   += row["transactions_count"]

        monthly_rows = []
        for (sid, pid), v in groups.items():
            avg_p = v["rev"] / v["units"] if v["units"] > 0 else 0
            monthly_rows.append({
                "store_id": sid, "product_id": pid, "year": year, "month": month,
                "units_sold": v["units"], "avg_unit_price": round(avg_p, 2),
                "revenue": round(v["rev"], 2), "discount_amount": round(v["disc"], 2),
                "net_revenue": round(v["net"], 2), "cogs": round(v["cogs"], 2),
                "gross_profit": round(v["profit"], 2), "transactions_count": v["txns"]
            })

        if monthly_rows:
            supabase.table("sales_monthly").insert(monthly_rows).execute()
            logger.info(f"✅ U shtuan {len(monthly_rows)} rreshta në sales_monthly.")

        # --- Hapi 3: Llogaritja e KPI-ve ---
        # Marrim kostot e transportit nga shipments
        shp_resp = supabase.table("shipments").select("transport_cost").gte("departure_time", month_start).lte("departure_time", month_end).execute()
        total_transport = sum(s["transport_cost"] for s in (shp_resp.data or []))

        kpi_rows = []
        stores = list(set(r["store_id"] for r in monthly_rows))
        for sid in stores:
            s_data = [r for r in monthly_rows if r["store_id"] == sid]
            rev = sum(r["net_revenue"] for r in s_data)
            cogs = sum(r["cogs"] for r in s_data)
            gm = sum(r["gross_profit"] for r in s_data)
            txns = sum(r["transactions_count"] for r in s_data)

            kpi_rows.append({
                "store_id": sid, "year": year, "month": month,
                "total_revenue": round(rev, 2), "total_cogs": round(cogs, 2),
                "gross_margin": round(gm, 2),
                "gross_margin_pct": round((gm/rev*100), 2) if rev > 0 else 0,
                "total_transactions": txns,
                "avg_basket_value": round((rev/txns), 2) if txns > 0 else 0,
                "transport_cost": round(total_transport / len(stores), 2), # Shpërndarje e thjeshtë
                "net_profit": round(gm - (total_transport / len(stores)), 2)
            })

        if kpi_rows:
            supabase.table("kpi_monthly").insert(kpi_rows).execute()
            logger.info(f"✅ KPI-të mujore u llogaritën për {len(kpi_rows)} dyqane.")

    except Exception as e:
        logger.error(f"❌ Gabim kritik në monthly_aggregator: {e}")
        raise e

if __name__ == "__main__":
    run_monthly_aggregation()