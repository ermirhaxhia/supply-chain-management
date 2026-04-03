# ============================================================
# aggregation/monthly_aggregator.py
# Agregon sales_daily -> sales_monthly + kpi_monthly
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
logger = logging.getLogger("monthly_aggregator")

def run_monthly_aggregation(dt: datetime = None):
    """
    Agregon të dhënat e muajit të kaluar në sales_monthly dhe kpi_monthly.
    """
    if dt is None:
        dt = datetime.now()

    # Përcaktimi i muajit të kaluar
    if dt.month == 1:
        year, month = dt.year - 1, 12
    else:
        year, month = dt.year, dt.month - 1

    month_start = f"{year}-{month:02d}-01"
    month_end   = f"{year}-{month:02d}-31"

    logger.info(f"📊 Duke nisur agregimin mujor për periudhën: {year}-{month:02d}")

    try:
        # --- 1. Leximi i të dhënave nga sales_daily ---
        daily_resp = supabase.table("sales_daily").select("*").gte("date", month_start).lte("date", month_end).execute()
        daily_data = daily_resp.data or []

        if not daily_data:
            logger.warning(f"⚠️ Nuk u gjetën të dhëna në sales_daily për muajin {month:02d}")
            return

        # --- 2. Agregimi i Shitjeve Mujore (Sipas Store + Product) ---
        groups = {}
        for row in daily_data:
            key = (row["store_id"], row["product_id"])
            if key not in groups:
                groups[key] = {
                    "units": 0, "rev": 0.0, "disc": 0.0, "net": 0.0, "cogs": 0.0, "profit": 0.0, "txns": 0
                }
            groups[key]["units"]  += row.get("units_sold", 0)
            groups[key]["rev"]    += row.get("revenue", 0.0)
            groups[key]["disc"]   += row.get("discount_amount", 0.0)
            groups[key]["net"]    += row.get("net_revenue", 0.0)
            groups[key]["cogs"]   += row.get("cogs", 0.0)
            groups[key]["profit"] += row.get("gross_profit", 0.0)
            groups[key]["txns"]   += row.get("transactions_count", 0)

        monthly_rows = []
        for (sid, pid), v in groups.items():
            # Llogaritja e çmimit mesatar (Për të shmangur error-in e kolonës NOT NULL)
            avg_p = v["rev"] / v["units"] if v["units"] > 0 else 0
            
            monthly_rows.append({
                "store_id": sid,
                "product_id": pid,
                "year": year,
                "month": month,
                "units_sold": v["units"],
                "avg_unit_price": round(avg_p, 2),
                "revenue": round(v["rev"], 2),
                "discount_amount": round(v["disc"], 2),
                "net_revenue": round(v["net"], 2),
                "cogs": round(v["cogs"], 2),
                "gross_profit": round(v["profit"], 2),
                "transactions_count": v["txns"]
            })

        if monthly_rows:
            supabase.table("sales_monthly").insert(monthly_rows).execute()
            logger.info(f"✅ U shtuan {len(monthly_rows)} rreshta në sales_monthly.")

        # --- 3. Llogaritja e KPI-ve Mujore ---
        # Marrim kostot e transportit për muajin
        shp_resp = supabase.table("shipments").select("transport_cost").gte("departure_time", f"{month_start}T00:00:00").lte("departure_time", f"{month_end}T23:59:59").execute()
        total_transport = sum(s["transport_cost"] for s in (shp_resp.data or []))

        kpi_rows = []
        unique_stores = list(set(r["store_id"] for r in monthly_rows))
        
        for sid in unique_stores:
            store_data = [r for r in monthly_rows if r["store_id"] == sid]
            
            total_net_rev = sum(r["net_revenue"] for r in store_data)
            total_cogs    = sum(r["cogs"] for r in store_data)
            total_gm      = sum(r["gross_profit"] for r in store_data)
            total_txns    = sum(r["transactions_count"] for r in store_data)
            
            # Kostoja e transportit e ndarë për dyqan
            store_transport = total_transport / len(unique_stores) if unique_stores else 0
            # Kostoja e mbajtjes së inventarit (estimim 2% e COGS)
            inv_holding_cost = total_cogs * 0.02 

            kpi_rows.append({
                "store_id": sid,
                "year": year,
                "month": month,
                "total_revenue": round(total_net_rev, 2),
                "total_cogs": round(total_cogs, 2),
                "gross_margin": round(total_gm, 2),
                "gross_margin_pct": round((total_gm / total_net_rev * 100), 2) if total_net_rev > 0 else 0,
                "total_transactions": total_txns,
                "avg_basket_value": round((total_net_rev / total_txns), 2) if total_txns > 0 else 0,
                "transport_cost": round(store_transport, 2),
                "inventory_cost": round(inv_holding_cost, 2),
                "net_profit": round(total_gm - store_transport - inv_holding_cost, 2)
            })

        if kpi_rows:
            supabase.table("kpi_monthly").insert(kpi_rows).execute()
            logger.info(f"✅ KPI-të mujore u llogaritën për {len(kpi_rows)} dyqane.")

    except Exception as e:
        logger.error(f"❌ Gabim kritik në monthly_aggregator: {e}")
        raise e

if __name__ == "__main__":
    run_monthly_aggregation()