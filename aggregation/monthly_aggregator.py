# ============================================================
# aggregation/monthly_aggregator.py
# Agregon të dhënat fund muaji → sales_monthly + kpi_monthly
# ============================================================

import sys
import os
import logging
import calendar
from datetime import datetime

# Shtohet rruga e projektit për të gjetur konfigurimet
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import supabase

# Konfigurimi i Log-eve
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("monthly_aggregator")

def run_monthly_aggregation(dt: datetime = None):
    """
    Agregon të dhënat e muajit të kaluar në sales_monthly dhe kpi_monthly.
    """
    
    # ============================================================
    # PJESA 1: PËRGATITJA E DATAVE
    # ============================================================
    if dt is None:
        dt = datetime.now()

    # Targetojmë muajin aktual për testim
    year, month = dt.year, dt.month

    # Gjejmë ditën e fundit të saktë të muajit
    last_day = calendar.monthrange(year, month)[1]

    month_start = f"{year}-{month:02d}-01"
    month_end   = f"{year}-{month:02d}-{last_day:02d}"

    logger.info(f"📊 Duke nisur agregimin mujor REAL për: {year}-{month:02d} (Deri më {last_day})")

    try:
        # ============================================================
        # PJESA 2: AGREGIMI I SHITJEVE (sales_daily → sales_monthly)
        # ============================================================
        
        # 1. Lexojmë të dhënat ditore nga Supabase
        daily_resp = supabase.table("sales_daily").select("*").gte("date", month_start).lte("date", month_end).execute()
        daily_data = daily_resp.data or []

        if not daily_data:
            logger.warning(f"⚠️ Nuk u gjetën të dhëna në sales_daily për {year}-{month:02d}")
            return

        # 2. Grupimi i të dhënave (Përdorim emra të plotë për të shmangur KeyError)
        groups = {}
        for row in daily_data:
            key = (row["store_id"], row["product_id"])
            if key not in groups:
                groups[key] = {
                    "units_sold": 0, 
                    "revenue": 0.0, 
                    "discount_amount": 0.0, 
                    "net_revenue": 0.0, 
                    "cogs": 0.0, 
                    "gross_profit": 0.0, 
                    "transactions_count": 0
                }
            
            groups[key]["units_sold"]        += row.get("units_sold", 0)
            groups[key]["revenue"]           += row.get("revenue", 0.0)
            groups[key]["discount_amount"]   += row.get("discount_amount", 0.0)
            groups[key]["net_revenue"]       += row.get("net_revenue", 0.0)
            groups[key]["cogs"]              += row.get("cogs", 0.0)
            groups[key]["gross_profit"]      += row.get("gross_profit", 0.0)
            groups[key]["transactions_count"] += row.get("transactions_count", 0)

        # 3. Përgatitja e rreshtave për sales_monthly
        monthly_sales_rows = []
        for (sid, pid), v in groups.items():
            avg_p = v["revenue"] / v["units_sold"] if v["units_sold"] > 0 else 0
            
            monthly_sales_rows.append({
                "store_id": sid,
                "product_id": pid,
                "year": year,
                "month": month,
                "units_sold": v["units_sold"],
                "avg_unit_price": round(avg_p, 2),
                "revenue": round(v["revenue"], 2),
                "discount_amount": round(v["discount_amount"], 2),
                "net_revenue": round(v["net_revenue"], 2),
                "cogs": round(v["cogs"] ,2),
                "gross_profit": round(v["gross_profit"], 2),
                "transactions_count": v["transactions_count"]
            })

        # 4. Insertimi në sales_monthly
        if monthly_sales_rows:
            supabase.table("sales_monthly").insert(monthly_sales_rows).execute()
            logger.info(f"✅ U mbush sales_monthly me {len(monthly_sales_rows)} rreshta.")

        # ============================================================
        # PJESA 3: LLOGARITJA E KPI-VE (kpi_monthly)
        # ============================================================
        
        # 3.1: Marrim kostot e transportit
        shp_resp = supabase.table("shipments").select("*").gte("departure_time", month_start).execute()
        shipments = shp_resp.data or []
        total_transport_all = sum(s["transport_cost"] for s in shipments)

        unique_stores = list(set(r["store_id"] for r in monthly_sales_rows))
        kpi_rows = []

        for sid in unique_stores:
            store_data = [r for r in monthly_sales_rows if r["store_id"] == sid]
            
            total_net_rev = sum(r["net_revenue"] for r in store_data)
            total_cogs    = sum(r["cogs"] for r in store_data)
            total_gm      = sum(r["gross_profit"] for r in store_data)
            total_txns    = sum(r["transactions_count"] for r in store_data)
            
            # Llogaritjet e kostove
            num_stores = len(unique_stores) if len(unique_stores) > 0 else 1
            store_transport = total_transport_all / num_stores
            inventory_holding_cost = total_cogs * 0.02
            total_cost = total_cogs + store_transport + inventory_holding_cost

            kpi_rows.append({
                "store_id": sid,
                "year": year,
                "month": month,
                "total_revenue": round(total_net_rev, 2),
                "total_cogs": round(total_cogs, 2),
                "gross_margin": round(total_gm, 2),
                "gross_margin_pct": round((total_gm / total_net_rev * 100), 2) if total_net_rev > 0 else 0,
                "total_transactions": total_txns,
                "avg_basket_value": round(total_net_rev / total_txns, 2) if total_txns > 0 else 0,
                "stockout_rate_pct": 0.0,
                "otd_pct": 98.5,
                "avg_lead_time_days": 1.5,
                "transport_cost": round(store_transport, 2),
                "inventory_cost": round(inventory_holding_cost, 2),
                "total_cost": round(total_cost, 2),
                "net_profit": round(total_gm - store_transport - inventory_holding_cost, 2)
            })

        # 3.2: Insertimi i KPI-ve
        if kpi_rows:
            supabase.table("kpi_monthly").insert(kpi_rows).execute()
            logger.info(f"✅ KPI-të mujore u shtuan për {len(kpi_rows)} dyqane.")

    except Exception as e:
        logger.error(f"❌ Gabim gjatë agregimit mujor: {e}")
        raise e

if __name__ == "__main__":
    run_monthly_aggregation()