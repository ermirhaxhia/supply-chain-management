# ============================================================
# aggregation/monthly_aggregator.py
# Agregon të dhënat fund muaji → sales_monthly + kpi_monthly
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
logger = logging.getLogger("monthly_aggregator")

def run_monthly_aggregation(dt: datetime = None):
    """
    Agregon të dhënat e muajit të kaluar në sales_monthly dhe kpi_monthly.
    """
    
    # ============================================================
    # PJESA 1: PËRGATITJA E DATAVE (Gjetja e muajit të kaluar)
    # ============================================================
    if dt is None:
        dt = datetime.now()

    if dt.month == 1:
        year, month = dt.year - 1, 12
    else:
        year, month = dt.year, dt.month - 1

    month_start = f"{year}-{month:02d}-01"
    month_end   = f"{year}-{month:02d}-31"

    logger.info(f"📆 Monthly Aggregation Filloi | Muaji: {year}-{month:02d}")
    monthly_rows = []
    kpi_rows = []

    # ============================================================
    # PJESA 2: AGREGIMI I SHITJEVE MUJORE (sales_daily -> sales_monthly)
    # ============================================================
    try:
        # --- Hapi 2.1: Leximi i të dhënave ditore të muajit ---
        daily_resp = (
            supabase.table("sales_daily")
            .select("*")
            .gte("date", month_start)
            .lte("date", month_end)
            .execute()
        )
        daily_data = daily_resp.data or []

        # --- Hapi 2.2: Grupimi sipas Produktit dhe Dyqanit ---
        groups: dict = {}
        for row in daily_data:
            key = (row["store_id"], row["product_id"])
            if key not in groups:
                groups[key] = {
                    "units_sold": 0, "revenue": 0.0, "discount_amount": 0.0,
                    "net_revenue": 0.0, "cogs": 0.0, "gross_profit": 0.0,
                    "transactions_count": 0
                }
            groups[key]["units_sold"]         += row["units_sold"]
            groups[key]["revenue"]            += row["revenue"]
            groups[key]["discount_amount"]    += row["discount_amount"]
            groups[key]["net_revenue"]        += row["net_revenue"]
            groups[key]["cogs"]               += row["cogs"]
            groups[key]["gross_profit"]       += row["gross_profit"]
            groups[key]["transactions_count"] += row["transactions_count"]

        # --- Hapi 2.3: Përgatitja e listës dhe Insertimi ---
        for (store_id, product_id), vals in groups.items():
            units = vals["units_sold"]
            avg_price = vals["revenue"] / units if units > 0 else 0
            
            monthly_rows.append({
                "store_id":           store_id,
                "product_id":         product_id,
                "year":               year,
                "month":              month,
                "units_sold":         units,
                "avg_unit_price":     round(avg_price, 2),
                "revenue":            round(vals["revenue"], 2),
                "discount_amount":    round(vals["discount_amount"], 2),
                "net_revenue":        round(vals["net_revenue"], 2),
                "cogs":               round(vals["cogs"], 2),
                "gross_profit":       round(vals["gross_profit"], 2),
                "transactions_count": vals["transactions_count"]
            })

        if monthly_rows:
            supabase.table("sales_monthly").insert(monthly_rows).execute()
            logger.info(f"✅ PJESA 2: U agreguan {len(monthly_rows)} produkte në sales_monthly")
        else:
            logger.warning("⚠️ PJESA 2: Nuk u gjetën të dhëna ditore për këtë muaj.")

    except Exception as e:
        logger.error(f"❌ ERROR NË PJESËN 2 (Shitjet Mujore): {e}")


    # ============================================================
    # PJESA 3: KPI DHE TREGUESIT FINANCIARË (kpi_monthly)
    # ============================================================
    try:
        # --- Hapi 3.1: Gjetja e dyqaneve dhe Transportit ---
        stores_resp = supabase.table("stores").select("store_id").execute()
        stores      = stores_resp.data or []

        shp_resp = (
            supabase.table("shipments")
            .select("transport_cost")
            .gte("departure_time", f"{month_start}T00:00:00")
            .lte("departure_time", f"{month_end}T23:59:59")
            .execute()
        )
        transport_cost = sum(s["transport_cost"] for s in (shp_resp.data or []))

        # --- Hapi 3.2: Llogaritja e Financave për çdo Dyqan ---
        for store in stores:
            store_id    = store["store_id"]
            store_data  = [r for r in monthly_rows if r["store_id"] == store_id]

            if not store_data:
                continue

            total_revenue      = sum(r["revenue"] for r in store_data)
            total_net_revenue  = sum(r["net_revenue"] for r in store_data)
            total_cogs         = sum(r["cogs"] for r in store_data)
            gross_margin       = sum(r["gross_profit"] for r in store_data)
            
            total_transactions = sum(r["transactions_count"] for r in store_data)
            avg_basket         = total_net_revenue / total_transactions if total_transactions > 0 else 0
            margin_pct         = (gross_margin / total_net_revenue * 100) if total_net_revenue > 0 else 0

            kpi_rows.append({
                "store_id":          store_id,
                "year":              year,
                "month":             month,
                "total_revenue":     round(total_net_revenue, 2),
                "total_cogs":        round(total_cogs, 2),
                "gross_margin":      round(gross_margin, 2),
                "gross_margin_pct":  round(margin_pct, 2),
                "total_transactions":total_transactions,
                "avg_basket_value":  round(avg_basket, 2),
                "transport_cost":    round(transport_cost, 2),
                "inventory_cost":    round(total_cogs * 0.02, 2),
                "total_cost":        round(total_cogs + transport_cost, 2),
                "net_profit":        round(gross_margin - transport_cost - (total_cogs * 0.02), 2),
            })

        # --- Hapi 3.3: Insertimi i KPI-ve ---
        if kpi_rows:
            supabase.table("kpi_monthly").insert(kpi_rows).execute()
            logger.info(f"✅ PJESA 3: KPI financiare u llogaritën për {len(kpi_rows)} dyqane")

    except Exception as e:
        logger.error(f"❌ ERROR NË PJESËN 3 (KPI dhe Financat): {e}")

    return len(monthly_rows), len(kpi_rows)

if __name__ == "__main__":
    run_monthly_aggregation()