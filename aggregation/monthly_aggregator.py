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
    Agregon të dhënat e muajit të kaluar në:
    - sales_monthly
    - kpi_monthly
    Ekzekutohet ditën 1 të çdo muaji ora 00:00.
    """
    if dt is None:
        dt = datetime.now()

    # Muaji i kaluar
    if dt.month == 1:
        year, month = dt.year - 1, 12
    else:
        year, month = dt.year, dt.month - 1

    month_start = f"{year}-{month:02d}-01"
    month_end   = f"{year}-{month:02d}-31"

    logger.info(f"📆 Monthly Aggregation | {year}-{month:02d}")

    try:
        # ── SALES MONTHLY ─────────────────────────────────
        daily_resp = (
            supabase.table("sales_daily")
            .select("*")
            .gte("date", month_start)
            .lte("date", month_end)
            .execute()
        )
        daily_data = daily_resp.data or []

        groups: dict = {}
        for row in daily_data:
            key = (row["store_id"], row["product_id"])
            if key not in groups:
                groups[key] = {
                    "transactions":  0,
                    "units_sold":    0,
                    "revenue":       0.0,
                    "discount_total":0.0,
                    "stockout_days": 0,
                }
            groups[key]["transactions"]  += row["transactions"]
            groups[key]["units_sold"]    += row["units_sold"]
            groups[key]["revenue"]       += row["revenue"]
            groups[key]["discount_total"]+= row["discount_total"]
            if row.get("stockout_flag"):
                groups[key]["stockout_days"] += 1

        monthly_rows = []
        for (store_id, product_id), vals in groups.items():
            txn = vals["transactions"]
            monthly_rows.append({
                "store_id":      store_id,
                "product_id":    product_id,
                "year":          year,
                "month":         month,
                "transactions":  txn,
                "units_sold":    vals["units_sold"],
                "revenue":       round(vals["revenue"], 2),
                "avg_basket":    round(vals["revenue"] / txn, 2) if txn > 0 else 0,
                "discount_total":round(vals["discount_total"], 2),
                "stockout_days": vals["stockout_days"],
            })

        if monthly_rows:
            supabase.table("sales_monthly").insert(monthly_rows).execute()
            logger.info(f"✅ Sales Monthly: {len(monthly_rows)} grupe")

        # ── KPI MONTHLY ───────────────────────────────────
        stores_resp = supabase.table("stores").select("store_id").execute()
        stores      = stores_resp.data or []

        kpi_rows = []
        for store in stores:
            store_id    = store["store_id"]
            store_data  = [r for r in monthly_rows if r["store_id"] == store_id]

            if not store_data:
                continue

            total_revenue      = sum(r["revenue"] for r in store_data)
            total_transactions = sum(r["transactions"] for r in store_data)
            total_units        = sum(r["units_sold"] for r in store_data)
            avg_basket         = total_revenue / total_transactions if total_transactions > 0 else 0

            # Transport kosto nga shipments
            shp_resp = (
                supabase.table("shipments")
                .select("transport_cost")
                .gte("departure_time", f"{month_start}T00:00:00")
                .lte("departure_time", f"{month_end}T23:59:59")
                .execute()
            )
            transport_cost = sum(
                s["transport_cost"] for s in (shp_resp.data or [])
            )

            # COGS (kosto mallrave) — ~74% e revenue
            total_cogs    = total_revenue * 0.74
            gross_margin  = total_revenue - total_cogs
            margin_pct    = (gross_margin / total_revenue * 100) if total_revenue > 0 else 0

            # Stockout rate
            total_days    = sum(r["stockout_days"] for r in store_data)
            stockout_rate = (total_days / (len(store_data) * 30) * 100) if store_data else 0

            kpi_rows.append({
                "store_id":          store_id,
                "year":              year,
                "month":             month,
                "total_revenue":     round(total_revenue, 2),
                "total_cogs":        round(total_cogs, 2),
                "gross_margin":      round(gross_margin, 2),
                "gross_margin_pct":  round(margin_pct, 2),
                "total_transactions":total_transactions,
                "avg_basket_value":  round(avg_basket, 2),
                "stockout_rate_pct": round(stockout_rate, 2),
                "otd_pct":           88.0,
                "avg_lead_time_days":2.5,
                "transport_cost":    round(transport_cost, 2),
                "inventory_cost":    round(total_cogs * 0.02, 2),
                "total_cost":        round(total_cogs + transport_cost, 2),
                "net_profit":        round(gross_margin - transport_cost, 2),
            })

        if kpi_rows:
            supabase.table("kpi_monthly").insert(kpi_rows).execute()
            logger.info(f"✅ KPI Monthly: {len(kpi_rows)} store-e")

        return len(monthly_rows), len(kpi_rows)

    except Exception as e:
        logger.error(f"❌ ERROR monthly aggregation: {e}")
        return 0, 0

if __name__ == "__main__":
    run_monthly_aggregation()