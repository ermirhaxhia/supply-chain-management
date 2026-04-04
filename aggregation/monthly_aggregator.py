# ============================================================
# aggregation/monthly_aggregator.py
# Agregon: sales_daily     → sales_monthly + kpi_monthly
#          inventory_daily → inventory_monthly
# FIX: Pagination + Timezone Albania + Inventory agregim
# ============================================================

import sys
import os
import logging
import calendar
from datetime import datetime
import pytz

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import supabase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("monthly_aggregator")

TZ_ALB = pytz.timezone("Europe/Tirane")


def fetch_all_rows(table: str, filters: dict) -> list:
    all_rows = []
    page_size = 1000
    offset = 0

    while True:
        query = supabase.table(table).select("*")
        for key, value in filters.items():
            if isinstance(value, tuple):
                op, val = value
                if op == "gte":   query = query.gte(key, val)
                elif op == "lte": query = query.lte(key, val)
                elif op == "eq":  query = query.eq(key, val)
            else:
                query = query.eq(key, value)

        resp  = query.range(offset, offset + page_size - 1).execute()
        batch = resp.data or []
        all_rows.extend(batch)
        logger.info(f"  📄 [{table}] Faqe {offset//page_size + 1}: {len(batch)} rreshta")

        if len(batch) < page_size:
            break
        offset += page_size

    logger.info(f"  ✅ [{table}] TOTAL: {len(all_rows)} rreshta")
    return all_rows


# ============================================================
# SALES: sales_daily → sales_monthly + kpi_monthly
# ============================================================
def aggregate_sales_monthly(year: int, month: int, month_start: str, month_end: str):
    logger.info(f"🛒 Duke agreguar SALES MONTHLY për {year}-{month:02d}...")

    daily_data = fetch_all_rows("sales_daily", {"date": ("gte", month_start)})
    daily_data = [r for r in daily_data if r["date"] <= month_end]

    if not daily_data:
        logger.warning(f"⚠️ Nuk u gjetën të dhëna në sales_daily për {year}-{month:02d}")
        return

    stores_found = set(r["store_id"] for r in daily_data)
    logger.info(f"🏪 Store-t e gjetura ({len(stores_found)}): {sorted(stores_found)}")

    # Grupimi Store × Product
    groups = {}
    for row in daily_data:
        key = (row["store_id"], row["product_id"])
        if key not in groups:
            groups[key] = {
                "units_sold": 0, "revenue": 0.0, "discount_amount": 0.0,
                "net_revenue": 0.0, "cogs": 0.0, "gross_profit": 0.0,
                "transactions_count": 0
            }
        g = groups[key]
        g["units_sold"]         += row.get("units_sold", 0)
        g["revenue"]            += row.get("revenue", 0.0)
        g["discount_amount"]    += row.get("discount_amount", 0.0)
        g["net_revenue"]        += row.get("net_revenue", 0.0)
        g["cogs"]               += row.get("cogs", 0.0)
        g["gross_profit"]       += row.get("gross_profit", 0.0)
        g["transactions_count"] += row.get("transactions_count", 0)

    monthly_rows = []
    for (sid, pid), v in groups.items():
        avg_p = v["revenue"] / v["units_sold"] if v["units_sold"] > 0 else 0
        monthly_rows.append({
            "store_id": sid, "product_id": pid,
            "year": year, "month": month,
            "units_sold":         v["units_sold"],
            "avg_unit_price":     round(avg_p, 2),
            "revenue":            round(v["revenue"], 2),
            "discount_amount":    round(v["discount_amount"], 2),
            "net_revenue":        round(v["net_revenue"], 2),
            "cogs":               round(v["cogs"], 2),
            "gross_profit":       round(v["gross_profit"], 2),
            "transactions_count": v["transactions_count"]
        })

    # Pastro duplicate
    existing = supabase.table("sales_monthly").select("id").eq("year", year).eq("month", month).limit(1).execute()
    if existing.data:
        supabase.table("sales_monthly").delete().eq("year", year).eq("month", month).execute()

    # INSERT batch
    total_inserted = 0
    for i in range(0, len(monthly_rows), 500):
        batch = monthly_rows[i:i + 500]
        supabase.table("sales_monthly").insert(batch).execute()
        total_inserted += len(batch)
        logger.info(f"  💾 Sales Batch {i//500 + 1}: {len(batch)} rreshta")

    logger.info(f"✅ sales_monthly: {total_inserted} rreshta për {len(stores_found)} dyqane")

    # KPI Monthly
    shp_data = fetch_all_rows("shipments", {"departure_time": ("gte", f"{month_start}T00:00:00")})
    shp_data = [s for s in shp_data if s["departure_time"][:10] <= month_end]
    total_transport_all = sum(s.get("transport_cost", 0) for s in shp_data)
    num_stores = len(stores_found) if stores_found else 1

    existing_kpi = supabase.table("kpi_monthly").select("id").eq("year", year).eq("month", month).limit(1).execute()
    if existing_kpi.data:
        supabase.table("kpi_monthly").delete().eq("year", year).eq("month", month).execute()

    kpi_rows = []
    for sid in stores_found:
        store_data      = [r for r in monthly_rows if r["store_id"] == sid]
        total_revenue   = sum(r["net_revenue"]       for r in store_data)
        total_cogs      = sum(r["cogs"]              for r in store_data)
        total_gm        = sum(r["gross_profit"]      for r in store_data)
        total_txns      = sum(r["transactions_count"] for r in store_data)
        store_transport = total_transport_all / num_stores
        inventory_cost  = total_cogs * 0.02
        total_cost      = total_cogs + store_transport + inventory_cost

        kpi_rows.append({
            "store_id": sid, "year": year, "month": month,
            "total_revenue":      round(total_revenue, 2),
            "total_cogs":         round(total_cogs, 2),
            "gross_margin":       round(total_gm, 2),
            "gross_margin_pct":   round(total_gm / total_revenue * 100, 2) if total_revenue > 0 else 0,
            "total_transactions": total_txns,
            "avg_basket_value":   round(total_revenue / total_txns, 2) if total_txns > 0 else 0,
            "stockout_rate_pct":  0.0,
            "otd_pct":            98.5,
            "avg_lead_time_days": 1.5,
            "transport_cost":     round(store_transport, 2),
            "inventory_cost":     round(inventory_cost, 2),
            "total_cost":         round(total_cost, 2),
            "net_profit":         round(total_gm - store_transport - inventory_cost, 2)
        })

    if kpi_rows:
        supabase.table("kpi_monthly").insert(kpi_rows).execute()
        logger.info(f"✅ kpi_monthly: {len(kpi_rows)} dyqane")

    # Pastro sales_daily
    supabase.table("sales_daily").delete().gte("date", month_start).lte("date", month_end).execute()
    logger.info(f"🧹 sales_daily pastruar për {year}-{month:02d}")

    return stores_found


# ============================================================
# INVENTORY: inventory_daily → inventory_monthly
# ============================================================
def aggregate_inventory_monthly(year: int, month: int, month_start: str, month_end: str):
    logger.info(f"📦 Duke agreguar INVENTORY MONTHLY për {year}-{month:02d}...")

    daily_data = fetch_all_rows("inventory_daily", {"date": ("gte", month_start)})
    daily_data = [r for r in daily_data if r["date"] <= month_end]

    if not daily_data:
        logger.warning(f"⚠️ Nuk u gjetën të dhëna në inventory_daily për {year}-{month:02d}")
        return

    stores_found = set(r["store_id"] for r in daily_data)

    # Grupimi Store × Product
    groups = {}
    for row in daily_data:
        key = (row["store_id"], row["product_id"])
        if key not in groups:
            groups[key] = {
                "avg_values":    [],
                "min_values":    [],
                "max_values":    [],
                "stockout_hours": 0,
                "expired_units":  0,
                "restock_count":  0
            }
        g = groups[key]
        g["avg_values"].append(row.get("avg_stock_level", 0))
        g["min_values"].append(row.get("min_stock_level", 0))
        g["max_values"].append(row.get("max_stock_level", 0))
        g["stockout_hours"] += row.get("stockout_hours", 0)
        g["expired_units"]  += row.get("expired_units", 0)
        g["restock_count"]  += row.get("restock_count", 0)

    monthly_rows = []
    for (sid, pid), v in groups.items():
        monthly_rows.append({
            "store_id":       sid,
            "product_id":     pid,
            "year":           year,
            "month":          month,
            "avg_stock_level": round(sum(v["avg_values"]) / len(v["avg_values"]), 2),
            "min_stock_level": min(v["min_values"]),
            "max_stock_level": max(v["max_values"]),
            "stockout_hours":  v["stockout_hours"],
            "expired_units":   v["expired_units"],
            "restock_count":   v["restock_count"]
        })

    # Pastro duplicate
    existing = supabase.table("inventory_monthly").select("id").eq("year", year).eq("month", month).limit(1).execute()
    if existing.data:
        supabase.table("inventory_monthly").delete().eq("year", year).eq("month", month).execute()

    # INSERT batch
    total_inserted = 0
    for i in range(0, len(monthly_rows), 500):
        batch = monthly_rows[i:i + 500]
        supabase.table("inventory_monthly").insert(batch).execute()
        total_inserted += len(batch)
        logger.info(f"  💾 Inventory Batch {i//500 + 1}: {len(batch)} rreshta")

    logger.info(f"✅ inventory_monthly: {total_inserted} rreshta për {len(stores_found)} dyqane")

    # Pastro inventory_daily
    supabase.table("inventory_daily").delete().gte("date", month_start).lte("date", month_end).execute()
    logger.info(f"🧹 inventory_daily pastruar për {year}-{month:02d}")


# ============================================================
# ENTRY POINT
# ============================================================
def run_monthly_aggregation(dt: datetime = None):
    now_alb = datetime.now(TZ_ALB)

    # Gjithmonë muaji i kaluar
    if now_alb.month == 1:
        year, month = now_alb.year - 1, 12
    else:
        year, month = now_alb.year, now_alb.month - 1

    last_day    = calendar.monthrange(year, month)[1]
    month_start = f"{year}-{month:02d}-01"
    month_end   = f"{year}-{month:02d}-{last_day:02d}"

    logger.info(
        f"📊 Agregim mujor → {year}-{month:02d} ({month_start} → {month_end}) | "
        f"Ora ALB: {now_alb.strftime('%d/%m %H:%M')} | "
        f"UTC: {datetime.utcnow().strftime('%d/%m %H:%M')}"
    )

    try:
        aggregate_sales_monthly(year, month, month_start, month_end)
        aggregate_inventory_monthly(year, month, month_start, month_end)
        logger.info(f"🎉 Agregimi mujor kompletuar për {year}-{month:02d}")
    except Exception as e:
        logger.error(f"❌ Gabim kritik gjatë agregimit mujor: {e}")
        raise


if __name__ == "__main__":
    run_monthly_aggregation()