# ============================================================
# aggregation/daily_aggregator.py
# Agregon: sales_hourly → sales_daily
#          inventory_log → inventory_daily
# FIX: Pagination + Timezone Albania + Inventory agregim
# ============================================================

import sys
import os
import logging
from datetime import datetime, timedelta
import pytz

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from config.settings import supabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("daily_aggregator")

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
# SALES: sales_hourly → sales_daily
# ============================================================
def aggregate_sales(date_str: str):
    logger.info(f"🛒 Duke agreguar SALES për {date_str}...")

    hourly_data = fetch_all_rows("sales_hourly", {"date": ("eq", date_str)})
    if not hourly_data:
        logger.warning(f"⚠️ Nuk u gjetën të dhëna në sales_hourly për {date_str}")
        return

    stores_found = set(r["store_id"] for r in hourly_data)
    logger.info(f"🏪 Store-t e gjetura ({len(stores_found)}): {sorted(stores_found)}")

    # Grupimi Store × Product
    summary = {}
    for row in hourly_data:
        key = (row["store_id"], row["product_id"])
        if key not in summary:
            summary[key] = {
                "units_sold": 0, "revenue": 0.0, "cogs": 0.0,
                "gross_profit": 0.0, "net_revenue": 0.0,
                "discount_amount": 0.0, "transactions_count": 0
            }
        s = summary[key]
        s["units_sold"]         += row.get("units_sold", 0)
        s["revenue"]            += row.get("revenue", 0.0)
        s["cogs"]               += row.get("cogs", 0.0)
        s["gross_profit"]       += row.get("gross_profit", 0.0)
        s["net_revenue"]        += row.get("net_revenue", 0.0)
        s["discount_amount"]    += row.get("discount_amount", 0.0)
        s["transactions_count"] += row.get("transactions_count", 0)

    daily_rows = []
    for (sid, pid), v in summary.items():
        avg_price = v["revenue"] / v["units_sold"] if v["units_sold"] > 0 else 0
        daily_rows.append({
            "date": date_str, "store_id": sid, "product_id": pid,
            "units_sold":         v["units_sold"],
            "avg_unit_price":     round(avg_price, 2),
            "revenue":            round(v["revenue"], 2),
            "cogs":               round(v["cogs"], 2),
            "gross_profit":       round(v["gross_profit"], 2),
            "net_revenue":        round(v["net_revenue"], 2),
            "discount_amount":    round(v["discount_amount"], 2),
            "transactions_count": v["transactions_count"]
        })

    # Pastro duplicate
    existing = supabase.table("sales_daily").select("id").eq("date", date_str).limit(1).execute()
    if existing.data:
        logger.warning(f"⚠️ sales_daily për {date_str} ekziston — duke fshirë...")
        supabase.table("sales_daily").delete().eq("date", date_str).execute()

    # INSERT batch
    total_inserted = 0
    for i in range(0, len(daily_rows), 500):
        batch = daily_rows[i:i + 500]
        supabase.table("sales_daily").insert(batch).execute()
        total_inserted += len(batch)
        logger.info(f"  💾 Sales Batch {i//500 + 1}: {len(batch)} rreshta")

    logger.info(f"✅ sales_daily: {total_inserted} rreshta për {len(stores_found)} dyqane")

    # Pastro sales_hourly
    supabase.table("sales_hourly").delete().eq("date", date_str).execute()
    logger.info(f"🧹 sales_hourly pastruar për {date_str}")


# ============================================================
# INVENTORY: inventory_log → inventory_daily
# ============================================================
def aggregate_inventory(date_str: str):
    logger.info(f"📦 Duke agreguar INVENTORY për {date_str}...")

    # Merr të gjitha lëvizjet e ditës
    log_data = fetch_all_rows("inventory_log", {
        "timestamp": ("gte", f"{date_str}T00:00:00"),
    })
    # Filtro vetëm ditën e djeshme
    log_data = [r for r in log_data if r["timestamp"][:10] == date_str]

    if not log_data:
        logger.warning(f"⚠️ Nuk u gjetën të dhëna në inventory_log për {date_str}")
        return

    stores_found = set(r["store_id"] for r in log_data)
    logger.info(f"🏪 Store-t e gjetura ({len(stores_found)}): {sorted(stores_found)}")

    # Grupimi Store × Product
    summary = {}
    for row in log_data:
        key = (row["store_id"], row["product_id"])
        if key not in summary:
            summary[key] = {
                "stock_values":   [],
                "stockout_hours": 0,
                "expired_units":  0,
                "restock_count":  0
            }
        s = summary[key]
        s["stock_values"].append(row["stock_after"])

        if row["stock_after"] == 0:
            s["stockout_hours"] += 1

        if row["change_reason"] == "Expired":
            s["expired_units"] += max(row["stock_before"] - row["stock_after"], 0)

        if row["change_reason"] == "Restock":
            s["restock_count"] += 1

    # Përgatit rreshtat
    inv_rows = []
    for (sid, pid), v in summary.items():
        vals = v["stock_values"]
        inv_rows.append({
            "date":             date_str,
            "store_id":         sid,
            "product_id":       pid,
            "avg_stock_level":  round(sum(vals) / len(vals), 2),
            "min_stock_level":  min(vals),
            "max_stock_level":  max(vals),
            "stockout_hours":   v["stockout_hours"],
            "expired_units":    v["expired_units"],
            "restock_count":    v["restock_count"]
        })

    # Pastro duplicate
    existing = supabase.table("inventory_daily").select("id").eq("date", date_str).limit(1).execute()
    if existing.data:
        logger.warning(f"⚠️ inventory_daily për {date_str} ekziston — duke fshirë...")
        supabase.table("inventory_daily").delete().eq("date", date_str).execute()

    # INSERT batch
    total_inserted = 0
    for i in range(0, len(inv_rows), 500):
        batch = inv_rows[i:i + 500]
        supabase.table("inventory_daily").insert(batch).execute()
        total_inserted += len(batch)
        logger.info(f"  💾 Inventory Batch {i//500 + 1}: {len(batch)} rreshta")

    logger.info(f"✅ inventory_daily: {total_inserted} rreshta për {len(stores_found)} dyqane")

    # Pastro inventory_log për ditën e djeshme
    supabase.table("inventory_log").delete().gte("timestamp", f"{date_str}T00:00:00").lte("timestamp", f"{date_str}T23:59:59").execute()
    logger.info(f"🧹 inventory_log pastruar për {date_str}")


# ============================================================
# ENTRY POINT
# ============================================================
def run_daily_aggregation(dt: datetime = None):
    now_alb   = datetime.now(TZ_ALB)
    yesterday = (now_alb - timedelta(days=1)).date()
    date_str  = yesterday.isoformat()

    logger.info(
        f"📅 Agregim ditor → {date_str} | "
        f"Ora ALB: {now_alb.strftime('%d/%m %H:%M')} | "
        f"UTC: {datetime.utcnow().strftime('%d/%m %H:%M')}"
    )

    try:
        aggregate_sales(date_str)
        aggregate_inventory(date_str)
        logger.info(f"🎉 Agregimi ditor kompletuar për {date_str}")
    except Exception as e:
        logger.error(f"❌ Gabim kritik në daily_aggregator: {e}")
        raise


if __name__ == "__main__":
    run_daily_aggregation()