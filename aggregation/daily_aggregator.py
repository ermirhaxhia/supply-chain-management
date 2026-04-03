# ============================================================
# aggregation/daily_aggregator.py
# Agregon sales_hourly -> sales_daily
# FIX: Pagination për të marrë TË GJITHA rreshtat (jo vetëm 1000)
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
logger = logging.getLogger("daily_aggregator")


def fetch_all_rows(table: str, filters: dict) -> list:
    """
    Merr TË GJITHA rreshtat nga një tabelë duke përdorur pagination.
    Supabase kthen max 1000 rreshta pa pagination — kjo funksion e rregullon.
    """
    all_rows = []
    page_size = 1000
    offset = 0

    while True:
        query = supabase.table(table).select("*")

        # Apliko filtrat
        for key, value in filters.items():
            if isinstance(value, tuple):
                op, val = value
                if op == "gte":
                    query = query.gte(key, val)
                elif op == "lte":
                    query = query.lte(key, val)
                elif op == "eq":
                    query = query.eq(key, val)
            else:
                query = query.eq(key, value)

        # Pagination
        resp = query.range(offset, offset + page_size - 1).execute()
        batch = resp.data or []
        all_rows.extend(batch)

        logger.info(f"  📄 [{table}] Faqe {offset//page_size + 1}: {len(batch)} rreshta")

        # Nëse morëm më pak se page_size → kemi mbaruar
        if len(batch) < page_size:
            break

        offset += page_size

    logger.info(f"  ✅ [{table}] TOTAL: {len(all_rows)} rreshta")
    return all_rows


def run_daily_aggregation(dt: datetime = None):
    """
    Merr të dhënat nga sales_hourly për datën e sotme
    dhe i agregon në sales_daily (të gjitha 15 dyqanet).
    """
    if dt is None:
        dt = datetime.now()

    date_str = dt.date().isoformat()
    logger.info(f"📅 Duke nisur agregimin ditor për: {date_str}")

    try:
        # ── 1. Lexo TË GJITHA rreshtat nga sales_hourly ──────────────
        hourly_data = fetch_all_rows("sales_hourly", {"date": ("eq", date_str)})

        if not hourly_data:
            logger.warning(f"⚠️ Nuk u gjetën të dhëna në sales_hourly për {date_str}")
            return

        # Verifikimi i store-ve të gjetura
        stores_found = set(r["store_id"] for r in hourly_data)
        logger.info(f"🏪 Store-t e gjetura: {sorted(stores_found)}")

        # ── 2. Grupimi (Store × Product) ─────────────────────────────
        summary = {}
        for row in hourly_data:
            key = (row["store_id"], row["product_id"])
            if key not in summary:
                summary[key] = {
                    "units_sold":         0,
                    "revenue":            0.0,
                    "cogs":               0.0,
                    "gross_profit":       0.0,
                    "net_revenue":        0.0,
                    "discount_amount":    0.0,
                    "transactions_count": 0
                }
            s = summary[key]
            s["units_sold"]         += row.get("units_sold", 0)
            s["revenue"]            += row.get("revenue", 0.0)
            s["cogs"]               += row.get("cogs", 0.0)
            s["gross_profit"]       += row.get("gross_profit", 0.0)
            s["net_revenue"]        += row.get("net_revenue", 0.0)
            s["discount_amount"]    += row.get("discount_amount", 0.0)
            s["transactions_count"] += row.get("transactions_count", 0)

        # ── 3. Përgatitja e rreshtave për sales_daily ─────────────────
        daily_rows = []
        for (sid, pid), v in summary.items():
            avg_price = v["revenue"] / v["units_sold"] if v["units_sold"] > 0 else 0
            daily_rows.append({
                "date":               date_str,
                "store_id":           sid,
                "product_id":         pid,
                "units_sold":         v["units_sold"],
                "avg_unit_price":     round(avg_price, 2),
                "revenue":            round(v["revenue"], 2),
                "cogs":               round(v["cogs"], 2),
                "gross_profit":       round(v["gross_profit"], 2),
                "net_revenue":        round(v["net_revenue"], 2),
                "discount_amount":    round(v["discount_amount"], 2),
                "transactions_count": v["transactions_count"]
            })

        # ── 4. Kontrollo nëse sot është agreguar tashmë ───────────────
        existing = supabase.table("sales_daily").select("id").eq("date", date_str).limit(1).execute()
        if existing.data:
            logger.warning(f"⚠️ sales_daily për {date_str} ekziston — duke fshirë dhe rindërtuar...")
            supabase.table("sales_daily").delete().eq("date", date_str).execute()

        # ── 5. INSERT në grupe (batch) ────────────────────────────────
        batch_size = 500
        total_inserted = 0
        for i in range(0, len(daily_rows), batch_size):
            batch = daily_rows[i:i + batch_size]
            supabase.table("sales_daily").insert(batch).execute()
            total_inserted += len(batch)
            logger.info(f"  💾 Batch {i//batch_size + 1}: {len(batch)} rreshta")

        logger.info(f"✅ sales_daily kompletuar: {total_inserted} rreshta për {len(stores_found)} dyqane")

        # ── 6. Pastro sales_hourly pas agregimit ──────────────────────
        supabase.table("sales_hourly").delete().eq("date", date_str).execute()
        logger.info(f"🧹 sales_hourly pastruar për {date_str}")

    except Exception as e:
        logger.error(f"❌ Gabim kritik në daily_aggregator: {e}")
        raise


if __name__ == "__main__":
    run_daily_aggregation()