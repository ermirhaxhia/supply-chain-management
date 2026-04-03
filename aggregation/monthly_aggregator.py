# ============================================================
# aggregation/monthly_aggregator.py
# Agregon sales_daily → sales_monthly + kpi_monthly
# FIX: Pagination për të marrë TË GJITHA rreshtat (jo vetëm 1000)
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


def fetch_all_rows(table: str, filters: dict) -> list:
    """
    Merr TË GJITHA rreshtat nga një tabelë duke përdorur pagination.
    Supabase kthen max 1000 rreshta pa pagination — ky funksion e rregullon.
    """
    all_rows = []
    page_size = 1000
    offset = 0

    while True:
        query = supabase.table(table).select("*")

        # Apliko filtrat dinamikë
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

        # Pagination range
        resp = query.range(offset, offset + page_size - 1).execute()
        batch = resp.data or []
        all_rows.extend(batch)

        logger.info(f"  📄 [{table}] Faqe {offset//page_size + 1}: {len(batch)} rreshta")

        # Nëse morëm më pak se page_size → kemi mbërritur në fund
        if len(batch) < page_size:
            break

        offset += page_size

    logger.info(f"  ✅ [{table}] TOTAL: {len(all_rows)} rreshta")
    return all_rows


def run_monthly_aggregation(dt: datetime = None):
    """
    Agregon të dhënat e muajit aktual:
      sales_daily   → sales_monthly
      shipments     → kpi_monthly
    Pastron sales_daily pas agregimit.
    """
    if dt is None:
        dt = datetime.now()

    # Përgatitja e periudhës mujore
    year, month   = dt.year, dt.month
    last_day      = calendar.monthrange(year, month)[1]
    month_start   = f"{year}-{month:02d}-01"
    month_end     = f"{year}-{month:02d}-{last_day:02d}"

    logger.info(f"📊 Agregim mujor: {year}-{month:02d} ({month_start} → {month_end})")

    try:
        # ── 1. Lexo TË GJITHA rreshtat nga sales_daily ───────────────
        # Përdorim filtrin gte dhe pastaj filtrimin manual për fundin e muajit
        daily_data = fetch_all_rows("sales_daily", {
            "date": ("gte", month_start),
        })
        daily_data = [r for r in daily_data if r["date"] <= month_end]

        if not daily_data:
            logger.warning(f"⚠️ Nuk u gjetën të dhëna në sales_daily për {year}-{month:02d}")
            return

        # Verifikimi i store-ve
        stores_found = set(r["store_id"] for r in daily_data)
        logger.info(f"🏪 Store-t e gjetura: {sorted(stores_found)}")

        # ── 2. Grupimi (Store × Product) ─────────────────────────────
        # Kjo siguron që çdo dyqan dhe produkt të ketë hapësirën e vet
        groups = {}
        for row in daily_data:
            key = (row["store_id"], row["product_id"])
            if key not in groups:
                groups[key] = {
                    "units_sold":         0,
                    "revenue":            0.0,
                    "discount_amount":    0.0,
                    "net_revenue":        0.0,
                    "cogs":               0.0,
                    "gross_profit":       0.0,
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

        # ── 3. Përgatitja e rreshtave për sales_monthly ──────────────
        monthly_rows = []
        for (sid, pid), v in groups.items():
            avg_p = v["revenue"] / v["units_sold"] if v["units_sold"] > 0 else 0
            monthly_rows.append({
                "store_id":           sid,
                "product_id":          pid,
                "year":                year,
                "month":               month,
                "units_sold":          v["units_sold"],
                "avg_unit_price":      round(avg_p, 2),
                "revenue":             round(v["revenue"], 2),
                "discount_amount":     round(v["discount_amount"], 2),
                "net_revenue":         round(v["net_revenue"], 2),
                "cogs":                round(v["cogs"], 2),
                "gross_profit":        round(v["gross_profit"], 2),
                "transactions_count":  v["transactions_count"]
            })

        # ── 4. Kontrollo dhe pastro dublikatat ────────────────────────
        existing = supabase.table("sales_monthly") \
            .select("id").eq("year", year).eq("month", month).limit(1).execute()
        
        if existing.data:
            logger.warning(f"⚠️ sales_monthly për {year}-{month:02d} ekziston — duke fshirë...")
            supabase.table("sales_monthly") \
                .delete().eq("year", year).eq("month", month).execute()

        # ── 5. INSERT në grupe (batch) ────────────────────────────────
        batch_size = 500
        total_inserted = 0
        for i in range(0, len(monthly_rows), batch_size):
            batch = monthly_rows[i:i + batch_size]
            supabase.table("sales_monthly").insert(batch).execute()
            total_inserted += len(batch)
            logger.info(f"  💾 Batch {i//batch_size + 1}: {len(batch)} rreshta")

        logger.info(f"✅ sales_monthly kompletuar: {total_inserted} rreshta")

        # ── 6. KPI_MONTHLY (Llogaritjet Operacionale) ──────────────────
        # Kostot e transportit nga tabela shipments
        shp_data = fetch_all_rows("shipments", {
            "departure_time": ("gte", month_start)
        })
        total_transport_all = sum(s.get("transport_cost", 0) for s in shp_data)
        num_stores = len(stores_found) if stores_found else 1

        # ── 7. Pastro KPI-të e vjetra nëse ekzistojnë ────────────────
        existing_kpi = supabase.table("kpi_monthly") \
            .select("id").eq("year", year).eq("month", month).limit(1).execute()
        
        if existing_kpi.data:
            logger.warning(f"⚠️ kpi_monthly për {year}-{month:02d} ekziston — duke fshirë...")
            supabase.table("kpi_monthly") \
                .delete().eq("year", year).eq("month", month).execute()

        # ── 8. Llogarit KPI për çdo store ────────────────────────────
        kpi_rows = []
        for sid in stores_found:
            # Filtrojmë shitjet e këtij dyqani specifk
            store_data = [r for r in monthly_rows if r["store_id"] == sid]

            total_revenue  = sum(r["net_revenue"]    for r in store_data)
            total_cogs     = sum(r["cogs"]           for r in store_data)
            total_gm       = sum(r["gross_profit"]   for r in store_data)
            total_txns     = sum(r["transactions_count"] for r in store_data)
            
            # Shpërndarja e kostove
            store_transport  = total_transport_all / num_stores
            inventory_cost   = total_cogs * 0.02
            total_cost       = total_cogs + store_transport + inventory_cost

            kpi_rows.append({
                "store_id":           sid,
                "year":               year,
                "month":              month,
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
            logger.info(f"✅ kpi_monthly kompletuar për {len(kpi_rows)} dyqane")

        # ── 9. Pastro sales_daily (Përfundimi i Ciklit) ───────────────
        supabase.table("sales_daily") \
            .delete().gte("date", month_start).lte("date", month_end).execute()
        logger.info(f"🧹 sales_daily u pastrua me sukses.")

    except Exception as e:
        logger.error(f"❌ Gabim kritik gjatë agregimit mujor: {e}")
        raise e


if __name__ == "__main__":
    run_monthly_aggregation()