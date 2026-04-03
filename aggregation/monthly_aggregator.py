# ============================================================
# aggregation/monthly_aggregator.py
# Agregon sales_daily → sales_monthly + kpi_monthly
# FIX 1: Pagination për të marrë TË GJITHA rreshtat
# FIX 2: Timezone Albania — agregon MUAJIN E KALUAR
# FIX 3: Shipments filter me lte për fund muaji
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

# Zona kohore e Shqipërisë
TZ_ALB = pytz.timezone("Europe/Tirane")


def fetch_all_rows(table: str, filters: dict) -> list:
    """
    Merr TË GJITHA rreshtat nga një tabelë duke përdorur pagination.
    Supabase kthen max 1000 rreshta pa pagination.
    """
    all_rows = []
    page_size = 1000
    offset = 0

    while True:
        query = supabase.table(table).select("*")

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

        resp = query.range(offset, offset + page_size - 1).execute()
        batch = resp.data or []
        all_rows.extend(batch)

        logger.info(f"  📄 [{table}] Faqe {offset//page_size + 1}: {len(batch)} rreshta")

        if len(batch) < page_size:
            break

        offset += page_size

    logger.info(f"  ✅ [{table}] TOTAL: {len(all_rows)} rreshta")
    return all_rows


def run_monthly_aggregation(dt: datetime = None):
    """
    LOGJIKA E DATËS:
      Serveri → UTC. Shqipëria → UTC+2.
      GitHub Actions thirr --job monthly në ditën e parë të muajit të ri.
      Gjithmonë agregojmë MUAJIN E KALUAR në orën e Shqipërisë.
      
      Shembull:
        April 1, 00:30 ALB = March 31, 22:30 UTC
        now_alb.month = April → muaji i kaluar = Mars ✅
    """
    # Ora aktuale në Shqipëri
    now_alb = datetime.now(TZ_ALB)

    # FIX: Agregojmë MUAJIN E KALUAR (jo muajin aktual)
    if now_alb.month == 1:
        year  = now_alb.year - 1
        month = 12
    else:
        year  = now_alb.year
        month = now_alb.month - 1

    last_day    = calendar.monthrange(year, month)[1]
    month_start = f"{year}-{month:02d}-01"
    month_end   = f"{year}-{month:02d}-{last_day:02d}"

    logger.info(
        f"📊 Agregim mujor → {year}-{month:02d} ({month_start} → {month_end}) | "
        f"Ora ALB: {now_alb.strftime('%d/%m %H:%M')} | "
        f"UTC: {datetime.utcnow().strftime('%d/%m %H:%M')}"
    )

    try:
        # ── 1. Lexo TË GJITHA rreshtat nga sales_daily ───────────────
        daily_data = fetch_all_rows("sales_daily", {
            "date": ("gte", month_start),
        })
        daily_data = [r for r in daily_data if r["date"] <= month_end]

        if not daily_data:
            logger.warning(f"⚠️ Nuk u gjetën të dhëna në sales_daily për {year}-{month:02d}")
            return

        stores_found = set(r["store_id"] for r in daily_data)
        logger.info(f"🏪 Store-t e gjetura ({len(stores_found)}): {sorted(stores_found)}")

        # ── 2. Grupimi (Store × Product) ─────────────────────────────
        groups = {}
        for row in daily_data:
            key = (row["store_id"], row["product_id"])
            if key not in groups:
                groups[key] = {
                    "units_sold": 0, "revenue": 0.0,
                    "discount_amount": 0.0, "net_revenue": 0.0,
                    "cogs": 0.0, "gross_profit": 0.0,
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
                "product_id":         pid,
                "year":               year,
                "month":              month,
                "units_sold":         v["units_sold"],
                "avg_unit_price":     round(avg_p, 2),
                "revenue":            round(v["revenue"], 2),
                "discount_amount":    round(v["discount_amount"], 2),
                "net_revenue":        round(v["net_revenue"], 2),
                "cogs":               round(v["cogs"], 2),
                "gross_profit":       round(v["gross_profit"], 2),
                "transactions_count": v["transactions_count"]
            })

        # ── 4. Kontrollo duplicate ────────────────────────────────────
        existing = supabase.table("sales_monthly") \
            .select("id").eq("year", year).eq("month", month).limit(1).execute()
        if existing.data:
            logger.warning(f"⚠️ sales_monthly për {year}-{month:02d} ekziston — duke fshirë...")
            supabase.table("sales_monthly") \
                .delete().eq("year", year).eq("month", month).execute()

        # ── 5. INSERT në batch ────────────────────────────────────────
        batch_size = 500
        total_inserted = 0
        for i in range(0, len(monthly_rows), batch_size):
            batch = monthly_rows[i:i + batch_size]
            supabase.table("sales_monthly").insert(batch).execute()
            total_inserted += len(batch)
            logger.info(f"  💾 Batch {i//batch_size + 1}: {len(batch)} rreshta")

        logger.info(f"✅ sales_monthly kompletuar: {total_inserted} rreshta për {len(stores_found)} dyqane")

        # ── 6. KPI_MONTHLY ────────────────────────────────────────────
        # FIX: Shipments me të dy filtrat gte + lte
        shp_data = fetch_all_rows("shipments", {
            "departure_time": ("gte", f"{month_start}T00:00:00"),
        })
        shp_data = [s for s in shp_data if s["departure_time"][:10] <= month_end]
        total_transport_all = sum(s.get("transport_cost", 0) for s in shp_data)
        num_stores = len(stores_found) if stores_found else 1

        # ── 7. Kontrollo KPI duplicate ────────────────────────────────
        existing_kpi = supabase.table("kpi_monthly") \
            .select("id").eq("year", year).eq("month", month).limit(1).execute()
        if existing_kpi.data:
            logger.warning(f"⚠️ kpi_monthly për {year}-{month:02d} ekziston — duke fshirë...")
            supabase.table("kpi_monthly") \
                .delete().eq("year", year).eq("month", month).execute()

        # ── 8. Llogarit KPI për çdo store ────────────────────────────
        kpi_rows = []
        for sid in stores_found:
            store_data = [r for r in monthly_rows if r["store_id"] == sid]

            total_revenue   = sum(r["net_revenue"]       for r in store_data)
            total_cogs      = sum(r["cogs"]              for r in store_data)
            total_gm        = sum(r["gross_profit"]      for r in store_data)
            total_txns      = sum(r["transactions_count"] for r in store_data)
            store_transport = total_transport_all / num_stores
            inventory_cost  = total_cogs * 0.02
            total_cost      = total_cogs + store_transport + inventory_cost

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

        # ── 9. Pastro sales_daily pas agregimit ──────────────────────
        supabase.table("sales_daily") \
            .delete().gte("date", month_start).lte("date", month_end).execute()
        logger.info(f"🧹 sales_daily pastruar për {year}-{month:02d}")

    except Exception as e:
        logger.error(f"❌ Gabim kritik gjatë agregimit mujor: {e}")
        raise


if __name__ == "__main__":
    run_monthly_aggregation()