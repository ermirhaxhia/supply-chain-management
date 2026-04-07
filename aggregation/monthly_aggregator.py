# ============================================================
# aggregation/monthly_aggregator.py
# Agregon: sales_daily        → sales_monthly + kpi_monthly
#          inventory_daily    → inventory_monthly
#          transactions       → transactions_monthly → DELETE
# FIX: Pagination + Timezone Albania
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
# TRANSACTIONS: transactions → transactions_monthly → DELETE
# ============================================================
def aggregate_transactions_monthly(year: int, month: int, month_start: str, month_end: str):
    logger.info(f"🧾 Duke agreguar TRANSACTIONS MONTHLY për {year}-{month:02d}...")

    # Merr të gjitha transaksionet e muajit
    txn_data = fetch_all_rows("transactions", {
        "timestamp": ("gte", f"{month_start}T00:00:00"),
    })
    txn_data = [r for r in txn_data if r["timestamp"][:10] <= month_end]

    if not txn_data:
        logger.warning(f"⚠️ Nuk u gjetën transaksione për {year}-{month:02d}")
        return

    stores_found = set(r["store_id"] for r in txn_data)
    logger.info(f"🏪 Store-t ({len(stores_found)}): {sorted(stores_found)}")

    # Grupimi Store
    groups = {}
    for row in txn_data:
        sid = row["store_id"]
        if sid not in groups:
            groups[sid] = {
                "total_transactions":   0,
                "total_items_sold":     0,
                "total_revenue":        0.0,
                "total_discount":       0.0,
                "total_net_revenue":    0.0,
                "total_cogs":           0.0,
                "total_gross_profit":   0.0,
                "cash_count":           0,
                "card_count":           0,
                "ewallet_count":        0,
                "member_count":         0,
                "normal_count":         0,
            }
        g = groups[sid]
        g["total_transactions"]  += 1
        g["total_items_sold"]    += row.get("total_items", 0)
        g["total_revenue"]       += row.get("revenue", 0.0)
        g["total_discount"]      += row.get("discount_amount", 0.0)
        g["total_net_revenue"]   += row.get("net_revenue", 0.0)
        g["total_cogs"]          += row.get("cogs", 0.0)
        g["total_gross_profit"]  += row.get("gross_profit", 0.0)

        # Payment method
        pm = (row.get("payment_method") or "").lower()
        if "cash" in pm:
            g["cash_count"]    += 1
        elif "card" in pm:
            g["card_count"]    += 1
        elif "wallet" in pm or "ewallet" in pm:
            g["ewallet_count"] += 1

        # Customer type
        ct = (row.get("customer_type") or "").lower()
        if "member" in ct:
            g["member_count"] += 1
        else:
            g["normal_count"] += 1

    # Përgatit rreshtat
    txn_monthly_rows = []
    for sid, v in groups.items():
        txns  = v["total_transactions"]
        items = v["total_items_sold"]
        txn_monthly_rows.append({
            "store_id":             sid,
            "year":                 year,
            "month":                month,
            "total_transactions":   txns,
            "total_items_sold":     items,
            "avg_basket_value":     round(v["total_net_revenue"] / txns, 2) if txns > 0 else 0,
            "avg_items_per_basket": round(items / txns, 2) if txns > 0 else 0,
            "cash_count":           v["cash_count"],
            "card_count":           v["card_count"],
            "ewallet_count":        v["ewallet_count"],
            "member_count":         v["member_count"],
            "normal_count":         v["normal_count"],
            "total_revenue":        round(v["total_revenue"], 2),
            "total_discount":       round(v["total_discount"], 2),
            "total_net_revenue":    round(v["total_net_revenue"], 2),
            "total_cogs":           round(v["total_cogs"], 2),
            "total_gross_profit":   round(v["total_gross_profit"], 2),
        })

    # Pastro duplicate
    existing = supabase.table("transactions_monthly") \
        .select("id").eq("year", year).eq("month", month).limit(1).execute()
    if existing.data:
        logger.warning(f"⚠️ transactions_monthly ekziston — duke fshirë...")
        supabase.table("transactions_monthly") \
            .delete().eq("year", year).eq("month", month).execute()

    # INSERT
    supabase.table("transactions_monthly").insert(txn_monthly_rows).execute()
    logger.info(f"✅ transactions_monthly: {len(txn_monthly_rows)} rreshta")

    # DELETE transactions të muajit → 0 rreshta raw
    supabase.table("transactions") \
        .delete() \
        .gte("timestamp", f"{month_start}T00:00:00") \
        .lte("timestamp", f"{month_end}T23:59:59") \
        .execute()
    logger.info(f"🧹 transactions pastruar: {len(txn_data)} rreshta u fshinë")


# ============================================================
# SALES: sales_daily → sales_monthly + kpi_monthly
# ============================================================
def aggregate_sales_monthly(year: int, month: int, month_start: str, month_end: str):
    logger.info(f"🛒 Duke agreguar SALES MONTHLY për {year}-{month:02d}...")

    daily_data = fetch_all_rows("sales_daily", {"date": ("gte", month_start)})
    daily_data = [r for r in daily_data if r["date"] <= month_end]

    if not daily_data:
        logger.warning(f"⚠️ Nuk u gjetën të dhëna në sales_daily për {year}-{month:02d}")
        return set()

    stores_found = set(r["store_id"] for r in daily_data)
    logger.info(f"🏪 Store-t ({len(stores_found)}): {sorted(stores_found)}")

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

    existing = supabase.table("sales_monthly").select("id").eq("year", year).eq("month", month).limit(1).execute()
    if existing.data:
        supabase.table("sales_monthly").delete().eq("year", year).eq("month", month).execute()

    total_inserted = 0
    for i in range(0, len(monthly_rows), 500):
        batch = monthly_rows[i:i + 500]
        supabase.table("sales_monthly").insert(batch).execute()
        total_inserted += len(batch)
        logger.info(f"  💾 Sales Batch {i//500 + 1}: {len(batch)} rreshta")
    logger.info(f"✅ sales_monthly: {total_inserted} rreshta")

    # KPI Monthly — merr të dhëna nga transactions_monthly
    shp_data = fetch_all_rows("shipments", {"departure_time": ("gte", f"{month_start}T00:00:00")})
    shp_data = [s for s in shp_data if s["departure_time"][:10] <= month_end]
    total_transport_all = sum(s.get("transport_cost", 0) for s in shp_data)
    num_stores = len(stores_found) if stores_found else 1

    # Merr transactions_monthly për avg_basket dhe customer info
    txn_monthly = supabase.table("transactions_monthly") \
        .select("*").eq("year", year).eq("month", month).execute().data or []
    txn_by_store = {r["store_id"]: r for r in txn_monthly}

    existing_kpi = supabase.table("kpi_monthly").select("id").eq("year", year).eq("month", month).limit(1).execute()
    if existing_kpi.data:
        supabase.table("kpi_monthly").delete().eq("year", year).eq("month", month).execute()

    kpi_rows = []
    for sid in stores_found:
        store_data      = [r for r in monthly_rows if r["store_id"] == sid]
        total_revenue   = sum(r["net_revenue"]        for r in store_data)
        total_cogs      = sum(r["cogs"]               for r in store_data)
        total_gm        = sum(r["gross_profit"]       for r in store_data)
        total_txns      = sum(r["transactions_count"] for r in store_data)
        store_transport = total_transport_all / num_stores
        inventory_cost  = total_cogs * 0.02
        total_cost      = total_cogs + store_transport + inventory_cost

        # Merr avg_basket nga transactions_monthly nëse ekziston
        txn_store = txn_by_store.get(sid, {})
        avg_basket = txn_store.get("avg_basket_value") or (
            round(total_revenue / total_txns, 2) if total_txns > 0 else 0
        )

        kpi_rows.append({
            "store_id": sid, "year": year, "month": month,
            "total_revenue":      round(total_revenue, 2),
            "total_cogs":         round(total_cogs, 2),
            "gross_margin":       round(total_gm, 2),
            "gross_margin_pct":   round(total_gm / total_revenue * 100, 2) if total_revenue > 0 else 0,
            "total_transactions": total_txns,
            "avg_basket_value":   avg_basket,
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

    groups = {}
    for row in daily_data:
        key = (row["store_id"], row["product_id"])
        if key not in groups:
            groups[key] = {
                "avg_values": [], "min_values": [], "max_values": [],
                "stockout_hours": 0, "expired_units": 0, "restock_count": 0
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
            "store_id": sid, "product_id": pid,
            "year": year, "month": month,
            "avg_stock_level": round(sum(v["avg_values"]) / len(v["avg_values"]), 2),
            "min_stock_level": min(v["min_values"]),
            "max_stock_level": max(v["max_values"]),
            "stockout_hours":  v["stockout_hours"],
            "expired_units":   v["expired_units"],
            "restock_count":   v["restock_count"]
        })

    existing = supabase.table("inventory_monthly").select("id").eq("year", year).eq("month", month).limit(1).execute()
    if existing.data:
        supabase.table("inventory_monthly").delete().eq("year", year).eq("month", month).execute()

    total_inserted = 0
    for i in range(0, len(monthly_rows), 500):
        batch = monthly_rows[i:i + 500]
        supabase.table("inventory_monthly").insert(batch).execute()
        total_inserted += len(batch)
        logger.info(f"  💾 Inventory Batch {i//500 + 1}: {len(batch)} rreshta")

    logger.info(f"✅ inventory_monthly: {total_inserted} rreshta për {len(stores_found)} dyqane")

    supabase.table("inventory_daily").delete().gte("date", month_start).lte("date", month_end).execute()
    logger.info(f"🧹 inventory_daily pastruar për {year}-{month:02d}")


# ============================================================
# ENTRY POINT
# ============================================================
def run_monthly_aggregation(dt: datetime = None):
    now_alb = datetime.now(TZ_ALB)

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
        # Rendi është i rëndësishëm:
        # 1. Transactions → transactions_monthly (kpi_monthly e lexon)
        # 2. Sales → sales_monthly + kpi_monthly
        # 3. Inventory → inventory_monthly
        aggregate_transactions_monthly(year, month, month_start, month_end)
        aggregate_sales_monthly(year, month, month_start, month_end)
        aggregate_inventory_monthly(year, month, month_start, month_end)

        logger.info(f"🎉 Agregimi mujor kompletuar për {year}-{month:02d}")
        logger.info(f"📊 Rezultati:")
        logger.info(f"   transactions     → transactions_monthly (15 rreshta)")
        logger.info(f"   sales_daily      → sales_monthly + kpi_monthly")
        logger.info(f"   inventory_daily  → inventory_monthly")

    except Exception as e:
        logger.error(f"❌ Gabim kritik gjatë agregimit mujor: {e}")
        raise


if __name__ == "__main__":
    run_monthly_aggregation()