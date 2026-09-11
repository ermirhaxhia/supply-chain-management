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
from config.constants import RAW_DATA_RETENTION_DAYS

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
    # sales_hourly s'fshihet më këtu — mbahet {RAW_DATA_RETENTION_DAYS} ditë
    # (shih purge_old_raw_data), jo fshirje e menjëhershme çdo ditë.


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
    # inventory_log s'fshihet më këtu — mbahet {RAW_DATA_RETENTION_DAYS} ditë
    # (shih purge_old_raw_data), jo fshirje e menjëhershme çdo ditë.


# ============================================================
# TRANSPORT: shipments → transport_daily
# (mungonte krejtësisht — transport_daily s'shkruhej askund,
# /api/v1/logistics/routes/performance kthente gjithmonë bosh)
# ============================================================
def aggregate_transport(date_str: str):
    logger.info(f"🚛 Duke agreguar TRANSPORT për {date_str}...")

    shp_data = fetch_all_rows("shipments", {
        "departure_time": ("gte", f"{date_str}T00:00:00"),
    })
    shp_data = [r for r in shp_data if r["departure_time"][:10] == date_str]

    if not shp_data:
        logger.warning(f"⚠️ Nuk u gjetën dërgesa për {date_str}")
        return

    routes_found = set(r["route_id"] for r in shp_data)
    logger.info(f"🛣️ Rrugët e gjetura ({len(routes_found)}): {sorted(routes_found)}")

    # Kapaciteti i kamionëve — për avg_load_pct (shipments s'e ruan direkt)
    vehicles_resp = supabase.table("vehicles").select("vehicle_id, capacity_kg").execute()
    capacity_map = {v["vehicle_id"]: v.get("capacity_kg", 0) for v in (vehicles_resp.data or [])}

    summary = {}
    for row in shp_data:
        rid = row["route_id"]
        if rid not in summary:
            summary[rid] = {
                "total_shipments": 0, "total_units": 0, "total_cost": 0.0,
                "total_delay": 0, "on_time_deliveries": 0,
                "fuel_consumed": 0.0, "load_pcts": []
            }
        s = summary[rid]
        s["total_shipments"] += 1
        s["total_units"]     += row.get("units_delivered", 0)
        s["total_cost"]      += row.get("transport_cost", 0.0)
        s["total_delay"]     += row.get("delay_minutes", 0)
        if row.get("delay_minutes", 0) == 0:
            s["on_time_deliveries"] += 1
        s["fuel_consumed"] += row.get("fuel_consumed", 0.0)

        cap = capacity_map.get(row.get("vehicle_id"))
        if cap:
            s["load_pcts"].append(row.get("load_kg", 0) / cap * 100)

    daily_rows = []
    for rid, v in summary.items():
        n = v["total_shipments"]
        daily_rows.append({
            "route_id":           rid,
            "date":               date_str,
            "total_shipments":    n,
            "total_units":        v["total_units"],
            "total_cost":         round(v["total_cost"], 2),
            "avg_delay_minutes":  round(v["total_delay"] / n, 2) if n else 0.0,
            "on_time_deliveries": v["on_time_deliveries"],
            "fuel_consumed":      round(v["fuel_consumed"], 2),
            "avg_load_pct":       round(sum(v["load_pcts"]) / len(v["load_pcts"]), 2) if v["load_pcts"] else 0.0,
        })

    # Pastro duplicate
    existing = supabase.table("transport_daily").select("id").eq("date", date_str).limit(1).execute()
    if existing.data:
        logger.warning(f"⚠️ transport_daily për {date_str} ekziston — duke fshirë...")
        supabase.table("transport_daily").delete().eq("date", date_str).execute()

    total_inserted = 0
    for i in range(0, len(daily_rows), 500):
        batch = daily_rows[i:i + 500]
        supabase.table("transport_daily").insert(batch).execute()
        total_inserted += len(batch)
        logger.info(f"  💾 Transport Batch {i//500 + 1}: {len(batch)} rreshta")

    logger.info(f"✅ transport_daily: {total_inserted} rreshta për {len(routes_found)} rrugë")


# ============================================================
# PASTRIM RAW DATA — rul {RAW_DATA_RETENTION_DAYS} ditësh
# (inventory_log dhe sales_hourly mbahen këtë periudhë, pastaj fshihen)
# ============================================================
def purge_old_raw_data(now_alb: datetime):
    cutoff_dt   = now_alb - timedelta(days=RAW_DATA_RETENTION_DAYS)
    cutoff_date = cutoff_dt.date().isoformat()
    logger.info(f"🧹 Duke pastruar raw data më të vjetra se {cutoff_date} ({RAW_DATA_RETENTION_DAYS} ditë)...")

    try:
        supabase.table("inventory_log").delete().lt("timestamp", f"{cutoff_date}T00:00:00").execute()
        logger.info(f"✅ inventory_log pastruar (< {cutoff_date})")
    except Exception as e:
        logger.error(f"❌ Pastrimi i inventory_log dështoi: {e}")

    try:
        supabase.table("sales_hourly").delete().lt("date", cutoff_date).execute()
        logger.info(f"✅ sales_hourly pastruar (< {cutoff_date})")
    except Exception as e:
        logger.error(f"❌ Pastrimi i sales_hourly dështoi: {e}")


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

    errors = []

    for name, fn in [
        ("sales",     lambda: aggregate_sales(date_str)),
        ("inventory", lambda: aggregate_inventory(date_str)),
        ("transport", lambda: aggregate_transport(date_str)),
    ]:
        try:
            fn()
        except Exception as e:
            logger.error(f"❌ [{name}] Dështoi: {e}")
            errors.append(f"{name}: {e}")

    try:
        purge_old_raw_data(now_alb)
    except Exception as e:
        logger.error(f"❌ [purge] Dështoi: {e}")
        errors.append(f"purge: {e}")

    if errors:
        logger.error(f"❌ Agregimi ditor përfundoi me {len(errors)} gabim/e:")
        for err in errors:
            logger.error(f"   • {err}")
        raise RuntimeError(f"Daily aggregation errors: {errors}")

    logger.info(f"🎉 Agregimi ditor kompletuar për {date_str}")


if __name__ == "__main__":
    run_daily_aggregation()