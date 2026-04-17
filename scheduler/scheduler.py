# ============================================================
# scheduler/scheduler.py
# FIX: Hiqen orët fikse — përdoret flag "has_run_today"
# Transport, Marketing, Purchasing ekzekutohen 1 herë/ditë
# pa u varur nga ora ekzakte e GitHub Actions
# ============================================================

import sys
import os
import logging
from datetime import datetime, timedelta
import pytz

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from config.settings import supabase
from simulation.demand_profile import load_simulation_config
from simulation.sales_module import run_sales_hour
from simulation.inventory_module import initialize_stock, run_inventory_hour
from simulation.purchasing_module import run_purchasing
from simulation.warehouse_module import run_warehouse_hour
from simulation.transport_module import run_transport_day
from simulation.marketing_module import (
    run_marketing_day,
    load_active_campaigns,
    deactivate_expired_campaigns
)

TZ = pytz.timezone("Europe/Tirane")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("scheduler")

_stores:     list = []
_products:   list = []
_warehouses: list = []
_routes:     list = []
_vehicles:   list = []
_drivers:    list = []
_categories: list = []


# ============================================================
# FLAG SYSTEM — Has Run Today?
# ============================================================
def has_run_today(key: str, today: str) -> bool:
    """
    Kontrollon nëse një modul është ekzekutuar sot.
    Lexon nga simulation_config: last_run_{key} = "YYYY-MM-DD"
    """
    try:
        resp = supabase.table("run_log").select("last_run").eq("key", key).execute()
        if resp.data:
            return str(resp.data[0]["last_run"]) == today
        return False
    except:
        return False


def mark_as_run(key: str, today: str):
    """
    Shënon që moduli është ekzekutuar sot.
    Upsert: krijon ose përditëson config_key.
    """
    try:
        supabase.table("run_log").update({"last_run": today}).eq("key", key).execute()
    except Exception as e:
        logger.warning(f"⚠️ mark_as_run dështoi: {e}")


# ============================================================
# NGARKIMI I TË DHËNAVE REFERENCE
# ============================================================
def load_all_data():
    global _stores, _products, _warehouses, _routes
    global _vehicles, _drivers, _categories

    logger.info("📦 Duke ngarkuar të dhënat reference...")

    try:
        _stores     = supabase.table("stores").select("*").execute().data or []
        _products   = supabase.table("products").select("*").execute().data or []
        _warehouses = supabase.table("warehouses").select("*").execute().data or []
        _routes     = supabase.table("routes").select("*").execute().data or []
        _vehicles   = supabase.table("vehicles").select("*").execute().data or []
        _drivers    = supabase.table("drivers").select("*").execute().data or []
        _categories = supabase.table("product_categories").select("*").execute().data or []
    except Exception as e:
        logger.critical(f"❌ Nuk u lidh me Supabase: {e}", exc_info=True)
        sys.exit(1)

    if not _stores or not _products:
        logger.critical("❌ Stores ose Products mungojnë.")
        sys.exit(1)

    logger.info(
        f"✅ Stores={len(_stores)} | Products={len(_products)} | "
        f"Warehouses={len(_warehouses)} | Categories={len(_categories)}"
    )


# ============================================================
# STOCK I VËRTETË NGA DB
# ============================================================
def get_real_stock(store_id: str, products: list) -> dict:
    """
    Merr stokun real nga inventory_log për ditën aktuale.
    Fallback: initialize_stock nëse nuk ka të dhëna.
    """
    try:
        today = datetime.now(TZ).date().isoformat()
        resp = (
            supabase.table("inventory_log")
            .select("product_id, stock_after")
            .eq("store_id", store_id)
            .gte("timestamp", f"{today}T00:00:00")
            .order("timestamp", desc=True)
            .execute()
        )
        if resp.data:
            stock = {}
            for row in resp.data:
                pid = row["product_id"]
                if pid not in stock:
                    stock[pid] = row["stock_after"]
            if stock:
                logger.info(f"   📦 Stock real nga DB: {len(stock)} produkte")
                return stock
    except Exception as e:
        logger.warning(f"⚠️ get_real_stock dështoi: {e}")

    # Fallback
    fallback = initialize_stock([{"store_id": store_id}], products)
    return fallback.get(store_id, {})


# ============================================================
# SIMULATION TICK
# ============================================================
def simulation_tick():
    dt    = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)
    today = dt.date().isoformat()

    logger.info("=" * 60)
    logger.info(f"🔄 TICK | {dt.strftime('%Y-%m-%d %H:%M')} ALB | UTC: {datetime.utcnow().strftime('%H:%M')}")
    logger.info("=" * 60)

    try:
        load_simulation_config()
        load_all_data()

        products_map = {p["product_id"]: p for p in _products}
        wh_id        = _warehouses[0]["warehouse_id"] if _warehouses else None

        # ── MARKETING — 1 herë/ditë ──────────────────────
        # Ekzekutohet herën e parë që thirret (orën e parë të ditës)
        # Nuk pret orën 07:00 — ekzekutohet sapo GitHub Actions fillon
        if not has_run_today("marketing", today):
            logger.info("📣 Duke ekzekutuar Marketing (1 herë sot)...")
            try:
                deactivate_expired_campaigns(dt)
                load_active_campaigns(dt)
                run_marketing_day(_categories, dt)
                mark_as_run("marketing", today)
            except Exception as e:
                logger.warning(f"⚠️ Marketing dështoi: {e}", exc_info=True)
        else:
            load_active_campaigns(dt)

        # ── TRANSPORT — 1 herë/ditë ───────────────────────
        # Ekzekutohet herën e parë të ditës pa kufi orash
        active_shipments = []
        if not has_run_today("transport", today):
            if _routes and _vehicles and _drivers:
                logger.info("🚚 Duke nisur flotën (1 herë sot)...")
                try:
                    stats = run_transport_day(_routes, _vehicles, _drivers, dt)
                    active_shipments = [{"status": "dispatched"}] * stats.get("shipments", 0)
                    mark_as_run("transport", today)
                    logger.info(f"✅ Transport: {stats.get('shipments',0)} dërgesa")
                except Exception as e:
                    logger.warning(f"⚠️ Transport dështoi: {e}", exc_info=True)
        else:
            logger.info("⏭️  Transport: tashmë ekzekutuar sot")

        # ── LOOP PËR ÇDO STORE ───────────────────────────
        for store in _stores:
            store_id   = store["store_id"]
            store_name = store.get("city", store_id)

            logger.info(f"── {store_id} [{store_name}] ─────────────────")

            # 1. SHITJET
            try:
                run_sales_hour(store, _products, dt)
            except Exception as e:
                logger.error(f"❌ Sales dështoi {store_id}: {e}", exc_info=True)
                continue

            # 2. LEXO SALES_HOURLY
            transactions_for_inv = []
            try:
                resp = (
                    supabase.table("sales_hourly")
                    .select("product_id, units_sold")
                    .eq("store_id", store_id)
                    .eq("date",     dt.date().isoformat())
                    .eq("hour",     dt.hour)
                    .execute()
                )
                transactions_for_inv = [
                    {"product_id": r["product_id"], "quantity": r["units_sold"]}
                    for r in (resp.data or [])
                ]
            except Exception as e:
                logger.warning(f"⚠️ sales_hourly lexim dështoi {store_id}: {e}")

            # 3. INVENTARI
            try:
                run_inventory_hour(
                    store, _products, products_map,
                    transactions_for_inv, dt
                )
            except Exception as e:
                logger.warning(f"⚠️ Inventory dështoi {store_id}: {e}")

            # 4. PURCHASING — 1 herë/ditë për çdo store
            # Flag unik për çdo store
            purchasing_key = f"purchasing_{store_id}"
            if not has_run_today(purchasing_key, today):
                if wh_id:
                    try:
                        real_stock   = get_real_stock(store_id, _products)
                        stock_cache  = {store_id: real_stock}
                        run_purchasing(store, _products, stock_cache, wh_id, dt)
                        mark_as_run(purchasing_key, today)
                    except Exception as e:
                        logger.warning(f"⚠️ Purchasing dështoi {store_id}: {e}")
            else:
                logger.info(f"   ⏭️  Purchasing {store_id}: ekzekutuar sot")

        # ── WAREHOUSE SNAPSHOTS ──────────────────────────
        try:
            run_warehouse_hour(_warehouses, active_shipments, dt)
        except Exception as e:
            logger.warning(f"⚠️ Warehouse dështoi: {e}")

        logger.info("=" * 60)
        logger.info(f"✅ TICK KOMPLETUAR | {dt.strftime('%H:%M')} ALB")
        logger.info("=" * 60)

    except Exception as e:
        logger.critical(f"❌ DËSHTIM KRITIK: {e}", exc_info=True)
        sys.exit(1)


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--job", choices=["sales", "daily", "monthly"], required=True)
    args = parser.parse_args()

    if args.job == "sales":
        simulation_tick()

    elif args.job == "daily":
        try:
            from aggregation.daily_aggregator import run_daily_aggregation
            run_daily_aggregation(datetime.now(TZ))
        except Exception as e:
            logger.critical(f"❌ Daily dështoi: {e}", exc_info=True)
            sys.exit(1)

    elif args.job == "monthly":
        try:
            from aggregation.monthly_aggregator import run_monthly_aggregation
            run_monthly_aggregation(datetime.now(TZ))
        except Exception as e:
            logger.critical(f"❌ Monthly dështoi: {e}", exc_info=True)
            sys.exit(1)