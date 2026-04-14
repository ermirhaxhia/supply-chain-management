# ============================================================
# scheduler/scheduler.py
# VERZIONI FINAL — GitHub Actions + Supabase + Error Handling
# FIX 1: Marketing thirret çdo ditë ora 07:00
# FIX 2: Stock cache përditësohet nga DB
# FIX 3: Transport thirret 1 herë jashtë loop-it
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
from simulation.marketing_module import (      # FIX #1
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


def load_all_data():
    global _stores, _products, _warehouses, _routes, _vehicles, _drivers, _categories

    logger.info("📦 Duke ngarkuar të dhënat reference nga Supabase...")

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

    if not _stores:
        logger.critical("❌ Tabela 'stores' është bosh.")
        sys.exit(1)
    if not _products:
        logger.critical("❌ Tabela 'products' është bosh.")
        sys.exit(1)

    logger.info(
        f"✅ Stores={len(_stores)} | Products={len(_products)} | "
        f"Warehouses={len(_warehouses)} | Categories={len(_categories)}"
    )


def get_stock_from_db(store_id: str) -> dict:
    """
    FIX #2: Merr stokun REAL nga inventory_log për ditën aktuale.
    Nëse nuk ka të dhëna sot → përdor initialize_stock si fallback.
    """
    try:
        today = datetime.now(TZ).date().isoformat()
        resp = (
            supabase.table("inventory_log")
            .select("product_id, stock_after")
            .eq("store_id", store_id)
            .eq("timestamp", today)
            .order("timestamp", desc=True)
            .execute()
        )
        if resp.data:
            # Merr stock_after më të fundit për çdo produkt
            stock = {}
            for row in resp.data:
                pid = row["product_id"]
                if pid not in stock:
                    stock[pid] = row["stock_after"]
            logger.info(f"   📦 Stock nga DB: {len(stock)} produkte")
            return stock
    except Exception as e:
        logger.warning(f"⚠️ get_stock_from_db dështoi: {e}")
    return {}


def simulation_tick():
    dt = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)

    logger.info("=" * 60)
    logger.info(f"🔄 SIMULATION TICK | {dt.strftime('%Y-%m-%d %H:%M')} (Tiranë)")
    logger.info("=" * 60)

    try:
        # ── A. Ngarko konfigurimin ────────────────────────
        try:
            load_simulation_config()
        except Exception as e:
            logger.critical(f"❌ load_simulation_config dështoi: {e}", exc_info=True)
            sys.exit(1)

        # ── B. Ngarko të dhënat reference ────────────────
        load_all_data()

        products_map = {p["product_id"]: p for p in _products}
        wh_id        = _warehouses[0]["warehouse_id"] if _warehouses else None

        # ── C. FIX #1: MARKETING (ora 07:00) ─────────────
        # Thirret 1 herë për të gjithë rrjetin, jo për çdo store
        if dt.hour == 7:
            logger.info("📣 Duke ekzekutuar Marketing Day...")
            try:
                deactivate_expired_campaigns(dt)
                load_active_campaigns(dt)
                run_marketing_day(_categories, dt)
            except Exception as e:
                logger.warning(f"⚠️ Marketing dështoi: {e}", exc_info=True)
        else:
            # Ngarko kampanjat aktive çdo orë (pa gjeneruar të reja)
            try:
                load_active_campaigns(dt)
            except Exception as e:
                logger.warning(f"⚠️ load_active_campaigns dështoi: {e}")

        # ── D. FIX #3: TRANSPORT (jashtë loop-it të store-ve) ──
        # Thirret 1 herë për të gjitha rrugët — jo 15 herë
        active_shipments = []
        if dt.hour == 6:
            if _routes and _vehicles and _drivers:
                try:
                    logger.info("🚚 Duke nisur flotën e transportit...")
                    stats = run_transport_day(_routes, _vehicles, _drivers, dt)
                    active_shipments = [{"status": "dispatched"}] * stats.get("shipments", 0)
                    logger.info(f"✅ Transport: {stats.get('shipments', 0)} dërgesa")
                except Exception as e:
                    logger.warning(f"⚠️ Transport dështoi: {e}", exc_info=True)

        # ── E. LOOP PËR ÇDO STORE ─────────────────────────
        for store in _stores:
            store_id   = store["store_id"]
            store_name = store.get("city", store_id)

            logger.info(f"── Store {store_id} [{store_name}] ──────────────")

            # ── 1. SHITJET ────────────────────────────────
            try:
                run_sales_hour(store, _products, dt)
            except Exception as e:
                logger.error(f"❌ run_sales_hour dështoi për {store_id}: {e}", exc_info=True)
                continue

            # ── 2. LEXO SALES_HOURLY PËR INVENTORY ───────
            transactions_for_inventory = []
            try:
                sales_resp = (
                    supabase.table("sales_hourly")
                    .select("product_id, units_sold")
                    .eq("store_id", store_id)
                    .eq("date", dt.date().isoformat())
                    .eq("hour", dt.hour)
                    .execute()
                )
                transactions_for_inventory = [
                    {"product_id": row["product_id"], "quantity": row["units_sold"]}
                    for row in (sales_resp.data or [])
                ]
            except Exception as e:
                logger.warning(f"⚠️ Leximi i sales_hourly dështoi për {store_id}: {e}")

            # ── 3. INVENTARI ──────────────────────────────
            try:
                run_inventory_hour(
                    store, _products, products_map,
                    transactions_for_inventory, dt
                )
            except Exception as e:
                logger.warning(f"⚠️ run_inventory_hour dështoi për {store_id}: {e}")

            # ── 4. FIX #2: PURCHASING (ora 08:00) ─────────
            # Merr stokun REAL nga DB, jo nga cache i vjetër
            if dt.hour == 8:
                if wh_id:
                    try:
                        # Merr stock real nga DB
                        real_stock = get_stock_from_db(store_id)
                        if not real_stock:
                            # Fallback: initialize_stock
                            fallback = initialize_stock([store], _products)
                            real_stock = fallback.get(store_id, {})

                        stock_cache_for_store = {store_id: real_stock}
                        run_purchasing(store, _products, stock_cache_for_store, wh_id, dt)
                    except Exception as e:
                        logger.warning(f"⚠️ Purchasing dështoi për {store_id}: {e}")

        # ── F. WAREHOUSE SNAPSHOTS ────────────────────────
        try:
            run_warehouse_hour(_warehouses, active_shipments, dt)
        except Exception as e:
            logger.warning(f"⚠️ run_warehouse_hour dështoi: {e}")

        logger.info("=" * 60)
        logger.info(f"✅ TICK KOMPLETUAR | {dt.strftime('%Y-%m-%d %H:%M')}")
        logger.info("=" * 60)

    except Exception as e:
        logger.critical(f"❌ DËSHTIM KRITIK: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--job",
        choices=["sales", "daily", "monthly"],
        required=True
    )
    args = parser.parse_args()

    if args.job == "sales":
        logger.info("🚀 Duke nisur: Simulation Tick...")
        simulation_tick()

    elif args.job == "daily":
        logger.info("📅 Duke nisur: Daily Aggregation...")
        try:
            from aggregation.daily_aggregator import run_daily_aggregation
            run_daily_aggregation(datetime.now(TZ))
            logger.info("✅ Daily Aggregation përfundoi.")
        except Exception as e:
            logger.critical(f"❌ Daily Aggregation dështoi: {e}", exc_info=True)
            sys.exit(1)

    elif args.job == "monthly":
        logger.info("📊 Duke nisur: Monthly Aggregation...")
        try:
            from aggregation.monthly_aggregator import run_monthly_aggregation
            run_monthly_aggregation(datetime.now(TZ))
            logger.info("✅ Monthly Aggregation përfundoi.")
        except Exception as e:
            logger.critical(f"❌ Monthly Aggregation dështoi: {e}", exc_info=True)
            sys.exit(1)