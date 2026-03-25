# ============================================================
# scheduler/scheduler.py
# Ekzekuton simulimin çdo orë 06:00-22:00
# ============================================================

import sys
import os
import logging
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import httpx
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import supabase
from simulation.demand_profile import load_simulation_config
from simulation.sales_module import run_sales_hour, load_products, load_stores
from simulation.inventory_module import (
    initialize_stock, run_inventory_hour
)
from simulation.purchasing_module import run_purchasing
from simulation.transport_module import run_transport_day
from simulation.warehouse_module import run_warehouse_hour
from simulation.marketing_module import run_marketing_day

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("scheduler")

# ============================================================
# CACHE GLOBAL
# ============================================================
_stores    = []
_products  = []
_warehouses= []
_vehicles  = []
_drivers   = []
_routes    = []
_categories= []
_stock_cache = {}
_initialized = False

def load_all_data():
    """Ngarko të gjitha të dhënat reference 1 herë."""
    global _stores, _products, _warehouses, _vehicles
    global _drivers, _routes, _categories, _initialized

    logger.info("📦 Duke ngarkuar të dhënat reference...")

    _stores     = supabase.table("stores").select("*").execute().data
    _products   = supabase.table("products").select("*").execute().data
    _warehouses = supabase.table("warehouses").select("*").execute().data
    _vehicles   = supabase.table("vehicles").select("*").execute().data
    _drivers    = supabase.table("drivers").select("*").execute().data
    _routes     = supabase.table("routes").select("*").execute().data
    _categories = supabase.table("product_categories").select("*").execute().data

    logger.info(
        f"✅ Stores={len(_stores)} | Products={len(_products)} | "
        f"Warehouses={len(_warehouses)} | Routes={len(_routes)}"
    )

    _initialized = True

# ============================================================
# SIMULATION TICK — Ekzekutohet çdo orë
# ============================================================
def simulation_tick():
    """
    Ekzekuton 1 cikël simulimi për të gjitha store-et.
    Thirrет çdo orë nga scheduler.
    """
    global _stock_cache, _initialized

    dt = datetime.now().replace(minute=0, second=0, microsecond=0)
    logger.info("=" * 60)
    logger.info(f"🔄 SIMULATION TICK | {dt.strftime('%Y-%m-%d %H:%M')}")
    logger.info("=" * 60)

    try:
        # ── 0. Ngarko config dhe data ─────────────────────
        load_simulation_config()

        if not _initialized:
            load_all_data()
            _stock_cache = initialize_stock(_stores, _products)

        products_map = {p["product_id"]: p for p in _products}

        # ── 1. Marketing (ora 07:00) ──────────────────────
        from simulation.marketing_module import run_marketing_day
        run_marketing_day(_categories, dt)

        # ── 2. Transport (ora 06:00 dhe 14:00) ───────────
        run_transport_day(_routes, _vehicles, _drivers, dt)

        # ── 3. Sales + Inventory për çdo store ───────────
        for store in _stores:
            store_id = store["store_id"]

            # Sales
            sales_stats = run_sales_hour(store, _products, dt)

            # Merr transaksionet e sapokrijuara
            txn_resp = (
                supabase.table("transactions")
                .select("product_id, quantity")
                .eq("store_id", store_id)
                .gte("timestamp", dt.isoformat())
                .execute()
            )
            transactions = txn_resp.data or []

            # Inventory
            run_inventory_hour(
                store, _products, products_map,
                transactions, dt
            )

            # Purchasing (ora 08:00)
            wh_id = _warehouses[0]["warehouse_id"]
            run_purchasing(store, _products, _stock_cache, wh_id, dt)

        # ── 4. Warehouse Snapshots ────────────────────────
        shp_resp = (
            supabase.table("shipments")
            .select("*")
            .gte("departure_time", dt.isoformat())
            .execute()
        )
        run_warehouse_hour(_warehouses, shp_resp.data or [], dt)

        logger.info(f"✅ TICK KOMPLETUAR | {dt.strftime('%H:%M')}")

    except Exception as e:
        logger.critical(f"❌ TICK DËSHTOI: {e}")

# ============================================================
# MAIN — Run manual ose Scheduler
# ============================================================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--manual",
        action="store_true",
        help="Run 1 tick manual tani"
    )
    args = parser.parse_args()

    if args.manual:
        # ── Run 1 tick manual ─────────────────────────────
        logger.info("🧪 MANUAL RUN — 1 tick simulimi")
        load_all_data()
        _stock_cache = initialize_stock(_stores, _products)
        simulation_tick()
        logger.info("✅ MANUAL RUN KOMPLETUAR")

    else:
        # ── Scheduler 06:00-22:00 ─────────────────────────
        logger.info("⏰ Duke startuar Scheduler...")
        load_all_data()
        _stock_cache = initialize_stock(_stores, _products)

        scheduler = BlockingScheduler(timezone="Europe/Tirane")

        # Çdo orë nga 06:00 deri 22:00
        scheduler.add_job(
            simulation_tick,
            CronTrigger(hour="6-22", minute="0"),
            id="simulation_tick",
            name="Simulation Hourly Tick",
            max_instances=1,
        )

        logger.info("✅ Scheduler aktiv: çdo orë 06:00-22:00")
        logger.info("⏰ Tick i radhës: ora e plotë tjetër")
        logger.info("   Ctrl+C për të ndaluar")

        try:
            scheduler.start()
        except KeyboardInterrupt:
            logger.info("⛔ Scheduler u ndal")


def keep_alive():
    """Ping API çdo 10 minuta për të parandaluar sleep."""
    try:
        url = "https://supply-chain-management-uc9u.onrender.com/health"
        httpx.get(url, timeout=10)
        logger.info("💓 Keep-alive ping dërguar")
    except Exception as e:
        logger.warning(f"⚠️  Keep-alive dështoi: {e}")

# Shto në scheduler:
scheduler.add_job(
    keep_alive,
    CronTrigger(minute="*/10"),
    id="keep_alive",
    name="Keep Alive Ping",
)