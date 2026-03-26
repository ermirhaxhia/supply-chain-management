# ============================================================
# scheduler/scheduler.py
# Ekzekuton simulimin çdo orë 06:00-22:00
# Dizajnuar për Render Background Worker (rri gjithmonë aktiv)
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
from simulation.sales_module import run_sales_hour
from simulation.inventory_module import initialize_stock, run_inventory_hour
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
# CACHE GLOBAL — mbetet gjallë ndërmjet tick-eve
# ============================================================
_stores      = []
_products    = []
_warehouses  = []
_vehicles    = []
_drivers     = []
_routes      = []
_categories  = []
_stock_cache = {}
_initialized = False


def load_all_data():
    """Ngarko të gjitha të dhënat reference 1 herë."""
    global _stores, _products, _warehouses, _vehicles
    global _drivers, _routes, _categories, _initialized

    logger.info("📦 Duke ngarkuar të dhënat reference...")

    _stores     = supabase.table("stores").select("*").execute().data or []
    _products   = supabase.table("products").select("*").execute().data or []
    _warehouses = supabase.table("warehouses").select("*").execute().data or []
    _vehicles   = supabase.table("vehicles").select("*").execute().data or []
    _drivers    = supabase.table("drivers").select("*").execute().data or []
    _routes     = supabase.table("routes").select("*").execute().data or []
    _categories = supabase.table("product_categories").select("*").execute().data or []

    if not _stores or not _products:
        raise RuntimeError("❌ Stores ose Products mungojnë në Supabase")

    logger.info(
        f"✅ Stores={len(_stores)} | Products={len(_products)} | "
        f"Warehouses={len(_warehouses)} | Routes={len(_routes)}"
    )
    _initialized = True


# ============================================================
# KEEP-ALIVE — Ping çdo 10 min që Render të mos e vrasë
# ============================================================
def keep_alive():
    """Ping API çdo 10 minuta për të parandaluar spin-down."""
    try:
        url = os.getenv(
            "API_URL",
            "https://supply-chain-management-uc9u.onrender.com/health"
        )
        response = httpx.get(url, timeout=10)
        logger.info(f"💓 Keep-alive ping → {response.status_code}")
    except Exception as e:
        logger.warning(f"⚠️  Keep-alive dështoi: {e}")


# ============================================================
# SIMULATION TICK — Ekzekutohet çdo orë
# ============================================================
def simulation_tick():
    global _stock_cache, _initialized

    dt = datetime.now().replace(minute=0, second=0, microsecond=0)

    logger.info("=" * 60)
    logger.info(f"🔄 SIMULATION TICK | {dt.strftime('%Y-%m-%d %H:%M')}")
    logger.info("=" * 60)

    try:
        load_simulation_config()

        if not _initialized:
            load_all_data()
            _stock_cache = initialize_stock(_stores, _products)

        products_map = {p["product_id"]: p for p in _products}
        wh_id        = _warehouses[0]["warehouse_id"]

        # ── 1. Marketing — çdo orë (largo guard clause) ──
        from simulation.marketing_module import run_marketing_day, load_active_campaigns
        load_active_campaigns(dt)
        if dt.hour == 7:
            run_marketing_day(_categories, dt)

        # ── 2. Transport — çdo orë (largo guard clause) ──
        from simulation.transport_module import run_transport_day
        # Thirr çdo orë, jo vetëm 6 dhe 14
        _run_transport_always(_routes, _vehicles, _drivers, dt)

        # ── 3. Sales + Inventory për çdo store ───────────
        for store in _stores:
            store_id = store["store_id"]

            # Shitjet
            run_sales_hour(store, _products, dt)

            # Merr transaksionet — shto buffer 5 min
            from datetime import timedelta
            ts_start = (dt - timedelta(minutes=5)).isoformat()

            txn_resp = (
                supabase.table("transactions")
                .select("product_id, quantity, total, discount_pct")
                .eq("store_id", store_id)
                .gte("timestamp", ts_start)
                .execute()
            )
            transactions = txn_resp.data or []

            # Inventory
            run_inventory_hour(
                store, _products, products_map, transactions, dt
            )

            # Purchasing — çdo orë (largo guard clause)
            if dt.hour == 8:
                run_purchasing(store, _products, _stock_cache, wh_id, dt)

        # ── 4. Warehouse Snapshots ────────────────────────
        shp_resp = (
            supabase.table("shipments")
            .select("*")
            .gte("departure_time", dt.isoformat())
            .execute()
        )
        run_warehouse_hour(_warehouses, shp_resp.data or [], dt)

        # ── 5. Agregim Orësh ← I MUNGONTE! ───────────────
        from aggregation.hourly_aggregator import run_hourly_aggregation
        run_hourly_aggregation(dt)

        # ── 6. Agregim Ditor (ora 23:00) ──────────────────
        if dt.hour == 23:
            from aggregation.daily_aggregator import run_daily_aggregation
            run_daily_aggregation(dt)

        # ── 7. Agregim Mujor (ditë 1, ora 00:00) ──────────
        if dt.day == 1 and dt.hour == 0:
            from aggregation.monthly_aggregator import run_monthly_aggregation
            run_monthly_aggregation(dt)

        logger.info(f"✅ TICK KOMPLETUAR | {dt.strftime('%H:%M')}")

    except Exception as e:
        logger.critical(f"❌ TICK DËSHTOI: {e}", exc_info=True)


def _run_transport_always(routes, vehicles, drivers, dt):
    """Transport çdo orë — jo vetëm 6 dhe 14."""
    from simulation.transport_module import generate_shipment
    import numpy as np
    import uuid
    from config.constants import SHIPMENT_ID_PREFIX

    shipments = []
    for route in routes:
        if not vehicles or not drivers:
            continue
        vehicle  = np.random.choice(vehicles)
        driver   = np.random.choice(drivers)
        shipment = generate_shipment(route, vehicle, driver, dt)
        if shipment:
            shipments.append(shipment)

    if shipments:
        from config.settings import supabase
        supabase.table("shipments").insert(shipments).execute()
        logger.info(f"🚛 Transport: {len(shipments)} dërgesa")

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Supply Chain Scheduler")
    parser.add_argument(
        "--manual",
        action="store_true",
        help="Run 1 tick manual tani dhe dil"
    )
    args = parser.parse_args()

    # Ngarko data gjithmonë
    load_all_data()
    _stock_cache = initialize_stock(_stores, _products)

    if args.manual:
        # ── Run 1 tick dhe dil ────────────────────────────
        logger.info("🧪 MANUAL RUN — 1 tick simulimi")
        simulation_tick()
        logger.info("✅ MANUAL RUN KOMPLETUAR")

    else:
        # ── Background Worker — rri gjithmonë aktiv ───────
        logger.info("⏰ Duke startuar BlockingScheduler...")

        scheduler = BlockingScheduler(timezone="Europe/Tirane")

        # Tick çdo orë 06:00-22:00
        scheduler.add_job(
            simulation_tick,
            CronTrigger(hour="6-22", minute="0", timezone="Europe/Tirane"),
            id="simulation_tick",
            name="Simulation Hourly Tick",
            max_instances=1,
            misfire_grace_time=300,  # 5 min tolerancë nëse vonohet
        )

        # Keep-alive çdo 10 minuta
        scheduler.add_job(
            keep_alive,
            CronTrigger(minute="*/10", timezone="Europe/Tirane"),
            id="keep_alive",
            name="Keep Alive Ping",
        )

        logger.info("✅ Scheduler aktiv: çdo orë 06:00-22:00 (Europe/Tirane)")
        logger.info("💓 Keep-alive: çdo 10 minuta")
        logger.info("   Ctrl+C për të ndaluar\n")

        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("⛔ Scheduler u ndal")