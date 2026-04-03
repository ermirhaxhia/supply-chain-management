# ============================================================
# scheduler/scheduler.py
# VERZIONI I KORRIGJUAR (Timezone, Error Handling, Window Scan)
# ============================================================

import sys
import os
import logging
from datetime import datetime, timedelta
import pytz  # Për korrigjimin e orës UTC -> Tiranë
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import httpx

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import supabase
from simulation.demand_profile import load_simulation_config
from simulation.sales_module import run_sales_hour
from simulation.inventory_module import initialize_stock, run_inventory_hour
from simulation.purchasing_module import run_purchasing
from simulation.warehouse_module import run_warehouse_hour

# ============================================================
# KONFIGURIMI I ORËS DHE LOGGING
# ============================================================
TZ = pytz.timezone("Europe/Tirane")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("scheduler")

# Variablat globale për cache
_stores, _products, _warehouses, _vehicles = [], [], [], []
_drivers, _routes, _categories = [], [], []
_stock_cache = {}
_initialized = False

def load_all_data():
    """Ngarko të dhënat reference nga Supabase."""
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
    _initialized = True

# ============================================================
# SIMULATION TICK — FUNKSIONI KRYESOR
# ============================================================
def simulation_tick():
    global _stock_cache, _initialized

    # ZGJIDHJA 1: Korrigjimi i Timezone (UTC -> Tiranë)
    dt = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)

    logger.info("=" * 60)
    logger.info(f"🔄 SIMULATION TICK | {dt.strftime('%Y-%m-%d %H:%M')} (Tiranë)")
    logger.info("=" * 60)

    try:
        load_simulation_config()

        if not _initialized:
            load_all_data()
            _stock_cache = initialize_stock(_stores, _products)

        products_map = {p["product_id"]: p for p in _products}
        wh_id = _warehouses[0]["warehouse_id"]

        # 1. Transporti
        _run_transport_always(_routes, _vehicles, _drivers, dt)

        # 2. Sales + Inventory për çdo store
        for store in _stores:
            store_id = store["store_id"]

            # Ekzekuto shitjet
            run_sales_hour(store, _products, dt)

            # ZGJIDHJA 3: Zgjerimi i dritares në 65 minuta
            # Siguron që inventory kap të gjitha transaksionet edhe nëse Sales vonohet
            ts_start = (dt - timedelta(minutes=65)).isoformat()

            txn_resp = (
                supabase.table("transactions")
                .select("product_id, quantity, total, discount_pct")
                .eq("store_id", store_id)
                .gte("timestamp", ts_start)
                .execute()
            )
            
            # Përditëso inventarin bazuar në shitjet e sapokryera
            run_inventory_hour(store, _products, products_map, txn_resp.data or [], dt)

            # Furnizimi (çdo ditë ora 08:00)
            if dt.hour == 8:
                run_purchasing(store, _products, _stock_cache, wh_id, dt)

        # 3. Warehouse Snapshots
        shp_resp = supabase.table("shipments").select("*").gte("departure_time", dt.isoformat()).execute()
        run_warehouse_hour(_warehouses, shp_resp.data or [], dt)

        logger.info(f"✅ TICK KOMPLETUAR | {dt.strftime('%H:%M')}")

    except Exception as e:
        # ZGJIDHJA 4: RAISE — Detyron GitHub Actions të dalë e KUQE ❌ nëse ka error
        logger.critical(f"❌ TICK DËSHTOI: {e}", exc_info=True)
        raise e 

def _run_transport_always(routes, vehicles, drivers, dt):
    from simulation.transport_module import generate_shipment
    import numpy as np
    ship_list = []
    for r in routes:
        if vehicles and drivers:
            v, d = np.random.choice(vehicles), np.random.choice(drivers)
            s = generate_shipment(r, v, d, dt)
            if s: ship_list.append(s)
    if ship_list:
        supabase.table("shipments").insert(ship_list).execute()

# ============================================================
# MAIN ENTRY POINT — Përditësuar me Agregimet e Reja
# ============================================================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", choices=["sales", "daily", "monthly"])
    parser.add_argument("--manual", action="store_true")
    args = parser.parse_args()

    load_all_data()
    _stock_cache = initialize_stock(_stores, _products)
    _initialized = True

    # ZGJIDHJA 2: Jobs të veçanta për të shmangur dështimin e agregimeve
    if args.job == "sales" or args.manual:
        simulation_tick()
    
    elif args.job == "daily":
        logger.info("📅 Duke ekzekutuar: Daily Aggregation...")
        from aggregation.daily_aggregator import run_daily_aggregation
        run_daily_aggregation(datetime.now(TZ))
        
    elif args.job == "monthly":
        logger.info("📊 Duke ekzekutuar: Monthly Aggregation...")
        from aggregation.monthly_aggregator import run_monthly_aggregation
        run_monthly_aggregation(datetime.now(TZ))