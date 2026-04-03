# ============================================================
# scheduler/scheduler.py
# VERZIONI FINAL — GitHub Actions + Supabase + Error Handling
# ============================================================

import sys
import os
import logging
from datetime import datetime, timedelta
import pytz

# ── PATH SETUP ───────────────────────────────────────────────
# Siguron që Python gjen modulet në dosjet simulation/ dhe config/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from config.settings import supabase
from simulation.demand_profile import load_simulation_config
from simulation.sales_module import run_sales_hour
from simulation.inventory_module import initialize_stock, run_inventory_hour
from simulation.purchasing_module import run_purchasing
from simulation.warehouse_module import run_warehouse_hour

# ── TIMEZONE ─────────────────────────────────────────────────
# Përdorim pytz për të trajtuar DST (UTC+1 dimër / UTC+2 verë)
TZ = pytz.timezone("Europe/Tirane")

# ── LOGGING ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("scheduler")

# ── VARIABLAT GLOBALE ─────────────────────────────────────────
# Ruhen në RAM gjatë ekzekutimit — nuk ri-ngarkohen për çdo store
_stores:     list = []
_products:   list = []
_warehouses: list = []


# ============================================================
# NGARKIMI I TË DHËNAVE REFERENCE
# ============================================================
def load_all_data():
    """
    Ngarko të gjitha të dhënat reference nga Supabase.
    Thirret një herë në fillim të çdo ekzekutimi.
    Nëse Stores ose Products mungojnë, procesi ndalon (sys.exit).
    """
    global _stores, _products, _warehouses

    logger.info("📦 Duke ngarkuar të dhënat reference nga Supabase...")

    try:
        _stores     = supabase.table("stores").select("*").execute().data or []
        _products   = supabase.table("products").select("*").execute().data or []
        _warehouses = supabase.table("warehouses").select("*").execute().data or []
    except Exception as e:
        logger.critical(f"❌ DËSHTIM: Nuk u lidh me Supabase: {e}", exc_info=True)
        sys.exit(1)

    # Validimi i të dhënave kritike
    if not _stores:
        logger.critical("❌ Tabela 'stores' është bosh ose nuk u arrit.")
        sys.exit(1)

    if not _products:
        logger.critical("❌ Tabela 'products' është bosh ose nuk u arrit.")
        sys.exit(1)

    if not _warehouses:
        logger.warning("⚠️  Tabela 'warehouses' është bosh — purchasing mund të dështojë.")

    logger.info(
        f"✅ U ngarkuan: {len(_stores)} stores | "
        f"{len(_products)} products | "
        f"{len(_warehouses)} warehouses"
    )


# ============================================================
# SIMULATION TICK — CIKLI KRYESOR ORAR
# ============================================================
def simulation_tick():
    """
    Ekzekuton ciklin e plotë të simulimit për 1 orë.

    Hapat:
      1. Llogarit orën aktuale në timezone-n e Shqipërisë
      2. Ngarkon konfigurimin dhe të dhënat reference
      3. Për çdo store:
         a. Gjeneron shitjet (transactions + sales_hourly)
         b. Lexon sales_hourly për të përditësuar stokun
         c. Ekzekuton inventory update
         d. Ekzekuton purchasing nëse është ora 08:00
      4. Ekzekuton warehouse snapshots
      5. Nëse ndodh gabim kritik, GitHub Actions del e KUQE (exit code 1)
    """

    # ── Ora aktuale në Tiranë (merr parasysh DST) ────────────
    dt = datetime.now(TZ).replace(minute=0, second=0, microsecond=0)

    logger.info("=" * 60)
    logger.info(f"🔄 SIMULATION TICK | {dt.strftime('%Y-%m-%d %H:%M')} (Tiranë)")
    logger.info("=" * 60)

    try:
        # ── A. Ngarko konfigurimin e simulimit ───────────────
        try:
            load_simulation_config()
            logger.info("✅ Simulation config u ngarkua.")
        except Exception as e:
            logger.critical(f"❌ load_simulation_config dështoi: {e}", exc_info=True)
            sys.exit(1)

        # ── B. Ngarko të dhënat reference ────────────────────
        load_all_data()

        # ── C. Përgatit strukturat ndihmëse ──────────────────
        _stock_cache = initialize_stock(_stores, _products)
        products_map = {p["product_id"]: p for p in _products}
        wh_id        = _warehouses[0]["warehouse_id"] if _warehouses else None

        # ── D. Loop për çdo store ─────────────────────────────
        for store in _stores:
            store_id   = store["store_id"]
            store_name = store.get("city", store_id)

            logger.info(f"── Store {store_id} [{store_name}] ──────────────────────")

            # ── 1. SHITJET ────────────────────────────────────
            # Gjeneron transaksionet dhe i dërgon në:
            #   → tabela 'transactions'  (header i faturës)
            #   → tabela 'sales_hourly'  (detajet e produkteve)
            try:
                run_sales_hour(store, _products, dt)
            except Exception as e:
                logger.error(
                    f"❌ run_sales_hour dështoi për {store_id}: {e}",
                    exc_info=True
                )
                # Vazhdojmë me store-in tjetër — nuk ndalet gjithë procesi
                continue

            # ── 2. LEXO SALES_HOURLY PËR INVENTORY ───────────
            # Tabela 'transactions' nuk ka product_id — detajet janë në 'sales_hourly'
            # run_inventory_hour pret: [{"product_id": "...", "quantity": N}, ...]
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

                # Riemëro 'units_sold' → 'quantity' siç e pret run_inventory_hour
                transactions_for_inventory = [
                    {
                        "product_id": row["product_id"],
                        "quantity":   row["units_sold"]
                    }
                    for row in (sales_resp.data or [])
                ]

                logger.info(
                    f"   📋 sales_hourly: {len(transactions_for_inventory)} "
                    f"rreshta për inventory."
                )

            except Exception as e:
                logger.warning(
                    f"⚠️  Leximi i sales_hourly dështoi për {store_id}: {e} "
                    f"— Inventory do të ekzekutohet me listë bosh.",
                    exc_info=True
                )

            # ── 3. INVENTARI ──────────────────────────────────
            # Përditëson stokun bazuar në shitjet e sapokryera
            try:
                run_inventory_hour(
                    store,
                    _products,
                    products_map,
                    transactions_for_inventory,
                    dt
                )
            except Exception as e:
                logger.warning(
                    f"⚠️  run_inventory_hour dështoi për {store_id}: {e}",
                    exc_info=True
                )

            # ── 4. FURNIZIMI (vetëm ora 08:00) ────────────────
            # Gjeneron porositë e furnizimit për dyqanin
            if dt.hour == 8:
                if wh_id:
                    try:
                        run_purchasing(store, _products, _stock_cache, wh_id, dt)
                        logger.info(f"   🛒 Purchasing u ekzekutua për {store_id}.")
                    except Exception as e:
                        logger.warning(
                            f"⚠️  run_purchasing dështoi për {store_id}: {e}",
                            exc_info=True
                        )
                else:
                    logger.warning(
                        f"⚠️  Purchasing anashkaluar për {store_id} "
                        f"— warehouse_id nuk u gjet."
                    )

        # ── E. WAREHOUSE SNAPSHOTS ────────────────────────────
        # Regjistron gjendjen e magazinës për këtë orë
        try:
            run_warehouse_hour(_warehouses, [], dt)
            logger.info("✅ Warehouse snapshots u ekzekutuan.")
        except Exception as e:
            logger.warning(f"⚠️  run_warehouse_hour dështoi: {e}", exc_info=True)

        # ── F. PËRFUNDIMI ─────────────────────────────────────
        logger.info("=" * 60)
        logger.info(f"✅ TICK KOMPLETUAR | {dt.strftime('%Y-%m-%d %H:%M')}")
        logger.info("=" * 60)

    except Exception as e:
        # Gabim i papritur kritik — del me exit code 1
        # GitHub Actions e shfaq si ❌ Failed
        logger.critical(f"❌ DËSHTIM KRITIK I PAPRITUR: {e}", exc_info=True)
        sys.exit(1)


# ============================================================
# ENTRY POINT — GitHub Actions e thërret këtë direkt
# ============================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Supply Chain Simulation Scheduler")
    parser.add_argument(
        "--job",
        choices=["sales", "daily", "monthly"],
        required=True,
        help="Lloji i punës: sales=tick orar | daily=agregim ditor | monthly=agregim mujor"
    )
    args = parser.parse_args()

    # ── JOB: SALES (Tick Orar) ────────────────────────────────
    if args.job == "sales":
        logger.info("🚀 Duke nisur: Simulation Tick (Orar)...")
        simulation_tick()

    # ── JOB: DAILY AGGREGATION ────────────────────────────────
    elif args.job == "daily":
        logger.info("📅 Duke nisur: Daily Aggregation...")
        try:
            from aggregation.daily_aggregator import run_daily_aggregation
            run_daily_aggregation(datetime.now(TZ))
            logger.info("✅ Daily Aggregation përfundoi.")
        except Exception as e:
            logger.critical(f"❌ Daily Aggregation dështoi: {e}", exc_info=True)
            sys.exit(1)

    # ── JOB: MONTHLY AGGREGATION ──────────────────────────────
    elif args.job == "monthly":
        logger.info("📊 Duke nisur: Monthly Aggregation...")
        try:
            from aggregation.monthly_aggregator import run_monthly_aggregation
            run_monthly_aggregation(datetime.now(TZ))
            logger.info("✅ Monthly Aggregation përfundoi.")
        except Exception as e:
            logger.critical(f"❌ Monthly Aggregation dështoi: {e}", exc_info=True)
            sys.exit(1)