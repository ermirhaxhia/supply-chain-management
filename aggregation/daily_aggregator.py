# ============================================================
# scheduler/scheduler.py
# Dirigjenti kryesor - Optimizuar për GitHub Actions
# ============================================================

import sys
import os
import argparse
import logging
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulation.demand_profile import load_simulation_config
from simulation.inventory_module import initialize_stock
from config.settings import supabase

# Importet e moduleve të tua (Sigurohu që ke vendosur kodet e reja!)
from simulation.sales_module import run_sales_hour
from aggregation.daily_aggregator import run_daily_aggregation
from aggregation.monthly_aggregator import run_monthly_aggregation

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("scheduler")

def main():
    parser = argparse.ArgumentParser(description="Supply Chain Dispatcher")
    parser.add_argument(
        "--job", 
        type=str, 
        required=True, 
        choices=["sales", "daily", "monthly"], 
        help="Cilën detyrë dëshiron të ekzekutosh?"
    )
    args = parser.parse_args()

    dt = datetime.now()
    logger.info("=" * 60)
    logger.info(f"🚀 GITHUB ACTIONS TRIGGERED: Job='{args.job.upper()}' | {dt.strftime('%Y-%m-%d %H:%M')}")
    logger.info("=" * 60)

    try:
        # 1. Ngarkojmë konfigurimet bazë që duhen gjithmonë
        load_simulation_config()

        if args.job == "sales":
            # Ekzekutohet çdo orë (psh. 06:00 - 22:00)
            logger.info("🛒 Po gjenerojmë shitjet e orës...")
            
            # Ngarko të dhënat vetëm për shitjet
            stores = supabase.table("stores").select("*").execute().data or []
            products = supabase.table("products").select("*").execute().data or []
            
            for store in stores:
                run_sales_hour(store, products, dt)
                
            logger.info("✅ Gjenerimi i shitjeve mbaroi!")

        elif args.job == "daily":
            # Ekzekutohet 1 herë në ditë (psh. ora 23:55)
            logger.info("📊 Po nisim agregimin DITOR (sales_daily)...")
            run_daily_aggregation(dt)
            logger.info("✅ Agregimi Ditor mbaroi!")

        elif args.job == "monthly":
            # Ekzekutohet 1 herë në muaj (psh. data 1)
            logger.info("📆 Po nisim agregimin MUJOR (sales_monthly & KPI)...")
            run_monthly_aggregation(dt)
            logger.info("✅ Agregimi Mujor mbaroi!")

    except Exception as e:
        logger.critical(f"❌ ERROR KRITIK NË SCHEDULER: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()