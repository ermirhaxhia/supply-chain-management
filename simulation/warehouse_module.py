# ============================================================
# simulation/warehouse_module.py
# Snapshot i gjendjes së magazinave çdo orë
# Kapacitet, inbound/outbound, labor hours
# ============================================================

import sys
import os
import logging
import numpy as np
import uuid
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import supabase
from config.constants import SNAPSHOT_ID_PREFIX
from simulation.demand_profile import load_simulation_config, get_config

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("warehouse_module")

# ============================================================
# GJENERO SNAPSHOT PËR 1 MAGAZINË
# ============================================================
def generate_warehouse_snapshot(
    warehouse: dict,
    shipments: list,
    dt:        datetime
) -> dict | None:
    """
    Gjeneron snapshot të gjendjes së magazinës për 1 orë.

    Llogarit:
    - Kapacitetin e përdorur
    - Inbound/Outbound units
    - Labor hours
    - Orders processed

    Returns:
        dict : snapshot i plotë
    """
    try:
        warehouse_id = warehouse["warehouse_id"]
        capacity_m3  = warehouse.get("capacity_m3", 1000)

        # ── Dërgesat e kësaj ore ──────────────────────────
        wh_shipments = [
            s for s in shipments
            if s.get("warehouse_id") == warehouse_id
        ]

        # ── Outbound (njësi të dërguara) ──────────────────
        outbound_units = sum(
            s.get("units_delivered", 0) for s in wh_shipments
        )

        # ── Inbound (mallra që hyjnë nga furnizuesit) ─────
        # Simulohet si % e outbound + variancë
        inbound_units = int(outbound_units * np.random.uniform(0.8, 1.2))
        inbound_units = max(0, inbound_units)

        # ── Kapaciteti i përdorur ─────────────────────────
        # Fillojmë me 60-80% dhe ndryshon me inbound/outbound
        base_utilization = np.random.uniform(0.60, 0.80)

        # Black Swan — çrregullim i supply chain
        active_event = get_config("active_event", 0.0)
        if active_event in [1, 2]:  # Luftë ose pandemi
            base_utilization = np.random.uniform(0.85, 0.98)
            logger.warning(f"⚠️  {warehouse_id}: Kapacitet kritik nga eventi!")

        used_capacity_m3 = round(capacity_m3 * base_utilization, 1)

        # ── Labor Hours ───────────────────────────────────
        # Peak orë → më shumë punëtorë
        if dt.hour in [8, 9, 17, 18]:
            labor_hours = np.random.uniform(15, 25)
        elif dt.hour in [6, 7, 20, 21]:
            labor_hours = np.random.uniform(5, 15)
        else:
            labor_hours = np.random.uniform(8, 18)

        # ── Orders Processed ──────────────────────────────
        orders_processed = len(wh_shipments) + int(
            np.random.poisson(3)  # porosi shtesë
        )

        snapshot = {
            "snapshot_id":     f"{SNAPSHOT_ID_PREFIX}-{uuid.uuid4().hex[:10].upper()}",
            "warehouse_id":    warehouse_id,
            "timestamp":       dt.isoformat(),
            "used_capacity_m3":used_capacity_m3,
            "inbound_units":   inbound_units,
            "outbound_units":  outbound_units,
            "labor_hours":     round(labor_hours, 1),
            "orders_processed":orders_processed,
        }

        logger.debug(
            f"🏭 {warehouse_id} | "
            f"Cap={base_utilization*100:.1f}% | "
            f"In={inbound_units} | Out={outbound_units} | "
            f"Labor={labor_hours:.1f}h"
        )

        return snapshot

    except Exception as e:
        logger.error(f"❌ ERROR në generate_warehouse_snapshot: {e}")
        return None

# ============================================================
# RUN WAREHOUSE — Snapshot për të gjitha magazinat
# ============================================================
def run_warehouse_hour(
    warehouses: list,
    shipments:  list,
    dt:         datetime
) -> dict:
    """
    Gjeneron snapshot për të gjitha magazinat për 1 orë.

    Returns:
        dict : statistikat e magazinave
    """
    logger.info(f"🏭 Warehouse Snapshot | {dt.strftime('%Y-%m-%d %H:%M')}")

    snapshots      = []
    total_capacity = 0.0
    total_used     = 0.0

    try:
        for warehouse in warehouses:
            snapshot = generate_warehouse_snapshot(
                warehouse, shipments, dt
            )
            if snapshot:
                snapshots.append(snapshot)
                total_capacity += warehouse.get("capacity_m3", 1000)
                total_used     += snapshot["used_capacity_m3"]

        # ── Batch INSERT ──────────────────────────────────
        inserted = 0
        if snapshots:
            response = supabase.table("warehouse_snapshot").insert(snapshots).execute()
            if response.data:
                inserted = len(response.data)
            else:
                logger.warning("⚠️  Warehouse snapshots: Nuk u kthye data")

        # ── Utilizimi mesatar ─────────────────────────────
        avg_utilization = (total_used / total_capacity * 100) if total_capacity > 0 else 0

    except Exception as e:
        logger.error(f"❌ ERROR në run_warehouse_hour: {e}")
        inserted = 0
        avg_utilization = 0

    stats = {
        "warehouses":       inserted,
        "avg_utilization":  round(avg_utilization, 1),
        "total_used_m3":    round(total_used, 1),
        "total_capacity_m3":round(total_capacity, 1),
    }

    logger.info(
        f"  ✅ Snapshots={inserted} | "
        f"Utilizim={avg_utilization:.1f}% | "
        f"Hapësirë={total_used:.0f}/{total_capacity:.0f} m³"
    )

    return stats

# ============================================================
# MAIN — Test
# ============================================================
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("TEST I WAREHOUSE MODULE")
    logger.info("=" * 60)

    try:
        load_simulation_config()

        wh_resp  = supabase.table("warehouses").select("*").execute()
        shp_resp = supabase.table("shipments").select("*").limit(50).execute()

        warehouses = wh_resp.data
        shipments  = shp_resp.data or []

        if not warehouses:
            logger.critical("❌ Nuk u gjetën magazina")
            sys.exit(1)

        logger.info(f"✅ Warehouses={len(warehouses)} | Shipments={len(shipments)}")

        test_dt = datetime.now().replace(
            minute=0, second=0, microsecond=0
        )

        stats = run_warehouse_hour(warehouses, shipments, test_dt)

        logger.info("=" * 60)
        logger.info("REZULTATI:")
        for k, v in stats.items():
            logger.info(f"  {k}: {v}")
        logger.info("=" * 60)
        logger.info("✅ TEST PËRFUNDOI ME SUKSES")

    except Exception as e:
        logger.critical(f"❌ TEST DËSHTOI: {e}")
        sys.exit(1)