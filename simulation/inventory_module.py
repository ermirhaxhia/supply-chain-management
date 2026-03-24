# ============================================================
# simulation/inventory_module.py
# Menaxhon stokun e çdo store — lëvizjet, reorder, stockout
# ============================================================

import sys
import os
import logging
import numpy as np
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import supabase
from config.constants import (
    STOCKOUT_PROBABILITY,
    EXPIRY_LOSS_RATE,
    SHRINKAGE_RATE,
    INITIAL_STOCK_FILL_PCT,
    LOG_ID_PREFIX,
)
from simulation.demand_profile import load_simulation_config, get_config

import uuid

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("inventory_module")

# ============================================================
# CACHE STOKU — {store_id: {product_id: stock_level}}
# ============================================================
_stock_cache: dict = {}

def initialize_stock(stores: list, products: list) -> dict:
    """
    Inicializon stokun për të gjitha store-et dhe produktet.
    Fillon me 75% të max_stock (INITIAL_STOCK_FILL_PCT).

    Returns:
        dict : {store_id: {product_id: stock_level}}
    """
    global _stock_cache
    logger.info("🔄 Duke inicializuar stokun për të gjitha store-et...")

    try:
        for store in stores:
            store_id = store["store_id"]
            _stock_cache[store_id] = {}

            for product in products:
                product_id = product["product_id"]
                max_stock  = product.get("max_stock", 100)
                initial    = int(max_stock * INITIAL_STOCK_FILL_PCT)
                _stock_cache[store_id][product_id] = initial

        total_entries = sum(len(v) for v in _stock_cache.values())
        logger.info(f"✅ Stoku inicializuar: {len(stores)} store × {len(products)} produkte = {total_entries} entries")

    except Exception as e:
        logger.error(f"❌ ERROR në initialize_stock: {e}")

    return _stock_cache

def get_stock_level(store_id: str, product_id: str) -> int:
    """Kthen nivelin aktual të stokut për 1 produkt në 1 store."""
    return _stock_cache.get(store_id, {}).get(product_id, 0)

def set_stock_level(store_id: str, product_id: str, level: int):
    """Vendos nivelin e stokut."""
    if store_id not in _stock_cache:
        _stock_cache[store_id] = {}
    _stock_cache[store_id][product_id] = max(0, level)

# ============================================================
# REGJISTRO LËVIZJEN E STOKUT
# ============================================================
def log_stock_change(
    store_id:    str,
    product_id:  str,
    stock_before:int,
    stock_after: int,
    reason:      str,
    dt:          datetime
) -> bool:
    """
    Regjistron çdo lëvizje stoku në inventory_log.

    Arsyet e ndryshimit:
      Sale      → shitje normale
      Restock   → furnizim nga magazina
      Expired   → produkt i skaduar
      Shrinkage → humbje vjedhje/dëmtim
      Adjustment→ korrigjim manual
    """
    try:
        log_entry = {
            "log_id":       f"{LOG_ID_PREFIX}-{uuid.uuid4().hex[:10].upper()}",
            "store_id":     store_id,
            "product_id":   product_id,
            "timestamp":    dt.isoformat(),
            "stock_before": stock_before,
            "stock_after":  stock_after,
            "change_reason":reason,
        }

        response = supabase.table("inventory_log").insert(log_entry).execute()

        if response.data:
            return True
        else:
            logger.warning(f"⚠️  inventory_log: Nuk u kthye data për {product_id}")
            return False

    except Exception as e:
        logger.error(f"❌ ERROR në log_stock_change: {e}")
        return False

# ============================================================
# ZBRAZ STOKUN NGA SHITJET
# ============================================================
def apply_sales_to_stock(
    store_id:     str,
    transactions: list,
    products_map: dict,
    dt:           datetime
) -> dict:
    stats = {
        "stockouts":        0,
        "low_stock_alerts": 0,
        "units_sold":       0,
    }
    log_entries = []  # ← batch

    try:
        for txn in transactions:
            product_id = txn.get("product_id")
            quantity   = txn.get("quantity", 1)
            product    = products_map.get(product_id, {})

            if not product:
                continue

            stock_before = get_stock_level(store_id, product_id)

            if stock_before <= 0:
                stats["stockouts"] += 1
                continue

            stock_after = max(0, stock_before - quantity)
            set_stock_level(store_id, product_id, stock_after)
            stats["units_sold"] += quantity

            log_entries.append({
                "log_id":       f"{LOG_ID_PREFIX}-{uuid.uuid4().hex[:10].upper()}",
                "store_id":     store_id,
                "product_id":   product_id,
                "timestamp":    dt.isoformat(),
                "stock_before": stock_before,
                "stock_after":  stock_after,
                "change_reason":"Sale",
            })

            reorder_point = product.get("reorder_point", 20)
            if stock_after <= reorder_point:
                stats["low_stock_alerts"] += 1
                logger.warning(
                    f"⚠️  LOW STOCK: {product_id} në {store_id} "
                    f"→ {stock_after} njësi"
                )

        # ── 1 batch insert ────────────────────────────
        if log_entries:
            batch_size = 500
            for i in range(0, len(log_entries), batch_size):
                batch = log_entries[i:i + batch_size]
                supabase.table("inventory_log").insert(batch).execute()
            logger.info(f"  📝 {len(log_entries)} sale logs inserted (batch)")

    except Exception as e:
        logger.error(f"❌ ERROR në apply_sales_to_stock: {e}")

    return stats
# ============================================================
# HUMBJET — SKADIM DHE SHRINKAGE
# ============================================================
def apply_losses(store_id: str, products: list, dt: datetime) -> dict:
    stats = {"expired_units": 0, "shrinkage_units": 0}
    if dt.hour != 6:
        return stats

    logger.info(f"🗑️  Duke aplikuar humbjet ditore për {store_id}...")

    log_entries = []  # ← mblidh të gjitha, insert 1 herë

    try:
        for product in products:
            product_id    = product["product_id"]
            is_perishable = product.get("shelf_life_days") is not None
            stock_level   = get_stock_level(store_id, product_id)

            if stock_level <= 0:
                continue

            # ── Skadim (vetëm perishable) ─────────────────
            if is_perishable:
                expired = int(stock_level * EXPIRY_LOSS_RATE)
                if expired > 0:
                    stock_before = stock_level
                    stock_level  = max(0, stock_level - expired)
                    set_stock_level(store_id, product_id, stock_level)
                    stats["expired_units"] += expired
                    log_entries.append({
                        "log_id":       f"{LOG_ID_PREFIX}-{uuid.uuid4().hex[:10].upper()}",
                        "store_id":     store_id,
                        "product_id":   product_id,
                        "timestamp":    dt.isoformat(),
                        "stock_before": stock_before,
                        "stock_after":  stock_level,
                        "change_reason":"Expired",
                    })

            # ── Shrinkage ─────────────────────────────────
            shrinkage = int(stock_level * SHRINKAGE_RATE)
            if shrinkage > 0:
                stock_before = stock_level
                stock_level  = max(0, stock_level - shrinkage)
                set_stock_level(store_id, product_id, stock_level)
                stats["shrinkage_units"] += shrinkage
                log_entries.append({
                    "log_id":       f"{LOG_ID_PREFIX}-{uuid.uuid4().hex[:10].upper()}",
                    "store_id":     store_id,
                    "product_id":   product_id,
                    "timestamp":    dt.isoformat(),
                    "stock_before": stock_before,
                    "stock_after":  stock_level,
                    "change_reason":"Shrinkage",
                })

        # ── 1 batch insert për të gjitha ─────────────────
        if log_entries:
            batch_size = 500
            for i in range(0, len(log_entries), batch_size):
                batch = log_entries[i:i + batch_size]
                supabase.table("inventory_log").insert(batch).execute()
            logger.info(f"  📝 {len(log_entries)} log entries inserted (batch)")

    except Exception as e:
        logger.error(f"❌ ERROR në apply_losses: {e}")

    logger.info(
        f"  📦 {store_id} — Expired: {stats['expired_units']} | "
        f"Shrinkage: {stats['shrinkage_units']}"
    )
    return stats
# ============================================================
# RESTOCK — Furnizo store nga magazina
# ============================================================
def apply_restock(store_id: str, products: list, dt: datetime) -> int:
    restocked  = 0
    log_entries = []

    try:
        lead_mult = get_config("lead_time_multiplier", 1.0)

        for product in products:
            product_id    = product["product_id"]
            stock_level   = get_stock_level(store_id, product_id)
            reorder_point = product.get("reorder_point", 20)
            reorder_qty   = product.get("reorder_qty",   50)
            max_stock     = product.get("max_stock",     100)

            if stock_level <= reorder_point:
                # Lead time realist: 1-3 ditë ~ Poisson(2)
                lead_time = int(np.random.poisson(2 * lead_mult))

                if lead_time == 0:
                    qty_to_add   = min(reorder_qty, max_stock - stock_level)
                    stock_before = stock_level
                    new_stock    = min(max_stock, stock_level + qty_to_add)

                    set_stock_level(store_id, product_id, new_stock)
                    restocked += 1

                    log_entries.append({
                        "log_id":       f"{LOG_ID_PREFIX}-{uuid.uuid4().hex[:10].upper()}",
                        "store_id":     store_id,
                        "product_id":   product_id,
                        "timestamp":    dt.isoformat(),
                        "stock_before": stock_before,
                        "stock_after":  new_stock,
                        "change_reason":"Restock",
                    })

        # Batch insert
        if log_entries:
            batch_size = 500
            for i in range(0, len(log_entries), batch_size):
                supabase.table("inventory_log").insert(
                    log_entries[i:i + batch_size]
                ).execute()
            logger.info(f"  📝 {len(log_entries)} restock logs inserted (batch)")

    except Exception as e:
        logger.error(f"❌ ERROR në apply_restock: {e}")

    if restocked > 0:
        logger.info(f"📦 {store_id} — Restock: {restocked} produkte")

    return restocked
# ============================================================
# RUN INVENTORY — Ekzekuto të gjitha për 1 orë
# ============================================================
def run_inventory_hour(
    store:        dict,
    products:     list,
    products_map: dict,
    transactions: list,
    dt:           datetime
) -> dict:
    """
    Ekzekuton të gjitha operacionet e stokut për 1 orë.

    Returns:
        dict : statistikat e plota të stokut
    """
    store_id = store["store_id"]
    logger.info(f"📦 Inventory: {store_id} | {dt.strftime('%H:%M')}")

    # 1. Humbjet ditore (vetëm ora 06:00)
    loss_stats = apply_losses(store_id, products, dt)

    # 2. Zbraz stokun nga shitjet
    sales_stats = apply_sales_to_stock(
        store_id, transactions, products_map, dt
    )

    # 3. Restock nëse nevojitet
    restocked = apply_restock(store_id, products, dt)

    # 4. Statistikat finale
    stats = {
        "store_id":          store_id,
        "hour":              dt.hour,
        "units_sold":        sales_stats["units_sold"],
        "stockouts":         sales_stats["stockouts"],
        "low_stock_alerts":  sales_stats["low_stock_alerts"],
        "expired_units":     loss_stats["expired_units"],
        "shrinkage_units":   loss_stats["shrinkage_units"],
        "restocked_products":restocked,
    }

    logger.info(
        f"  ✅ Sold={stats['units_sold']} | "
        f"Stockouts={stats['stockouts']} | "
        f"Alerts={stats['low_stock_alerts']} | "
        f"Restock={restocked}"
    )

    return stats

# ============================================================
# MAIN — Test
# ============================================================
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("TEST I INVENTORY MODULE")
    logger.info("=" * 60)

    try:
        load_simulation_config()

        # Ngarko data
        stores_resp   = supabase.table("stores").select("*").execute()
        products_resp = supabase.table("products").select("*").execute()

        stores   = stores_resp.data
        products = products_resp.data

        if not stores or not products:
            logger.critical("❌ Të dhënat bazë mungojnë")
            sys.exit(1)

        # Products map për kërkim të shpejtë
        products_map = {p["product_id"]: p for p in products}

        # Inicializo stokun
        initialize_stock(stores, products)

        # Testo 1 store
        test_store = stores[0]
        test_dt    = datetime.now().replace(
            hour=6, minute=0, second=0, microsecond=0
        )

        # Simulo disa transaksione testimi
        test_transactions = [
            {"product_id": products[i]["product_id"], "quantity": 2}
            for i in range(20)
        ]

        stats = run_inventory_hour(
            test_store, products, products_map,
            test_transactions, test_dt
        )

        logger.info("=" * 60)
        logger.info("REZULTATI:")
        for k, v in stats.items():
            logger.info(f"  {k}: {v}")
        logger.info("=" * 60)
        logger.info("✅ TEST PËRFUNDOI ME SUKSES")

    except Exception as e:
        logger.critical(f"❌ TEST DËSHTOI: {e}")
        sys.exit(1)