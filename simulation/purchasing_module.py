# ============================================================
# simulation/purchasing_module.py
# Menaxhon porositë te furnizuesit (Purchase Orders)
# Aktivizohet kur stoku bie nën reorder_point
# ============================================================

import sys
import os
import logging
import numpy as np
import uuid
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import supabase
from config.constants import PO_ID_PREFIX, SUPPLIER_DEFECT_STD
from simulation.demand_profile import load_simulation_config, get_config

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("purchasing_module")

# ============================================================
# CACHE — Porosi aktive për të shmangur dyfishimin
# {store_id: {product_id: po_id}}
# ============================================================
_active_orders: dict = {}

def has_active_order(store_id: str, product_id: str) -> bool:
    """Kontrollon nëse ekziston tashmë një porosi aktive."""
    return product_id in _active_orders.get(store_id, {})

def register_order(store_id: str, product_id: str, po_id: str):
    """Regjistron një porosi aktive."""
    if store_id not in _active_orders:
        _active_orders[store_id] = {}
    _active_orders[store_id][product_id] = po_id

def complete_order(store_id: str, product_id: str):
    """Mbyll një porosi kur dorëzohet."""
    if store_id in _active_orders:
        _active_orders[store_id].pop(product_id, None)

# ============================================================
# GJENERO PURCHASE ORDER
# ============================================================
def generate_purchase_order(
    store_id:     str,
    product:      dict,
    warehouse_id: str,
    stock_level:  int,
    dt:           datetime
) -> dict | None:
    """
    Gjeneron 1 Purchase Order kur stoku bie nën reorder_point.

    Lead time ~ Poisson(avg_lead_days) × lead_time_multiplier
    Defect rate ~ Normal(base_rate, std)

    Returns:
        dict : purchase order ose None nëse dështon
    """
    try:
        product_id    = product["product_id"]
        supplier_id   = product["supplier_id"]
        reorder_qty   = product.get("reorder_qty",   50)
        unit_cost     = product.get("unit_cost",     100)

        # Nuk krijo porosi nëse ekziston një aktive
        if has_active_order(store_id, product_id):
            logger.debug(f"⏭️  {product_id}: Porosi aktive ekziston")
            return None

        # ── Lead Time ~ Poisson ───────────────────────────
        lead_mult     = get_config("lead_time_multiplier", 1.0)
        base_lead     = np.random.randint(1, 4)          # 1-3 ditë bazë
        lead_time     = max(1, int(np.random.poisson(base_lead * lead_mult)))
        expected_date = dt.date() + timedelta(days=lead_time)

        # ── Quantity — sa të porosisësh ───────────────────
        # Quantity discount: sa më shumë → çmim më i ulët
        qty_multiplier = get_config("reorder_multiplier", 1.0)
        qty_ordered    = int(reorder_qty * qty_multiplier)

        # ── Defect Rate ───────────────────────────────────
        base_defect  = 0.02
        defect_rate  = max(0, np.random.normal(base_defect, SUPPLIER_DEFECT_STD))

        # ── Quantity discount ──────────────────────────────
        if qty_ordered >= 100:
            discount = 0.10   # 10% zbritje
        elif qty_ordered >= 50:
            discount = 0.05   # 5% zbritje
        else:
            discount = 0.0

        unit_cost_final = unit_cost * (1 - discount)
        total_cost      = round(qty_ordered * unit_cost_final, 2)

        # ── Black Swan: çmim i lartë nga kriza ────────────
        import_mult  = get_config("import_price_multiplier", 1.0)
        total_cost  *= import_mult

        po_id = f"{PO_ID_PREFIX}-{uuid.uuid4().hex[:10].upper()}"

        po = {
            "po_id":         po_id,
            "supplier_id":   supplier_id,
            "product_id":    product_id,
            "warehouse_id":  warehouse_id,
            "order_date":    dt.date().isoformat(),
            "expected_date": expected_date.isoformat(),
            "actual_date":   None,
            "qty_ordered":   qty_ordered,
            "qty_received":  0,
            "unit_cost":     round(unit_cost_final, 2),
            "total_cost":    round(total_cost, 2),
            "status":        "Pending",
        }

        register_order(store_id, product_id, po_id)
        return po

    except Exception as e:
        logger.error(f"❌ ERROR në generate_purchase_order për {product.get('product_id')}: {e}")
        return None

# ============================================================
# RUN PURCHASING — Ekzekuto për 1 store, 1 herë në ditë
# ============================================================
def run_purchasing(
    store:        dict,
    products:     list,
    stock_cache:  dict,
    warehouse_id: str,
    dt:           datetime
) -> dict:
    """
    Kontrollon stokun e çdo produkti dhe krijon
    Purchase Orders për ato nën reorder_point.

    Ekzekutohet 1 herë në ditë (ora 08:00).

    Returns:
        dict : statistikat e porosive
    """
    if dt.hour != 8:
        return {"orders_created": 0, "total_cost": 0}

    store_id = store["store_id"]

    #Për t'i hapur rrugë porosive të ditës tjetër
    global _active_orders
    _active_orders.pop(store_id, None)
    logger.info(f"🛍️  Purchasing: {store_id} | {dt.strftime('%Y-%m-%d')}")

    orders      = []
    total_cost  = 0.0
    skipped     = 0

    try:
        store_stock = stock_cache.get(store_id, {})

        for product in products:
            product_id    = product["product_id"]
            stock_level   = store_stock.get(product_id, 0)
            reorder_point = product.get("reorder_point", 20)

            # Kontrollo nëse duhet porosi
            if stock_level <= reorder_point:
                po = generate_purchase_order(
                    store_id, product,
                    warehouse_id, stock_level, dt
                )
                if po:
                    orders.append(po)
                    total_cost += po["total_cost"]
                else:
                    skipped += 1

        # ── Batch INSERT në Supabase ──────────────────────
        inserted = 0
        if orders:
            batch_size = 500
            for i in range(0, len(orders), batch_size):
                batch    = orders[i:i + batch_size]
                response = supabase.table("purchase_orders").insert(batch).execute()
                if response.data:
                    inserted += len(response.data)
                else:
                    logger.warning(f"⚠️  Batch {i//batch_size+1}: Nuk u kthye data")

    except Exception as e:
        logger.error(f"❌ ERROR në run_purchasing: {e}")

    stats = {
        "store_id":      store_id,
        "orders_created":inserted,
        "orders_skipped":skipped,
        "total_cost":    round(total_cost, 2),
    }

    logger.info(
        f"  ✅ Porosi={inserted} | "
        f"Skipped={skipped} | "
        f"Kosto Totale={total_cost:,.0f} Lekë"
    )

    return stats

# ============================================================
# MAIN — Test
# ============================================================
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("TEST I PURCHASING MODULE")
    logger.info("=" * 60)

    try:
        load_simulation_config()

        stores_resp   = supabase.table("stores").select("*").execute()
        products_resp = supabase.table("products").select("*").execute()
        wh_resp       = supabase.table("warehouses").select("*").execute()

        stores     = stores_resp.data
        products   = products_resp.data
        warehouses = wh_resp.data

        if not stores or not products or not warehouses:
            logger.critical("❌ Të dhënat bazë mungojnë")
            sys.exit(1)

        # Simulo stock të ulët për të gjitha produktet
        test_stock = {
            stores[0]["store_id"]: {
                p["product_id"]: p.get("reorder_point", 20) - 5
                for p in products
            }
        }

        test_store    = stores[0]
        test_wh       = warehouses[0]["warehouse_id"]
        test_dt       = datetime.now().replace(
            hour=8, minute=0, second=0, microsecond=0
        )

        stats = run_purchasing(test_store, products, test_stock, test_wh, test_dt)

        logger.info("=" * 60)
        logger.info("REZULTATI:")
        for k, v in stats.items():
            logger.info(f"  {k}: {v}")
        logger.info("=" * 60)
        logger.info("✅ TEST PËRFUNDOI ME SUKSES")

    except Exception as e:
        logger.critical(f"❌ TEST DËSHTOI: {e}")
        sys.exit(1)