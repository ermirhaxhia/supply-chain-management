# ============================================================
# simulation/sales_module.py
# Gjeneron transaksione realiste çdo orë
# Mbulon të gjithë skenarët e shportës
# ============================================================

import sys
import os
import logging
import numpy as np
import uuid
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import supabase
from config.constants import (
    BASKET_TYPE_PROBABILITIES,
    BASKET_SIZE_PARAMS,
    BASKET_HOUR_MODIFIER,
    BASKET_WEEKDAY_MODIFIER,
    PAYMENT_METHODS,
    CUSTOMER_TYPES,
    DISCOUNT_PROBABILITY,
    DISCOUNT_RANGE,
    STOCKOUT_PROBABILITY,
    TRANSACTION_ID_PREFIX,
)
from simulation.demand_profile import get_customers, load_simulation_config, get_config

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("sales_module")

# ============================================================
# CACHE PRODUKTET DHE STORE-ET
# ============================================================
_products_cache: list = []
_stores_cache:   list = []

def load_products() -> list:
    """Ngarko të gjitha produktet nga Supabase."""
    global _products_cache
    try:
        response = supabase.table("products").select("*").execute()
        if response.data:
            _products_cache = response.data
            logger.info(f"✅ U ngarkuan {len(_products_cache)} produkte")
        else:
            logger.error("❌ Nuk u gjetën produkte në Supabase")
    except Exception as e:
        logger.error(f"❌ ERROR duke ngarkuar produktet: {e}")
    return _products_cache

def load_stores() -> list:
    """Ngarko të gjitha store-et nga Supabase."""
    global _stores_cache
    try:
        response = supabase.table("stores").select("*").execute()
        if response.data:
            _stores_cache = response.data
            logger.info(f"✅ U ngarkuan {len(_stores_cache)} store-e")
        else:
            logger.error("❌ Nuk u gjetën store-et në Supabase")
    except Exception as e:
        logger.error(f"❌ ERROR duke ngarkuar store-et: {e}")
    return _stores_cache

# ============================================================
# BASKET TYPE — Cakto tipin e shportës
# ============================================================
def get_basket_type(dt: datetime) -> str:
    """
    Cakton tipin e shportës bazuar në orë dhe ditë.
    Kombino probabilitetet bazë me modifikuesit e orës.

    Returns:
        str : "quick" | "medium" | "family" | "bulk"
    """
    try:
        # Probabilitetet e modifikuara sipas orës
        hour_mod = BASKET_HOUR_MODIFIER.get(dt.hour, {
            "quick": 0.35, "medium": 0.40,
            "family": 0.20, "bulk": 0.05
        })

        # Normalizo probabilitetet
        total = sum(hour_mod.values())
        probs = [hour_mod[t] / total for t in ["quick", "medium", "family", "bulk"]]

        basket_type = np.random.choice(
            ["quick", "medium", "family", "bulk"],
            p=probs
        )
        return basket_type

    except Exception as e:
        logger.error(f"❌ ERROR në get_basket_type: {e}")
        return "medium"

# ============================================================
# BASKET SIZE — Numri i produkteve në shportë
# ============================================================
def get_basket_size(basket_type: str, dt: datetime) -> int:
    """
    Gjeneron numrin e produkteve për 1 faturë.
    Përdor parametrat e shpërndarjes për çdo tip.

    Returns:
        int : numri i produkteve (min 1)
    """
    try:
        params = BASKET_SIZE_PARAMS[basket_type]

        # Gjenero nga shpërndarja normale e cunguar
        size = int(np.random.normal(params["mean"], params["std"]))

        # Apliko modifikuesin e ditës
        weekday_mod = BASKET_WEEKDAY_MODIFIER.get(dt.weekday(), 1.0)
        size = int(size * weekday_mod)

        # Clip brenda limiteve
        size = max(params["min"], min(params["max"], size))
        return size

    except Exception as e:
        logger.error(f"❌ ERROR në get_basket_size: {e}")
        return 1

# ============================================================
# SELEKTO PRODUKTET PËR SHPORTË
# ============================================================
def select_products_for_basket(
    basket_type: str,
    basket_size: int,
    products: list
) -> list:
    """
    Zgjedh produktet për shportën duke respektuar
    logjikën realiste:
    - Blerje e shpejtë → produkte të kategorive A dhe B
    - Blerje familjare → mix i kategorive
    - Blerje e madhe  → produkte me volum të lartë

    Returns:
        list : lista e produkteve të zgjedhura
    """
    try:
        if not products:
            logger.error("❌ Lista e produkteve është bosh")
            return []

        if basket_type == "quick":
            # Prefero produkte A dhe B (të njohura, të shpejta)
            preferred = [p for p in products if p.get("abc_class") in ["A", "B"]]
            pool = preferred if preferred else products

        elif basket_type == "medium":
            # Mix normal i të gjitha kategorive
            pool = products

        elif basket_type == "family":
            # Mix i gjerë — produkte nga kategori të ndryshme
            pool = products
            # Siguro diversitet kategorish
            categories = list(set(p["category_id"] for p in products))
            selected = []
            remaining = basket_size

            # Merr 1-2 produkte nga çdo kategori
            for cat in np.random.choice(categories,
                                         min(len(categories), basket_size),
                                         replace=False):
                cat_products = [p for p in pool if p["category_id"] == cat]
                if cat_products and remaining > 0:
                    selected.append(np.random.choice(cat_products))
                    remaining -= 1

            # Plotëso me produkte random nëse duhen më shumë
            while remaining > 0:
                selected.append(np.random.choice(pool))
                remaining -= 1

            return selected[:basket_size]

        elif basket_type == "bulk":
            # Produkte me volum të lartë — kryesisht A
            preferred = [p for p in products if p.get("abc_class") == "A"]
            pool = preferred if len(preferred) >= basket_size else products

        else:
            pool = products

        # Zgjedh produkte (me zëvendësim — i njëjti produkt mund të jetë 2x)
        selected = list(np.random.choice(pool, size=basket_size, replace=True))
        return selected

    except Exception as e:
        logger.error(f"❌ ERROR në select_products_for_basket: {e}")
        return [np.random.choice(products)] if products else []

# ============================================================
# GJENERO 1 TRANSAKSION (FATURË + DETAJET)
# ============================================================
def generate_transaction(
    store: dict,
    products: list,
    dt: datetime
) -> dict | None:
    """
    Gjeneron 1 faturë të plotë të ndarë në dysh:
    - header (për tabelën transactions)
    - items  (për tabelën sales_hourly)
    """
    try:
        # ── 1. Basket type dhe size ──────────────────────
        basket_type = get_basket_type(dt)
        basket_size = get_basket_size(basket_type, dt)

        # ── 2. Zgjedh produktet ──────────────────────────
        selected_products = select_products_for_basket(
            basket_type, basket_size, products
        )
        if not selected_products:
            logger.warning(f"⚠️  Store {store['store_id']}: Nuk u zgjodhën produkte")
            return None

        # ── 3. Stockout check ────────────────────────────
        available_products = []
        for p in selected_products:
            if np.random.random() > STOCKOUT_PROBABILITY:
                available_products.append(p)

        if not available_products:
            logger.debug(f"🚫 Store {store['store_id']}: Stockout total për këtë faturë")
            return None

        # ── 4. Discount (Për të gjithë faturën) ──────────
        discount_pct = 0.0
        promo_id     = None
        promo_active = get_config("promo_active", 0.0)

        if promo_active == 1.0:
            discount_pct = float(get_config("promo_discount_pct", 0.0))
            promo_id     = "PROMO-ACTIVE"
        elif np.random.random() < DISCOUNT_PROBABILITY:
            val = float(np.random.randint(DISCOUNT_RANGE[0], DISCOUNT_RANGE[1]))
            discount_pct = min(val, 20.0) 

        # ── 5. Llogaritjet Financiare Dhe Detajet ────────
        basket_items = []
        
        # Variablat Agreguese të Faturës
        total_items           = 0
        total_revenue_gross   = 0.0
        total_discount_amount = 0.0
        total_net_revenue     = 0.0
        total_cogs            = 0.0

        for p in available_products:
            price = float(p.get("unit_price", 100))
            # Nëse nuk e ke kolonën 'unit_cost' te db e produkteve, supozojmë 60%
            unit_cost = float(p.get("unit_cost", price * 0.6))
            
            # Logjika e sasisë (qty)
            if price < 150:
                qty = np.random.randint(1, 5)   
            elif price < 500:
                qty = np.random.randint(1, 3)   
            else:
                qty = 1                          

            if basket_type == "quick":
                qty = 1
            elif basket_type == "bulk":
                qty = np.random.randint(2, 6) if price < 200 else 1

            # Matematika për këtë Rresht (Produkt)
            item_gross_revenue = price * qty
            item_discount      = item_gross_revenue * (discount_pct / 100)
            item_net_rev       = item_gross_revenue - item_discount
            item_cogs          = unit_cost * qty
            item_profit        = item_net_rev - item_cogs

            # Përditëso totalet e faturës (Header)
            total_items           += qty
            total_revenue_gross   += item_gross_revenue
            total_discount_amount += item_discount
            total_net_revenue     += item_net_rev
            total_cogs            += item_cogs

            # Shto rreshtin te lista e produkteve (Këto shkojnë te sales_hourly)
            basket_items.append({
                "store_id":           store["store_id"],
                "product_id":         p["product_id"],
                "date":               dt.date().isoformat(),
                "hour":               dt.hour,
                "units_sold":         qty,
                "revenue":            round(item_gross_revenue, 2),
                "discount_amount":    round(item_discount, 2),
                "net_revenue":        round(item_net_rev, 2),
                "cogs":               round(item_cogs, 2),
                "gross_profit":       round(item_profit, 2),
                "transactions_count": 1 # Për agregimin e numrit të faturave
            })

        # ── 6. Ndërtimi i Header të Faturës ──────────────
        transaction_id = f"{TRANSACTION_ID_PREFIX}-{uuid.uuid4().hex[:10].upper()}"
        customer_type  = np.random.choice(list(CUSTOMER_TYPES.keys()), p=list(CUSTOMER_TYPES.values()))
        payment_method = np.random.choice(list(PAYMENT_METHODS.keys()), p=list(PAYMENT_METHODS.values()))

        transaction_header = {
            "transaction_id":  transaction_id,
            "store_id":        store["store_id"],
            "timestamp":       dt.isoformat(),
            "customer_type":   customer_type,
            "payment_method":  payment_method,
            "promotion_id":    promo_id,
            "total_items":     total_items,
            "revenue":         round(total_revenue_gross, 2),
            "discount_amount": round(total_discount_amount, 2),
            "net_revenue":     round(total_net_revenue, 2),
            "cogs":            round(total_cogs, 2),
            "gross_profit":    round(total_net_revenue - total_cogs, 2)
        }

        # Kthen të dyja objektet pa humbur asgjë
        return {
            "header": transaction_header,
            "items": basket_items
        }
    
    except Exception as e:
        # Kapim çdo gjë dhe e logojmë saktësisht ku ndodhi
        logger.error(f"❌ ERROR në generate_transaction: {e}")
        return None
    


# ============================================================
# GJENERO TË GJITHA TRANSAKSIONET PËR 1 ORË
# ============================================================
def run_sales_hour(store: dict, products: list, dt: datetime) -> dict:
    """
    Orkestron gjenerimin dhe insertimin e të gjitha shitjeve për 1 dyqan në 1 orë specifike.
    
    Hapat që ndjek:
    1. Llogarit numrin e klientëve bazuar në profilin e kërkesës për orën/ditën.
    2. Gjeneron të dhënat në memorje (përmes `generate_transaction`) dhe i ndan në 2 lista.
    3. Ekzekuton Insert në Supabase për Faturat (Transactions) në grupe prej 500.
    4. Ekzekuton Insert në Supabase për Detajet (Sales Hourly) në grupe prej 500.
    5. Mbledh dhe kthen statistikat përmbledhëse të kësaj ore.
    
    Siguria: Përdor blloqe `try...except` të izoluara për çdo insertim. Nëse tabela
    e detajeve dështon, tabela e faturave nuk preket, duke bërë izolimin e gabimit.
    """
    logger.info(f"🏪 Store {store['store_id']} [{store['city']}] | {dt.strftime('%Y-%m-%d %H:%M')}")

    num_customers = get_customers(store, dt)

    all_headers       = []
    all_items         = []
    total_net_revenue = 0.0
    failed_count      = 0
    basket_type_stats = {"quick": 0, "medium": 0, "family": 0, "bulk": 0}

    # 1. Përgatitja e të dhënave në memorje
    for _ in range(num_customers):
        txn = generate_transaction(store, products, dt)
        if txn:
            all_headers.append(txn["header"])
            all_items.extend(txn["items"])
            
            # Kujdes: Tani përdorim net_revenue për të pasqyruar realitetin e xhiros
            total_net_revenue += txn["header"]["net_revenue"] 
            
            basket_type = get_basket_type(dt)
            basket_type_stats[basket_type] = basket_type_stats.get(basket_type, 0) + 1
        else:
            failed_count += 1

    # 2. INSERT PËR TRANSAKSIONET (HEADERS)
    inserted_headers = 0
    if all_headers:
        try:
            batch_size = 500
            for i in range(0, len(all_headers), batch_size):
                batch = all_headers[i:i + batch_size]
                response = supabase.table("transactions").insert(batch).execute()
                if response.data:
                    inserted_headers += len(response.data)
                else:
                    logger.warning(f"⚠️  Batch TRANSACTIONS {i//batch_size + 1}: Nuk u kthye data")
        except Exception as e:
            logger.error(f"❌ ERROR Kritik INSERT transactions: {e}")

    # 3. INSERT PËR DETAJET ORARE (ITEMS)
    inserted_items = 0
    if all_items:
        try:
            batch_size = 500
            for i in range(0, len(all_items), batch_size):
                batch = all_items[i:i + batch_size]
                response = supabase.table("sales_hourly").insert(batch).execute()
                if response.data:
                    inserted_items += len(response.data)
                else:
                    logger.warning(f"⚠️  Batch SALES_HOURLY {i//batch_size + 1}: Nuk u kthye data")
        except Exception as e:
            logger.error(f"❌ ERROR Kritik INSERT sales_hourly: {e}")

    # 4. Statistikat për Printimin Final
    stats = {
        "store_id":       store["store_id"],
        "hour":           dt.hour,
        "customers":      num_customers,
        "transactions":   inserted_headers, # Printon sa fatura u ngarkuan
        "items_sold":     inserted_items,   # Printon sa rreshta shkuan te sales_hourly
        "failed":         failed_count,
        "total_revenue":  round(total_net_revenue, 2),
        "avg_basket":     round(total_net_revenue / inserted_headers, 2) if inserted_headers > 0 else 0,
        "basket_types":   basket_type_stats,
    }

    logger.info(
        f"  ✅ Klientë={num_customers} | "
        f"Fatura={inserted_headers} | "
        f"Produkte(Rreshta)={inserted_items} | "
        f"Net Revenue={total_net_revenue:,.0f} Lekë | "
        f"Avg={stats['avg_basket']:,.0f} Lekë"
    )

    return stats

# ============================================================
# MAIN — Test i plotë
# ============================================================
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("TEST I SALES MODULE")
    logger.info("=" * 60)

    try:
        # Ngarko config + data
        load_simulation_config()
        products = load_products()
        stores   = load_stores()

        if not products or not stores:
            logger.critical("❌ Të dhënat bazë mungojnë")
            sys.exit(1)

        # Testo vetëm 1 store dhe 1 orë
        test_store = stores[0]
        test_dt    = datetime.now().replace(
            minute=0, second=0, microsecond=0
        )

        logger.info(f"🧪 Duke testuar: {test_store['store_name']} | {test_dt.strftime('%H:%M')}")
        stats = run_sales_hour(test_store, products, test_dt)

        logger.info("=" * 60)
        logger.info("REZULTATI:")
        logger.info(f"  Klientë:    {stats['customers']}")
        logger.info(f"  Fatura:     {stats['transactions']}")
        logger.info(f"  Revenue:    {stats['total_revenue']:,.0f} Lekë")
        logger.info(f"  Avg Basket: {stats['avg_basket']:,.0f} Lekë")
        logger.info(f"  Basket Types: {stats['basket_types']}")
        logger.info("=" * 60)
        logger.info("✅ TEST PËRFUNDOI ME SUKSES")

    except Exception as e:
        logger.critical(f"❌ TEST DËSHTOI: {e}")
        sys.exit(1)