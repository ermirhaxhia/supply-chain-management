# ============================================================
# config/constants.py
# Parametrat globalë të simulimit — dokumentuar plotësisht
# ============================================================

# ============================================================
# ORARI I SIMULIMIT
# ============================================================
SIMULATION_START_HOUR = 6
SIMULATION_END_HOUR   = 22
SIMULATION_HOURS      = list(range(SIMULATION_START_HOUR, SIMULATION_END_HOUR + 1))

# ============================================================
# BASKET SIZE — Shpërndarja e produkteve/faturë
# Bazë: literatura retail + zakone shqiptare
# ============================================================

# Probabilitetet e 4 tipareve të klientit
BASKET_TYPE_PROBABILITIES = {
    "quick":    0.35,   # Blerje e shpejtë  (1-3 produkte)
    "medium":   0.40,   # Blerje mesatare   (4-7 produkte)
    "family":   0.20,   # Blerje familjare  (8-15 produkte)
    "bulk":     0.05,   # Blerje e madhe    (16-35 produkte)
}

# Parametrat Negative Binomial për çdo tip (n, p)
# E(X) = n*(1-p)/p
BASKET_SIZE_PARAMS = {
    "quick":  {"min": 1,  "max": 3,  "mean": 1.5, "std": 0.5},
    "medium": {"min": 3,  "max": 6,  "mean": 4.0, "std": 1.0},
    "family": {"min": 6,  "max": 12, "mean": 8.0, "std": 2.0},
    "bulk":   {"min": 10, "max": 25, "mean": 15.0,"std": 4.0},
}

# Modifikues sipas orës
# (mëngjes = blerje të shpejta, mbrëmje = familjare)
BASKET_HOUR_MODIFIER = {
    6:  {"quick": 0.70, "medium": 0.20, "family": 0.08, "bulk": 0.02},
    7:  {"quick": 0.65, "medium": 0.25, "family": 0.08, "bulk": 0.02},
    8:  {"quick": 0.60, "medium": 0.30, "family": 0.08, "bulk": 0.02},
    9:  {"quick": 0.50, "medium": 0.35, "family": 0.12, "bulk": 0.03},
    10: {"quick": 0.40, "medium": 0.40, "family": 0.15, "bulk": 0.05},
    11: {"quick": 0.35, "medium": 0.40, "family": 0.20, "bulk": 0.05},
    12: {"quick": 0.40, "medium": 0.38, "family": 0.18, "bulk": 0.04},
    13: {"quick": 0.45, "medium": 0.35, "family": 0.16, "bulk": 0.04},
    14: {"quick": 0.40, "medium": 0.38, "family": 0.18, "bulk": 0.04},
    15: {"quick": 0.35, "medium": 0.40, "family": 0.20, "bulk": 0.05},
    16: {"quick": 0.30, "medium": 0.40, "family": 0.25, "bulk": 0.05},
    17: {"quick": 0.25, "medium": 0.38, "family": 0.30, "bulk": 0.07},
    18: {"quick": 0.20, "medium": 0.35, "family": 0.38, "bulk": 0.07},
    19: {"quick": 0.22, "medium": 0.35, "family": 0.35, "bulk": 0.08},
    20: {"quick": 0.25, "medium": 0.38, "family": 0.30, "bulk": 0.07},
    21: {"quick": 0.40, "medium": 0.38, "family": 0.18, "bulk": 0.04},
    22: {"quick": 0.60, "medium": 0.30, "family": 0.08, "bulk": 0.02},
}

# Modifikues sipas ditës së javës
BASKET_WEEKDAY_MODIFIER = {
    0: 0.85,    # E Hënë   — blerje të vogla
    1: 0.88,    # E Martë
    2: 0.90,    # E Mërkurë
    3: 0.92,    # E Enjte
    4: 1.10,    # E Premte — fillim fundjavë
    5: 1.30,    # E Shtunë — blerje familjare
    6: 1.20,    # E Diel   — blerje javore
}

# ============================================================
# PAYMENT METHOD — Mënyrat e pagesës
# ============================================================
PAYMENT_METHODS = {
    "Cash":   0.55,     # 55% cash (Shqipëri — kryesisht cash)
    "Card":   0.38,     # 38% kartë
    "eWallet":0.07,     # 7%  eWallet (Revolut, PayPal)
}

# ============================================================
# CUSTOMER TYPE
# ============================================================
CUSTOMER_TYPES = {
    "Member": 0.30,     # 30% klientë të regjistruar
    "Normal": 0.70,     # 70% klientë normal
}

# ============================================================
# DISCOUNT — Zbritjet
# ============================================================
DISCOUNT_PROBABILITY  = 0.15    # 15% e faturave kanë zbritje
DISCOUNT_RANGE        = (5, 30) # % zbritje (min, max)

# ============================================================
# STOKU — Parametrat
# ============================================================
STOCKOUT_PROBABILITY       = 0.05   # 5% probabilitet stockout
EXPIRY_LOSS_RATE  = 0.005   # 0.5% 
SHRINKAGE_RATE    = 0.001   # 0.1% humbje nga vjedhja/dëmtimi
INITIAL_STOCK_FILL_PCT     = 0.75   # Store fillon me 75% stok

# ============================================================
# TRANSPORTI
# ============================================================
FUEL_CONSUMPTION_STD       = 0.05   # ±5% devijim konsumi karburantit
TRANSPORT_DELAY_PROBABILITY= 0.12   # 12% probabilitet vonesë
TRANSPORT_DELAY_MINUTES    = {
    "min": 5,
    "max": 45,
    "mean": 15,
}
DELIVERIES_PER_STORE_PER_DAY = 2    # dërgesa mesatare/ditë

# ============================================================
# PURCHASING / FURNIZIMI
# ============================================================
LEAD_TIME_DISTRIBUTION = "poisson"  # Lead time ~ Poisson(λ)
REORDER_CHECK_HOUR     = 8          # Kontrollo stokun çdo ditë ora 8
SUPPLIER_DEFECT_STD    = 0.01       # ±1% devijim defect rate

# ============================================================
# MARKETING
# ============================================================
CAMPAIGN_ACTIVE_PROBABILITY = 0.10  # 10% mundësi kampanje aktive/ditë
CAMPAIGN_DURATION_DAYS      = {
    "min": 3,
    "max": 14,
    "mean": 7,
}
PROMO_DEMAND_LIFT_RANGE = (0.10, 0.35)  # +10% deri +35% rritje kërkese

# ============================================================
# AGREGIMI
# ============================================================
HOURLY_AGGREGATE_MINUTE  = 55   # Agregim në minutën :55 të çdo ore
DAILY_AGGREGATE_HOUR     = 23   # Agregim ditor ora 23:00
MONTHLY_AGGREGATE_DAY    = 1    # Agregim mujor ditën 1 të muajit
RAW_DATA_RETENTION_DAYS  = 30   # Fshi raw data > 30 ditë

# ============================================================
# ID GENERATION
# ============================================================
TRANSACTION_ID_PREFIX  = "TXN"
SHIPMENT_ID_PREFIX     = "SHP"
PO_ID_PREFIX           = "PO"
LOG_ID_PREFIX          = "LOG"
SNAPSHOT_ID_PREFIX     = "SNP"