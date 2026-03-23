# ============================================================
# simulation/demand_profile.py
# Profili i kërkesës — Poisson jo-homogjen me multipliers
# λ_i(t) = λ_i_base × trend(m) × weekly(d) × hourly(h) × event(config)
# ============================================================

import sys
import os
import logging
import numpy as np
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import supabase

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("demand_profile")

# ============================================================
# SEZONALITET MUJOR — Trend(m)
# Bazë: literatura FMCG + zakone shqiptare
# ============================================================
MONTHLY_TREND = {
    1:  0.85,   # Janar   — pas Vitit të Ri, kursim
    2:  0.88,   # Shkurt  — normal i ulët
    3:  0.92,   # Mars    — fillim pranvere
    4:  1.10,   # Prill   — Pashkë + mot i mirë
    5:  1.05,   # Maj     — normal i lartë
    6:  1.10,   # Qershor — fillim pushimesh
    7:  1.20,   # Korrik  — pikë verore
    8:  1.25,   # Gusht   — maksimum veror
    9:  1.05,   # Shtator — rikthim normaliteti
    10: 0.95,   # Tetor   — ulje graduale
    11: 1.00,   # Nëntor  — normal
    12: 1.45,   # Dhjetor — Krishtlindje + Viti i Ri
}

# ============================================================
# SEZONALITET JAVOR — Weekly(d)
# 0=E Hënë ... 6=E Diel
# ============================================================
WEEKLY_MULTIPLIER = {
    0: 0.80,    # E Hënë   — fillim jave, i qetë
    1: 0.85,    # E Martë
    2: 0.90,    # E Mërkurë
    3: 0.95,    # E Enjte
    4: 1.25,    # E Premte — blerje para fundjave
    5: 1.40,    # E Shtunë — PEAK javor
    6: 1.30,    # E Diel   — familjet blejnë
}

# ============================================================
# SEZONALITET DITOR — Hourly(h)
# Orët 06:00-22:00
# ============================================================
HOURLY_MULTIPLIER = {
    6:  0.40,   # 06:00 — hapje, shumë i qetë
    7:  0.55,   # 07:00 — mëngjes herët
    8:  0.75,   # 08:00 — para punës
    9:  0.90,   # 09:00 — normal mëngjes
    10: 1.00,   # 10:00 — bazë
    11: 1.40,   # 11:00 — para drekës, rritje
    12: 1.60,   # 12:00 — PEAK drekë
    13: 1.50,   # 13:00 — pas drekës
    14: 1.10,   # 14:00 — qetësim pasdite
    15: 1.00,   # 15:00 — normal
    16: 1.20,   # 16:00 — rritje pas punës
    17: 1.70,   # 17:00 — PEAK pasdite
    18: 1.80,   # 18:00 — SUPER PEAK
    19: 1.60,   # 19:00 — ende i lartë
    20: 1.20,   # 20:00 — ulje graduale
    21: 0.80,   # 21:00 — mbyllje afër
    22: 0.40,   # 22:00 — mbyllje
}

# ============================================================
# FESTAT KOMBËTARE SHQIPTARE
# Ndikimi: multiplier i kërkesës
# ============================================================
ALBANIAN_HOLIDAYS = {
    "01-01": ("Viti i Ri",          2.00),
    "01-02": ("Viti i Ri (2)",      1.50),
    "03-14": ("Dita e Verës",       1.40),
    "03-22": ("Nevruz",             1.20),
    "04-20": ("Pashkë Katolike",    1.80),
    "04-21": ("Pashkë Ortodokse",   1.80),
    "05-01": ("Dita e Punës",       1.50),
    "10-19": ("Kurban Bajram",      1.70),
    "11-28": ("Dita e Flamurit",    1.60),
    "11-29": ("Dita e Çlirimit",    1.40),
    "12-08": ("Dita Kombëtare",     1.30),
    "12-24": ("Krishtlindja Evë",   1.90),
    "12-25": ("Krishtlindja",       1.80),
    "12-31": ("Fundviti Evë",       2.20),
}

# ============================================================
# CACHE i SIMULATION CONFIG
# Lexohet 1 herë nga Supabase dhe ruhet në memorie
# ============================================================
_config_cache: dict = {}

def load_simulation_config() -> dict:
    """
    Lexon parametrat e simulimit nga Supabase.
    Kthen dict me config_key → config_value.
    """
    global _config_cache
    try:
        response = supabase.table("simulation_config").select("config_key, config_value").execute()
        if response.data:
            _config_cache = {row["config_key"]: row["config_value"] for row in response.data}
            logger.info(f"✅ Simulation config u ngarkua: {len(_config_cache)} parametra")
        else:
            logger.warning("⚠️  Simulation config është bosh — përdoren vlerat default")
            _config_cache = {}
    except Exception as e:
        logger.error(f"❌ Nuk u lexua simulation_config: {e} — përdoren vlerat default")
        _config_cache = {}
    return _config_cache

def get_config(key: str, default: float = 1.0) -> float:
    """Kthen vlerën e një parametri nga cache."""
    return _config_cache.get(key, default)

# ============================================================
# FUNKSIONI KRYESOR — λ_i(t)
# ============================================================
def get_lambda(store: dict, dt: datetime) -> float:
    """
    Llogarit λ për një store të caktuar në kohën dt.

    Formula:
      λ(t) = λ_final × trend(m) × weekly(d) × hourly(h)
             × holiday(dt) × event(config) × noise

    Parameters:
        store : dict me të dhënat e store-it (nga Supabase)
        dt    : datetime e momentit të simulimit

    Returns:
        float : numri i pritshëm i klientëve për orën dt
    """
    try:
        # ── 1. Lambda bazë e store-it ──────────────────────
        lambda_base = store.get("lambda_final", 40.0)

        # ── 2. Trend mujor ────────────────────────────────
        trend = MONTHLY_TREND.get(dt.month, 1.0)

        # ── 3. Sezonalitet javor ──────────────────────────
        weekly = WEEKLY_MULTIPLIER.get(dt.weekday(), 1.0)

        # ── 4. Sezonalitet ditor ──────────────────────────
        hourly = HOURLY_MULTIPLIER.get(dt.hour, 1.0)

        # ── 5. Festa kombëtare ────────────────────────────
        date_key = dt.strftime("%m-%d")
        holiday_mult = 1.0
        if date_key in ALBANIAN_HOLIDAYS:
            holiday_name, holiday_mult = ALBANIAN_HOLIDAYS[date_key]
            logger.info(f"🎉 Festë aktive: {holiday_name} (×{holiday_mult})")

        # ── 6. Black Swan Events (nga simulation_config) ──
        demand_mult  = get_config("demand_multiplier",  1.0)
        active_event = get_config("active_event",       0.0)
        intensity    = get_config("event_intensity",    0.0)

        event_mult = 1.0
        if active_event == 1:   # Luftë/Krizë
            event_mult = max(0.3, 1.0 - (intensity / 10) * 0.7)
            logger.warning(f"⚔️  Luftë aktive — event_mult={event_mult:.2f}")
        elif active_event == 2: # Pandemi
            event_mult = max(0.2, 1.0 - (intensity / 10) * 0.8)
            logger.warning(f"🦠 Pandemi aktive — event_mult={event_mult:.2f}")
        elif active_event == 3: # Grevë Transporti
            event_mult = max(0.6, 1.0 - (intensity / 10) * 0.4)
            logger.warning(f"✊ Grevë transporti — event_mult={event_mult:.2f}")
        elif active_event == 4: # Thatësirë
            event_mult = max(0.7, 1.0 - (intensity / 10) * 0.3)
            logger.warning(f"☀️  Thatësirë — event_mult={event_mult:.2f}")

        # ── 7. Zhurmë stokastike (±10%) ───────────────────
        noise = np.random.normal(1.0, 0.10)
        noise = max(0.7, min(1.3, noise))  # clip extreme values

        # ── 8. Lambda finale ──────────────────────────────
        lambda_final = (
            lambda_base
            * trend
            * weekly
            * hourly
            * holiday_mult
            * demand_mult
            * event_mult
            * noise
        )

        # Siguro vlerë pozitive
        lambda_final = max(1.0, lambda_final)

        logger.debug(
            f"Store {store.get('store_id')} | "
            f"λ_base={lambda_base:.1f} | "
            f"trend={trend:.2f} | weekly={weekly:.2f} | "
            f"hourly={hourly:.2f} | holiday={holiday_mult:.2f} | "
            f"event={event_mult:.2f} | noise={noise:.2f} | "
            f"λ_final={lambda_final:.1f}"
        )

        return lambda_final

    except Exception as e:
        logger.error(f"❌ ERROR në get_lambda për store {store.get('store_id')}: {e}")
        return 40.0  # fallback i sigurt

# ============================================================
# GJENERO NUMRIN E KLIENTËVE
# ============================================================
def get_customers(store: dict, dt: datetime) -> int:
    """
    Gjeneron numrin aktual të klientëve
    duke përdorur shpërndarjen Poisson.

    customers ~ Poisson(λ(t))

    Returns:
        int : numri i klientëve për orën dt
    """
    try:
        lam = get_lambda(store, dt)
        customers = int(np.random.poisson(lam))
        logger.info(
            f"🛒 Store {store.get('store_id')} [{store.get('city')}] | "
            f"{dt.strftime('%H:%M')} | λ={lam:.1f} | "
            f"Klientë={customers}"
        )
        return customers
    except Exception as e:
        logger.error(f"❌ ERROR në get_customers: {e}")
        return int(np.random.poisson(40))  # fallback

# ============================================================
# TEST — ekzekuto direkt për të testuar
# ============================================================
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("TEST I DEMAND PROFILE")
    logger.info("=" * 60)

    try:
        # Ngarko config
        load_simulation_config()

        # Ngarko store-et nga Supabase
        response = supabase.table("stores").select("*").execute()
        if not response.data:
            logger.error("❌ Nuk u gjetën store-et në Supabase")
            sys.exit(1)

        stores = response.data
        logger.info(f"✅ U ngarkuan {len(stores)} store-e")

        # Testo për orë të ndryshme
        test_hours = [8, 12, 18, 21]
        dt_base = datetime.now().replace(minute=0, second=0, microsecond=0)

        logger.info("-" * 60)
        for store in stores:
            for hour in test_hours:
                dt = dt_base.replace(hour=hour)
                customers = get_customers(store, dt)

        logger.info("=" * 60)
        logger.info("✅ TEST PËRFUNDOI ME SUKSES")

    except Exception as e:
        logger.critical(f"❌ TEST DËSHTOI: {e}")
        sys.exit(1)