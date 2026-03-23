# ============================================================
# config/settings.py
# Menaxhimi i konfigurimit dhe lidhja me Supabase
# ============================================================

import os
import logging
from dotenv import load_dotenv
from supabase import create_client, Client

# ============================================================
# LOGGING SETUP
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("settings")

# ============================================================
# NGARKO .env
# ============================================================
load_dotenv()
logger.info("✅ .env u ngarkua me sukses")

# ============================================================
# SUPABASE CREDENTIALS
# ============================================================
SUPABASE_URL         = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL:
    logger.critical("❌ SUPABASE_URL mungon në .env")
    raise EnvironmentError("SUPABASE_URL nuk është vendosur në .env")

if not SUPABASE_SERVICE_KEY:
    logger.critical("❌ SUPABASE_SERVICE_KEY mungon në .env")
    raise EnvironmentError("SUPABASE_SERVICE_KEY nuk është vendosur në .env")

# ============================================================
# API KEYS
# ============================================================
API_KEY_MASTER      = os.getenv("API_KEY_MASTER")
API_KEY_SALES       = os.getenv("API_KEY_SALES")
API_KEY_INVENTORY   = os.getenv("API_KEY_INVENTORY")
API_KEY_LOGISTICS   = os.getenv("API_KEY_LOGISTICS")
API_KEY_PROCUREMENT = os.getenv("API_KEY_PROCUREMENT")

_api_keys = {
    "API_KEY_MASTER":      API_KEY_MASTER,
    "API_KEY_SALES":       API_KEY_SALES,
    "API_KEY_INVENTORY":   API_KEY_INVENTORY,
    "API_KEY_LOGISTICS":   API_KEY_LOGISTICS,
    "API_KEY_PROCUREMENT": API_KEY_PROCUREMENT,
}

for key_name, key_value in _api_keys.items():
    if not key_value:
        logger.warning(f"⚠️  {key_name} mungon në .env")
    else:
        logger.info(f"✅ {key_name} u ngarkua")

# ============================================================
# SIMULATION PARAMETERS
# ============================================================
FUEL_BASE_PRICE = float(os.getenv("FUEL_BASE_PRICE", 185))
logger.info(f"✅ Çmimi bazë karburantit: {FUEL_BASE_PRICE} Lekë/litër")

# ============================================================
# SUPABASE CLIENT
# ============================================================
def get_supabase_client() -> Client:
    """
    Krijon dhe kthen Supabase client.
    Hedh exception nëse lidhja dështon.
    """
    try:
        client: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        logger.info("✅ Supabase client u krijua me sukses")
        return client
    except Exception as e:
        logger.critical(f"❌ Supabase client dështoi: {e}")
        raise ConnectionError(f"Nuk u lidh me Supabase: {e}")

# Instanco 1 herë — përdoret nga të gjitha modulet
try:
    supabase: Client = get_supabase_client()
except ConnectionError as e:
    logger.critical(str(e))
    raise