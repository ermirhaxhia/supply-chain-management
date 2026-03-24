# ============================================================
# api/auth.py
# API Key authentication për të 4 API-t
# ============================================================

import logging
from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader
from config.settings import (
    API_KEY_MASTER,
    API_KEY_SALES,
    API_KEY_INVENTORY,
    API_KEY_LOGISTICS,
    API_KEY_PROCUREMENT,
)

# ============================================================
# LOGGING
# ============================================================
logger = logging.getLogger("auth")

# ============================================================
# HEADER DEFINITION
# ============================================================
api_key_header = APIKeyHeader(
    name="X-API-Key",
    auto_error=True
)

# ============================================================
# HELPER
# ============================================================
def _verify(api_key: str, allowed_keys: list, endpoint: str):
    """
    Verifikon API key dhe hedh exception nëse është i pavlefshëm.
    """
    valid_keys = [k for k in allowed_keys if k]

    if api_key not in valid_keys:
        logger.warning(f"🚫 API Key i pavlefshëm për {endpoint}: {api_key[:8]}...")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error":   "API Key i pavlefshëm",
                "endpoint": endpoint,
                "hint":    "Kontrollo X-API-Key header"
            }
        )
    logger.debug(f"✅ Auth OK për {endpoint}")

# ============================================================
# VERIFIKUESIT PËR ÇDO API
# ============================================================
def verify_sales_key(api_key: str = Security(api_key_header)):
    _verify(api_key, [API_KEY_SALES, API_KEY_MASTER], "Sales API")

def verify_inventory_key(api_key: str = Security(api_key_header)):
    _verify(api_key, [API_KEY_INVENTORY, API_KEY_MASTER], "Inventory API")

def verify_logistics_key(api_key: str = Security(api_key_header)):
    _verify(api_key, [API_KEY_LOGISTICS, API_KEY_MASTER], "Logistics API")

def verify_procurement_key(api_key: str = Security(api_key_header)):
    _verify(api_key, [API_KEY_PROCUREMENT, API_KEY_MASTER], "Procurement API")

def verify_master_key(api_key: str = Security(api_key_header)):
    _verify(api_key, [API_KEY_MASTER], "Master API")