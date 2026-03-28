# ============================================================
# api/auth.py
# API Key authentication — Header + Query Parameter
# ============================================================

import logging
from fastapi import Security, HTTPException, status, Request
from fastapi.security import APIKeyHeader, APIKeyQuery
from config.settings import (
    API_KEY_MASTER,
    API_KEY_SALES,
    API_KEY_INVENTORY,
    API_KEY_LOGISTICS,
    API_KEY_PROCUREMENT,
)

logger = logging.getLogger("auth")

# ── Header + Query param definitions ─────────────────────
api_key_header = APIKeyHeader(name="X-API-Key",  auto_error=False)
api_key_query  = APIKeyQuery(name="api_key",      auto_error=False)

# ── Helper ────────────────────────────────────────────────
def _verify(
    header_key: str,
    query_key:  str,
    allowed:    list,
    endpoint:   str
):
    api_key = header_key or query_key

    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error":   "API Key mungon",
                "hint":    "Shto ?api_key=YOUR_KEY në URL ose X-API-Key header",
                "endpoint": endpoint,
            }
        )

    valid = [k for k in allowed if k]
    if api_key not in valid:
        logger.warning(f"🚫 Key i pavlefshëm për {endpoint}: {api_key[:8]}...")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error":    "API Key i pavlefshëm",
                "endpoint": endpoint,
            }
        )

    logger.debug(f"✅ Auth OK për {endpoint}")

# ── Verifikuesit ──────────────────────────────────────────
def verify_sales_key(
    h: str = Security(api_key_header),
    q: str = Security(api_key_query),
):
    _verify(h, q, [API_KEY_SALES, API_KEY_MASTER], "Sales API")

def verify_inventory_key(
    h: str = Security(api_key_header),
    q: str = Security(api_key_query),
):
    _verify(h, q, [API_KEY_INVENTORY, API_KEY_MASTER], "Inventory API")

def verify_logistics_key(
    h: str = Security(api_key_header),
    q: str = Security(api_key_query),
):
    _verify(h, q, [API_KEY_LOGISTICS, API_KEY_MASTER], "Logistics API")

def verify_procurement_key(
    h: str = Security(api_key_header),
    q: str = Security(api_key_query),
):
    _verify(h, q, [API_KEY_PROCUREMENT, API_KEY_MASTER], "Procurement API")

def verify_master_key(
    h: str = Security(api_key_header),
    q: str = Security(api_key_query),
):
    _verify(h, q, [API_KEY_MASTER], "Master API")