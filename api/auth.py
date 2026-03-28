# ============================================================
# api/auth.py
# API Key — Header ose ?api_key= në URL
# ============================================================

import logging
from fastapi import HTTPException, status, Request

logger = logging.getLogger("auth")

def _get_key(request: Request) -> str:
    """Lexo key nga header ose query param."""
    return (
        request.headers.get("X-API-Key") or
        request.query_params.get("api_key") or
        ""
    )

def _verify(key: str, allowed: list, endpoint: str):
    if not key:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"error": "API Key mungon", "hint": "Shto ?api_key=YOUR_KEY"}
        )
    if key not in [k for k in allowed if k]:
        logger.warning(f"🚫 Key i pavlefshëm për {endpoint}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"error": "API Key i pavlefshëm", "endpoint": endpoint}
        )

from config.settings import (
    API_KEY_MASTER, API_KEY_SALES,
    API_KEY_INVENTORY, API_KEY_LOGISTICS, API_KEY_PROCUREMENT,
)

def verify_sales_key(request: Request):
    _verify(_get_key(request), [API_KEY_SALES, API_KEY_MASTER], "Sales API")

def verify_inventory_key(request: Request):
    _verify(_get_key(request), [API_KEY_INVENTORY, API_KEY_MASTER], "Inventory API")

def verify_logistics_key(request: Request):
    _verify(_get_key(request), [API_KEY_LOGISTICS, API_KEY_MASTER], "Logistics API")

def verify_procurement_key(request: Request):
    _verify(_get_key(request), [API_KEY_PROCUREMENT, API_KEY_MASTER], "Procurement API")

def verify_master_key(request: Request):
    _verify(_get_key(request), [API_KEY_MASTER], "Master API")