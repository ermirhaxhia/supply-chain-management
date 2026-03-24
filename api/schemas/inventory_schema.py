# ============================================================
# api/schemas/inventory_schema.py
# Pydantic models për Inventory API
# ============================================================

from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class InventoryLogOut(BaseModel):
    log_id:         str
    store_id:       str
    product_id:     str
    timestamp:      datetime
    stock_before:   int
    stock_after:    int
    change_reason:  str

class InventoryDailyOut(BaseModel):
    id:              int
    store_id:        str
    product_id:      str
    date:            str
    avg_stock_level: float
    min_stock_level: int
    max_stock_level: int
    stockout_hours:  int
    expired_units:   int
    restock_count:   int

class StockAlertOut(BaseModel):
    store_id:       str
    product_id:     str
    product_name:   Optional[str]
    stock_level:    int
    reorder_point:  int
    status:         str