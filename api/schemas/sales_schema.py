# ============================================================
# api/schemas/sales_schema.py
# Pydantic models për Sales API
# ============================================================

from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class TransactionOut(BaseModel):
    transaction_id: str
    store_id:       str
    product_id:     str
    timestamp:      datetime
    quantity:       int
    unit_price:     float
    discount_pct:   float
    total:          float
    payment_method: str
    promotion_id:   Optional[str]
    customer_type:  str

class SalesHourlyOut(BaseModel):
    id:             int
    store_id:       str
    product_id:     str
    date:           str
    hour:           int
    transactions:   int
    units_sold:     int
    revenue:        float
    avg_basket:     float
    discount_total: float

class SalesDailyOut(BaseModel):
    id:             int
    store_id:       str
    product_id:     str
    date:           str
    transactions:   int
    units_sold:     int
    revenue:        float
    avg_basket:     float
    discount_total: float
    stockout_flag:  bool

class SalesKPIOut(BaseModel):
    store_id:          str
    total_revenue:     float
    total_transactions:int
    avg_basket:        float
    top_product:       Optional[str]
    period:            str