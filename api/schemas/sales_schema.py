# ============================================================
# api/schemas/sales_schema.py
# Pydantic models për Sales API
# ============================================================

from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class TransactionOut(BaseModel):
    """Header i faturës — produktet individuale janë te SalesHourlyOut."""
    transaction_id:  str
    store_id:        str
    timestamp:       datetime
    customer_type:   str
    payment_method:  str
    promotion_id:    Optional[str]
    total_items:     int
    revenue:         float
    discount_amount: float
    net_revenue:     float
    cogs:            float
    gross_profit:    float

class SalesHourlyOut(BaseModel):
    id:                 int
    store_id:           str
    product_id:         str
    date:               str
    hour:               int
    units_sold:         int
    revenue:            float
    discount_amount:    float
    net_revenue:        float
    cogs:               float
    gross_profit:       float
    transactions_count: int

class SalesDailyOut(BaseModel):
    id:                 int
    date:               str
    store_id:           str
    product_id:         str
    units_sold:         int
    avg_unit_price:     float
    revenue:            float
    discount_amount:    float
    net_revenue:        float
    cogs:               float
    gross_profit:       float
    transactions_count: int

class SalesMonthlyOut(BaseModel):
    id:                 int
    store_id:           str
    product_id:         str
    year:               int
    month:              int
    units_sold:         int
    avg_unit_price:     float
    revenue:            float
    discount_amount:    float
    net_revenue:        float
    cogs:               float
    gross_profit:       float
    transactions_count: int

class SalesKPIOut(BaseModel):
    """Përputhet me kpi_monthly — përdoret nga GET /api/v1/sales/kpi."""
    store_id:           str
    year:               int
    month:              int
    total_revenue:      float
    total_cogs:         float
    gross_margin:       float
    gross_margin_pct:   float
    total_transactions: int
    avg_basket_value:   float
    stockout_rate_pct:  float
    otd_pct:            float
    avg_lead_time_days: float
    transport_cost:     float
    inventory_cost:     float
    total_cost:         float
    net_profit:         float