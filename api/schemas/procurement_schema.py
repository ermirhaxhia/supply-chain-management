# ============================================================
# api/schemas/procurement_schema.py
# Pydantic models për Procurement API
# ============================================================

from pydantic import BaseModel
from typing import Optional

class PurchaseOrderOut(BaseModel):
    po_id:         str
    supplier_id:   str
    product_id:    str
    warehouse_id:  str
    order_date:    str
    expected_date: str
    actual_date:   Optional[str]
    qty_ordered:   int
    qty_received:  int
    unit_cost:     float
    total_cost:    float
    status:        str

class SupplierScoreOut(BaseModel):
    """Përputhet me përgjigjen e GET /api/v1/procurement/suppliers/scorecard."""
    supplier_id:      str
    supplier_name:    str
    city:             str
    total_orders:     int
    delivered_orders: int
    on_time_rate:     float
    total_cost_lek:   float
    reliability_score:float

class CampaignOut(BaseModel):
    campaign_id:      str
    campaign_name:    str
    type:             str
    start_date:       str
    end_date:         str
    category_id:      str
    discount_pct:     float
    cost:             float
    revenue_lift_pct: Optional[float]