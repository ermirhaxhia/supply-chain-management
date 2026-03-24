# ============================================================
# api/schemas/logistics_schema.py
# Pydantic models për Logistics API
# ============================================================

from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class ShipmentOut(BaseModel):
    shipment_id:    str
    route_id:       str
    vehicle_id:     str
    driver_id:      str
    departure_time: datetime
    actual_arrival: Optional[datetime]
    delay_minutes:  int
    units_delivered:int
    load_kg:        float
    fuel_consumed:  float
    fuel_price:     float
    transport_cost: float
    status:         str

class WarehouseSnapshotOut(BaseModel):
    snapshot_id:      str
    warehouse_id:     str
    timestamp:        datetime
    used_capacity_m3: float
    inbound_units:    int
    outbound_units:   int
    labor_hours:      float
    orders_processed: int

class TransportDailyOut(BaseModel):
    id:                 int
    route_id:           str
    date:               str
    total_shipments:    int
    total_units:        int
    total_cost:         float
    avg_delay_minutes:  float
    on_time_deliveries: int
    fuel_consumed:      float
    avg_load_pct:       float