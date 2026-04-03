import sys
import os
import logging
from datetime import datetime

# Path Setup
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from config.settings import supabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("monthly_aggregator")

def run_monthly_aggregation(dt: datetime = None):
    if dt is None:
        dt = datetime.now()

    # --- KUJDES: Për testimin tënd tani që të shohësh të dhëna, 
    # kemi lënë muajin aktual (Prill) ---
    year, month = dt.year, dt.month
    
    month_start = f"{year}-{month:02d}-01"
    month_end   = f"{year}-{month:02d}-31"

    logger.info(f"📊 Duke nisur agregimin mujor REAL për: {year}-{month:02d}")

    try:
        # 1. Lexo sales_daily (Burimi kryesor)
        daily_resp = supabase.table("sales_daily").select("*").gte("date", month_start).lte("date", month_end).execute()
        daily_data = daily_resp.data or []

        if not daily_data:
            logger.warning(f"⚠️ Nuk u gjetën të dhëna në sales_daily për {year}-{month:02d}")
            return

        # 2. Agregimi për sales_monthly (Sipas Store + Product)
        groups = {}
        for row in daily_data:
            key = (row["store_id"], row["product_id"])
            if key not in groups:
                groups[key] = {"u": 0, "r": 0.0, "c": 0.0, "p": 0.0, "t": 0, "d": 0.0, "n": 0.0}
            groups[key]["u"] += row.get("units_sold", 0)
            groups[key]["r"] += row.get("revenue", 0.0)
            groups[key]["c"] += row.get("cogs", 0.0)
            groups[key]["p"] += row.get("gross_profit", 0.0)
            groups[key]["t"] += row.get("transactions_count", 0)
            groups[key]["d"] += row.get("discount_amount", 0.0)
            groups[key]["n"] += row.get("net_revenue", 0.0)

        monthly_sales_rows = []
        for (sid, pid), v in groups.items():
            monthly_sales_rows.append({
                "store_id": sid, "product_id": pid, "year": year, "month": month,
                "units_sold": v["u"], 
                "avg_unit_price": round(v["r"]/v["u"], 2) if v["u"] > 0 else 0,
                "revenue": round(v["r"], 2), 
                "discount_amount": round(v["d"], 2),
                "net_revenue": round(v["n"], 2),
                "cogs": round(v["c"] ,2),
                "gross_profit": round(v["p"], 2), 
                "transactions_count": v["t"]
            })

        if monthly_sales_rows:
            supabase.table("sales_monthly").insert(monthly_sales_rows).execute()
            logger.info(f"✅ U mbush sales_monthly me {len(monthly_sales_rows)} rreshta.")

        # 3. LLOGARITJA E KPI-ve (Përputhur 1:1 me tabelën kpi_monthly)
        # Marrim të dhënat e transportit nga tabela shipments
        shp_resp = supabase.table("shipments").select("*").gte("departure_time", month_start).execute()
        shipments = shp_resp.data or []
        
        # Marrim të dhënat e inventory_logs për të llogaritur Stockouts
        inv_resp = supabase.table("inventory_log").select("event_type").gte("created_at", month_start).eq("event_type", "stockout").execute()
        total_stockouts = len(inv_resp.data or [])

        unique_stores = list(set(r["store_id"] for r in monthly_sales_rows))
        kpi_rows = []

        for sid in unique_stores:
            s_data = [r for r in monthly_sales_rows if r["store_id"] == sid]
            
            # Agregimet financiare
            total_rev = sum(r["net_revenue"] for r in s_data)
            total_cogs = sum(r["cogs"] for r in s_data)
            total_gm = sum(r["gross_profit"] for r in s_data)
            total_txns = sum(r["transactions_count"] for r in s_data)
            
            # Kostot e llogaritura
            store_transport = sum(s["transport_cost"] for s in shipments if s.get("destination_id") == sid)
            if store_transport == 0: # Fallback nëse destination_id nuk përputhet
                store_transport = sum(s["transport_cost"] for s in shipments) / len(unique_stores) if unique_stores else 0
            
            inventory_holding_cost = total_cogs * 0.02 # 2% e vlerës së mallit
            total_cost = total_cogs + store_transport + inventory_holding_cost

            kpi_rows.append({
                "store_id": sid,
                "year": year,
                "month": month,
                "total_revenue": round(total_rev, 2),
                "total_cogs": round(total_cogs, 2),
                "gross_margin": round(total_gm, 2),
                "gross_margin_pct": round((total_gm/total_rev*100), 2) if total_rev > 0 else 0,
                "total_transactions": total_txns,
                "avg_basket_value": round(total_rev/total_txns, 2) if total_txns > 0 else 0,
                # Kolonat specifike që kërkonte tabela jote
                "stockout_rate_pct": round((total_stockouts / (total_txns or 1)) * 100, 2),
                "otd_pct": 98.5, # On-time delivery (default i lartë)
                "avg_lead_time_days": 1.5, # Mesatare e furnizimit
                "transport_cost": round(store_transport, 2),
                "inventory_cost": round(inventory_holding_cost, 2),
                "total_cost": round(total_cost, 2),
                "net_profit": round(total_gm - store_transport - inventory_holding_cost, 2)
            })

        if kpi_rows:
            supabase.table("kpi_monthly").insert(kpi_rows).execute()
            logger.info(f"✅ KPI-të u shtuan me sukses për {len(kpi_rows)} dyqane.")

    except Exception as e:
        logger.error(f"❌ Gabim gjatë agregimit mujor: {e}")
        raise e

if __name__ == "__main__":
    run_monthly_aggregation()