# ============================================================
# simulation/transport_module.py
# Menaxhon dërgesat nga magazinat → supermarketet
# Kosto karburantit, vonesa, kapacitet kamioni
# ============================================================

import sys
import os
import logging
import numpy as np
import uuid
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import supabase
from config.constants import (
    TRANSPORT_DELAY_PROBABILITY,
    TRANSPORT_DELAY_MINUTES,
    DELIVERIES_PER_STORE_PER_DAY,
    FUEL_CONSUMPTION_STD,
    SHIPMENT_ID_PREFIX,
)
from simulation.demand_profile import load_simulation_config, get_config

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("transport_module")

# ============================================================
# GJENERO 1 DËRGESË
# ============================================================
def generate_shipment(
    route:    dict,
    vehicle:  dict,
    driver:   dict,
    dt:       datetime
) -> dict | None:
    """
    Gjeneron 1 dërgesë nga magazina → store.

    Formula kosto:
      transport_cost = distance_km × fuel_price × consumption × fuel_mult
                     + driver_cost_fixed

    Vonesa ~ Poisson(λ_delay) nëse random > threshold

    Returns:
        dict : shipment i plotë
    """
    try:
        route_id     = route["route_id"]
        distance_km  = route.get("distance_km",  20.0)
        duration_min = route.get("duration_min",  30)

        # ── Kapaciteti dhe ngarkesa ───────────────────────
        capacity_kg  = vehicle.get("capacity_kg",  5000)
        capacity_m3  = vehicle.get("capacity_m3",  30)

        # Ngarkesa 60-95% e kapacitetit
        load_pct     = np.random.uniform(0.60, 0.95)
        load_kg      = round(capacity_kg * load_pct, 1)
        units        = int(load_kg / 0.8)  # ~0.8 kg/njësi mesatarisht

        # ── Karburanti ────────────────────────────────────
        fuel_mult    = get_config("fuel_multiplier", 1.0)
        base_price   = float(os.getenv("FUEL_BASE_PRICE", 185))
        fuel_price   = base_price * fuel_mult

        # Konsumi me variancë ±5%
        base_consumption = vehicle.get("consumption_l_km", 0.28)
        consumption      = base_consumption * np.random.normal(1.0, FUEL_CONSUMPTION_STD)
        consumption      = max(0.20, consumption)
        fuel_consumed    = round(distance_km * consumption, 2)

        # ── Kosto transporti ──────────────────────────────
        fuel_cost        = fuel_consumed * fuel_price
        driver_cost      = np.random.uniform(500, 1500)  # Lekë/dërgesë
        transport_cost   = round(fuel_cost + driver_cost, 2)
        cost_per_km      = round(transport_cost / distance_km, 2)

        # ── Vonesa ────────────────────────────────────────
        transport_disrupt = get_config("transport_disruption", 0.0)
        delay_prob        = TRANSPORT_DELAY_PROBABILITY + (transport_disrupt * 0.5)

        delay_minutes = 0
        if np.random.random() < delay_prob:
            delay_minutes = int(np.random.normal(
                TRANSPORT_DELAY_MINUTES["mean"],
                10
            ))
            delay_minutes = max(
                TRANSPORT_DELAY_MINUTES["min"],
                min(TRANSPORT_DELAY_MINUTES["max"], delay_minutes)
            )

        # Shto vonesa shtesë nga Black Swan
        extra_delay   = int(get_config("transport_delay_min", 0.0))
        delay_minutes += extra_delay

        # ── Kohët ─────────────────────────────────────────
        departure_time  = dt
        travel_time     = duration_min + delay_minutes
        actual_arrival  = departure_time + timedelta(minutes=travel_time)
        on_time         = delay_minutes == 0

        status = "Delivered" if actual_arrival <= dt + timedelta(hours=4) else "Delayed"

        shipment = {
            "shipment_id":    f"{SHIPMENT_ID_PREFIX}-{uuid.uuid4().hex[:10].upper()}",
            "route_id":       route_id,
            "vehicle_id":     vehicle["vehicle_id"],
            "driver_id":      driver["driver_id"],
            "departure_time": departure_time.isoformat(),
            "actual_arrival": actual_arrival.isoformat(),
            "delay_minutes":  delay_minutes,
            "units_delivered":units,
            "load_kg":        load_kg,
            "fuel_consumed":  fuel_consumed,
            "fuel_price":     round(fuel_price, 2),
            "transport_cost": transport_cost,
            "status":         status,
        }

        logger.debug(
            f"🚛 Route {route_id} | "
            f"Distance={distance_km}km | "
            f"Cost={transport_cost:,.0f}L | "
            f"Delay={delay_minutes}min | "
            f"Status={status}"
        )

        return shipment

    except Exception as e:
        logger.error(f"❌ ERROR në generate_shipment: {e}")
        return None

# ============================================================
# RUN TRANSPORT — Ekzekuto dërgesat ditore
# ============================================================
def run_transport_day(
    routes:   list,
    vehicles: list,
    drivers:  list,
    dt:       datetime
) -> dict:
    """
    Ekzekuton dërgesat për të gjitha rrugët.
    Ekzekutohet 2 herë në ditë (ora 06:00 dhe 14:00).

    Returns:
        dict : statistikat e transportit
    """
    if dt.hour not in [6, 14]:
        return {"shipments": 0, "total_cost": 0}

    logger.info(f"🚛 Transport Run | {dt.strftime('%Y-%m-%d %H:%M')}")

    shipments      = []
    total_cost     = 0.0
    total_delay    = 0
    on_time_count  = 0

    try:
        # Çdo rrugë → 1 dërgesë
        for route in routes:
            # Zgjidh vehicle dhe driver random
            if not vehicles or not drivers:
                logger.error("❌ Nuk ka vehicles ose drivers")
                continue

            vehicle = np.random.choice(vehicles)
            driver  = np.random.choice(drivers)

            shipment = generate_shipment(route, vehicle, driver, dt)
            if shipment:
                shipments.append(shipment)
                total_cost  += shipment["transport_cost"]
                total_delay += shipment["delay_minutes"]
                if shipment["delay_minutes"] == 0:
                    on_time_count += 1

        # ── Batch INSERT ──────────────────────────────────
        inserted = 0
        if shipments:
            batch_size = 500
            for i in range(0, len(shipments), batch_size):
                batch    = shipments[i:i + batch_size]
                response = supabase.table("shipments").insert(batch).execute()
                if response.data:
                    inserted += len(response.data)

        # ── OTD % ─────────────────────────────────────────
        otd_pct = (on_time_count / len(shipments) * 100) if shipments else 0
        avg_delay = (total_delay / len(shipments)) if shipments else 0

    except Exception as e:
        logger.error(f"❌ ERROR në run_transport_day: {e}")
        inserted = 0

    stats = {
        "shipments":    inserted,
        "total_cost":   round(total_cost, 2),
        "avg_delay":    round(avg_delay, 1),
        "otd_pct":      round(otd_pct, 1),
        "on_time":      on_time_count,
    }

    logger.info(
        f"  ✅ Dërgesa={inserted} | "
        f"Kosto={total_cost:,.0f}L | "
        f"OTD={otd_pct:.1f}% | "
        f"Avg Vonesë={avg_delay:.1f} min"
    )

    return stats

# ============================================================
# MAIN — Test
# ============================================================
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("TEST I TRANSPORT MODULE")
    logger.info("=" * 60)

    try:
        load_simulation_config()

        routes_resp   = supabase.table("routes").select("*").execute()
        vehicles_resp = supabase.table("vehicles").select("*").execute()
        drivers_resp  = supabase.table("drivers").select("*").execute()

        routes   = routes_resp.data
        vehicles = vehicles_resp.data
        drivers  = drivers_resp.data

        if not routes or not vehicles or not drivers:
            logger.critical("❌ Të dhënat bazë mungojnë")
            sys.exit(1)

        logger.info(f"✅ Routes={len(routes)} | Vehicles={len(vehicles)} | Drivers={len(drivers)}")

        # Testo ora 06:00
        test_dt = datetime.now().replace(
            hour=6, minute=0, second=0, microsecond=0
        )

        stats = run_transport_day(routes, vehicles, drivers, test_dt)

        logger.info("=" * 60)
        logger.info("REZULTATI:")
        for k, v in stats.items():
            logger.info(f"  {k}: {v}")
        logger.info("=" * 60)
        logger.info("✅ TEST PËRFUNDOI ME SUKSES")

    except Exception as e:
        logger.critical(f"❌ TEST DËSHTOI: {e}")
        sys.exit(1)