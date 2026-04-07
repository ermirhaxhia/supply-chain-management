# ============================================================
# simulation/marketing_module.py
# Menaxhon kampanjat marketing dhe ndikimin në kërkesë
# FIX 1: Deaktivizim automatik i kampanjave të mbaruara
# FIX 2: get_campaign_info kthen edhe discount_pct
# FIX 3: Logjikë më realiste probabiliteti
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
    CAMPAIGN_ACTIVE_PROBABILITY,
    CAMPAIGN_DURATION_DAYS,
    PROMO_DEMAND_LIFT_RANGE,
)
from simulation.demand_profile import load_simulation_config, get_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("marketing_module")

# ============================================================
# CACHE — Kampanjat aktive
# ============================================================
_active_campaigns: list = []


def load_active_campaigns(dt: datetime) -> list:
    """
    Ngarko kampanjat aktive nga Supabase.
    Kampanja është aktive nëse start_date <= sot <= end_date.
    """
    global _active_campaigns
    try:
        today = dt.date().isoformat()
        response = (
            supabase.table("campaigns")
            .select("*")
            .lte("start_date", today)
            .gte("end_date", today)
            .execute()
        )
        _active_campaigns = response.data or []
        if _active_campaigns:
            logger.info(f"📣 Kampanja aktive: {len(_active_campaigns)}")
        else:
            logger.info("📣 Asnjë kampanjë aktive sot")
        return _active_campaigns
    except Exception as e:
        logger.error(f"❌ ERROR duke ngarkuar kampanjat: {e}")
        return []


# ============================================================
# FIX #2 — INFO KAMPANJE (Demand Lift + Discount)
# ============================================================
def get_campaign_info(category_id: str) -> dict:
    """
    Kthen të dhënat e kampanjës aktive për 1 kategori:
      - demand_multiplier : rritja e kërkesës (p.sh. 1.20)
      - discount_pct      : zbritja % (p.sh. 15.0)
      - campaign_id       : ID e kampanjës

    Returns:
        dict me demand_multiplier=1.0 dhe discount_pct=0.0 nëse s'ka kampanjë
    """
    result = {
        "demand_multiplier": 1.0,
        "discount_pct":      0.0,
        "campaign_id":       None
    }
    try:
        for campaign in _active_campaigns:
            if campaign.get("category_id") == category_id:
                lift_pct = campaign.get("revenue_lift_pct", 0) / 100
                result["demand_multiplier"] = 1.0 + lift_pct
                result["discount_pct"]      = campaign.get("discount_pct", 0.0)
                result["campaign_id"]       = campaign.get("campaign_id")
                break
    except Exception as e:
        logger.error(f"❌ ERROR në get_campaign_info: {e}")
    return result


# Backward compatible për sales_module që e thirr ende
def get_campaign_demand_lift(category_id: str) -> float:
    return get_campaign_info(category_id)["demand_multiplier"]


# ============================================================
# GJENERO KAMPANJË TË RE
# ============================================================
def generate_campaign(categories: list, dt: datetime) -> dict | None:
    try:
        if not categories:
            return None

        category      = np.random.choice(categories)
        campaign_type = np.random.choice(
            ["Discount", "Bundle", "Loyalty"],
            p=[0.50, 0.30, 0.20]
        )

        if campaign_type == "Discount":
            discount_pct = float(np.random.choice([10, 15, 20, 25, 30]))
        elif campaign_type == "Bundle":
            discount_pct = float(np.random.choice([15, 20, 25]))
        else:
            discount_pct = float(np.random.choice([5, 10]))

        duration = int(np.random.normal(
            CAMPAIGN_DURATION_DAYS["mean"], 2
        ))
        duration = max(
            CAMPAIGN_DURATION_DAYS["min"],
            min(CAMPAIGN_DURATION_DAYS["max"], duration)
        )

        base_cost   = np.random.uniform(50000, 200000)
        demand_lift = np.random.uniform(*PROMO_DEMAND_LIFT_RANGE)

        return {
            "campaign_id":      f"CMP-{uuid.uuid4().hex[:8].upper()}",
            "campaign_name":    f"{campaign_type} {category['category_name']} {dt.strftime('%b%Y')}",
            "type":             campaign_type,
            "start_date":       dt.date().isoformat(),
            "end_date":         (dt.date() + timedelta(days=duration)).isoformat(),
            "category_id":      category["category_id"],
            "discount_pct":     discount_pct,
            "cost":             round(base_cost, 2),
            "revenue_lift_pct": round(demand_lift * 100, 2),
        }

    except Exception as e:
        logger.error(f"❌ ERROR në generate_campaign: {e}")
        return None


# ============================================================
# FIX #1 — DEAKTIVIZO KAMPANJAT E MBARUARA
# ============================================================
def deactivate_expired_campaigns(dt: datetime):
    """
    Kontrollo nëse kampanjat aktive kanë mbaruar.
    Nëse po → pastro simulation_config.
    """
    global _active_campaigns
    today = dt.date().isoformat()

    expired = [c for c in _active_campaigns if c.get("end_date", "") < today]

    if expired:
        logger.info(f"⏰ Kampanja të mbaruara: {len(expired)} — duke deaktivizuar...")

        # Pastro simulation_config
        supabase.table("simulation_config").update(
            {"config_value": 0.0}
        ).eq("config_key", "promo_active").execute()

        supabase.table("simulation_config").update(
            {"config_value": 0.0}
        ).eq("config_key", "promo_discount_pct").execute()

        supabase.table("simulation_config").update(
            {"config_value": 0.0}
        ).eq("config_key", "promo_demand_lift").execute()

        for c in expired:
            logger.info(f"  ❌ Kampanjë e mbaruar: {c['campaign_name']} (mbaroi {c['end_date']})")

        # Rifresho cache
        _active_campaigns = [c for c in _active_campaigns if c.get("end_date", "") >= today]
    else:
        logger.info("✅ Asnjë kampanjë e mbaruar sot")


# ============================================================
# ROI KALKULIM
# ============================================================
def calculate_campaign_roi(
    campaign: dict,
    revenue_before: float,
    revenue_during: float
) -> dict:
    try:
        cost         = campaign.get("cost", 1)
        revenue_lift = revenue_during - revenue_before
        roi_pct      = ((revenue_lift - cost) / cost * 100) if cost > 0 else 0

        stats = {
            "campaign_id":    campaign["campaign_id"],
            "revenue_before": round(revenue_before, 2),
            "revenue_during": round(revenue_during, 2),
            "revenue_lift":   round(revenue_lift, 2),
            "cost":           round(cost, 2),
            "roi_pct":        round(roi_pct, 2),
        }

        logger.info(
            f"📊 ROI {campaign['campaign_id']}: "
            f"Lift={revenue_lift:,.0f}L | "
            f"Kosto={cost:,.0f}L | "
            f"ROI={roi_pct:.1f}%"
        )
        return stats
    except Exception as e:
        logger.error(f"❌ ERROR në calculate_campaign_roi: {e}")
        return {}


# ============================================================
# RUN MARKETING — Ekzekuto 1 herë në ditë ora 07:00
# ============================================================
def run_marketing_day(categories: list, dt: datetime) -> dict:
    """
    Logjika e marketingut për 1 ditë:
    1. Deaktivizo kampanjat e mbaruara  ← FIX #1
    2. Ngarko kampanjat aktive
    3. Probabilitet për kampanjë të re
    """
    if dt.hour != 7:
        return {"campaigns_active": len(_active_campaigns)}

    logger.info(f"📣 Marketing Day | {dt.strftime('%Y-%m-%d')}")

    # FIX #1: Kontrollo kampanjat e mbaruara PARA se të gjenerojmë të reja
    deactivate_expired_campaigns(dt)

    # Rifresho kampanjat aktive
    active = load_active_campaigns(dt)

    new_campaign = None
    inserted     = 0

    try:
        promo_from_config = get_config("promo_active", 0.0)

        if promo_from_config == 1.0:
            logger.info("📣 Kampanjë tashmë aktive nga simulation_config")

        elif np.random.random() < CAMPAIGN_ACTIVE_PROBABILITY:
            new_campaign = generate_campaign(categories, dt)

            if new_campaign:
                response = supabase.table("campaigns").insert(new_campaign).execute()
                if response.data:
                    inserted = 1
                    logger.info(
                        f"✅ Kampanjë e re: {new_campaign['campaign_name']} | "
                        f"Zbritje={new_campaign['discount_pct']}% | "
                        f"{new_campaign['start_date']} → {new_campaign['end_date']}"
                    )

                    # Aktivizo simulation_config
                    supabase.table("simulation_config").update(
                        {"config_value": 1.0}
                    ).eq("config_key", "promo_active").execute()

                    supabase.table("simulation_config").update(
                        {"config_value": new_campaign["discount_pct"]}
                    ).eq("config_key", "promo_discount_pct").execute()

                    supabase.table("simulation_config").update(
                        {"config_value": new_campaign["revenue_lift_pct"] / 100}
                    ).eq("config_key", "promo_demand_lift").execute()

                    # Rifresho cache me kampanjën e re
                    _active_campaigns.append(new_campaign)

    except Exception as e:
        logger.error(f"❌ ERROR në run_marketing_day: {e}")

    stats = {
        "campaigns_active": len(active),
        "new_campaign":     inserted,
        "campaign_name":    new_campaign["campaign_name"] if new_campaign else "—",
    }

    logger.info(f"  ✅ Aktive={len(active)} | Të reja={inserted}")
    return stats


# ============================================================
# MAIN — Test Manual
# ============================================================
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("TEST I MARKETING MODULE")
    logger.info("=" * 60)

    try:
        load_simulation_config()

        cat_resp   = supabase.table("product_categories").select("*").execute()
        categories = cat_resp.data

        if not categories:
            logger.critical("❌ Nuk u gjetën kategori")
            sys.exit(1)

        test_dt = datetime.now().replace(hour=7, minute=0, second=0, microsecond=0)

        load_active_campaigns(test_dt)
        deactivate_expired_campaigns(test_dt)

        new_campaign = generate_campaign(categories, test_dt)
        if new_campaign:
            response = supabase.table("campaigns").insert(new_campaign).execute()
            if response.data:
                logger.info(
                    f"✅ Kampanjë e re: {new_campaign['campaign_name']} | "
                    f"Zbritje={new_campaign['discount_pct']}% | "
                    f"{new_campaign['start_date']} → {new_campaign['end_date']}"
                )
            else:
                logger.error("❌ Kampanja nuk u insertua")

        logger.info("=" * 60)
        logger.info("✅ TEST PËRFUNDOI")

    except Exception as e:
        logger.critical(f"❌ TEST DËSHTOI: {e}")
        sys.exit(1)