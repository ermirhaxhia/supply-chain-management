# ============================================================
# simulation/marketing_module.py
# Menaxhon kampanjat marketing dhe ndikimin në kërkesë
# Kampanja → zbritje → rritje shitjesh → ROI kalkulim
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

# ============================================================
# LOGGING
# ============================================================
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
            .gte("end_date",   today)
            .execute()
        )
        _active_campaigns = response.data or []
        if _active_campaigns:
            logger.info(f"📣 Kampanja aktive: {len(_active_campaigns)}")
        return _active_campaigns
    except Exception as e:
        logger.error(f"❌ ERROR duke ngarkuar kampanjat: {e}")
        return []

# ============================================================
# GJENERO KAMPANJË TË RE
# ============================================================
def generate_campaign(
    categories: list,
    dt:         datetime
) -> dict | None:
    """
    Gjeneron 1 kampanjë të re marketing.

    Tipet e kampanjave:
      Discount → zbritje direkte
      Bundle   → bli 2 merr 1
      Loyalty  → pikë besnikërie

    Returns:
        dict : kampanja e re ose None
    """
    try:
        if not categories:
            logger.warning("⚠️  Nuk ka kategori për kampanjë")
            return None

        # Zgjidh kategorinë target
        category = np.random.choice(categories)

        # Tipi i kampanjës
        campaign_type = np.random.choice(
            ["Discount", "Bundle", "Loyalty"],
            p=[0.50, 0.30, 0.20]
        )

        # Zbritja
        if campaign_type == "Discount":
            discount_pct = float(np.random.choice([10, 15, 20, 25, 30]))
        elif campaign_type == "Bundle":
            discount_pct = float(np.random.choice([15, 20, 25]))
        else:
            discount_pct = float(np.random.choice([5, 10]))

        # Kohëzgjatja
        duration = int(np.random.normal(
            CAMPAIGN_DURATION_DAYS["mean"],
            2
        ))
        duration = max(
            CAMPAIGN_DURATION_DAYS["min"],
            min(CAMPAIGN_DURATION_DAYS["max"], duration)
        )

        # Kosto e kampanjës
        base_cost = np.random.uniform(50000, 200000)  # Lekë

        # Revenue lift i pritshëm
        demand_lift = np.random.uniform(*PROMO_DEMAND_LIFT_RANGE)

        campaign = {
            "campaign_id":    f"CMP-{uuid.uuid4().hex[:8].upper()}",
            "campaign_name":  f"{campaign_type} {category['category_name']} {dt.strftime('%b%Y')}",
            "type":           campaign_type,
            "start_date":     dt.date().isoformat(),
            "end_date":       (dt.date() + timedelta(days=duration)).isoformat(),
            "category_id":    category["category_id"],
            "discount_pct":   discount_pct,
            "cost":           round(base_cost, 2),
            "revenue_lift_pct": round(demand_lift * 100, 2),
        }

        return campaign

    except Exception as e:
        logger.error(f"❌ ERROR në generate_campaign: {e}")
        return None

# ============================================================
# NDIKIM I KAMPANJËS NË KËRKESË
# ============================================================
def get_campaign_demand_lift(category_id: str) -> float:
    """
    Kthen multiplikatorin e kërkesës për 1 kategori
    bazuar në kampanjat aktive.

    Returns:
        float : multiplier (1.0 = pa ndikim)
    """
    try:
        for campaign in _active_campaigns:
            if campaign.get("category_id") == category_id:
                lift_pct = campaign.get("revenue_lift_pct", 0) / 100
                return 1.0 + lift_pct
        return 1.0
    except Exception as e:
        logger.error(f"❌ ERROR në get_campaign_demand_lift: {e}")
        return 1.0

# ============================================================
# KALKULIM ROI
# ============================================================
def calculate_campaign_roi(
    campaign:        dict,
    revenue_before:  float,
    revenue_during:  float
) -> dict:
    """
    Llogarit ROI të kampanjës.

    ROI = (Revenue_lift - Cost) / Cost × 100

    Returns:
        dict : statistikat e ROI
    """
    try:
        cost           = campaign.get("cost", 1)
        revenue_lift   = revenue_during - revenue_before
        roi_pct        = ((revenue_lift - cost) / cost * 100) if cost > 0 else 0

        stats = {
            "campaign_id":    campaign["campaign_id"],
            "revenue_before": round(revenue_before, 2),
            "revenue_during": round(revenue_during, 2),
            "revenue_lift":   round(revenue_lift, 2),
            "cost":           round(cost, 2),
            "roi_pct":        round(roi_pct, 2),
        }

        logger.info(
            f"📊 ROI Kampanjë {campaign['campaign_id']}: "
            f"Lift={revenue_lift:,.0f}L | "
            f"Kosto={cost:,.0f}L | "
            f"ROI={roi_pct:.1f}%"
        )

        return stats

    except Exception as e:
        logger.error(f"❌ ERROR në calculate_campaign_roi: {e}")
        return {}

# ============================================================
# RUN MARKETING — Ekzekuto 1 herë në ditë
# ============================================================
def run_marketing_day(
    categories: list,
    dt:         datetime
) -> dict:
    """
    Ekzekuton logjikën e marketingut për 1 ditë.
    Ekzekutohet ora 07:00.

    1. Ngarko kampanjat aktive
    2. Probabilitet për kampanjë të re
    3. Aktivizo/deaktivizo në simulation_config

    Returns:
        dict : statistikat e marketingut
    """
    if dt.hour != 7:
        return {"campaigns_active": len(_active_campaigns)}

    logger.info(f"📣 Marketing Day | {dt.strftime('%Y-%m-%d')}")

    # Ngarko kampanjat aktive
    active = load_active_campaigns(dt)

    new_campaign = None
    inserted     = 0

    try:
        # ── Probabilitet për kampanjë të re ──────────────
        promo_from_config = get_config("promo_active", 0.0)

        if promo_from_config == 1.0:
            logger.info("📣 Kampanjë aktive nga simulation_config")

        elif np.random.random() < CAMPAIGN_ACTIVE_PROBABILITY:
            # Gjenero kampanjë të re automatikisht
            new_campaign = generate_campaign(categories, dt)

            if new_campaign:
                response = supabase.table("campaigns").insert(new_campaign).execute()
                if response.data:
                    inserted = 1
                    logger.info(
                        f"✅ Kampanjë e re: {new_campaign['campaign_name']} | "
                        f"Zbritje={new_campaign['discount_pct']}% | "
                        f"Kohëzgjatja={new_campaign['start_date']} → {new_campaign['end_date']}"
                    )

                    # Aktivizo në simulation_config
                    supabase.table("simulation_config").update({
                        "config_value": 1.0
                    }).eq("config_key", "promo_active").execute()

                    supabase.table("simulation_config").update({
                        "config_value": new_campaign["discount_pct"]
                    }).eq("config_key", "promo_discount_pct").execute()

                    supabase.table("simulation_config").update({
                        "config_value": new_campaign["revenue_lift_pct"] / 100
                    }).eq("config_key", "promo_demand_lift").execute()

    except Exception as e:
        logger.error(f"❌ ERROR në run_marketing_day: {e}")

    stats = {
        "campaigns_active":  len(active),
        "new_campaign":      inserted,
        "campaign_name":     new_campaign["campaign_name"] if new_campaign else "—",
    }

    logger.info(
        f"  ✅ Aktive={len(active)} | "
        f"Të reja={inserted}"
    )

    return stats

# ============================================================
# MAIN — Test
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

        logger.info(f"✅ Kategori={len(categories)}")

        # Testo direkt generate_campaign + insert
        test_dt      = datetime.now().replace(
            hour=7, minute=0, second=0, microsecond=0
        )

        # Ngarko kampanjat aktive
        load_active_campaigns(test_dt)

        # Gjenero 1 kampanjë direkt
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
        else:
            logger.error("❌ Kampanja nuk u gjenerua")

        logger.info("=" * 60)
        logger.info("✅ TEST PËRFUNDOI ME SUKSES")

    except Exception as e:
        logger.critical(f"❌ TEST DËSHTOI: {e}")
        sys.exit(1)