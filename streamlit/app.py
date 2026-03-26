# ============================================================
# streamlit/app.py
# Supply Chain Management — Admin Control Panel
# 4 faqe: Monitor | Analytics | Anomaly | Control Panel
# ============================================================

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta, date
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import supabase

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Supply Chain — Admin Panel",
    page_icon="⛓️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# CSS
# ============================================================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Inter:wght@300;400;500;600&display=swap');

    :root {
        --bg:        #0a0e1a;
        --surface:   #111827;
        --border:    #1e2d45;
        --accent:    #00d4ff;
        --green:     #00ff88;
        --red:       #ff4444;
        --yellow:    #ffcc00;
        --text:      #e2e8f0;
        --muted:     #64748b;
    }

    .stApp { background-color: var(--bg); color: var(--text); }
    .stApp > header { background: transparent; }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: var(--surface);
        border-right: 1px solid var(--border);
    }

    /* Metric cards */
    [data-testid="metric-container"] {
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 16px;
    }

    /* Status badge */
    .badge-green { color: var(--green); font-family: 'JetBrains Mono'; font-weight: 700; }
    .badge-red   { color: var(--red);   font-family: 'JetBrains Mono'; font-weight: 700; }
    .badge-yellow{ color: var(--yellow);font-family: 'JetBrains Mono'; font-weight: 700; }

    /* Card */
    .card {
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: 10px;
        padding: 20px;
        margin-bottom: 16px;
    }

    /* Tick row */
    .tick-row {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 8px 0;
        border-bottom: 1px solid var(--border);
        font-family: 'JetBrains Mono';
        font-size: 13px;
    }

    /* Title */
    .page-title {
        font-family: 'JetBrains Mono';
        font-size: 22px;
        font-weight: 700;
        color: var(--accent);
        letter-spacing: -0.5px;
        margin-bottom: 4px;
    }

    .page-sub {
        color: var(--muted);
        font-size: 13px;
        margin-bottom: 24px;
    }

    /* Table status */
    .tbl-row {
        display: flex;
        justify-content: space-between;
        padding: 6px 0;
        border-bottom: 1px solid var(--border);
        font-family: 'JetBrains Mono';
        font-size: 13px;
    }

    /* Anomaly card */
    .anomaly-red {
        background: rgba(255,68,68,0.08);
        border: 1px solid rgba(255,68,68,0.3);
        border-radius: 8px;
        padding: 16px;
        margin-bottom: 12px;
    }
    .anomaly-yellow {
        background: rgba(255,204,0,0.08);
        border: 1px solid rgba(255,204,0,0.3);
        border-radius: 8px;
        padding: 16px;
        margin-bottom: 12px;
    }

    /* Divider */
    hr { border-color: var(--border); }

    /* Buttons */
    .stButton > button {
        background: transparent;
        border: 1px solid var(--accent);
        color: var(--accent);
        font-family: 'JetBrains Mono';
        font-size: 12px;
        border-radius: 6px;
        padding: 4px 12px;
    }
    .stButton > button:hover {
        background: var(--accent);
        color: var(--bg);
    }

    /* Slider */
    .stSlider > div > div { background: var(--accent); }

    /* Select */
    .stSelectbox > div > div {
        background: var(--surface);
        border-color: var(--border);
        color: var(--text);
    }
</style>
""", unsafe_allow_html=True)

# ============================================================
# PLOTLY THEME
# ============================================================
PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(17,24,39,0.8)",
    font=dict(family="JetBrains Mono", color="#e2e8f0", size=11),
    xaxis=dict(gridcolor="#1e2d45", linecolor="#1e2d45", zerolinecolor="#1e2d45"),
    yaxis=dict(gridcolor="#1e2d45", linecolor="#1e2d45", zerolinecolor="#1e2d45"),
    margin=dict(l=40, r=20, t=40, b=40),
)

# ============================================================
# DATA HELPERS — cache 5 min
# ============================================================
@st.cache_data(ttl=300)
def get_table_counts() -> dict:
    tables = [
        "transactions", "inventory_log", "shipments",
        "purchase_orders", "warehouse_snapshot",
        "sales_hourly", "sales_daily", "sales_monthly",
        "inventory_daily", "transport_daily", "kpi_monthly",
    ]
    counts = {}
    for t in tables:
        try:
            r = supabase.table(t).select("*", count="exact").limit(1).execute()
            counts[t] = r.count or 0
        except Exception:
            counts[t] = -1
    return counts

@st.cache_data(ttl=300)
def get_hourly_ticks(hours: int = 24) -> pd.DataFrame:
    since = (datetime.now() - timedelta(hours=hours)).isoformat()
    try:
        r = (supabase.table("transactions")
             .select("store_id, timestamp")
             .gte("timestamp", since)
             .execute())
        if not r.data:
            return pd.DataFrame()
        df = pd.DataFrame(r.data)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["hour"] = df["timestamp"].dt.floor("h")
        return df.groupby("hour").agg(
            txn_count=("store_id", "count"),
            stores=("store_id", "nunique")
        ).reset_index()
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_daily_revenue(days: int = 15) -> pd.DataFrame:
    since = (date.today() - timedelta(days=days)).isoformat()
    try:
        r = (supabase.table("sales_daily")
             .select("date, revenue")
             .gte("date", since)
             .execute())
        if not r.data:
            return pd.DataFrame()
        df = pd.DataFrame(r.data)
        df["date"] = pd.to_datetime(df["date"])
        return df.groupby("date")["revenue"].sum().reset_index()
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_basket_distribution() -> pd.DataFrame:
    try:
        r = (supabase.table("transactions")
             .select("total")
             .limit(2000)
             .execute())
        if not r.data:
            return pd.DataFrame()
        return pd.DataFrame(r.data)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_store_revenue_today() -> pd.DataFrame:
    today = date.today().isoformat()
    try:
        r = (supabase.table("transactions")
             .select("store_id, total")
             .gte("timestamp", today)
             .execute())
        if not r.data:
            return pd.DataFrame()
        df = pd.DataFrame(r.data)
        return df.groupby("store_id")["total"].sum().reset_index()
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_simulation_config() -> dict:
    try:
        r = supabase.table("simulation_config").select("*").execute()
        return {row["config_key"]: row for row in (r.data or [])}
    except Exception:
        return {}

@st.cache_data(ttl=300)
def get_stores() -> list:
    try:
        r = supabase.table("stores").select("store_id, store_name, city").execute()
        return r.data or []
    except Exception:
        return []

# ============================================================
# ANOMALY DETECTION
# ============================================================
def detect_anomaly(values: list, threshold: float = 2.0) -> tuple[bool, float]:
    """
    z-score = (x - μ) / σ
    Anomali = devijim > threshold*σ nga mesatarja historike
    Kërkon min 7 ditë histori.
    """
    if len(values) < 7:
        return False, 0.0
    history = values[:-1]
    mean = np.mean(history)
    std  = np.std(history)
    if std == 0:
        return False, 0.0
    z = abs(values[-1] - mean) / std
    return z > threshold, round(z, 2)

@st.cache_data(ttl=300)
def get_store_anomalies() -> list:
    """Gjen anomali në revenue ditore për çdo store."""
    anomalies = []
    since = (date.today() - timedelta(days=21)).isoformat()
    try:
        r = (supabase.table("sales_daily")
             .select("store_id, date, revenue, stockout_flag")
             .gte("date", since)
             .execute())
        if not r.data:
            return []
        df = pd.DataFrame(r.data)
        df["date"] = pd.to_datetime(df["date"])
        df = df.groupby(["store_id", "date"])["revenue"].sum().reset_index()

        stores_data = df.groupby("store_id")
        for store_id, grp in stores_data:
            grp = grp.sort_values("date")
            vals = grp["revenue"].tolist()
            if len(vals) < 7:
                continue
            is_anom, z = detect_anomaly(vals)
            if is_anom:
                pct_change = ((vals[-1] - np.mean(vals[:-1])) / max(np.mean(vals[:-1]), 1)) * 100
                anomalies.append({
                    "store_id":   store_id,
                    "z_score":    z,
                    "pct_change": round(pct_change, 1),
                    "revenue_today": round(vals[-1], 0),
                    "revenue_avg":   round(np.mean(vals[:-1]), 0),
                    "severity":  "high" if z > 3 else "medium",
                })
    except Exception:
        pass
    return sorted(anomalies, key=lambda x: x["z_score"], reverse=True)

# ============================================================
# SIDEBAR NAVIGATION
# ============================================================
with st.sidebar:
    st.markdown("""
    <div style='font-family:JetBrains Mono; font-size:18px;
                font-weight:700; color:#00d4ff; margin-bottom:4px;'>
        ⛓️ SUPPLY CHAIN
    </div>
    <div style='color:#64748b; font-size:11px; margin-bottom:24px;'>
        Admin Control Panel
    </div>
    """, unsafe_allow_html=True)

    page = st.radio(
        "Navigo",
        ["📊 Simulation Monitor",
         "📈 Performance Analytics",
         "🚨 Anomaly Detector",
         "🎛️  Control Panel"],
        label_visibility="collapsed",
    )

    st.markdown("---")
    st.markdown(f"""
    <div style='font-family:JetBrains Mono; font-size:11px; color:#64748b;'>
        🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}<br>
        Timezone: Europe/Tiranë
    </div>
    """, unsafe_allow_html=True)

    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# ============================================================
# FAQJA 1 — SIMULATION MONITOR
# ============================================================
if page == "📊 Simulation Monitor":

    st.markdown('<div class="page-title">📊 SIMULATION MONITOR</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Health check live — Tabelat, tick-et, statusi i sistemit</div>', unsafe_allow_html=True)

    # ── Status Cards ────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)

    # Test API
    try:
        supabase.table("stores").select("store_id").limit(1).execute()
        db_status = ("🟢 Connected", "green")
    except Exception:
        db_status = ("🔴 Error", "red")

    # Last tick
    ticks_df = get_hourly_ticks(24)
    if not ticks_df.empty:
        last_tick = ticks_df["hour"].max()
        last_tick_str = last_tick.strftime("%H:%M")
        next_tick_str = (last_tick + timedelta(hours=1)).strftime("%H:%M")
    else:
        last_tick_str = "—"
        next_tick_str = "—"

    with col1:
        st.metric("🌐 API Status", "Online")
    with col2:
        st.metric("🗄️  Database", db_status[0])
    with col3:
        st.metric("⏰ Last Tick", last_tick_str)
    with col4:
        st.metric("⏭️  Next Tick", next_tick_str)

    st.markdown("---")

    col_left, col_right = st.columns([1, 1])

    # ── Tick Health ─────────────────────────────────────────
    with col_left:
        st.markdown("**TICK HEALTH — 24h**")
        if not ticks_df.empty:
            ticks_df_sorted = ticks_df.sort_values("hour", ascending=False).head(10)
            for _, row in ticks_df_sorted.iterrows():
                ok = row["txn_count"] > 0
                icon = "✅" if ok else "❌"
                status = f"{int(row['txn_count'])} txn | {int(row['stores'])} stores" if ok else "0 txn | ERROR"
                st.markdown(f"""
                <div class="tick-row">
                    <span>{icon}</span>
                    <span style="color:#00d4ff">{row['hour'].strftime('%H:%M')}</span>
                    <span style="color:#e2e8f0">{status}</span>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown('<div class="badge-yellow">⚠️ Nuk ka tick data për 24h</div>', unsafe_allow_html=True)

    # ── Tabela Status ────────────────────────────────────────
    with col_right:
        st.markdown("**TABELA STATUS**")
        counts = get_table_counts()

        thresholds = {
            "transactions":       (100,  "🔑 Kryesore"),
            "inventory_log":      (50,   "📦 Stoku"),
            "shipments":          (10,   "🚛 Transport"),
            "purchase_orders":    (5,    "🛍️  Blerje"),
            "warehouse_snapshot": (10,   "🏭 Magazina"),
            "sales_hourly":       (10,   "📊 Agreg/h"),
            "sales_daily":        (5,    "📊 Agreg/d"),
            "sales_monthly":      (1,    "📊 Agreg/m"),
            "inventory_daily":    (5,    "📦 Inv/d"),
            "transport_daily":    (3,    "🚛 Trans/d"),
            "kpi_monthly":        (1,    "📈 KPI"),
        }

        for tbl, (threshold, label) in thresholds.items():
            cnt = counts.get(tbl, -1)
            if cnt < 0:
                icon  = "❌"
                color = "#ff4444"
                val   = "ERROR"
            elif cnt == 0:
                icon  = "❌"
                color = "#ff4444"
                val   = "EMPTY"
            elif cnt < threshold:
                icon  = "⚠️"
                color = "#ffcc00"
                val   = f"{cnt:,}"
            else:
                icon  = "✅"
                color = "#00ff88"
                val   = f"{cnt:,}"

            st.markdown(f"""
            <div class="tbl-row">
                <span>{icon} <span style="color:#64748b">{label}</span>
                      <span style="color:#e2e8f0"> {tbl}</span></span>
                <span style="color:{color}; font-weight:700">{val}</span>
            </div>
            """, unsafe_allow_html=True)

    # ── Tick Chart ───────────────────────────────────────────
    st.markdown("---")
    st.markdown("**TRANSAKSIONE / ORË (24h)**")

    if not ticks_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=ticks_df["hour"],
            y=ticks_df["txn_count"],
            marker_color="#00d4ff",
            marker_opacity=0.8,
            name="Transaksione",
        ))
        fig.update_layout(**PLOTLY_LAYOUT, height=220,
                          title=None, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Nuk ka të dhëna për grafikun")

# ============================================================
# FAQJA 2 — PERFORMANCE ANALYTICS
# ============================================================
elif page == "📈 Performance Analytics":

    st.markdown('<div class="page-title">📈 PERFORMANCE ANALYTICS</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Grafike, trends dhe analiza e performancës</div>', unsafe_allow_html=True)

    # ── Row 1: Revenue + Klientë ────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**XHIRO DITORE — 15 ditët e fundit**")
        rev_df = get_daily_revenue(15)
        if not rev_df.empty:
            # Trend line
            x_num = np.arange(len(rev_df))
            z     = np.polyfit(x_num, rev_df["revenue"], 1)
            trend = np.poly1d(z)(x_num)

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=rev_df["date"], y=rev_df["revenue"],
                marker_color="#00d4ff", marker_opacity=0.7,
                name="Revenue",
            ))
            fig.add_trace(go.Scatter(
                x=rev_df["date"], y=trend,
                line=dict(color="#ff4444", width=2, dash="dash"),
                name="Trend",
            ))
            fig.update_layout(**PLOTLY_LAYOUT, height=280,
                              yaxis_title="Lekë",
                              legend=dict(orientation="h", y=1.1))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Nuk ka të dhëna revenue")

    with col2:
        st.markdown("**KLIENTË/ORË vs λ TEORIKE (sot)**")
        ticks_df = get_hourly_ticks(24)
        if not ticks_df.empty:
            ticks_df["hour_int"] = ticks_df["hour"].dt.hour

            # λ teorike nga demand_profile (HOURLY_MULTIPLIER × 40 bazë)
            HOURLY_MULT = {
                6:0.40,7:0.55,8:0.75,9:0.90,10:1.00,11:1.40,
                12:1.60,13:1.50,14:1.10,15:1.00,16:1.20,17:1.70,
                18:1.80,19:1.60,20:1.20,21:0.80,22:0.40
            }
            lambda_base = 40.0
            ticks_df["lambda_teorike"] = ticks_df["hour_int"].map(
                lambda h: HOURLY_MULT.get(h, 1.0) * lambda_base
            )

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=ticks_df["hour"], y=ticks_df["txn_count"],
                mode="lines+markers",
                line=dict(color="#00ff88", width=2),
                marker=dict(size=6),
                name="Reale",
            ))
            fig.add_trace(go.Scatter(
                x=ticks_df["hour"], y=ticks_df["lambda_teorike"],
                mode="lines",
                line=dict(color="#ffcc00", width=2, dash="dot"),
                name="λ Teorike",
            ))
            fig.update_layout(**PLOTLY_LAYOUT, height=280,
                              legend=dict(orientation="h", y=1.1))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Nuk ka të dhëna tick")

    # ── Row 2: Basket + Store Revenue ───────────────────────
    col3, col4 = st.columns(2)

    with col3:
        st.markdown("**BASKET SIZE DISTRIBUTION**")
        basket_df = get_basket_distribution()
        if not basket_df.empty:
            fig = go.Figure()
            fig.add_trace(go.Histogram(
                x=basket_df["total"],
                nbinsx=40,
                marker_color="#00d4ff",
                marker_opacity=0.75,
                name="Fatura",
            ))
            fig.update_layout(**PLOTLY_LAYOUT, height=260,
                              xaxis_title="Vlera faturës (Lekë)",
                              yaxis_title="Frekuenca",
                              showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Nuk ka të dhëna basket")

    with col4:
        st.markdown("**XHIRO PËR STORE — Sot**")
        store_rev = get_store_revenue_today()
        if not store_rev.empty:
            store_rev = store_rev.sort_values("total", ascending=True)
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=store_rev["total"],
                y=store_rev["store_id"],
                orientation="h",
                marker_color="#00d4ff",
                marker_opacity=0.8,
            ))
            fig.update_layout(**PLOTLY_LAYOUT, height=260,
                              xaxis_title="Lekë",
                              showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Nuk ka të dhëna store revenue sot")

    # ── Row 3: KPI Summary ───────────────────────────────────
    st.markdown("---")
    st.markdown("**KPI SUMMARY — Muaji i fundit**")

    try:
        kpi_resp = (supabase.table("kpi_monthly")
                    .select("*")
                    .order("year", desc=True)
                    .order("month", desc=True)
                    .limit(1)
                    .execute())
        if kpi_resp.data:
            kpi = kpi_resp.data[0]
            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("💰 Revenue",      f"{kpi['total_revenue']:,.0f} L")
            k2.metric("📊 Gross Margin", f"{kpi['gross_margin_pct']:.1f}%")
            k3.metric("💵 Net Profit",   f"{kpi['net_profit']:,.0f} L")
            k4.metric("🚚 OTD %",        f"{kpi['otd_pct']:.1f}%")
            k5.metric("📦 Stockout %",   f"{kpi['stockout_rate_pct']:.2f}%")
        else:
            st.info("KPI mujore nuk janë gjeneruara ende")
    except Exception as e:
        st.warning(f"KPI: {e}")

# ============================================================
# FAQJA 3 — ANOMALY DETECTOR
# ============================================================
elif page == "🚨 Anomaly Detector":

    st.markdown('<div class="page-title">🚨 ANOMALY DETECTOR</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Izolim automatik i devijimeve > 2σ nga mesatarja historike</div>', unsafe_allow_html=True)

    # Metodologjia
    with st.expander("📐 Metodologjia — Z-Score"):
        st.markdown("""
        **Formula:**
        ```
        z = (x - μ) / σ
        ```
        - `x` = revenue sot
        - `μ` = mesatarja e 20 ditëve të fundit (pa sot)
        - `σ` = devijimi standard

        **Kufiri:** `z > 2.0` → anomali | `z > 3.0` → kritike

        Anomali konsiderohet **vetëm** nëse nuk ka indikator aktiv
        (festë kombëtare ose kampanjë marketing).

        Kërkon minimum **7 ditë** histori për detektim të besueshëm.
        """)

    # Threshold slider
    threshold = st.slider("Threshold Z-Score", 1.5, 4.0, 2.0, 0.1)

    anomalies = get_store_anomalies()

    # Filter me threshold
    anomalies = [a for a in anomalies if a["z_score"] >= threshold]

    if anomalies:
        st.markdown(f"""
        <div style='color:#ff4444; font-family:JetBrains Mono;
                    font-size:16px; margin-bottom:16px;'>
            🔴 ANOMALI AKTIVE: {len(anomalies)}
        </div>
        """, unsafe_allow_html=True)

        for a in anomalies:
            css_class = "anomaly-red" if a["severity"] == "high" else "anomaly-yellow"
            icon      = "🔴" if a["severity"] == "high" else "🟡"
            direction = "📉" if a["pct_change"] < 0 else "📈"
            cause     = "Stockout i mundshëm?" if a["pct_change"] < 0 else "Kampanjë aktive?"

            st.markdown(f"""
            <div class="{css_class}">
                <div style='font-family:JetBrains Mono; font-weight:700; margin-bottom:8px;'>
                    {icon} {a['store_id']} — Revenue {direction} {a['pct_change']:+.1f}% vs avg
                </div>
                <div style='display:flex; gap:32px; font-size:13px; color:#94a3b8;'>
                    <span>Z-Score: <b style='color:#e2e8f0'>{a['z_score']}</b></span>
                    <span>Sot: <b style='color:#e2e8f0'>{a['revenue_today']:,.0f} L</b></span>
                    <span>Avg: <b style='color:#e2e8f0'>{a['revenue_avg']:,.0f} L</b></span>
                    <span>Shkak i mundshëm: <b style='color:#ffcc00'>{cause}</b></span>
                </div>
            </div>
            """, unsafe_allow_html=True)

        # ── Z-Score Chart ─────────────────────────────────
        st.markdown("---")
        st.markdown("**Z-SCORE PËR STORE**")

        store_ids = [a["store_id"] for a in anomalies]
        z_scores  = [a["z_score"]  for a in anomalies]
        colors    = ["#ff4444" if a["severity"] == "high" else "#ffcc00"
                     for a in anomalies]

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=store_ids, y=z_scores,
            marker_color=colors,
            name="Z-Score",
        ))
        fig.add_hline(y=2.0, line_color="#64748b",
                      line_dash="dash", annotation_text="2σ threshold")
        fig.add_hline(y=3.0, line_color="#ff4444",
                      line_dash="dash", annotation_text="3σ kritike")
        fig.update_layout(**PLOTLY_LAYOUT, height=300,
                          yaxis_title="Z-Score", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    else:
        st.markdown("""
        <div style='text-align:center; padding:60px; color:#64748b;
                    font-family:JetBrains Mono;'>
            ✅ Asnjë anomali e detektuar<br>
            <span style='font-size:12px'>Të gjitha store-et brenda {:.1f}σ</span>
        </div>
        """.format(threshold), unsafe_allow_html=True)

    # ── Store Detail ──────────────────────────────────────
    st.markdown("---")
    st.markdown("**ANALIZO STORE**")
    stores = get_stores()
    if stores:
        store_options = {f"{s['store_id']} — {s['store_name']} ({s['city']})": s["store_id"]
                         for s in stores}
        selected = st.selectbox("Zgjidh store", list(store_options.keys()))
        sel_id   = store_options[selected]

        since = (date.today() - timedelta(days=21)).isoformat()
        try:
            r = (supabase.table("sales_daily")
                 .select("date, revenue")
                 .eq("store_id", sel_id)
                 .gte("date", since)
                 .execute())
            if r.data:
                df = pd.DataFrame(r.data)
                df["date"] = pd.to_datetime(df["date"])
                df = df.groupby("date")["revenue"].sum().reset_index()
                df = df.sort_values("date")

                mean_h = df["revenue"].iloc[:-1].mean() if len(df) > 1 else 0
                std_h  = df["revenue"].iloc[:-1].std()  if len(df) > 1 else 0

                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=df["date"], y=df["revenue"],
                    mode="lines+markers",
                    line=dict(color="#00d4ff", width=2),
                    name="Revenue",
                ))
                # Banda ±2σ
                if std_h > 0:
                    fig.add_hrect(
                        y0=mean_h - 2*std_h, y1=mean_h + 2*std_h,
                        fillcolor="rgba(0,212,255,0.05)",
                        line_width=0,
                        annotation_text="±2σ zona normale",
                    )
                    fig.add_hline(y=mean_h, line_color="#64748b",
                                  line_dash="dash",
                                  annotation_text="μ mesatare")
                fig.update_layout(**PLOTLY_LAYOUT, height=280,
                                  yaxis_title="Lekë", showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info(f"Nuk ka të dhëna sales_daily për {sel_id}")
        except Exception as e:
            st.error(f"Error: {e}")

# ============================================================
# FAQJA 4 — CONTROL PANEL
# ============================================================
elif page == "🎛️  Control Panel":

    st.markdown('<div class="page-title">🎛️  CONTROL PANEL</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Black Swan events, çmimet, parametrat e simulimit</div>', unsafe_allow_html=True)

    config = get_simulation_config()

    def cfg(key: str, default=0.0):
        return config.get(key, {}).get("config_value", default)

    def update_config(key: str, value: float):
        try:
            supabase.table("simulation_config").update(
                {"config_value": value,
                 "updated_at": datetime.now().isoformat()}
            ).eq("config_key", key).execute()
            st.cache_data.clear()
            return True
        except Exception as e:
            st.error(f"❌ Update dështoi: {e}")
            return False

    # ── Simulation Status ─────────────────────────────────
    st.markdown("### ⚙️ Statusi i Simulimit")
    col1, col2 = st.columns(2)

    with col1:
        sim_active = cfg("simulation_active", 1.0)
        new_active = st.toggle("Simulimi Aktiv", value=bool(sim_active))
        if new_active != bool(sim_active):
            update_config("simulation_active", 1.0 if new_active else 0.0)
            st.success("✅ Statusi u përditësua")

    with col2:
        sim_speed = cfg("simulation_speed", 1.0)
        new_speed = st.select_slider(
            "Shpejtësia",
            options=[1.0, 2.0, 5.0],
            value=sim_speed,
            format_func=lambda x: f"{int(x)}x",
        )
        if new_speed != sim_speed:
            update_config("simulation_speed", new_speed)
            st.success("✅ Shpejtësia u përditësua")

    st.markdown("---")

    # ── Black Swan Events ─────────────────────────────────
    st.markdown("### 🌪️ Black Swan Events")
    st.markdown("""
    <div style='color:#64748b; font-size:13px; margin-bottom:16px;'>
        Krizat janë të paparashikueshme — ndrysho manualisht kur ndodhin.
        Festat janë të deklaruara në kod dhe aktivizohen automatikisht.
    </div>
    """, unsafe_allow_html=True)

    event_options = {
        0: "✅ Normal — Pa event",
        1: "⚔️  Luftë / Konflikt",
        2: "🦠 Pandemi",
        3: "✊ Grevë Transporti",
        4: "☀️  Thatësirë / Krizë Ushqimore",
    }

    current_event = int(cfg("active_event", 0))
    new_event     = st.selectbox(
        "Eventi Aktiv",
        options=list(event_options.keys()),
        format_func=lambda x: event_options[x],
        index=current_event,
    )

    if new_event != current_event:
        update_config("active_event", float(new_event))
        if new_event == 0:
            # Reset intensity
            update_config("event_intensity", 0.0)
            update_config("event_duration_days", 0.0)
        st.success(f"✅ Eventi u ndryshua: {event_options[new_event]}")
        st.rerun()

    if new_event != 0:
        col_a, col_b = st.columns(2)
        with col_a:
            intensity = cfg("event_intensity", 0.0)
            new_int   = st.slider("Intensiteti (0-10)", 0.0, 10.0,
                                   float(intensity), 0.5)
            if new_int != intensity:
                update_config("event_intensity", new_int)
                st.success("✅ Intensiteti u përditësua")
        with col_b:
            duration = cfg("event_duration_days", 0.0)
            new_dur  = st.number_input("Kohëzgjatja (ditë, 0=i pacaktuar)",
                                        min_value=0, max_value=365,
                                        value=int(duration))
            if new_dur != int(duration):
                update_config("event_duration_days", float(new_dur))
                st.success("✅ Kohëzgjatja u përditësua")

    st.markdown("---")

    # ── Çmimet ────────────────────────────────────────────
    st.markdown("### 💰 Çmimet & Multipliers")

    cols = st.columns(3)

    params_prices = [
        ("fuel_multiplier",         "⛽ Karburant",     0.5, 3.0, 1.0),
        ("food_price_multiplier",   "🍎 Ushqime",       0.5, 2.0, 1.0),
        ("import_price_multiplier", "📦 Mallra Importi",0.5, 3.0, 1.0),
    ]

    for i, (key, label, min_v, max_v, default) in enumerate(params_prices):
        with cols[i]:
            current = cfg(key, default)
            new_val = st.slider(label, min_v, max_v, float(current), 0.05)
            if abs(new_val - current) > 0.001:
                update_config(key, new_val)
                st.success(f"✅ {label} u përditësua")
            color = "#00ff88" if new_val == 1.0 else (
                    "#ff4444" if new_val > 1.5 else "#ffcc00")
            st.markdown(f"""
            <div style='text-align:center; font-family:JetBrains Mono;
                        color:{color}; font-size:20px; font-weight:700;'>
                {new_val:.2f}x
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    # ── Kërkesa & Stoku ───────────────────────────────────
    st.markdown("### 📊 Kërkesa & Stoku")

    col_b1, col_b2 = st.columns(2)

    with col_b1:
        demand_mult = cfg("demand_multiplier", 1.0)
        new_dm      = st.slider("📈 Demand Multiplier", 0.1, 3.0,
                                 float(demand_mult), 0.1)
        if abs(new_dm - demand_mult) > 0.001:
            update_config("demand_multiplier", new_dm)
            st.success("✅ Demand multiplier u përditësua")

        demand_vol = cfg("demand_volatility", 0.1)
        new_dv     = st.slider("📉 Demand Volatility (σ)", 0.0, 1.0,
                                float(demand_vol), 0.01)
        if abs(new_dv - demand_vol) > 0.001:
            update_config("demand_volatility", new_dv)
            st.success("✅ Volatility u përditësua")

    with col_b2:
        stockout_s = cfg("stockout_sensitivity", 1.0)
        new_ss     = st.slider("📦 Stockout Sensitivity", 0.1, 3.0,
                                float(stockout_s), 0.1)
        if abs(new_ss - stockout_s) > 0.001:
            update_config("stockout_sensitivity", new_ss)
            st.success("✅ Stockout sensitivity u përditësua")

        reorder_m = cfg("reorder_multiplier", 1.0)
        new_rm    = st.slider("🔄 Reorder Multiplier", 0.5, 3.0,
                               float(reorder_m), 0.1)
        if abs(new_rm - reorder_m) > 0.001:
            update_config("reorder_multiplier", new_rm)
            st.success("✅ Reorder multiplier u përditësua")

    st.markdown("---")

    # ── Transporti ────────────────────────────────────────
    st.markdown("### 🚛 Transporti")

    col_t1, col_t2, col_t3 = st.columns(3)

    with col_t1:
        lead_m  = cfg("lead_time_multiplier", 1.0)
        new_lm  = st.slider("⏱️ Lead Time Multiplier", 0.5, 5.0,
                             float(lead_m), 0.1)
        if abs(new_lm - lead_m) > 0.001:
            update_config("lead_time_multiplier", new_lm)
            st.success("✅ U përditësua")

    with col_t2:
        trans_d = cfg("transport_disruption", 0.0)
        new_td  = st.slider("🚧 Transport Disruption", 0.0, 1.0,
                             float(trans_d), 0.05)
        if abs(new_td - trans_d) > 0.001:
            update_config("transport_disruption", new_td)
            st.success("✅ U përditësua")

    with col_t3:
        delay_m = cfg("transport_delay_min", 0.0)
        new_dm2 = st.number_input("⏰ Vonesë Shtesë (min)",
                                   min_value=0, max_value=300,
                                   value=int(delay_m))
        if new_dm2 != int(delay_m):
            update_config("transport_delay_min", float(new_dm2))
            st.success("✅ U përditësua")

    st.markdown("---")

    # ── Promocioni ────────────────────────────────────────
    st.markdown("### 🎯 Promocioni Manual")

    col_p1, col_p2, col_p3 = st.columns(3)

    with col_p1:
        promo_a = cfg("promo_active", 0.0)
        new_pa  = st.toggle("Promo Aktive", value=bool(promo_a))
        if new_pa != bool(promo_a):
            update_config("promo_active", 1.0 if new_pa else 0.0)
            st.success("✅ U përditësua")

    with col_p2:
        promo_d = cfg("promo_discount_pct", 0.0)
        new_pd  = st.slider("Zbritja %", 0.0, 50.0,
                             float(promo_d), 1.0)
        if abs(new_pd - promo_d) > 0.001:
            update_config("promo_discount_pct", new_pd)

    with col_p3:
        promo_l = cfg("promo_demand_lift", 0.0)
        new_pl  = st.slider("Demand Lift", 0.0, 1.0,
                             float(promo_l), 0.05)
        if abs(new_pl - promo_l) > 0.001:
            update_config("promo_demand_lift", new_pl)

    # ── Config Table ──────────────────────────────────────
    st.markdown("---")
    with st.expander("📋 Tabela e plotë — simulation_config"):
        if config:
            rows = []
            for k, v in sorted(config.items()):
                rows.append({
                    "config_key":   k,
                    "config_value": v["config_value"],
                    "description":  v.get("description", ""),
                    "updated_at":   v.get("updated_at", ""),
                })
            st.dataframe(
                pd.DataFrame(rows),
                use_container_width=True,
                hide_index=True,
            )