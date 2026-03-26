# ============================================================
# streamlit/app.py
# Supply Chain Observability Dashboard — Premium UI
# Bloomberg Terminal × Palantir Foundry aesthetic
# MOBILE RESPONSIVE VERSION
# ============================================================

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import supabase

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Supply Chain Intelligence",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ============================================================
# DESIGN SYSTEM — Bloomberg × Palantir
# ============================================================
COLORS = {
    "bg":           "#0A0C10",
    "surface":      "#0F1217",
    "surface2":     "#151820",
    "border":       "#1E2430",
    "border2":      "#252D3A",
    "gold":         "#C9A84C",
    "gold2":        "#E8C97A",
    "green":        "#00C896",
    "green2":       "#00A87A",
    "red":          "#FF4757",
    "red2":         "#CC3344",
    "blue":         "#4A9EFF",
    "blue2":        "#2070CC",
    "text":         "#E8EAF0",
    "text2":        "#8892A4",
    "text3":        "#4A5568",
    "amber":        "#FFB020",
    "purple":       "#7C5CFC",
}

FONT_MONO  = "'JetBrains Mono', 'Fira Code', monospace"
FONT_SANS  = "'DM Sans', 'Sora', sans-serif"
FONT_DISPLAY = "'Sora', 'DM Sans', sans-serif"

# ============================================================
# RESPONSIVE CSS WITH MEDIA QUERIES
# ============================================================
def inject_css():
    st.markdown(f"""
    <link href="https://fonts.googleapis.com/css2?family=Sora:wght@300;400;500;600;700&family=DM+Sans:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">

    <style>
    /* ── Reset & Base ─────────────────────────────────── */
    .stApp {{
        background: {COLORS['bg']};
        font-family: {FONT_SANS};
    }}
    .main .block-container {{
        padding: 1rem 1rem 1.5rem;
        max-width: 100%;
    }}
    @media (min-width: 768px) {{
        .main .block-container {{
            padding: 1.5rem 2rem 2rem;
        }}
    }}
    
    /* ── Hide Streamlit Branding ──────────────────────── */
    #MainMenu, footer, header {{ visibility: hidden; }}
    .stDeployButton {{ display: none; }}

    /* ── Hide sidebar completely ──────────────────────── */
    [data-testid="stSidebar"],
    [data-testid="collapsedControl"],
    section[data-testid="stSidebar"],
    div[data-testid="stSidebarCollapsedControl"] {{
        display: none !important;
        width: 0 !important;
        visibility: hidden !important;
    }}
    .main .block-container {{
        padding: 0 1rem 1.5rem !important;
        max-width: 100% !important;
    }}
    @media (min-width: 768px) {{
        .main .block-container {{
            padding: 0 2rem 2rem !important;
        }}
    }}

    /* ── TOP NAVBAR ───────────────────────────────────── */
    .scm-navbar {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 0 0 0 0;
        height: 54px;
        background: {COLORS['surface']};
        border-bottom: 1px solid {COLORS['border']};
        margin: 0 -1rem 1.5rem;
        padding: 0 1rem;
        position: sticky;
        top: 0;
        z-index: 999;
        gap: 0.5rem;
    }}
    @media (min-width: 768px) {{
        .scm-navbar {{
            margin: 0 -2rem 2rem;
            padding: 0 2rem;
            height: 56px;
        }}
    }}

    /* Logo group */
    .scm-logo {{
        display: flex;
        align-items: center;
        gap: 8px;
        flex-shrink: 0;
    }}
    .scm-logo-mark {{
        width: 26px;
        height: 26px;
        background: linear-gradient(135deg, {COLORS['gold']}, {COLORS['gold2']});
        clip-path: polygon(50% 0%, 100% 25%, 100% 75%, 50% 100%, 0% 75%, 0% 25%);
        flex-shrink: 0;
    }}
    @media (min-width: 768px) {{
        .scm-logo-mark {{ width: 30px; height: 30px; }}
    }}
    .scm-logo-text {{
        font-family: {FONT_DISPLAY};
        font-size: 0.72rem;
        font-weight: 600;
        color: {COLORS['text']};
        letter-spacing: 0.06em;
        text-transform: uppercase;
        white-space: nowrap;
    }}
    @media (min-width: 768px) {{
        .scm-logo-text {{ font-size: 0.8rem; }}
    }}

    /* Nav tabs */
    .scm-tabs {{
        display: flex;
        align-items: center;
        gap: 2px;
        overflow-x: auto;
        scrollbar-width: none;
        -webkit-overflow-scrolling: touch;
        flex: 1;
        justify-content: flex-end;
    }}
    .scm-tabs::-webkit-scrollbar {{ display: none; }}

    .scm-tab {{
        display: flex;
        align-items: center;
        gap: 6px;
        padding: 6px 10px;
        border-radius: 6px;
        font-size: 0.68rem;
        font-weight: 500;
        color: {COLORS['text3']};
        white-space: nowrap;
        cursor: pointer;
        text-decoration: none !important;
        transition: all 0.15s;
        font-family: {FONT_SANS};
        letter-spacing: 0.02em;
        border: 1px solid transparent;
        flex-shrink: 0;
    }}
    @media (min-width: 768px) {{
        .scm-tab {{
            padding: 7px 14px;
            font-size: 0.75rem;
            gap: 7px;
        }}
    }}
    .scm-tab:hover {{
        color: {COLORS['text2']};
        background: {COLORS['surface2']};
    }}
    .scm-tab.active {{
        color: {COLORS['gold']};
        background: {COLORS['gold']}12;
        border-color: {COLORS['gold']}30;
    }}
    .scm-tab-icon {{
        font-size: 0.75rem;
        line-height: 1;
    }}
    @media (min-width: 768px) {{
        .scm-tab-icon {{ font-size: 0.85rem; }}
    }}
    /* Hide label on very small screens */
    .scm-tab-label {{ display: none; }}
    @media (min-width: 480px) {{
        .scm-tab-label {{ display: inline; }}
    }}

    /* Live indicator */
    .scm-live {{
        display: flex;
        align-items: center;
        gap: 5px;
        font-family: {FONT_MONO};
        font-size: 0.6rem;
        color: {COLORS['green']};
        flex-shrink: 0;
        padding-left: 0.5rem;
        border-left: 1px solid {COLORS['border']};
        display: none;
    }}
    @media (min-width: 600px) {{
        .scm-live {{ display: flex; }}
    }}
    .scm-live-dot {{
        width: 5px;
        height: 5px;
        border-radius: 50%;
        background: {COLORS['green']};
        animation: pulse 2s infinite;
    }}

    /* ── Logo Area ────────────────────────────────────── */
    .logo-container {{
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 0.5rem 0 1.5rem;
        border-bottom: 1px solid {COLORS['border']};
        margin-bottom: 1rem;
    }}
    .logo-mark {{
        width: 32px;
        height: 32px;
        background: linear-gradient(135deg, {COLORS['gold']}, {COLORS['gold2']});
        clip-path: polygon(50% 0%, 100% 25%, 100% 75%, 50% 100%, 0% 75%, 0% 25%);
        flex-shrink: 0;
    }}
    @media (min-width: 768px) {{
        .logo-mark {{
            width: 36px;
            height: 36px;
        }}
    }}
    .logo-text {{
        font-family: {FONT_DISPLAY};
        font-size: 0.8rem;
        font-weight: 600;
        color: {COLORS['text']};
        letter-spacing: 0.05em;
        text-transform: uppercase;
        line-height: 1.2;
    }}
    @media (min-width: 768px) {{
        .logo-text {{
            font-size: 0.85rem;
        }}
    }}
    .logo-sub {{
        font-size: 0.6rem;
        color: {COLORS['text2']};
        font-weight: 400;
        letter-spacing: 0.08em;
        text-transform: uppercase;
    }}
    @media (min-width: 768px) {{
        .logo-sub {{
            font-size: 0.65rem;
        }}
    }}

    /* ── Nav Items ────────────────────────────────────── */
    .nav-section {{
        font-size: 0.6rem;
        font-weight: 600;
        color: {COLORS['text3']};
        letter-spacing: 0.12em;
        text-transform: uppercase;
        margin: 1.25rem 0 0.5rem;
        padding-left: 0.5rem;
    }}

    /* ── Page Header ──────────────────────────────────── */
    .page-header {{
        display: flex;
        flex-direction: column;
        gap: 0.75rem;
        margin-bottom: 1.25rem;
        padding-bottom: 1rem;
        border-bottom: 1px solid {COLORS['border']};
    }}
    @media (min-width: 768px) {{
        .page-header {{
            flex-direction: row;
            align-items: flex-end;
            justify-content: space-between;
            margin-bottom: 2rem;
            padding-bottom: 1.25rem;
        }}
    }}
    .page-title {{
        font-family: {FONT_DISPLAY};
        font-size: 1.25rem;
        font-weight: 600;
        color: {COLORS['text']};
        letter-spacing: -0.02em;
        margin: 0;
        line-height: 1.1;
    }}
    @media (min-width: 768px) {{
        .page-title {{
            font-size: 1.6rem;
        }}
    }}
    .page-subtitle {{
        font-size: 0.75rem;
        color: {COLORS['text2']};
        margin-top: 0.25rem;
        font-weight: 400;
        letter-spacing: 0.01em;
        line-height: 1.3;
    }}
    @media (min-width: 768px) {{
        .page-subtitle {{
            font-size: 0.8rem;
            margin-top: 0.3rem;
        }}
    }}
    .page-timestamp {{
        font-family: {FONT_MONO};
        font-size: 0.65rem;
        color: {COLORS['text3']};
        text-align: left;
    }}
    @media (min-width: 768px) {{
        .page-timestamp {{
            text-align: right;
            font-size: 0.7rem;
        }}
    }}
    .live-dot {{
        display: inline-block;
        width: 6px;
        height: 6px;
        border-radius: 50%;
        background: {COLORS['green']};
        margin-right: 6px;
        animation: pulse 2s infinite;
        vertical-align: middle;
    }}
    @keyframes pulse {{
        0%, 100% {{ opacity: 1; box-shadow: 0 0 0 0 {COLORS['green']}44; }}
        50% {{ opacity: 0.8; box-shadow: 0 0 0 4px {COLORS['green']}00; }}
    }}

    /* ── KPI Cards ────────────────────────────────────── */
    .kpi-grid {{
        display: grid;
        grid-template-columns: repeat(2, 1fr);
        gap: 1px;
        background: {COLORS['border']};
        border: 1px solid {COLORS['border']};
        border-radius: 8px;
        overflow: hidden;
        margin-bottom: 1rem;
    }}
    @media (min-width: 640px) {{
        .kpi-grid {{
            grid-template-columns: repeat(4, 1fr);
            margin-bottom: 1.5rem;
        }}
    }}
    .kpi-card {{
        background: {COLORS['surface']};
        padding: 1rem;
        position: relative;
        transition: background 0.2s;
    }}
    @media (min-width: 768px) {{
        .kpi-card {{
            padding: 1.4rem 1.6rem;
        }}
    }}
    .kpi-card:hover {{
        background: {COLORS['surface2']};
    }}
    .kpi-card::before {{
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 2px;
    }}
    .kpi-card.gold::before  {{ background: linear-gradient(90deg, {COLORS['gold']}, transparent); }}
    .kpi-card.green::before {{ background: linear-gradient(90deg, {COLORS['green']}, transparent); }}
    .kpi-card.blue::before  {{ background: linear-gradient(90deg, {COLORS['blue']}, transparent); }}
    .kpi-card.red::before   {{ background: linear-gradient(90deg, {COLORS['red']}, transparent); }}
    .kpi-card.amber::before {{ background: linear-gradient(90deg, {COLORS['amber']}, transparent); }}

    .kpi-label {{
        font-size: 0.6rem;
        font-weight: 600;
        color: {COLORS['text3']};
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin-bottom: 0.4rem;
    }}
    @media (min-width: 768px) {{
        .kpi-label {{
            font-size: 0.65rem;
            letter-spacing: 0.1em;
            margin-bottom: 0.6rem;
        }}
    }}
    .kpi-value {{
        font-family: {FONT_MONO};
        font-size: 1.25rem;
        font-weight: 500;
        color: {COLORS['text']};
        line-height: 1;
        margin-bottom: 0.3rem;
        letter-spacing: -0.02em;
    }}
    @media (min-width: 768px) {{
        .kpi-value {{
            font-size: 1.75rem;
            margin-bottom: 0.5rem;
        }}
    }}
    .kpi-delta {{
        font-size: 0.65rem;
        font-weight: 500;
        font-family: {FONT_MONO};
    }}
    @media (min-width: 768px) {{
        .kpi-delta {{
            font-size: 0.7rem;
        }}
    }}
    .kpi-delta.up   {{ color: {COLORS['green']}; }}
    .kpi-delta.down {{ color: {COLORS['red']}; }}
    .kpi-delta.neutral {{ color: {COLORS['text2']}; }}

    /* ── Section Headers ──────────────────────────────── */
    .section-header {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin: 1.25rem 0 0.75rem;
    }}
    @media (min-width: 768px) {{
        .section-header {{
            margin: 1.75rem 0 1rem;
        }}
    }}
    .section-title {{
        font-family: {FONT_DISPLAY};
        font-size: 0.7rem;
        font-weight: 600;
        color: {COLORS['text2']};
        letter-spacing: 0.06em;
        text-transform: uppercase;
    }}
    @media (min-width: 768px) {{
        .section-title {{
            font-size: 0.8rem;
            letter-spacing: 0.08em;
        }}
    }}
    .section-line {{
        flex: 1;
        height: 1px;
        background: {COLORS['border']};
        margin-left: 0.75rem;
    }}
    @media (min-width: 768px) {{
        .section-line {{
            margin-left: 1rem;
        }}
    }}

    /* ── Chart Container ──────────────────────────────── */
    .chart-card {{
        background: {COLORS['surface']};
        border: 1px solid {COLORS['border']};
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 0.75rem;
        overflow-x: auto;
    }}
    @media (min-width: 768px) {{
        .chart-card {{
            padding: 1.25rem 1.5rem;
            margin-bottom: 1rem;
        }}
    }}
    .chart-title {{
        font-size: 0.7rem;
        font-weight: 600;
        color: {COLORS['text2']};
        letter-spacing: 0.05em;
        text-transform: uppercase;
        margin-bottom: 0.75rem;
    }}
    @media (min-width: 768px) {{
        .chart-title {{
            font-size: 0.75rem;
            letter-spacing: 0.06em;
            margin-bottom: 1rem;
        }}
    }}

    /* ── Status Table ─────────────────────────────────── */
    .status-row {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 0.5rem 0;
        border-bottom: 1px solid {COLORS['border']};
        font-size: 0.75rem;
        flex-wrap: wrap;
        gap: 0.25rem;
    }}
    @media (min-width: 768px) {{
        .status-row {{
            padding: 0.65rem 0;
            font-size: 0.8rem;
            flex-wrap: nowrap;
            gap: 0;
        }}
    }}
    .status-row:last-child {{ border-bottom: none; }}
    .status-name {{ 
        color: {COLORS['text2']}; 
        font-family: {FONT_MONO};
        word-break: break-word;
        flex: 1;
        min-width: 120px;
    }}
    .status-count {{
        font-family: {FONT_MONO};
        font-weight: 500;
        color: {COLORS['text']};
        font-size: 0.8rem;
    }}
    @media (min-width: 768px) {{
        .status-count {{
            font-size: inherit;
        }}
    }}
    .badge {{
        padding: 2px 6px;
        border-radius: 3px;
        font-size: 0.55rem;
        font-weight: 600;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        font-family: {FONT_MONO};
        white-space: nowrap;
    }}
    @media (min-width: 768px) {{
        .badge {{
            padding: 2px 8px;
            font-size: 0.6rem;
            letter-spacing: 0.06em;
        }}
    }}
    .badge-ok     {{ background: {COLORS['green']}18; color: {COLORS['green']}; border: 1px solid {COLORS['green']}30; }}
    .badge-warn   {{ background: {COLORS['amber']}18; color: {COLORS['amber']}; border: 1px solid {COLORS['amber']}30; }}
    .badge-error  {{ background: {COLORS['red']}18;   color: {COLORS['red']};   border: 1px solid {COLORS['red']}30; }}

    /* ── Anomaly Card ─────────────────────────────────── */
    .anomaly-card {{
        background: {COLORS['surface']};
        border: 1px solid {COLORS['border']};
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 0.5rem;
        display: flex;
        align-items: flex-start;
        gap: 0.75rem;
        transition: border-color 0.2s;
    }}
    @media (min-width: 768px) {{
        .anomaly-card {{
            padding: 1.25rem 1.5rem;
            margin-bottom: 0.75rem;
            gap: 1rem;
        }}
    }}
    .anomaly-card:hover {{ border-color: {COLORS['border2']}; }}
    .anomaly-card.critical {{ border-left: 3px solid {COLORS['red']}; }}
    .anomaly-card.warning  {{ border-left: 3px solid {COLORS['amber']}; }}
    .anomaly-card.info     {{ border-left: 3px solid {COLORS['blue']}; }}
    .anomaly-icon {{
        width: 28px;
        height: 28px;
        border-radius: 6px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 0.8rem;
        flex-shrink: 0;
    }}
    @media (min-width: 768px) {{
        .anomaly-icon {{
            width: 32px;
            height: 32px;
            font-size: 0.85rem;
        }}
    }}
    .anomaly-icon.critical {{ background: {COLORS['red']}15; }}
    .anomaly-icon.warning  {{ background: {COLORS['amber']}15; }}
    .anomaly-icon.info     {{ background: {COLORS['blue']}15; }}
    .anomaly-title {{
        font-size: 0.8rem;
        font-weight: 600;
        color: {COLORS['text']};
        margin-bottom: 0.15rem;
        line-height: 1.2;
    }}
    @media (min-width: 768px) {{
        .anomaly-title {{
            font-size: 0.85rem;
            margin-bottom: 0.2rem;
        }}
    }}
    .anomaly-detail {{
        font-size: 0.7rem;
        color: {COLORS['text2']};
        line-height: 1.4;
    }}
    @media (min-width: 768px) {{
        .anomaly-detail {{
            font-size: 0.73rem;
            line-height: 1.5;
        }}
    }}
    .anomaly-meta {{
        font-family: {FONT_MONO};
        font-size: 0.6rem;
        color: {COLORS['text3']};
        margin-top: 0.25rem;
    }}
    @media (min-width: 768px) {{
        .anomaly-meta {{
            font-size: 0.65rem;
            margin-top: 0.3rem;
        }}
    }}

    /* ── Control Panel ────────────────────────────────── */
    .control-card {{
        background: {COLORS['surface']};
        border: 1px solid {COLORS['border']};
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 0.75rem;
    }}
    @media (min-width: 768px) {{
        .control-card {{
            padding: 1.5rem;
            margin-bottom: 1rem;
        }}
    }}
    .control-label {{
        font-size: 0.6rem;
        font-weight: 600;
        color: {COLORS['text3']};
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin-bottom: 0.4rem;
    }}
    @media (min-width: 768px) {{
        .control-label {{
            font-size: 0.65rem;
            letter-spacing: 0.1em;
            margin-bottom: 0.5rem;
        }}
    }}

    /* ── Streamlit Component Overrides ───────────────── */
    .stSelectbox > div > div {{
        background: {COLORS['surface2']} !important;
        border: 1px solid {COLORS['border']} !important;
        color: {COLORS['text']} !important;
        border-radius: 6px !important;
    }}
    .stSlider > div > div > div {{
        background: {COLORS['gold']} !important;
    }}
    .stButton > button {{
        background: {COLORS['surface2']} !important;
        border: 1px solid {COLORS['border2']} !important;
        color: {COLORS['text']} !important;
        border-radius: 6px !important;
        font-family: {FONT_SANS} !important;
        font-size: 0.75rem !important;
        font-weight: 500 !important;
        padding: 0.4rem 1rem !important;
        transition: all 0.15s !important;
        width: 100%;
    }}
    @media (min-width: 768px) {{
        .stButton > button {{
            font-size: 0.8rem !important;
            padding: 0.5rem 1.25rem !important;
            width: auto;
        }}
    }}
    .stButton > button:hover {{
        border-color: {COLORS['gold']} !important;
        color: {COLORS['gold']} !important;
    }}
    .stButton.primary > button {{
        background: {COLORS['gold']}18 !important;
        border-color: {COLORS['gold']}60 !important;
        color: {COLORS['gold']} !important;
    }}
    div[data-testid="metric-container"] {{
        display: none;
    }}
    .stDataFrame {{
        background: {COLORS['surface']} !important;
    }}
    
    /* ── Responsive Columns ───────────────────────────── */
    @media (max-width: 768px) {{
        div[data-testid="column"] {{
            width: 100% !important;
            flex: 0 0 100% !important;
            min-width: 100% !important;
        }}
    }}
    
    /* ── Radio Button Mobile Fix ──────────────────────── */
    @media (max-width: 768px) {{
        .stRadio > div {{
            flex-direction: column !important;
        }}
        .stRadio > div > label {{
            padding: 0.5rem 0 !important;
        }}
    }}
    
    /* ── Plotly Chart Responsiveness ────────────────── */
    .js-plotly-plot {{
        width: 100% !important;
    }}
    .js-plotly-plot .plotly {{
        width: 100% !important;
    }}
    
    /* ── Sidebar Footer ──────────────────────────────── */
    [data-testid="stSidebar"] .stButton > button {{
        margin-top: 1rem;
    }}
    </style>
    """, unsafe_allow_html=True)

# ============================================================
# PLOTLY THEME
# ============================================================
def plotly_layout(title="", height=320, showlegend=True):
    return dict(
        title=dict(
            text=title,
            font=dict(family=FONT_DISPLAY, size=12, color=COLORS['text2']),
            x=0, xanchor='left',
        ),
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONT_SANS, color=COLORS['text2']),
        showlegend=showlegend,
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            bordercolor=COLORS['border'],
            borderwidth=1,
            font=dict(size=10),
        ),
        margin=dict(l=0, r=0, t=36 if title else 10, b=0),
        xaxis=dict(
            gridcolor=COLORS['border'],
            gridwidth=1,
            zeroline=False,
            tickfont=dict(size=9, family=FONT_MONO, color=COLORS['text3']),
            linecolor=COLORS['border'],
        ),
        yaxis=dict(
            gridcolor=COLORS['border'],
            gridwidth=1,
            zeroline=False,
            tickfont=dict(size=9, family=FONT_MONO, color=COLORS['text3']),
            linecolor="rgba(0,0,0,0)",
        ),
    )

def line_chart(x, y, color=None, name="", fill=True, dash="solid"):
    color = color or COLORS['gold']
    traces = [go.Scatter(
        x=x, y=y,
        name=name,
        mode="lines",
        line=dict(color=color, width=2, dash=dash),
        fill="tozeroy" if fill else "none",
        fillcolor=f"{color}10",
        hovertemplate=f"<b>%{{y:,.0f}}</b><br>%{{x}}<extra></extra>",
    )]
    return traces

# ============================================================
# DATA LAYER
# ============================================================
@st.cache_data(ttl=300)
def fetch_table_counts():
    tables = [
        "transactions", "inventory_log", "shipments",
        "warehouse_snapshot", "purchase_orders", "sales_hourly",
        "sales_daily", "sales_monthly", "kpi_monthly"
    ]
    result = {}
    for t in tables:
        try:
            resp = supabase.table(t).select("id", count="exact").limit(1).execute()
            result[t] = resp.count or 0
        except:
            try:
                resp = supabase.table(t).select("*", count="exact").limit(1).execute()
                result[t] = resp.count or 0
            except:
                result[t] = 0
    return result

@st.cache_data(ttl=120)
def fetch_recent_transactions(limit=5000):
    try:
        resp = (
            supabase.table("transactions")
            .select("store_id, product_id, timestamp, quantity, total, discount_pct, payment_method, customer_type")
            .order("timestamp", desc=True)
            .limit(limit)
            .execute()
        )
        return pd.DataFrame(resp.data or [])
    except:
        return pd.DataFrame()

@st.cache_data(ttl=120)
def fetch_sales_hourly(limit=2000):
    try:
        resp = (
            supabase.table("sales_hourly")
            .select("*")
            .order("date", desc=True)
            .limit(limit)
            .execute()
        )
        return pd.DataFrame(resp.data or [])
    except:
        return pd.DataFrame()

@st.cache_data(ttl=120)
def fetch_shipments(limit=500):
    try:
        resp = (
            supabase.table("shipments")
            .select("*")
            .order("departure_time", desc=True)
            .limit(limit)
            .execute()
        )
        return pd.DataFrame(resp.data or [])
    except:
        return pd.DataFrame()

@st.cache_data(ttl=120)
def fetch_warehouse_snapshots(limit=500):
    try:
        resp = (
            supabase.table("warehouse_snapshot")
            .select("*")
            .order("timestamp", desc=True)
            .limit(limit)
            .execute()
        )
        return pd.DataFrame(resp.data or [])
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def fetch_stores():
    try:
        resp = supabase.table("stores").select("store_id, store_name, city, lambda_final").execute()
        return pd.DataFrame(resp.data or [])
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def fetch_simulation_config():
    try:
        resp = supabase.table("simulation_config").select("config_key, config_value, description").execute()
        return {r["config_key"]: r["config_value"] for r in (resp.data or [])}
    except:
        return {}

# ============================================================
# COMPONENTS
# ============================================================
def render_logo():
    st.markdown("""
    <div class="logo-container">
        <div class="logo-mark"></div>
        <div>
            <div class="logo-text">Supply Chain</div>
            <div class="logo-sub">Intelligence Platform</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

def render_page_header(title, subtitle="", live=True):
    st.markdown(f"""
    <div class="page-header">
        <div>
            <div class="page-title">{title}</div>
            <div class="page-subtitle">{subtitle}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

def render_kpi_cards(cards):
    """
    cards = [{"label", "value", "delta", "delta_dir", "color"}, ...]
    """
    cards_html = "".join([f"""
    <div class="kpi-card {c.get('color','gold')}">
        <div class="kpi-label">{c['label']}</div>
        <div class="kpi-value">{c['value']}</div>
        <div class="kpi-delta {c.get('delta_dir','neutral')}">{c.get('delta','—')}</div>
    </div>""" for c in cards])
    st.markdown(f'<div class="kpi-grid">{cards_html}</div>', unsafe_allow_html=True)

def render_section(title):
    st.markdown(f"""
    <div class="section-header">
        <div class="section-title">{title}</div>
        <div class="section-line"></div>
    </div>
    """, unsafe_allow_html=True)

def render_status_table(rows):
    rows_html = "".join([f"""
    <div class="status-row">
        <span class="status-name">{r['name']}</span>
        <div style="display:flex;align-items:center;gap:8px;">
            <span class="status-count">{r['count']:,}</span>
            <span class="badge badge-{r['status']}">{r['label']}</span>
        </div>
    </div>""" for r in rows])
    st.markdown(f'<div class="chart-card">{rows_html}</div>', unsafe_allow_html=True)

def render_anomaly(severity, title, detail, meta):
    st.markdown(f"""
    <div class="anomaly-card {severity}">
        <div class="anomaly-icon {severity}">{"⚠" if severity=="warning" else "⊗" if severity=="critical" else "◈"}</div>
        <div>
            <div class="anomaly-title">{title}</div>
            <div class="anomaly-detail">{detail}</div>
            <div class="anomaly-meta">{meta}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

# ============================================================
# ANOMALY DETECTION
# ============================================================
def detect_anomalies(df_hourly, config):
    """
    Z-score anomaly detection.
    Anomali = |z| > 2.0 pa indikator aktiv.
    """
    anomalies = []
    if df_hourly.empty:
        return anomalies

    active_event = config.get("active_event", 0)
    promo_active = config.get("promo_active", 0)

    try:
        df_hourly["date"] = pd.to_datetime(df_hourly["date"])
        daily = df_hourly.groupby("date")["revenue"].sum().reset_index()
        daily = daily.sort_values("date")

        if len(daily) < 3:
            return anomalies

        values = daily["revenue"].values
        mu  = np.mean(values[:-1])
        sig = np.std(values[:-1])

        if sig > 0:
            z = (values[-1] - mu) / sig
            pct = ((values[-1] - mu) / mu * 100) if mu > 0 else 0

            if abs(z) > 2.5 and active_event == 0 and promo_active == 0:
                direction = "rritje" if z > 0 else "rënie"
                severity  = "critical" if abs(z) > 3 else "warning"
                anomalies.append({
                    "severity": severity,
                    "title":    f"Xhiro ditore — {direction} e papritur",
                    "detail":   f"Devijim {abs(pct):.1f}% nga mesatarja historike pa asnjë indikator aktiv (festë/kampanjë).",
                    "meta":     f"z-score = {z:.2f} · μ = {mu:,.0f} L · σ = {sig:,.0f} L",
                })

        # Per-store anomalies
        store_daily = df_hourly.groupby(["store_id","date"])["revenue"].sum().reset_index()
        for sid, grp in store_daily.groupby("store_id"):
            grp = grp.sort_values("date")
            if len(grp) < 4:
                continue
            vals = grp["revenue"].values
            mu2  = np.mean(vals[:-1])
            sig2 = np.std(vals[:-1])
            if sig2 > 0:
                z2 = (vals[-1] - mu2) / sig2
                if abs(z2) > 2.8 and active_event == 0:
                    pct2 = ((vals[-1] - mu2) / mu2 * 100) if mu2 > 0 else 0
                    anomalies.append({
                        "severity": "warning",
                        "title":    f"{sid} — Anomali xhiro",
                        "detail":   f"Devijim {abs(pct2):.1f}% nga sjellja tipike e store-it.",
                        "meta":     f"z-score = {z2:.2f} · Ditë analizuar: {len(grp)}",
                    })

    except Exception as e:
        pass

    return anomalies[:6]

# ============================================================
# PAGE 1 — SIMULATION MONITOR
# ============================================================
def page_monitor():
    render_page_header(
        "Simulation Monitor",
        "Real-time health check i ekosistemit të simulimit"
    )

    counts  = fetch_table_counts()
    config  = fetch_simulation_config()
    df_txn  = fetch_recent_transactions(1000)

    # ── KPI Row ──────────────────────────────────────────
    total_txn  = counts.get("transactions", 0)
    total_inv  = counts.get("inventory_log", 0)
    total_shp  = counts.get("shipments", 0)
    total_snap = counts.get("warehouse_snapshot", 0)

    sim_active = config.get("simulation_active", 1.0)
    event_id   = int(config.get("active_event", 0))
    event_map  = {0: "—", 1: "Luftë", 2: "Pandemi", 3: "Grevë", 4: "Thatësirë"}

    render_kpi_cards([
        {"label": "Transaksione", "value": f"{total_txn:,}", "delta": "Kumulativ", "delta_dir": "neutral", "color": "gold"},
        {"label": "Inventory Logs", "value": f"{total_inv:,}", "delta": "Kumulativ", "delta_dir": "neutral", "color": "green"},
        {"label": "Dërgesa", "value": f"{total_shp:,}", "delta": f"{total_shp//15 if total_shp else 0} / store", "delta_dir": "neutral", "color": "blue"},
        {"label": "Event Aktiv", "value": event_map.get(event_id, "—"), "delta": "simulation_config", "delta_dir": "neutral" if event_id == 0 else "down", "color": "red" if event_id > 0 else "gold"},
    ])

    # ── Database Status ───────────────────────────────────
    col1, col2 = st.columns([1, 1])

    with col1:
        render_section("Database Status")
        thresholds = {
            "transactions":       (1000, "ACTIVE"),
            "inventory_log":      (500,  "ACTIVE"),
            "shipments":          (15,   "ACTIVE"),
            "warehouse_snapshot": (10,   "ACTIVE"),
            "purchase_orders":    (1,    "ACTIVE"),
            "sales_hourly":       (100,  "ACTIVE"),
        }
        rows = []
        for tbl, (thresh, lbl) in thresholds.items():
            cnt = counts.get(tbl, 0)
            if cnt >= thresh:
                status, badge = "ok", "OK"
            elif cnt > 0:
                status, badge = "warn", "LOW"
            else:
                status, badge = "error", "EMPTY"
            rows.append({"name": tbl, "count": cnt, "status": status, "label": badge})
        render_status_table(rows)

    with col2:
        render_section("Simulation Config")
        cfg_items = [
            ("simulation_active",    "Simulim Aktiv",     "ok"   if config.get("simulation_active",1)==1 else "error"),
            ("demand_multiplier",    "Demand Multiplier", "ok"   if config.get("demand_multiplier",1)==1 else "warn"),
            ("fuel_multiplier",      "Fuel Multiplier",   "ok"   if config.get("fuel_multiplier",1)==1 else "warn"),
            ("transport_disruption", "Transport",         "error" if config.get("transport_disruption",0)>0 else "ok"),
            ("promo_active",         "Promo Aktive",      "warn"  if config.get("promo_active",0)==1 else "ok"),
            ("active_event",         "Black Swan Event",  "error" if config.get("active_event",0)>0 else "ok"),
        ]
        rows2 = []
        for key, label, status in cfg_items:
            val = config.get(key, "—")
            badge_map = {"ok": "NORMAL", "warn": "AKTIV", "error": "ALERT"}
            rows2.append({"name": label, "count": val, "status": status, "label": badge_map[status]})

        rows_html2 = "".join([f"""
        <div class="status-row">
            <span class="status-name">{r['name']}</span>
            <div style="display:flex;align-items:center;gap:8px;">
                <span class="status-count" style="font-size:0.75rem">{r['count']}</span>
                <span class="badge badge-{r['status']}">{r['label']}</span>
            </div>
        </div>""" for r in rows2])
        st.markdown(f'<div class="chart-card">{rows_html2}</div>', unsafe_allow_html=True)

    # ── Transactions per hour (last 24h) ──────────────────
    render_section("Vëllimi i Transaksioneve — 24 orët e fundit")

    if not df_txn.empty:
        df_txn["timestamp"] = pd.to_datetime(df_txn["timestamp"])
        df_txn["hour"] = df_txn["timestamp"].dt.floor("h")
        hourly = df_txn.groupby("hour").agg(
            transactions=("total","count"),
            revenue=("total","sum")
        ).reset_index().tail(24)

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(
            x=hourly["hour"], y=hourly["transactions"],
            name="Transaksione",
            marker_color="rgba(201,168,76,0.38)",
            marker_line_color=COLORS['gold'],
            marker_line_width=1,
            hovertemplate="<b>%{y:,}</b> transaksione<extra></extra>",
        ), secondary_y=False)
        fig.add_trace(go.Scatter(
            x=hourly["hour"], y=hourly["revenue"],
            name="Revenue (L)",
            mode="lines",
            line=dict(color=COLORS['green'], width=2),
            hovertemplate="<b>%{y:,.0f}</b> L<extra></extra>",
        ), secondary_y=True)

        layout = plotly_layout(height=280)
        layout["yaxis"]["title"] = dict(text="Transaksione", font=dict(size=9))
        layout["yaxis2"] = dict(
            gridcolor="rgba(0,0,0,0)",
            tickfont=dict(size=9, family=FONT_MONO, color=COLORS['text3']),
            zeroline=False,
        )
        fig.update_layout(**layout)

        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="chart-card"><p style="color:#4A5568;font-size:0.8rem;text-align:center;padding:2rem">Nuk ka të dhëna akoma</p></div>', unsafe_allow_html=True)

# ============================================================
# PAGE 2 — PERFORMANCE ANALYTICS
# ============================================================
def page_analytics():
    render_page_header(
        "Performance Analytics",
        "Analiza e thelluar e shitjeve, klientëve dhe fitimit"
    )

    df_hourly = fetch_sales_hourly(3000)
    df_txn    = fetch_recent_transactions(5000)
    stores_df = fetch_stores()

    if df_txn.empty:
        st.markdown('<div class="chart-card"><p style="color:#4A5568;font-size:0.8rem;text-align:center;padding:3rem">Nuk ka të dhëna akoma. Prit që scheduler të gjenerojë të dhëna.</p></div>', unsafe_allow_html=True)
        return

    df_txn["timestamp"] = pd.to_datetime(df_txn["timestamp"])
    df_txn["date"] = df_txn["timestamp"].dt.date
    df_txn["hour"] = df_txn["timestamp"].dt.hour

    # ── KPI Row ──────────────────────────────────────────
    total_rev  = df_txn["total"].sum()
    total_cnt  = len(df_txn)
    avg_basket = df_txn["total"].mean()
    member_pct = (df_txn["customer_type"] == "Member").mean() * 100

    render_kpi_cards([
        {"label": "Revenue Total", "value": f"{total_rev/1e6:.2f}M", "delta": "Lekë", "delta_dir": "up", "color": "gold"},
        {"label": "Transaksione", "value": f"{total_cnt:,}", "delta": "5000 mostrat", "delta_dir": "neutral", "color": "green"},
        {"label": "Avg Basket", "value": f"{avg_basket:,.0f}", "delta": "Lekë / faturë", "delta_dir": "neutral", "color": "blue"},
        {"label": "Member Rate", "value": f"{member_pct:.1f}%", "delta": "Klientë të regjistruar", "delta_dir": "up" if member_pct > 30 else "neutral", "color": "amber"},
    ])

    # ── Revenue by Store ──────────────────────────────────
    render_section("Revenue Breakdown — Sipas Store")

    col1, col2 = st.columns([3, 2])

    with col1:
        store_rev = df_txn.groupby("store_id")["total"].sum().reset_index()
        store_rev = store_rev.sort_values("total", ascending=True).tail(15)

        # Ndërto ngjyrat manualisht — colorscale + color=array
        # shkakton ValueError në disa versione Plotly
        max_rev = store_rev["total"].max() if not store_rev.empty else 1
        bar_colors = [
            COLORS['gold'] if v == max_rev
            else ("rgba(201,168,76,0.56)" if v > max_rev * 0.7
            else ("rgba(201,168,76,0.38)" if v > max_rev * 0.4
            else COLORS['surface2']))
            for v in store_rev["total"]
        ]

        fig = go.Figure(go.Bar(
            x=store_rev["total"],
            y=store_rev["store_id"],
            orientation="h",
            marker=dict(
                color=bar_colors,
                line_width=0,
            ),
            hovertemplate="<b>%{y}</b><br>Revenue: %{x:,.0f} L<extra></extra>",
        ))
        layout = plotly_layout(height=340, showlegend=False)
        layout["xaxis"]["tickformat"] = ",.0f"
        layout["yaxis"]["tickfont"]["size"] = 9
        fig.update_layout(**layout)

        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        payment_dist = df_txn["payment_method"].value_counts().reset_index()
        payment_dist.columns = ["method", "count"]

        fig2 = go.Figure(go.Pie(
            labels=payment_dist["method"],
            values=payment_dist["count"],
            hole=0.72,
            marker=dict(
                colors=[COLORS['gold'], COLORS['green'], COLORS['blue']],
                line=dict(color=COLORS['bg'], width=3),
            ),
            textinfo="percent",
            textfont=dict(size=10, family=FONT_MONO, color=COLORS['text']),
            hovertemplate="<b>%{label}</b><br>%{value:,} transaksione<extra></extra>",
        ))
        fig2.add_annotation(
            text=f"{total_cnt:,}<br><span style='font-size:9px'>TOTAL</span>",
            x=0.5, y=0.5, showarrow=False,
            font=dict(family=FONT_MONO, size=16, color=COLORS['text']),
            align="center",
        )
        layout2 = plotly_layout(height=340, showlegend=True)
        layout2.pop("xaxis", None)
        layout2.pop("yaxis", None)
        layout2["legend"]["orientation"] = "h"
        layout2["legend"]["y"] = -0.1
        fig2.update_layout(**layout2)

        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Payment Distribution</div>', unsafe_allow_html=True)
        st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Hourly Pattern (Poisson validation) ──────────────
    render_section("Profili Ditor — Validim Poisson")

    hourly_avg = df_txn.groupby("hour")["total"].agg(["count","mean","sum"]).reset_index()
    hourly_avg.columns = ["hour","transactions","avg_basket","revenue"]

    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(
        x=hourly_avg["hour"], y=hourly_avg["transactions"],
        name="Transaksione/orë",
        mode="lines+markers",
        line=dict(color=COLORS['gold'], width=2.5),
        marker=dict(size=6, color=COLORS['gold'], line=dict(color=COLORS['bg'], width=2)),
        fill="tozeroy",
        fillcolor="rgba(201,168,76,0.05)",
        hovertemplate="Ora %{x}:00 — <b>%{y:,}</b> transaksione<extra></extra>",
    ))
    fig3.add_trace(go.Scatter(
        x=hourly_avg["hour"], y=hourly_avg["avg_basket"],
        name="Avg Basket (L)",
        mode="lines",
        line=dict(color=COLORS['blue'], width=1.5, dash="dot"),
        yaxis="y2",
        hovertemplate="Avg Basket: <b>%{y:,.0f}</b> L<extra></extra>",
    ))

    layout3 = plotly_layout(height=300, showlegend=True)
    layout3["xaxis"]["tickvals"] = list(range(6, 23))
    layout3["xaxis"]["ticktext"] = [f"{h:02d}h" for h in range(6, 23)]
    layout3["yaxis2"] = dict(
        overlaying="y", side="right",
        gridcolor="rgba(0,0,0,0)",
        tickfont=dict(size=9, family=FONT_MONO, color=COLORS['text3']),
        zeroline=False,
    )
    fig3.update_layout(**layout3)

    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar": False})
    st.markdown('</div>', unsafe_allow_html=True)

    # ── Customer Type & Basket Distribution ──────────────
    render_section("Shpërndarja e Shportës")

    col3, col4 = st.columns(2)

    with col3:
        fig4 = go.Figure()
        bins = np.histogram(df_txn["total"].clip(upper=10000), bins=40)
        fig4.add_trace(go.Bar(
            x=bins[1][:-1], y=bins[0],
            marker_color="rgba(0,200,150,0.44)",
            marker_line_color=COLORS['green'],
            marker_line_width=0.5,
            name="Fatura",
            hovertemplate="<b>%{y:,}</b> fatura — %{x:,.0f}L<extra></extra>",
        ))
        layout4 = plotly_layout("Shpërndarja Basket Size", height=260, showlegend=False)
        layout4["xaxis"]["tickformat"] = ",.0f"
        fig4.update_layout(**layout4)
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.plotly_chart(fig4, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    with col4:
        daily_rev = df_txn.groupby("date")["total"].sum().reset_index().sort_values("date").tail(14)

        if len(daily_rev) >= 2:
            trend = np.polyfit(range(len(daily_rev)), daily_rev["total"], 1)
            trend_line = np.polyval(trend, range(len(daily_rev)))

            fig5 = go.Figure()
            fig5.add_trace(go.Bar(
                x=daily_rev["date"].astype(str), y=daily_rev["total"],
                marker_color="rgba(74,158,255,0.31)",
                marker_line_color=COLORS['blue'],
                marker_line_width=0.5,
                name="Revenue/ditë",
                hovertemplate="<b>%{y:,.0f}</b> L<extra></extra>",
            ))
            fig5.add_trace(go.Scatter(
                x=daily_rev["date"].astype(str), y=trend_line,
                mode="lines",
                line=dict(color=COLORS['gold'], width=2, dash="dot"),
                name="Trend",
                hovertemplate="Trend: <b>%{y:,.0f}</b> L<extra></extra>",
            ))
            layout5 = plotly_layout("Revenue Ditor + Trend", height=260)
            layout5["xaxis"]["tickangle"] = -30
            layout5["xaxis"]["tickfont"]["size"] = 8
            layout5["yaxis"]["tickformat"] = ",.0f"
            fig5.update_layout(**layout5)
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            st.plotly_chart(fig5, use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)
# ============================================================
# PAGE 3 — ANOMALY DETECTOR
# ============================================================
def page_anomalies():
    render_page_header(
        "Anomaly Detector",
        "Detektim automatik i devijimeve me z-score · threshold: |z| > 2.0σ"
    )

    df_hourly = fetch_sales_hourly(3000)
    config    = fetch_simulation_config()

    # KPI anomaly overview
    render_kpi_cards([
        {"label": "Metodologjia", "value": "Z-Score", "delta": "Normal · |z| > 2.0σ", "delta_dir": "neutral", "color": "blue"},
        {"label": "Window Analizi", "value": "7+ ditë", "delta": "Histori minimale", "delta_dir": "neutral", "color": "gold"},
        {"label": "Black Swan", "value": "4 Tiparë", "delta": "Luftë · Pandemi · Grevë · Thatësirë", "delta_dir": "neutral", "color": "amber"},
        {"label": "Refresh", "value": "5 min", "delta": "Cache TTL", "delta_dir": "neutral", "color": "green"},
    ])

    # Detect
    anomalies = detect_anomalies(df_hourly, config)

    render_section(f"Anomali të Detektuara — {len(anomalies)} gjetje")

    if not anomalies:
        st.markdown("""
        <div class="chart-card" style="text-align:center;padding:3rem">
            <div style="font-size:1.5rem;margin-bottom:0.5rem">◈</div>
            <div style="color:#4A5568;font-size:0.8rem">Asnjë anomali e detektuar.<br>Të gjitha store-et janë brenda normave statistikore.</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        for a in anomalies:
            render_anomaly(a["severity"], a["title"], a["detail"], a["meta"])

    # Z-score chart per store
    render_section("Z-Score Radar — Të gjitha Store-et")

    if not df_hourly.empty:
        try:
            df_hourly["date"] = pd.to_datetime(df_hourly["date"])
            store_daily = df_hourly.groupby(["store_id","date"])["revenue"].sum().reset_index()

            z_scores = []
            for sid, grp in store_daily.groupby("store_id"):
                grp = grp.sort_values("date")
                if len(grp) < 3:
                    continue
                vals = grp["revenue"].values
                mu, sig = np.mean(vals[:-1]), np.std(vals[:-1])
                z = (vals[-1] - mu) / sig if sig > 0 else 0
                z_scores.append({"store_id": sid, "z_score": z, "revenue": vals[-1]})

            if z_scores:
                df_z = pd.DataFrame(z_scores).sort_values("z_score")
                colors_z = [
                    COLORS['red']   if abs(z) > 2.5 else
                    COLORS['amber'] if abs(z) > 1.5 else
                    COLORS['green']
                    for z in df_z["z_score"]
                ]

                fig = go.Figure()
                fig.add_vline(x=2.0,  line=dict(color="rgba(255,71,87,0.31)",   width=1, dash="dot"))
                fig.add_vline(x=-2.0, line=dict(color="rgba(255,71,87,0.31)",   width=1, dash="dot"))
                fig.add_vline(x=0,    line=dict(color=COLORS['border2'],       width=1))
                fig.add_vrect(x0=-2, x1=2, fillcolor="rgba(0,200,150,0.02)", line_width=0)

                fig.add_trace(go.Bar(
                    x=df_z["z_score"],
                    y=df_z["store_id"],
                    orientation="h",
                    marker=dict(color=colors_z, line_width=0),
                    hovertemplate="<b>%{y}</b><br>z-score: %{x:.2f}<extra></extra>",
                    name="Z-Score",
                ))

                layout = plotly_layout(height=380, showlegend=False)
                layout["xaxis"]["title"] = dict(text="Z-Score · Devijim nga mesatarja historike", font=dict(size=9))
                layout["shapes"] = []
                fig.update_layout(**layout)

                st.markdown('<div class="chart-card">', unsafe_allow_html=True)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
                st.markdown('</div>', unsafe_allow_html=True)
        except Exception as e:
            st.markdown(f'<div class="chart-card"><p style="color:#4A5568;font-size:0.8rem">Z-score chart: {e}</p></div>', unsafe_allow_html=True)

# ============================================================
# PAGE 4 — CONTROL PANEL
# ============================================================
def page_control():
    render_page_header(
        "Control Panel",
        "Menaxhimi i parametrave të simulimit dhe Black Swan events",
        live=False
    )

    config = fetch_simulation_config()

    # Status
    sim_active = config.get("simulation_active", 1)
    status_color = COLORS['green'] if sim_active == 1 else COLORS['red']
    status_text  = "RUNNING" if sim_active == 1 else "STOPPED"

    render_kpi_cards([
        {"label": "Status", "value": status_text, "delta": "Simulation Engine", "delta_dir": "up" if sim_active==1 else "down", "color": "green" if sim_active==1 else "red"},
        {"label": "Demand Mult.", "value": f"{config.get('demand_multiplier', 1.0):.2f}×", "delta": "Normal = 1.00", "delta_dir": "neutral", "color": "gold"},
        {"label": "Fuel Mult.", "value": f"{config.get('fuel_multiplier', 1.0):.2f}×", "delta": "Normal = 1.00", "delta_dir": "neutral", "color": "blue"},
        {"label": "Event Aktiv", "value": str(int(config.get("active_event", 0))), "delta": "0 = Asnjë event", "delta_dir": "down" if config.get("active_event",0) > 0 else "neutral", "color": "red" if config.get("active_event",0) > 0 else "gold"},
    ])

    col1, col2 = st.columns(2)

    with col1:
        render_section("Black Swan Event Injector")
        st.markdown('<div class="control-card">', unsafe_allow_html=True)

        st.markdown('<div class="control-label">Tipi i Eventit</div>', unsafe_allow_html=True)
        event_type = st.selectbox(
            "", ["— Asnjë Event —", "Luftë / Krizë Rajonale", "Pandemi", "Grevë Transporti", "Thatësirë Bujqësore"],
            label_visibility="collapsed"
        )

        st.markdown('<div class="control-label" style="margin-top:1rem">Intensiteti (1-10)</div>', unsafe_allow_html=True)
        intensity = st.slider("", 1, 10, 5, label_visibility="collapsed")

        event_map = {
            "— Asnjë Event —":            {"active_event": 0, "demand_multiplier": 1.0, "fuel_multiplier": 1.0, "lead_time_multiplier": 1.0, "transport_disruption": 0.0},
            "Luftë / Krizë Rajonale":      {"active_event": 1, "demand_multiplier": max(0.3, 1 - intensity*0.07), "fuel_multiplier": 1 + intensity*0.035, "lead_time_multiplier": 1 + intensity*0.15, "transport_disruption": intensity*0.05},
            "Pandemi":                      {"active_event": 2, "demand_multiplier": max(0.2, 1 - intensity*0.08), "fuel_multiplier": 1.0, "lead_time_multiplier": 1 + intensity*0.1, "transport_disruption": intensity*0.03},
            "Grevë Transporti":             {"active_event": 3, "demand_multiplier": max(0.7, 1 - intensity*0.03), "fuel_multiplier": 1.0, "lead_time_multiplier": 1 + intensity*0.2, "transport_disruption": intensity*0.1},
            "Thatësirë Bujqësore":          {"active_event": 4, "demand_multiplier": max(0.8, 1 - intensity*0.02), "fuel_multiplier": 1.0, "lead_time_multiplier": 1 + intensity*0.05, "transport_disruption": 0.0},
        }

        params = event_map.get(event_type, event_map["— Asnjë Event —"])

        # Preview
        st.markdown(f"""
        <div style="background:{COLORS['bg']};border:1px solid {COLORS['border']};border-radius:6px;padding:0.75rem 1rem;margin:1rem 0;font-family:{FONT_MONO};font-size:0.72rem;color:{COLORS['text2']}">
        demand_multiplier    → <span style="color:{COLORS['gold']}">{params['demand_multiplier']:.2f}</span><br>
        fuel_multiplier      → <span style="color:{COLORS['gold']}">{params['fuel_multiplier']:.2f}</span><br>
        lead_time_multiplier → <span style="color:{COLORS['gold']}">{params['lead_time_multiplier']:.2f}</span><br>
        transport_disruption → <span style="color:{COLORS['gold']}">{params['transport_disruption']:.2f}</span>
        </div>
        """, unsafe_allow_html=True)

        if st.button("Injekto Event në Simulim", type="primary" if event_type != "— Asnjë Event —" else "secondary"):
            try:
                for key, val in params.items():
                    supabase.table("simulation_config").update(
                        {"config_value": val}
                    ).eq("config_key", key).execute()
                st.success(f"Event i aplikuar: {event_type}")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Error: {e}")

        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        render_section("Price Shock Manager")
        st.markdown('<div class="control-card">', unsafe_allow_html=True)

        st.markdown('<div class="control-label">Ndryshim Çmimit Karburantit (%)</div>', unsafe_allow_html=True)
        fuel_pct = st.slider("Karburant", -30, 100, 0, label_visibility="collapsed")

        st.markdown('<div class="control-label">Ndryshim Çmimit Ushqimeve (%)</div>', unsafe_allow_html=True)
        food_pct = st.slider("Ushqime", -20, 80, 0, label_visibility="collapsed")

        st.markdown('<div class="control-label">Ndryshim Import (%)</div>', unsafe_allow_html=True)
        import_pct = st.slider("Import", -10, 100, 0, label_visibility="collapsed")

        st.markdown(f"""
        <div style="background:{COLORS['bg']};border:1px solid {COLORS['border']};border-radius:6px;padding:0.75rem 1rem;margin:1rem 0;font-family:{FONT_MONO};font-size:0.72rem;color:{COLORS['text2']}">
        fuel_multiplier         → <span style="color:{COLORS['gold']}">{1 + fuel_pct/100:.2f}</span><br>
        food_price_multiplier   → <span style="color:{COLORS['gold']}">{1 + food_pct/100:.2f}</span><br>
        import_price_multiplier → <span style="color:{COLORS['gold']}">{1 + import_pct/100:.2f}</span>
        </div>
        """, unsafe_allow_html=True)

        if st.button("Apliko Price Shock"):
            try:
                updates = {
                    "fuel_multiplier":         1 + fuel_pct/100,
                    "food_price_multiplier":   1 + food_pct/100,
                    "import_price_multiplier": 1 + import_pct/100,
                }
                for key, val in updates.items():
                    supabase.table("simulation_config").update(
                        {"config_value": round(val, 4)}
                    ).eq("config_key", key).execute()
                st.success("Price shock i aplikuar.")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Error: {e}")

        st.markdown('</div>', unsafe_allow_html=True)

        render_section("Reset Parametrat")
        st.markdown('<div class="control-card">', unsafe_allow_html=True)
        st.markdown(f'<p style="color:{COLORS["text2"]};font-size:0.78rem">Rivendos të gjithë parametrat në vlerat normale (1.0).</p>', unsafe_allow_html=True)

        if st.button("Reset të gjitha parametrat"):
            try:
                defaults = {
                    "demand_multiplier":      1.0,
                    "fuel_multiplier":        1.0,
                    "food_price_multiplier":  1.0,
                    "import_price_multiplier":1.0,
                    "lead_time_multiplier":   1.0,
                    "transport_disruption":   0.0,
                    "active_event":           0.0,
                    "event_intensity":        0.0,
                    "promo_active":           0.0,
                }
                for key, val in defaults.items():
                    supabase.table("simulation_config").update(
                        {"config_value": val}
                    ).eq("config_key", key).execute()
                st.success("Të gjithë parametrat u rivendosën.")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Error: {e}")

        st.markdown('</div>', unsafe_allow_html=True)

# ============================================================
# NAVIGATION CONFIG
# ============================================================
PAGES = [
    {"id": "monitor",    "label": "Monitor",    "icon": "◉", "fn": None},
    {"id": "analytics",  "label": "Analytics",  "icon": "◈", "fn": None},
    {"id": "anomalies",  "label": "Anomalies",  "icon": "◎", "fn": None},
    {"id": "control",    "label": "Control",    "icon": "◐", "fn": None},
]

# ============================================================
# MAIN APP
# ============================================================
def main():
    inject_css()

    # ── Routing via query params ─────────────────────────
    params   = st.query_params
    page_id  = params.get("p", "monitor")
    valid    = [pg["id"] for pg in PAGES]
    if page_id not in valid:
        page_id = "monitor"

    # ── TOP NAVBAR ───────────────────────────────────────
    tabs_html = ""
    for pg in PAGES:
        active = "active" if pg["id"] == page_id else ""
        tabs_html += f"""
        <a class="scm-tab {active}" href="?p={pg['id']}">
            <span class="scm-tab-icon">{pg['icon']}</span>
            <span class="scm-tab-label">{pg['label']}</span>
        </a>"""

    now_str = datetime.now().strftime("%H:%M:%S")
    st.markdown(f"""
    <div class="scm-navbar">
        <div class="scm-logo">
            <div class="scm-logo-mark"></div>
            <span class="scm-logo-text">Supply Chain</span>
        </div>
        <div class="scm-tabs">
            {tabs_html}
        </div>
        <div class="scm-live">
            <div class="scm-live-dot"></div>
            {now_str}
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Router ───────────────────────────────────────────
    if page_id == "monitor":
        page_monitor()
    elif page_id == "analytics":
        page_analytics()
    elif page_id == "anomalies":
        page_anomalies()
    elif page_id == "control":
        page_control()

if __name__ == "__main__":
    main()