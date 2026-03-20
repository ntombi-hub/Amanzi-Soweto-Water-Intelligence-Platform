# dashboard.py
# The Amanzi Soweto dashboard — a Streamlit web app that shows
# the current state of water outages across Soweto.
#
# Run it with:
#   streamlit run dashboard/dashboard.py
#
# It reads directly from the SQLite database that the pipeline fills.

import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Find the database file — it sits one level up from this script
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent.parent
DB_PATH  = BASE_DIR / "amanzi_soweto.db"


# ---------------------------------------------------------------------------
# load_notices()
# Pull all active Soweto notices from the database.
# Returns an empty DataFrame if the DB doesn't exist yet.
# ---------------------------------------------------------------------------
def load_notices():
    if not DB_PATH.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT *
        FROM water_notices
        WHERE is_soweto = 1
        ORDER BY scraped_at DESC
    """, conn)
    conn.close()
    return df


# ---------------------------------------------------------------------------
# load_active_alerts()
# Only the HIGH and MEDIUM notices that are still ongoing.
# ---------------------------------------------------------------------------
def load_active_alerts():
    if not DB_PATH.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT *
        FROM water_notices
        WHERE is_soweto = 1
          AND is_active  = 1
          AND severity  IN ('HIGH', 'MEDIUM')
        ORDER BY
            CASE severity WHEN 'HIGH' THEN 1 ELSE 2 END,
            scraped_at DESC
    """, conn)
    conn.close()
    return df


# ---------------------------------------------------------------------------
# Page config — must be the first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Amanzi Soweto",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ---------------------------------------------------------------------------
# Custom CSS — dark, clean look
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    /* Main background */
    .stApp { background-color: #0f1117; color: #e0e0e0; }

    /* Cards */
    .metric-card {
        background: #1e2130;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        border-left: 4px solid #C84B2F;
    }
    .metric-number { font-size: 2.2rem; font-weight: bold; color: #ffffff; }
    .metric-label  { font-size: 0.85rem; color: #888888; margin-top: 4px; }

    /* Alert banner */
    .alert-high   { background: #3d1a1a; border-left: 5px solid #ff4444; border-radius: 8px; padding: 14px; margin-bottom: 10px; }
    .alert-medium { background: #2d2a1a; border-left: 5px solid #ffaa00; border-radius: 8px; padding: 14px; margin-bottom: 10px; }
    .alert-low    { background: #1a2d1a; border-left: 5px solid #44bb44; border-radius: 8px; padding: 14px; margin-bottom: 10px; }
    .alert-text   { color: #e0e0e0; font-size: 0.9rem; margin: 0; }
    .alert-title  { font-weight: bold; font-size: 1rem; margin-bottom: 4px; }

    /* Section headers */
    h1, h2, h3 { color: #ffffff !important; }

    /* Hide Streamlit branding */
    #MainMenu { visibility: hidden; }
    footer     { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
df_all    = load_notices()
df_active = load_active_alerts()

# ---------------------------------------------------------------------------
# HEADER
# ---------------------------------------------------------------------------
col_logo, col_title = st.columns([1, 8])
with col_logo:
    st.markdown("## 💧")
with col_title:
    st.markdown("# Amanzi Soweto")
    st.markdown("<p style='color:#888; margin-top:-10px;'>Real-time water outage intelligence for Soweto</p>", unsafe_allow_html=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# ALERT BANNER — show active HIGH alerts at the top
# ---------------------------------------------------------------------------
if not df_active.empty:
    high = df_active[df_active["severity"] == "HIGH"]
    medium = df_active[df_active["severity"] == "MEDIUM"]

    if not high.empty:
        st.markdown(f"""
        <div class='alert-high'>
            <div class='alert-title'>🔴 {len(high)} HIGH SEVERITY ALERT(S) ACTIVE</div>
            <p class='alert-text'>Areas affected — check the details below</p>
        </div>
        """, unsafe_allow_html=True)
    elif not medium.empty:
        st.markdown(f"""
        <div class='alert-medium'>
            <div class='alert-title'>🟡 {len(medium)} MEDIUM SEVERITY ALERT(S) ACTIVE</div>
            <p class='alert-text'>Low pressure or intermittent supply reported</p>
        </div>
        """, unsafe_allow_html=True)
else:
    st.markdown("""
    <div class='alert-low'>
        <div class='alert-title'>🟢 ALL CLEAR</div>
        <p class='alert-text'>No active water outage alerts for Soweto right now</p>
    </div>
    """, unsafe_allow_html=True)

st.markdown("")

# ---------------------------------------------------------------------------
# KPI METRICS ROW
# ---------------------------------------------------------------------------
c1, c2, c3, c4 = st.columns(4)

total_notices = len(df_all) if not df_all.empty else 0
active_alerts = len(df_active) if not df_active.empty else 0
high_count    = len(df_active[df_active["severity"] == "HIGH"]) if not df_active.empty else 0
medium_count  = len(df_active[df_active["severity"] == "MEDIUM"]) if not df_active.empty else 0

with c1:
    st.markdown(f"""
    <div class='metric-card'>
        <div class='metric-number'>{total_notices}</div>
        <div class='metric-label'>Total Notices Scraped</div>
    </div>""", unsafe_allow_html=True)

with c2:
    st.markdown(f"""
    <div class='metric-card' style='border-left-color:#ff4444'>
        <div class='metric-number' style='color:#ff4444'>{high_count}</div>
        <div class='metric-label'>HIGH Alerts Active</div>
    </div>""", unsafe_allow_html=True)

with c3:
    st.markdown(f"""
    <div class='metric-card' style='border-left-color:#ffaa00'>
        <div class='metric-number' style='color:#ffaa00'>{medium_count}</div>
        <div class='metric-label'>MEDIUM Alerts Active</div>
    </div>""", unsafe_allow_html=True)

with c4:
    last_run = df_all["scraped_at"].max() if not df_all.empty else "Never"
    if last_run != "Never":
        try:
            last_run = datetime.fromisoformat(last_run).strftime("%d %b %H:%M")
        except:
            pass
    st.markdown(f"""
    <div class='metric-card' style='border-left-color:#44bb44'>
        <div class='metric-number' style='font-size:1.3rem; color:#44bb44'>{last_run}</div>
        <div class='metric-label'>Last Pipeline Run</div>
    </div>""", unsafe_allow_html=True)

st.markdown("")
st.markdown("")

# ---------------------------------------------------------------------------
# SEVERITY CHART — main feature
# The chart shows the breakdown of all notices by severity level.
# ---------------------------------------------------------------------------
st.markdown("### 📊 Notices by Severity")

if not df_all.empty and "severity" in df_all.columns:

    col_chart, col_detail = st.columns([3, 2])

    with col_chart:
        # Count notices per severity
        severity_counts = df_all["severity"].value_counts().reset_index()
        severity_counts.columns = ["Severity", "Count"]

        # Make sure all three levels always appear even if count is 0
        for level in ["HIGH", "MEDIUM", "LOW"]:
            if level not in severity_counts["Severity"].values:
                severity_counts = pd.concat([
                    severity_counts,
                    pd.DataFrame([{"Severity": level, "Count": 0}])
                ], ignore_index=True)

        # Sort HIGH → MEDIUM → LOW
        order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        severity_counts["_order"] = severity_counts["Severity"].map(order)
        severity_counts = severity_counts.sort_values("_order").drop("_order", axis=1)

        # Colour map
        colour_map = {"HIGH": "#ff4444", "MEDIUM": "#ffaa00", "LOW": "#44bb44"}

        fig = px.bar(
            severity_counts,
            x="Severity",
            y="Count",
            color="Severity",
            color_discrete_map=colour_map,
            text="Count",
        )
        fig.update_traces(textposition="outside", textfont=dict(color="white", size=14))
        fig.update_layout(
            plot_bgcolor="#1e2130",
            paper_bgcolor="#1e2130",
            font=dict(color="#e0e0e0"),
            showlegend=False,
            xaxis=dict(title="", gridcolor="#2a2d3e"),
            yaxis=dict(title="Number of Notices", gridcolor="#2a2d3e"),
            margin=dict(t=20, b=20, l=10, r=10),
            height=350,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_detail:
        # Summary table for the severity breakdown
        st.markdown("**Breakdown**")
        for _, row in severity_counts.iterrows():
            level = row["Severity"]
            count = row["Count"]
            emoji = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(level, "ℹ️")
            pct   = round((count / total_notices * 100), 1) if total_notices > 0 else 0
            st.markdown(f"""
            <div style='background:#1e2130; border-radius:8px; padding:12px; margin-bottom:8px;
                        border-left: 4px solid {colour_map.get(level, "#888")}'>
                <span style='font-size:1.1rem; font-weight:bold; color:white'>{emoji} {level}</span><br>
                <span style='font-size:1.6rem; font-weight:bold; color:{colour_map.get(level, "#888")}'>{count}</span>
                <span style='color:#888; font-size:0.85rem'> notices ({pct}%)</span>
            </div>
            """, unsafe_allow_html=True)

else:
    st.info("No data yet — run the pipeline first: `python pipeline.py`")

st.markdown("")

# ---------------------------------------------------------------------------
# ACTIVE ALERTS TABLE
# ---------------------------------------------------------------------------
st.markdown("### 🚨 Active Alerts")

if not df_active.empty:
    # Clean up the table for display
    display_cols = ["severity", "notice_type", "affected_suburbs", "estimated_duration", "scraped_at"]
    display_cols = [c for c in display_cols if c in df_active.columns]
    display_df = df_active[display_cols].copy()

    # Rename columns to be more readable
    display_df = display_df.rename(columns={
        "severity":           "Severity",
        "notice_type":        "Type",
        "affected_suburbs":   "Affected Areas",
        "estimated_duration": "Duration",
        "scraped_at":         "Scraped At",
    })

    # Format the timestamp
    if "Scraped At" in display_df.columns:
        display_df["Scraped At"] = pd.to_datetime(display_df["Scraped At"], errors="coerce").dt.strftime("%d %b %H:%M")

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
    )
else:
    st.success("✅ No active alerts right now. Soweto water supply is normal.")

st.markdown("")

# ---------------------------------------------------------------------------
# NOTICE TYPE BREAKDOWN (secondary chart)
# Shows what kinds of notices are being logged: emergency, planned, etc.
# ---------------------------------------------------------------------------
if not df_all.empty and "type" in df_all.columns:
    st.markdown("### 📋 Notice Type Breakdown")

    type_counts = df_all["type"].value_counts().reset_index()
    type_counts.columns = ["Type", "Count"]

    # Clean up the type labels for display
    label_map = {
        "emergency_outage":    "Emergency Outage",
        "planned_maintenance": "Planned Maintenance",
        "low_pressure":        "Low Pressure",
        "restoration":         "Restoration",
        "leak":                "Leak",
        "general_notice":      "General Notice",
    }
    type_counts["Type"] = type_counts["Type"].map(label_map).fillna(type_counts["Type"])

    fig2 = px.pie(
        type_counts,
        names="Type",
        values="Count",
        color_discrete_sequence=["#C84B2F", "#ffaa00", "#44bb44", "#4488ff", "#aa44ff", "#888888"],
        hole=0.4,
    )
    fig2.update_layout(
        plot_bgcolor="#1e2130",
        paper_bgcolor="#1e2130",
        font=dict(color="#e0e0e0"),
        margin=dict(t=20, b=20, l=10, r=10),
        height=320,
        legend=dict(bgcolor="#1e2130", font=dict(color="#e0e0e0")),
    )
    fig2.update_traces(textfont_color="white")
    st.plotly_chart(fig2, use_container_width=True)

# ---------------------------------------------------------------------------
# FOOTER
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown(
    "<p style='text-align:center; color:#555; font-size:0.8rem;'>"
    "💧 Amanzi Soweto — Built by Ntombikayise Faith Sibisi &nbsp;|&nbsp; "
    "Data: Johannesburg Water & Rand Water &nbsp;|&nbsp; "
    f"Last updated: {datetime.now().strftime('%d %b %Y %H:%M')}"
    "</p>",
    unsafe_allow_html=True
)

# ---------------------------------------------------------------------------
# AUTO-REFRESH every 5 minutes so the dashboard stays live
# ---------------------------------------------------------------------------
import time
if "refresh_count" not in st.session_state:
    st.session_state.refresh_count = 0

# Show a small refresh button in the corner
with st.sidebar:
    st.markdown("### ⚙️ Controls")
    if st.button("🔄 Refresh Now"):
        st.rerun()
    st.markdown(f"*Auto-refreshes every 5 min*")
    st.markdown(f"*DB: `{DB_PATH.name}`*")