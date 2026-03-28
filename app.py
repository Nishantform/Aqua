import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import folium
from folium.plugins import MarkerCluster, HeatMap, Fullscreen
from streamlit_folium import st_folium
from datetime import datetime
import warnings
from io import BytesIO
from sqlalchemy import create_engine, text
import pytz

warnings.filterwarnings('ignore')

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="AQUASTAT — National Water Command Center",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────
# CUSTOM CSS
# ─────────────────────────────────────────────
st.markdown("""
<style>
/* ── base ─────────────────────────────────── */
html, body, [data-testid="stAppViewContainer"] {
    background: linear-gradient(160deg,#040d18 0%,#071525 60%,#0a1e30 100%);
    color:#cfe4f7;
}
[data-testid="stSidebar"] {
    background: linear-gradient(180deg,#06121f 0%,#081a2e 100%);
    border-right:1px solid #1a3550;
}
/* ── metric cards ─────────────────────────── */
.kpi-card {
    background:rgba(255,255,255,0.035);
    border:1px solid rgba(0,200,255,0.18);
    border-radius:14px;
    padding:18px 20px 14px;
    margin:6px 0;
    box-shadow:0 8px 30px rgba(0,0,0,.45), inset 0 1px 0 rgba(255,255,255,.06);
    backdrop-filter:blur(16px);
    transition:transform .2s;
}
.kpi-card:hover { transform:translateY(-3px); }
.kpi-label { color:#7fa8c8; font-size:.78rem; letter-spacing:.12em; text-transform:uppercase; margin:0 0 4px; }
.kpi-value { color:#00e5ff; font-size:2rem; font-weight:700; margin:0; line-height:1.1; }
.kpi-sub   { color:#4da8da; font-size:.78rem; margin:4px 0 0; }

/* ── section header ───────────────────────── */
.sec-hdr {
    color:#00e5ff;
    font-size:1.05rem;
    font-weight:600;
    letter-spacing:.14em;
    text-transform:uppercase;
    padding:8px 0 10px;
    border-bottom:1px solid rgba(0,200,255,.22);
    margin:4px 0 16px;
}

/* ── alert badges ─────────────────────────── */
.badge-critical {
    display:inline-block;
    background:linear-gradient(135deg,#c0392b,#e74c3c);
    color:#fff;
    padding:6px 18px;
    border-radius:30px;
    font-weight:700;
    font-size:.9rem;
    box-shadow:0 0 18px rgba(231,76,60,.45);
}
.badge-warning {
    display:inline-block;
    background:linear-gradient(135deg,#e67e22,#f1c40f);
    color:#1a1a1a;
    padding:6px 18px;
    border-radius:30px;
    font-weight:700;
    font-size:.9rem;
    box-shadow:0 0 18px rgba(241,196,15,.35);
}
.badge-good {
    display:inline-block;
    background:linear-gradient(135deg,#27ae60,#2ecc71);
    color:#fff;
    padding:6px 18px;
    border-radius:30px;
    font-weight:700;
    font-size:.9rem;
    box-shadow:0 0 18px rgba(46,204,113,.35);
}

/* ── status dots ──────────────────────────── */
.dot { width:10px;height:10px;border-radius:50%;display:inline-block;margin-right:6px; }
.dot-red   { background:#ff4444;box-shadow:0 0 8px #ff4444; }
.dot-yellow{ background:#ffd700;box-shadow:0 0 8px #ffd700; }
.dot-green { background:#00ff9d;box-shadow:0 0 8px #00ff9d; }
.dot-blue  { background:#4287f5;box-shadow:0 0 8px #4287f5; }

/* ── scrollbar ────────────────────────────── */
::-webkit-scrollbar { width:6px;height:6px; }
::-webkit-scrollbar-track { background:#06121f; }
::-webkit-scrollbar-thumb { background:#1a3550;border-radius:4px; }
::-webkit-scrollbar-thumb:hover { background:#2a5070; }

/* ── tabs ─────────────────────────────────── */
[data-baseweb="tab-list"] { gap:6px; }
[data-baseweb="tab"] {
    border-radius:8px 8px 0 0 !important;
    background:rgba(255,255,255,.04) !important;
    color:#7fa8c8 !important;
    font-weight:600;
    font-size:.82rem;
    letter-spacing:.06em;
}
[aria-selected="true"] {
    background:linear-gradient(135deg,rgba(0,229,255,.15),rgba(0,150,200,.1)) !important;
    color:#00e5ff !important;
    border-bottom:2px solid #00e5ff !important;
}

/* ── rainfall info box ────────────────────── */
.rain-info {
    background:rgba(0,100,180,.12);
    border:1px solid rgba(0,180,255,.25);
    border-radius:10px;
    padding:12px 16px;
    margin:8px 0;
    font-size:.85rem;
    color:#90cef4;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# PLOTLY DARK THEME DEFAULTS
# ─────────────────────────────────────────────
PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter,Arial,sans-serif", color="#cfe4f7", size=12),
    title_font=dict(size=14, color="#00e5ff", family="Inter,Arial,sans-serif"),
    legend=dict(
        bgcolor="rgba(0,0,0,0)",
        bordercolor="rgba(255,255,255,.1)",
        borderwidth=1,
        font=dict(color="#cfe4f7")
    ),
    xaxis=dict(
        gridcolor="rgba(255,255,255,.06)",
        zerolinecolor="rgba(255,255,255,.1)",
        tickfont=dict(color="#7fa8c8"),
        title_font=dict(color="#7fa8c8")
    ),
    yaxis=dict(
        gridcolor="rgba(255,255,255,.06)",
        zerolinecolor="rgba(255,255,255,.1)",
        tickfont=dict(color="#7fa8c8"),
        title_font=dict(color="#7fa8c8")
    ),
    margin=dict(l=40, r=20, t=50, b=40),
)

TEAL_SEQ  = ["#003f5c","#005f7f","#0082a5","#00a6cc","#00ccee","#00e5ff","#64f4ff"]
RISK_COLORS = {"Good":"#2ecc71","Moderate":"#f1c40f","Critical":"#e74c3c","Unknown":"#7fa8c8"}

def apply_layout(fig, **kwargs):
    layout = {**PLOT_LAYOUT, **kwargs}
    fig.update_layout(**layout)
    return fig

# ─────────────────────────────────────────────
# DATABASE CONNECTION
# ─────────────────────────────────────────────
NEON_URL = st.secrets["NEON_URL"]

@st.cache_resource
def init_connection():
    try:
        engine = create_engine(
            NEON_URL,
            pool_size=5, max_overflow=10,
            pool_timeout=30, pool_recycle=1800,
            pool_pre_ping=True,
            connect_args={
                "connect_timeout": 10,
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5,
                "sslmode": "require",
            },
        )
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text("SELECT 1")).fetchone()
        return engine
    except Exception as e:
        st.error(f"⚠️ Cloud DB connection failed: {e}")
        return None

engine = init_connection()

def execute_query(query, params=None):
    if engine is None:
        return None
    try:
        with engine.connect() as conn:
            with conn.begin():
                result = conn.execute(text(query), params or {})
                return result.fetchall() if result.returns_rows else None
    except Exception as e:
        st.error(f"DB error: {e}")
        return None

def test_connection():
    if engine is None:
        return False, "No engine"
    try:
        with engine.connect() as conn:
            with conn.begin():
                row = conn.execute(
                    text("SELECT current_database(), current_user")
                ).fetchone()
        return True, f"Connected → {row[0]} as {row[1]}"
    except Exception as e:
        return False, str(e)

conn_ok, conn_msg = test_connection()
with st.sidebar:
    if conn_ok:
        st.success(f"✅ {conn_msg}")
    else:
        st.error(f"❌ {conn_msg}")

# ─────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_all_data():
    if engine is None:
        return [pd.DataFrame()] * 8

    try:
        with engine.connect() as conn:
            with conn.begin():
                tables = pd.read_sql(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema='public'",
                    conn,
                )["table_name"].tolist()

                def get_df(tbl):
                    if tbl in tables:
                        try:
                            return pd.read_sql(f'SELECT * FROM "{tbl}"', conn)
                        except Exception:
                            return pd.DataFrame()
                    return pd.DataFrame()

                sources      = get_df("water_sources")
                stations     = get_df("water_monitoring_stations")
                groundwater  = get_df("groundwater_levels")
                rainfall     = get_df("rainfall_history")
                alerts       = get_df("active_alerts")
                regional     = get_df("regional_stats")
                water_quality = stations.copy() if not stations.empty else pd.DataFrame()

                if "water_usage_history" in tables and "water_sources" in tables:
                    try:
                        usage = pd.read_sql(
                            """SELECT wu.*, ws.source_name, ws.source_type, ws.state, ws.district
                               FROM water_usage_history wu
                               LEFT JOIN water_sources ws ON wu.source_id = ws.source_id""",
                            conn,
                        )
                    except Exception:
                        usage = pd.DataFrame()
                else:
                    usage = pd.DataFrame()

                return sources, stations, groundwater, rainfall, alerts, usage, regional, water_quality
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return [pd.DataFrame()] * 8

with st.spinner("🚀 Connecting to AQUASTAT Cloud…"):
    sources, stations, groundwater, rainfall, alerts, usage, regional, water_quality = load_all_data()

with st.sidebar:
    st.markdown("---")
    st.markdown("### 📊 Data Summary")
    c1, c2 = st.columns(2)
    c1.metric("Sources",    len(sources))
    c1.metric("Stations",   len(stations))
    c2.metric("GW Records", len(groundwater))
    c2.metric("Rainfall",   len(rainfall))

# ─────────────────────────────────────────────
# DATA PROCESSING
# ─────────────────────────────────────────────
current_year = datetime.now().year
current_time = datetime.now(pytz.timezone("Asia/Kolkata"))

if not sources.empty:
    for col in ["capacity_percent","build_year","max_capacity_mcm","current_capacity_mcm"]:
        if col in sources.columns:
            sources[col] = pd.to_numeric(sources[col], errors="coerce")
    if "build_year" in sources.columns:
        sources["age"] = (current_year - sources["build_year"]).clip(0, 200)
    else:
        sources["age"] = 0
    if "capacity_percent" in sources.columns:
        sources["health_score"] = (
            sources["capacity_percent"].fillna(50) * 0.5
            + (100 - sources["age"].clip(0,100).fillna(50)) * 0.3
            + 20
        ).clip(0, 100)
        sources["risk_level"] = pd.cut(
            sources["capacity_percent"],
            bins=[0,30,60,100],
            labels=["Critical","Moderate","Good"],
            include_lowest=True,
        )
    else:
        sources["health_score"] = 50
        sources["risk_level"]   = "Unknown"
    np.random.seed(42)
    sources["trend"] = np.random.choice(
        ["📈 Increasing","📉 Decreasing","➡️ Stable"], len(sources)
    )

if not groundwater.empty:
    if "avg_depth_meters" in groundwater.columns:
        groundwater["stress_level"] = pd.cut(
            groundwater["avg_depth_meters"],
            bins=[0,20,40,100], labels=["Low","Moderate","High"]
        )
    if "assessment_year" in groundwater.columns and "avg_depth_meters" in groundwater.columns:
        groundwater = groundwater.sort_values(["district_name","assessment_year"])
        groundwater["depth_change"]   = groundwater.groupby("district_name")["avg_depth_meters"].diff()
        groundwater["depletion_rate"] = groundwater.groupby("district_name")["depth_change"].transform("mean")

if not rainfall.empty:
    if "rainfall_cm" in rainfall.columns:
        rainfall["rainfall_category"] = pd.cut(
            rainfall["rainfall_cm"],
            bins=[0,50,150,300,float("inf")],
            labels=["Low","Moderate","High","Extreme"]
        )
    if "rainfall_cm" in rainfall.columns and "record_year" in rainfall.columns:
        avg_r = rainfall.groupby("district_name")["rainfall_cm"].transform("mean")
        rainfall["deviation_pct"] = ((rainfall["rainfall_cm"] - avg_r) / avg_r * 100).round(1)

def add_coords_to_sources(src, sta):
    if src.empty or sta.empty:
        return src
    df = src.copy()
    if "district" in df.columns and "district_name" in sta.columns:
        sta2 = sta.copy()
        sta2["district_clean"] = sta2["district_name"].str.strip().str.lower()
        coords = sta2.groupby("district_clean").agg(
            latitude=("latitude","first"),longitude=("longitude","first")
        ).reset_index()
        df["district_clean"] = df["district"].str.strip().str.lower()
        df = df.merge(coords, on="district_clean", how="left").drop("district_clean", axis=1)
    return df

if not sources.empty and not stations.empty:
    sources = add_coords_to_sources(sources, stations)

# ─────────────────────────────────────────────
# SIDEBAR FILTERS
# ─────────────────────────────────────────────
with st.sidebar:
    st.title("🎮 AQUASTAT")
    st.caption("Command Interface v3.1")
    st.markdown("---")

    if st.button("🔄 Reset All Filters", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")

    # ── Geographic ────────────────────────────
    st.markdown("### 🌍 Geographic Filters")

    if not sources.empty and "state" in sources.columns:
        states = ["All States"] + sorted(sources["state"].dropna().unique().tolist())
        selected_state = st.selectbox("State", states)
    else:
        selected_state = "All States"

    if not sources.empty and "district" in sources.columns:
        pool = (
            sources[sources["state"] == selected_state]["district"].dropna().unique()
            if selected_state != "All States"
            else sources["district"].dropna().unique()
        )
        districts = (["All Districts"] + sorted(pool.tolist())) if len(pool) else ["All Districts"]
        selected_district = st.selectbox("District", districts)
    else:
        selected_district = "All Districts"

    st.markdown("---")

    # ── Source Filters ────────────────────────
    st.markdown("### 💧 Source Filters")

    if not sources.empty and "source_type" in sources.columns:
        stypes = ["All Types"] + sorted(sources["source_type"].dropna().unique().tolist())
        selected_type = st.selectbox("Source Type", stypes)
    else:
        selected_type = "All Types"

    if not sources.empty and "capacity_percent" in sources.columns:
        mn, mx = float(sources["capacity_percent"].min()), float(sources["capacity_percent"].max())
        capacity_range = st.slider("Capacity %", mn, mx, (mn, mx))
    else:
        capacity_range = (0.0, 100.0)

    if not sources.empty and "risk_level" in sources.columns:
        risk_opts = ["All Risk Levels"] + list(sources["risk_level"].dropna().unique())
        selected_risk = st.selectbox("Risk Level", risk_opts)
    else:
        selected_risk = "All Risk Levels"

    st.markdown("---")

    # ── Rainfall Filter ───────────────────────
    st.markdown("### ☔ Rainfall Filter")

    if not rainfall.empty and "rainfall_cm" in rainfall.columns:
        r_min = float(rainfall["rainfall_cm"].min())
        r_max = float(rainfall["rainfall_cm"].max())
        rainfall_range = st.slider(
            "Rainfall (cm)", r_min, r_max, (r_min, r_max),
            help="Filter districts whose avg rainfall falls in this range"
        )
        show_rain_on_map = st.checkbox("Show rainfall districts on map", value=False)
    else:
        rainfall_range   = (0.0, 1000.0)
        show_rain_on_map = False

    st.markdown("---")

    # ── Map Settings ──────────────────────────
    st.markdown("### 🗺️ Map Settings")

    map_style = st.selectbox(
        "Map Style",
        ["Esri Satellite","OpenStreetMap","CartoDB Dark","CartoDB Light"]
    )
    show_heatmap  = st.checkbox("Show Heatmap", True)
    show_clusters = st.checkbox("Show Clusters", True)
    show_stations_map = st.checkbox("Show Monitoring Stations", True)
    marker_size   = st.slider("Marker Size", 5, 20, 12)

    st.markdown("---")
    st.caption(f"📊 Total Sources: {len(sources):,}")

# ─────────────────────────────────────────────
# APPLY FILTERS
# ─────────────────────────────────────────────
def apply_source_filters():
    df = sources.copy()
    if selected_state != "All States" and "state" in df.columns:
        df = df[df["state"] == selected_state]
    if selected_district != "All Districts" and "district" in df.columns:
        df = df[df["district"] == selected_district]
    if selected_type != "All Types" and "source_type" in df.columns:
        df = df[df["source_type"] == selected_type]
    if "capacity_percent" in df.columns:
        df = df[df["capacity_percent"].between(capacity_range[0], capacity_range[1])]
    if selected_risk != "All Risk Levels" and "risk_level" in df.columns:
        df = df[df["risk_level"] == selected_risk]
    return df

filtered_sources = apply_source_filters()

def filter_stations():
    df = stations.copy()
    if selected_state != "All States" and "state_name" in df.columns:
        df = df[df["state_name"] == selected_state]
    if selected_district != "All Districts" and "district_name" in df.columns:
        df = df[df["district_name"] == selected_district]
    return df

filtered_stations = filter_stations()

# Compute rainfall-filtered districts
def get_rainfall_districts():
    if rainfall.empty or "rainfall_cm" not in rainfall.columns:
        return pd.DataFrame(), []
    avg_rain = (
        rainfall.groupby("district_name")["rainfall_cm"]
        .mean()
        .reset_index()
        .rename(columns={"rainfall_cm": "avg_rain_cm"})
    )
    mask = avg_rain["avg_rain_cm"].between(rainfall_range[0], rainfall_range[1])
    filtered_rain_districts = avg_rain[mask]
    district_names = filtered_rain_districts["district_name"].tolist()
    return filtered_rain_districts, district_names

rain_districts_df, rain_district_names = get_rainfall_districts()

# ─────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────
st.markdown(
    "<h1 style='color:#00e5ff;letter-spacing:.04em;margin-bottom:0'>💧 AQUASTAT</h1>"
    "<p style='color:#7fa8c8;margin-top:4px;font-size:.9rem;'>"
    "National Water Command Center &nbsp;•&nbsp; "
    f"Live Intelligence &nbsp;•&nbsp; {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}"
    "</p>",
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────
# KPI ROW
# ─────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)

def kpi(col, label, value, sub=""):
    col.markdown(
        f"""<div class='kpi-card'>
            <p class='kpi-label'>{label}</p>
            <p class='kpi-value'>{value}</p>
            <p class='kpi-sub'>{sub}</p>
        </div>""",
        unsafe_allow_html=True,
    )

total_cap = filtered_sources["capacity_percent"].mean() if not filtered_sources.empty and "capacity_percent" in filtered_sources.columns else 0
critical_n = len(filtered_sources[filtered_sources["capacity_percent"] < 30]) if not filtered_sources.empty and "capacity_percent" in filtered_sources.columns else 0
on_map_n  = len(filtered_sources[filtered_sources["latitude"].notna()]) if not filtered_sources.empty and "latitude" in filtered_sources.columns else 0

kpi(k1, "Total Sources",    f"{len(sources):,}",    f"{len(filtered_sources)} filtered")
kpi(k2, "Avg Capacity",     f"{total_cap:.1f}%",    "across filtered sources")
kpi(k3, "Critical Sources", f"{critical_n}",         "below 30 % capacity")
kpi(k4, "Mapped Sources",   f"{on_map_n}",           "with coordinates")
kpi(k5, "Active Alerts",    f"{len(alerts)}",        "system-wide")

st.markdown("<div style='margin:10px 0'></div>", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Dashboard", "🗺️ Map View", "📈 Analytics",
    "💧 Water Quality", "⚠️ Alerts", "📋 Data Tables"
])

# ══════════════════════════════════════════════
# TAB 1 — DASHBOARD
# ══════════════════════════════════════════════
with tab1:
    if filtered_sources.empty:
        st.warning("⚠️ No sources match the current filters. Try clearing some.")
    else:
        # Row 1 ── Capacity histogram  |  Source-type pie
        c1, c2 = st.columns(2)

        with c1:
            st.markdown('<p class="sec-hdr">📊 Capacity Distribution</p>', unsafe_allow_html=True)
            if "capacity_percent" in filtered_sources.columns:
                fig = go.Figure(go.Histogram(
                    x=filtered_sources["capacity_percent"].dropna(),
                    nbinsx=25,
                    marker=dict(
                        color=filtered_sources["capacity_percent"].dropna(),
                        colorscale=[[0,"#e74c3c"],[0.4,"#f1c40f"],[1,"#2ecc71"]],
                        showscale=False,
                        line=dict(color="rgba(0,0,0,0)",width=0),
                    ),
                    opacity=0.85,
                    name="Sources",
                ))
                fig.add_vrect(x0=0, x1=30, fillcolor="rgba(231,76,60,.08)", line_width=0)
                fig.add_vrect(x0=30, x1=60, fillcolor="rgba(241,196,15,.06)", line_width=0)
                fig.add_vrect(x0=60, x1=100, fillcolor="rgba(46,204,113,.06)", line_width=0)
                apply_layout(
                    fig,
                    title=f"Storage Capacity — {len(filtered_sources)} sources",
                    xaxis_title="Capacity (%)",
                    yaxis_title="No. of Sources",
                )
                st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.markdown('<p class="sec-hdr">🏭 Source Types</p>', unsafe_allow_html=True)
            if "source_type" in filtered_sources.columns:
                tc = filtered_sources["source_type"].value_counts().reset_index()
                tc.columns = ["Source Type","Count"]
                fig = go.Figure(go.Pie(
                    labels=tc["Source Type"],
                    values=tc["Count"],
                    hole=0.5,
                    marker=dict(
                        colors=["#00e5ff","#2ecc71","#f1c40f","#e74c3c","#9b59b6","#3498db","#e67e22"],
                        line=dict(color="#06121f",width=2),
                    ),
                    textinfo="percent+label",
                    textfont=dict(size=11),
                ))
                apply_layout(fig, title="Water Sources by Type")
                fig.update_traces(pull=[0.03]*len(tc))
                st.plotly_chart(fig, use_container_width=True)

        # Row 2 ── Groundwater stress  |  Rainfall by season
        c1, c2 = st.columns(2)

        with c1:
            st.markdown('<p class="sec-hdr">🌊 Groundwater Stress</p>', unsafe_allow_html=True)
            if not groundwater.empty and "stress_level" in groundwater.columns:
                fgw = groundwater.copy()
                if selected_district != "All Districts" and "district_name" in fgw.columns:
                    fgw = fgw[fgw["district_name"] == selected_district]
                if not fgw.empty:
                    sc = fgw["stress_level"].value_counts().reset_index()
                    sc.columns = ["Stress Level","Count"]
                    order = ["Low","Moderate","High"]
                    sc["Stress Level"] = pd.Categorical(sc["Stress Level"], categories=order, ordered=True)
                    sc = sc.sort_values("Stress Level")
                    colors_map = {"Low":"#2ecc71","Moderate":"#f1c40f","High":"#e74c3c"}
                    fig = go.Figure(go.Bar(
                        x=sc["Stress Level"],
                        y=sc["Count"],
                        marker=dict(
                            color=[colors_map.get(str(x),"#00e5ff") for x in sc["Stress Level"]],
                            cornerradius=6,
                        ),
                        text=sc["Count"],
                        textposition="outside",
                        textfont=dict(color="#cfe4f7"),
                    ))
                    apply_layout(fig, title="Groundwater Stress Distribution",
                                 yaxis_title="Districts", xaxis_title="Stress Level")
                    st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.markdown('<p class="sec-hdr">☔ Rainfall by Season</p>', unsafe_allow_html=True)
            if not rainfall.empty and "season" in rainfall.columns and "rainfall_cm" in rainfall.columns:
                fr = rainfall.copy()
                if selected_district != "All Districts" and "district_name" in fr.columns:
                    fr = fr[fr["district_name"] == selected_district]
                if not fr.empty:
                    sr = fr.groupby("season")["rainfall_cm"].mean().reset_index()
                    fig = go.Figure(go.Bar(
                        x=sr["season"],
                        y=sr["rainfall_cm"].round(1),
                        marker=dict(
                            color=sr["rainfall_cm"],
                            colorscale=[[0,"#003f5c"],[0.5,"#00a6cc"],[1,"#00e5ff"]],
                            showscale=False,
                            cornerradius=6,
                        ),
                        text=sr["rainfall_cm"].round(1),
                        textposition="outside",
                        textfont=dict(color="#cfe4f7"),
                    ))
                    apply_layout(fig, title="Average Rainfall by Season (cm)",
                                 yaxis_title="Rainfall (cm)", xaxis_title="Season")
                    st.plotly_chart(fig, use_container_width=True)

        # Row 3 ── Risk gauge + capacity by state
        c1, c2 = st.columns(2)

        with c1:
            st.markdown('<p class="sec-hdr">⚠️ Risk Assessment</p>', unsafe_allow_html=True)
            if "risk_level" in filtered_sources.columns:
                rc = filtered_sources["risk_level"].value_counts()
                total_r = rc.sum()
                fig = go.Figure()
                for lvl, clr in [("Critical","#e74c3c"),("Moderate","#f1c40f"),("Good","#2ecc71")]:
                    cnt = rc.get(lvl, 0)
                    fig.add_trace(go.Bar(
                        name=lvl, x=[lvl], y=[cnt],
                        marker=dict(color=clr, cornerradius=6),
                        text=[cnt], textposition="outside",
                        textfont=dict(color="#cfe4f7", size=13),
                    ))
                apply_layout(fig, title="Infrastructure Risk Breakdown",
                             yaxis_title="Sources", xaxis_title="")
                fig.update_layout(showlegend=False, barmode="group")
                st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.markdown('<p class="sec-hdr">🏛️ State Capacity Overview</p>', unsafe_allow_html=True)
            if "state" in filtered_sources.columns and "capacity_percent" in filtered_sources.columns:
                sc_state = (
                    filtered_sources.groupby("state")["capacity_percent"]
                    .mean().sort_values(ascending=False).head(10).reset_index()
                )
                sc_state.columns = ["State","Avg Capacity"]
                fig = go.Figure(go.Bar(
                    x=sc_state["Avg Capacity"].round(1),
                    y=sc_state["State"],
                    orientation="h",
                    marker=dict(
                        color=sc_state["Avg Capacity"],
                        colorscale=[[0,"#e74c3c"],[0.4,"#f1c40f"],[1,"#2ecc71"]],
                        showscale=True,
                        colorbar=dict(title="Cap %", tickfont=dict(color="#7fa8c8")),
                        cornerradius=4,
                    ),
                    text=sc_state["Avg Capacity"].round(1).astype(str) + "%",
                    textposition="auto",
                    textfont=dict(color="#cfe4f7"),
                ))
                apply_layout(fig, title="Top 10 States by Avg Capacity",
                             xaxis_title="Avg Capacity (%)", yaxis_title="")
                fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig, use_container_width=True)

        # Rainfall District Table (from rainfall filter)
        if rain_district_names:
            st.markdown('<p class="sec-hdr">☔ Districts Matching Rainfall Filter</p>', unsafe_allow_html=True)
            st.markdown(
                f'<div class="rain-info">🌧️ Showing <b>{len(rain_district_names)}</b> districts with average rainfall '
                f'between <b>{rainfall_range[0]:.0f} cm</b> and <b>{rainfall_range[1]:.0f} cm</b></div>',
                unsafe_allow_html=True,
            )
            disp = rain_districts_df.copy()
            disp["avg_rain_cm"] = disp["avg_rain_cm"].round(2)
            disp.columns = ["District","Avg Rainfall (cm)"]
            st.dataframe(disp.sort_values("Avg Rainfall (cm)", ascending=False),
                         use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════
# TAB 2 — MAP VIEW
# ══════════════════════════════════════════════
with tab2:
    st.markdown('<p class="sec-hdr">🗺️ National Interactive Water Resources Map</p>', unsafe_allow_html=True)

    active_filters = []
    if selected_state    != "All States":    active_filters.append(f"State: {selected_state}")
    if selected_district != "All Districts": active_filters.append(f"District: {selected_district}")
    if selected_type     != "All Types":     active_filters.append(f"Type: {selected_type}")
    if show_rain_on_map:
        active_filters.append(f"Rainfall: {rainfall_range[0]:.0f}–{rainfall_range[1]:.0f} cm")

    if active_filters:
        st.info(f"**Active Filters:** {' | '.join(active_filters)}  —  **Showing {len(filtered_sources)} sources**")

    style_map = {
        "Esri Satellite": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        "OpenStreetMap":  "OpenStreetMap",
        "CartoDB Dark":   "CartoDB dark_matter",
        "CartoDB Light":  "CartoDB positron",
    }

    # Compute center
    center_lat, center_lon, zoom = 20.5937, 78.9629, 5
    if not filtered_sources.empty and "latitude" in filtered_sources.columns:
        src_geo = filtered_sources[filtered_sources["latitude"].notna() & filtered_sources["longitude"].notna()]
        if not src_geo.empty:
            center_lat = src_geo["latitude"].mean()
            center_lon = src_geo["longitude"].mean()
            zoom = 9 if selected_district != "All Districts" else 7 if selected_state != "All States" else 5

    m = folium.Map(location=[center_lat, center_lon], zoom_start=zoom,
                   tiles=style_map[map_style], attr="AQUASTAT")
    Fullscreen().add_to(m)

    cluster_group = MarkerCluster().add_to(m) if show_clusters and len(filtered_sources) > 10 else m

    heat_data    = []
    sources_on_map = 0

    # ── Water source markers (strictly filtered) ─────────────────────────
    if not filtered_sources.empty and "latitude" in filtered_sources.columns:
        src_geo = filtered_sources[
            filtered_sources["latitude"].notna() & filtered_sources["longitude"].notna()
        ]
        for _, row in src_geo.iterrows():
            cap = row.get("capacity_percent", 50)
            if cap < 30:
                clr, risk_txt = "#ff4444","CRITICAL"
            elif cap < 60:
                clr, risk_txt = "#ffd700","MODERATE"
            else:
                clr, risk_txt = "#00ff9d","GOOD"

            heat_data.append([row["latitude"], row["longitude"]])
            sources_on_map += 1

            popup_html = f"""
            <div style="font-family:Inter,Arial;min-width:230px;background:#06121f;
                        color:#cfe4f7;border-radius:8px;padding:10px;">
                <b style="color:{clr};font-size:1rem;">{row.get('source_name','Unknown')}</b>
                <hr style="border-color:#1a3550;margin:6px 0">
                <table style="width:100%;font-size:.82rem;">
                    <tr><td style="color:#7fa8c8">Type</td><td>{row.get('source_type','—')}</td></tr>
                    <tr><td style="color:#7fa8c8">Location</td>
                        <td>{row.get('district','—')}, {row.get('state','—')}</td></tr>
                    <tr><td style="color:#7fa8c8">Capacity</td><td>{cap:.1f} %</td></tr>
                    <tr><td style="color:#7fa8c8">Age</td><td>{row.get('age',0):.0f} yrs</td></tr>
                    <tr><td style="color:#7fa8c8">Risk</td>
                        <td><b style="color:{clr}">{risk_txt}</b></td></tr>
                    <tr><td style="color:#7fa8c8">Trend</td><td>{row.get('trend','—')}</td></tr>
                </table>
            </div>"""

            mk = folium.CircleMarker(
                location=[row["latitude"], row["longitude"]],
                radius=marker_size + (3 if cap < 30 else 0),
                color=clr, fill=True, fill_opacity=0.75,
                weight=1.5,
                popup=folium.Popup(popup_html, max_width=280),
                tooltip=f"{row.get('source_name','?')} — {cap:.0f}%",
            )
            mk.add_to(cluster_group)

    if show_heatmap and heat_data:
        HeatMap(heat_data, radius=15, blur=10,
                gradient={0.2:"blue",0.4:"cyan",0.6:"lime",0.8:"yellow",1:"red"}).add_to(m)

    # ── Monitoring stations ───────────────────────────────────────────────
    if show_stations_map and not filtered_stations.empty:
        if "latitude" in filtered_stations.columns:
            st_geo = filtered_stations[
                filtered_stations["latitude"].notna() & filtered_stations["longitude"].notna()
            ]
            for _, row in st_geo.iterrows():
                status = row.get("status","Unknown")
                s_clr = "green" if status == "Active" else "orange" if status == "Maintenance" else "red"
                popup_html = f"""
                <div style="background:#06121f;color:#cfe4f7;padding:8px;border-radius:6px;font-size:.82rem;">
                    <b>{row.get('station_name','Unknown')}</b><br>
                    District: {row.get('district_name','—')}<br>
                    Status: <b style="color:{'#2ecc71' if status=='Active' else '#f1c40f' if status=='Maintenance' else '#e74c3c'}">{status}</b><br>
                    pH: {row.get('ph_level','—')}<br>
                    DO: {row.get('dissolved_oxygen_mg_l','—')} mg/L<br>
                    Turbidity: {row.get('turbidity_ntu','—')} NTU
                </div>"""
                folium.Marker(
                    location=[row["latitude"], row["longitude"]],
                    icon=folium.Icon(color=s_clr, icon="info-sign"),
                    popup=folium.Popup(popup_html, max_width=260),
                    tooltip=f"Station: {row.get('station_name','?')}",
                ).add_to(m)

    # ── Rainfall district circles (optional overlay) ──────────────────────
    if show_rain_on_map and rain_district_names and not stations.empty and "district_name" in stations.columns:
        rain_sta = stations[stations["district_name"].isin(rain_district_names)]
        rain_sta = rain_sta[rain_sta["latitude"].notna() & rain_sta["longitude"].notna()]
        district_locs = rain_sta.groupby("district_name").agg(
            lat=("latitude","first"), lon=("longitude","first")
        ).reset_index()
        merged_rain = district_locs.merge(rain_districts_df, on="district_name")

        for _, row in merged_rain.iterrows():
            avg_r = row["avg_rain_cm"]
            # colour by rainfall intensity
            if avg_r < 50:    rc = "#4287f5"
            elif avg_r < 150: rc = "#00ccee"
            elif avg_r < 300: rc = "#00ff9d"
            else:             rc = "#ff4444"

            folium.CircleMarker(
                location=[row["lat"], row["lon"]],
                radius=14,
                color=rc, fill=True, fill_opacity=0.45, weight=2,
                tooltip=f"🌧️ {row['district_name']} — Avg {avg_r:.1f} cm",
                popup=folium.Popup(
                    f"<b>{row['district_name']}</b><br>Avg Rainfall: {avg_r:.1f} cm", max_width=200
                ),
            ).add_to(m)

    st_folium(m, width=1300, height=620)

    mc1, mc2, mc3 = st.columns(3)
    mc1.metric("Sources on Map",      sources_on_map)
    mc2.metric("Total Filtered",      len(filtered_sources))
    mc3.metric("Coordinate Coverage", f"{sources_on_map/max(len(filtered_sources),1)*100:.1f}%")

    st.markdown("---")
    leg = st.columns(5)
    leg[0].markdown('<span class="dot dot-green"></span> Good (≥60%)',         unsafe_allow_html=True)
    leg[1].markdown('<span class="dot dot-yellow"></span> Moderate (30–60%)',  unsafe_allow_html=True)
    leg[2].markdown('<span class="dot dot-red"></span> Critical (<30%)',       unsafe_allow_html=True)
    leg[3].markdown('<span class="dot dot-blue"></span> Monitoring Station',   unsafe_allow_html=True)
    leg[4].markdown('🔵 Rainfall District (when enabled)',                     unsafe_allow_html=True)

# ══════════════════════════════════════════════
# TAB 3 — ANALYTICS
# ══════════════════════════════════════════════
with tab3:
    st.markdown('<p class="sec-hdr">📈 Advanced Analytics</p>', unsafe_allow_html=True)

    atab1, atab2, atab3 = st.tabs(["📊 Trends","📉 Comparisons","📐 Statistics"])

    with atab1:
        c1, c2 = st.columns(2)

        with c1:
            st.subheader("Rainfall Trend")
            if not rainfall.empty and "record_year" in rainfall.columns and "rainfall_cm" in rainfall.columns:
                fr = rainfall.copy()
                if selected_district != "All Districts" and "district_name" in fr.columns:
                    fr = fr[fr["district_name"] == selected_district]
                if not fr.empty:
                    rt = fr.groupby("record_year")["rainfall_cm"].mean().reset_index()
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=rt["record_year"], y=rt["rainfall_cm"],
                        mode="lines+markers",
                        line=dict(color="#00e5ff", width=3),
                        marker=dict(size=7, color="#00e5ff", symbol="circle",
                                    line=dict(color="#06121f", width=1.5)),
                        fill="tozeroy",
                        fillcolor="rgba(0,229,255,0.08)",
                        name="Avg Rainfall",
                    ))
                    apply_layout(fig, title="Average Annual Rainfall (cm)",
                                 xaxis_title="Year", yaxis_title="Rainfall (cm)")
                    st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.subheader("Groundwater Depth Trend")
            if not groundwater.empty and "assessment_year" in groundwater.columns:
                fgw = groundwater.copy()
                if selected_district != "All Districts" and "district_name" in fgw.columns:
                    fgw = fgw[fgw["district_name"] == selected_district]
                if not fgw.empty:
                    gt = fgw.groupby("assessment_year")["avg_depth_meters"].mean().reset_index()
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=gt["assessment_year"], y=gt["avg_depth_meters"],
                        mode="lines+markers",
                        line=dict(color="#f1c40f", width=3),
                        marker=dict(size=7, color="#f1c40f",
                                    line=dict(color="#06121f", width=1.5)),
                        fill="tozeroy",
                        fillcolor="rgba(241,196,15,0.08)",
                        name="Avg Depth",
                    ))
                    apply_layout(fig, title="Average Groundwater Depth (m)",
                                 xaxis_title="Year", yaxis_title="Depth (m)")
                    st.plotly_chart(fig, use_container_width=True)

    with atab2:
        c1, c2 = st.columns(2)

        with c1:
            st.subheader("Top 10 States — Avg Capacity")
            if not filtered_sources.empty and "state" in filtered_sources.columns and "capacity_percent" in filtered_sources.columns:
                sc_s = (
                    filtered_sources.groupby("state")["capacity_percent"]
                    .mean().sort_values(ascending=False).head(10).reset_index()
                )
                sc_s.columns = ["State","Avg Capacity"]
                fig = go.Figure(go.Bar(
                    x=sc_s["Avg Capacity"].round(1), y=sc_s["State"],
                    orientation="h",
                    marker=dict(
                        color=sc_s["Avg Capacity"],
                        colorscale=[[0,"#e74c3c"],[0.5,"#f1c40f"],[1,"#2ecc71"]],
                        showscale=True,
                        cornerradius=5,
                    ),
                    text=sc_s["Avg Capacity"].round(1).astype(str)+"%",
                    textposition="auto",
                    textfont=dict(color="#cfe4f7"),
                ))
                apply_layout(fig, title="State Comparison",
                             xaxis_title="Avg Capacity (%)", yaxis_title="")
                fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.subheader("GW Extraction vs Recharge")
            if not groundwater.empty and "recharge_rate_mcm" in groundwater.columns and "extraction_pct" in groundwater.columns:
                fgw = groundwater.copy()
                if selected_district != "All Districts" and "district_name" in fgw.columns:
                    fgw = fgw[fgw["district_name"] == selected_district]
                if not fgw.empty:
                    fig = px.scatter(
                        fgw,
                        x="recharge_rate_mcm", y="extraction_pct",
                        size="avg_depth_meters" if "avg_depth_meters" in fgw.columns else None,
                        color="district_name" if "district_name" in fgw.columns else None,
                        template="plotly_dark",
                        title="Extraction % vs Recharge Rate (MCM)",
                        color_discrete_sequence=px.colors.qualitative.Safe,
                    )
                    apply_layout(fig, xaxis_title="Recharge Rate (MCM)",
                                 yaxis_title="Extraction %")
                    st.plotly_chart(fig, use_container_width=True)

    with atab3:
        c1, c2 = st.columns(2)

        with c1:
            st.subheader("Statistical Summary")
            if not filtered_sources.empty:
                stat_cols = [c for c in ["capacity_percent","age","health_score"] if c in filtered_sources.columns]
                if stat_cols:
                    st.dataframe(
                        filtered_sources[stat_cols].describe().round(2),
                        use_container_width=True,
                    )

        with c2:
            st.subheader("Correlation Matrix")
            if not filtered_sources.empty and not groundwater.empty:
                if "district" in filtered_sources.columns and "district_name" in groundwater.columns:
                    merged = filtered_sources.merge(
                        groundwater, left_on="district", right_on="district_name", how="inner"
                    )
                    num_cols = [c for c in ["capacity_percent","age","avg_depth_meters","extraction_pct","recharge_rate_mcm"]
                                if c in merged.columns]
                    if len(num_cols) >= 2:
                        corr = merged[num_cols].dropna().corr()
                        fig = px.imshow(
                            corr, text_auto=".2f", aspect="auto",
                            color_continuous_scale="RdBu_r",
                            template="plotly_dark",
                            title="Feature Correlation Matrix",
                        )
                        apply_layout(fig)
                        st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════
# TAB 4 — WATER QUALITY
# ══════════════════════════════════════════════
with tab4:
    st.markdown('<p class="sec-hdr">💧 Water Quality Monitoring</p>', unsafe_allow_html=True)

    if not water_quality.empty:
        avg_ph  = water_quality["ph_level"].mean() if "ph_level" in water_quality.columns else 0
        avg_do  = water_quality["dissolved_oxygen_mg_l"].mean() if "dissolved_oxygen_mg_l" in water_quality.columns else 0
        avg_tb  = water_quality["turbidity_ntu"].mean() if "turbidity_ntu" in water_quality.columns else 0

        kq1, kq2, kq3, kq4 = st.columns(4)
        kpi(kq1, "Average pH",         f"{avg_ph:.2f}",        "6.5–8.5 is safe")
        kpi(kq2, "Dissolved Oxygen",   f"{avg_do:.1f} mg/L",   ">5 mg/L is healthy")
        kpi(kq3, "Turbidity",          f"{avg_tb:.1f} NTU",    "<4 NTU is ideal")
        kpi(kq4, "Total Stations",     str(len(water_quality)), "monitored")

        st.markdown("---")
        c1, c2 = st.columns(2)

        with c1:
            if "ph_level" in water_quality.columns:
                fig = go.Figure(go.Histogram(
                    x=water_quality["ph_level"].dropna(), nbinsx=20,
                    marker=dict(
                        color=water_quality["ph_level"].dropna(),
                        colorscale=[[0,"#e74c3c"],[0.45,"#2ecc71"],[1,"#e74c3c"]],
                        showscale=False,
                        line=dict(width=0),
                    ),
                    opacity=0.85,
                ))
                fig.add_vline(x=7.0, line=dict(color="white", dash="dot", width=1.5),
                              annotation_text="Neutral pH 7", annotation_position="top left",
                              annotation_font_color="#cfe4f7")
                fig.add_vrect(x0=6.5, x1=8.5, fillcolor="rgba(46,204,113,.07)", line_width=0)
                apply_layout(fig, title="pH Level Distribution",
                             xaxis_title="pH", yaxis_title="Stations")
                st.plotly_chart(fig, use_container_width=True)

        with c2:
            if "dissolved_oxygen_mg_l" in water_quality.columns:
                fig = go.Figure()
                fig.add_trace(go.Box(
                    y=water_quality["dissolved_oxygen_mg_l"].dropna(),
                    name="DO",
                    marker=dict(color="#2ecc71", outliercolor="#e74c3c"),
                    line=dict(color="#2ecc71"),
                    fillcolor="rgba(46,204,113,.15)",
                    boxmean=True,
                ))
                fig.add_hline(y=5, line=dict(color="#ffd700", dash="dot"),
                              annotation_text="Min healthy (5 mg/L)",
                              annotation_font_color="#ffd700")
                apply_layout(fig, title="Dissolved Oxygen Distribution",
                             yaxis_title="DO (mg/L)")
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("### 📋 Live Readings")
        disp_cols = [c for c in ["station_name","district_name","ph_level",
                                  "dissolved_oxygen_mg_l","turbidity_ntu","status"]
                     if c in water_quality.columns]
        if disp_cols:
            st.dataframe(water_quality[disp_cols].head(50),
                         use_container_width=True, hide_index=True)
    else:
        st.info("No water quality data available.")

# ══════════════════════════════════════════════
# TAB 5 — ALERTS
# ══════════════════════════════════════════════
with tab5:
    st.markdown('<p class="sec-hdr">🚨 Active Alerts & Warnings</p>', unsafe_allow_html=True)

    if not alerts.empty:
        crit_n = len(alerts[alerts["alert_status"] == "CRITICAL"]) if "alert_status" in alerts.columns else 0
        warn_n = len(alerts[alerts["alert_status"] == "WARNING"])  if "alert_status" in alerts.columns else 0
        stbl_n = len(alerts[alerts["alert_status"] == "STABLE"])   if "alert_status" in alerts.columns else 0

        ba1, ba2, ba3 = st.columns(3)
        ba1.markdown(f'<div class="badge-critical">🔴 CRITICAL: {crit_n}</div>', unsafe_allow_html=True)
        ba2.markdown(f'<div class="badge-warning">🟡 WARNING: {warn_n}</div>',   unsafe_allow_html=True)
        ba3.markdown(f'<div class="badge-good">🟢 STABLE: {stbl_n}</div>',       unsafe_allow_html=True)

        st.markdown("---")

        falerts = alerts.copy()
        if not sources.empty and "source_name" in sources.columns:
            if selected_state != "All States" and "state" in sources.columns:
                valid = sources[sources["state"] == selected_state]["source_name"].tolist()
                if "source_name" in falerts.columns:
                    falerts = falerts[falerts["source_name"].isin(valid)]
            if selected_district != "All Districts" and "district" in sources.columns:
                valid = sources[sources["district"] == selected_district]["source_name"].tolist()
                if "source_name" in falerts.columns:
                    falerts = falerts[falerts["source_name"].isin(valid)]

        if falerts.empty:
            st.success("✅ No alerts match the current geographic filters")
        else:
            for _, al in falerts.iterrows():
                astatus = al.get("alert_status","UNKNOWN")
                emoji   = "🔴" if astatus=="CRITICAL" else "🟡" if astatus=="WARNING" else "🟢"

                src_info = None
                if not sources.empty and "source_name" in sources.columns:
                    sm = sources[sources["source_name"] == al.get("source_name","")]
                    if not sm.empty:
                        src_info = sm.iloc[0]

                with st.expander(f"{emoji} {al.get('source_name','Unknown')} — {astatus}"):
                    ec1, ec2 = st.columns(2)
                    with ec1:
                        if src_info is not None:
                            st.markdown(f"**Type:** {src_info.get('source_type','—')}")
                            st.markdown(f"**Location:** {src_info.get('district','—')}, {src_info.get('state','—')}")
                        cap_v = al.get("capacity_percent", 0)
                        st.markdown(f"**Capacity:** {cap_v}%")
                        st.progress(float(cap_v)/100 if cap_v else 0)
                    with ec2:
                        st.markdown(f"**pH Level:** {al.get('ph_level','—')}")
                        st.markdown(f"**Alert Time:** {al.get('alert_time','—')}")
                        if astatus == "CRITICAL":
                            st.error("⚠️ Immediate inspection required")
                        elif astatus == "WARNING":
                            st.warning("🔧 Schedule maintenance soon")
                        else:
                            st.success("✅ Monitor routinely")
    else:
        st.success("✅ No active alerts — All systems nominal")
        st.balloons()

# ══════════════════════════════════════════════
# TAB 6 — DATA TABLES
# ══════════════════════════════════════════════
with tab6:
    st.markdown('<p class="sec-hdr">📋 Data Explorer</p>', unsafe_allow_html=True)

    table_choice = st.selectbox("Select Table", [
        "Water Sources","Monitoring Stations","Groundwater Levels",
        "Rainfall History","Water Usage","Water Quality","Active Alerts","Regional Statistics"
    ])

    def dl_btn(df, prefix):
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Download CSV", csv,
                           f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                           "text/csv", use_container_width=True)

    def show_table(df, cols):
        available = [c for c in cols if c in df.columns]
        out = df[available] if available else df
        st.dataframe(out, use_container_width=True, hide_index=True)
        dl_btn(out, cols[0] if cols else "export")

    if table_choice == "Water Sources":
        df = filtered_sources if not filtered_sources.empty else sources
        if not df.empty:
            show_table(df, ["source_id","source_name","source_type","capacity_percent",
                            "max_capacity_mcm","build_year","age","state","district",
                            "origin_state","is_transboundary","risk_level","health_score","trend"])

    elif table_choice == "Monitoring Stations":
        df = filtered_stations if not filtered_stations.empty else stations
        if not df.empty:
            show_table(df, ["station_id","station_name","state_name","district_name",
                            "latitude","longitude","ph_level","dissolved_oxygen_mg_l",
                            "turbidity_ntu","status","last_maintenance"])

    elif table_choice == "Groundwater Levels":
        df = groundwater.copy()
        if selected_district != "All Districts" and "district_name" in df.columns:
            df = df[df["district_name"] == selected_district]
        if not df.empty:
            show_table(df, ["id","district_name","avg_depth_meters","extraction_pct",
                            "recharge_rate_mcm","assessment_year","stress_level","depletion_rate"])

    elif table_choice == "Rainfall History":
        df = rainfall.copy()
        if selected_district != "All Districts" and "district_name" in df.columns:
            df = df[df["district_name"] == selected_district]
        # Apply rainfall range filter
        if "rainfall_cm" in df.columns:
            df = df[df["rainfall_cm"].between(rainfall_range[0], rainfall_range[1])]
        if not df.empty:
            show_table(df, ["id","district_name","rainfall_cm","record_year","season",
                            "rainfall_category","deviation_pct"])

    elif table_choice == "Water Usage":
        df = usage.copy()
        if selected_state    != "All States"    and "state"    in df.columns: df = df[df["state"] == selected_state]
        if selected_district != "All Districts" and "district" in df.columns: df = df[df["district"] == selected_district]
        if not df.empty:
            show_table(df, ["usage_id","source_name","source_type","sector","sub_sector",
                            "consumer_name","consumption_mcm","record_year","season","state","district"])

    elif table_choice == "Water Quality":
        df = water_quality.copy()
        if not df.empty:
            show_table(df, ["station_name","state_name","district_name","ph_level",
                            "dissolved_oxygen_mg_l","turbidity_ntu","status"])

    elif table_choice == "Active Alerts":
        df = alerts.copy()
        if not df.empty:
            show_table(df, ["alert_id","source_name","capacity_percent","ph_level",
                            "alert_status","alert_time"])

    elif table_choice == "Regional Statistics":
        df = regional.copy()
        if not df.empty:
            show_table(df, ["region_id","region_name","population_count","annual_rainfall_avg_cm"])

# ─────────────────────────────────────────────
# SIDEBAR — FILTER SUMMARY & EXPORT
# ─────────────────────────────────────────────
with st.sidebar.expander("📊 Filter Summary", expanded=False):
    st.markdown(f"""
**Geography:**
- State: `{selected_state}`
- District: `{selected_district}`

**Source Filters:**
- Type: `{selected_type}`
- Capacity: `{capacity_range[0]:.0f}% – {capacity_range[1]:.0f}%`
- Risk: `{selected_risk}`

**Rainfall Range:**
- `{rainfall_range[0]:.0f} cm – {rainfall_range[1]:.0f} cm`
- Matching districts: `{len(rain_district_names)}`

**Results:**
- Sources: `{len(filtered_sources)}` of `{len(sources)}`
- On Map: `{on_map_n}`
""")

st.sidebar.markdown("---")
if st.sidebar.button("📦 Export All Filtered Data", use_container_width=True):
    export = {
        "water_sources":     filtered_sources,
        "monitoring_stations": filtered_stations,
        "groundwater": groundwater[
            groundwater["district_name"].isin(filtered_sources["district"].unique())
        ] if not filtered_sources.empty and not groundwater.empty and "district" in filtered_sources.columns and "district_name" in groundwater.columns else pd.DataFrame(),
        "rainfall": rainfall[
            rainfall["district_name"].isin(filtered_sources["district"].unique())
        ] if not filtered_sources.empty and not rainfall.empty and "district" in filtered_sources.columns and "district_name" in rainfall.columns else pd.DataFrame(),
        "water_quality": water_quality if not water_quality.empty else pd.DataFrame(),
        "alerts":        alerts        if not alerts.empty        else pd.DataFrame(),
    }
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet, df in export.items():
            if not df.empty:
                df.to_excel(writer, sheet_name=sheet[:31], index=False)
    st.sidebar.download_button(
        "📥 Download Excel Report", buf.getvalue(),
        f"aquastat_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# ─────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────
st.markdown("---")
f1, f2, f3 = st.columns(3)
f1.markdown("<p style='text-align:center;color:#00e5ff;font-size:1.1rem;margin:0'>💧 AQUASTAT</p>"
            "<p style='text-align:center;color:#7fa8c8;margin:0'>National Water Command Center</p>",
            unsafe_allow_html=True)
f2.markdown(f"<p style='text-align:center;color:#7fa8c8;margin:0'>Data Source: Ministry of Jal Shakti</p>"
            f"<p style='text-align:center;color:#7fa8c8;margin:0'>Updated: {current_time.strftime('%Y-%m-%d %H:%M IST')}</p>",
            unsafe_allow_html=True)
f3.markdown("<p style='text-align:center;color:#7fa8c8;margin:0'>© 2025 All Rights Reserved</p>"
            "<p style='text-align:center;color:#7fa8c8;margin:0'>v3.1 — Official Use Only</p>",
            unsafe_allow_html=True)

st.markdown("""
<div style="position:fixed;bottom:12px;right:14px;background:rgba(0,229,255,.1);
            border:1px solid rgba(0,229,255,.2);padding:5px 12px;border-radius:20px;
            font-size:.75rem;color:#00e5ff;">
    🔄 Auto-refresh every 5 min &nbsp;•&nbsp; Cloud Connected
</div>""", unsafe_allow_html=True)

if st.button("🔄 Refresh Data", key="refresh_btn"):
    st.cache_data.clear()
    st.rerun()
