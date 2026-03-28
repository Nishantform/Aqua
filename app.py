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

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="AQUASTAT — National Water Command Center",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
html, body, [data-testid="stAppViewContainer"] {
    background: linear-gradient(160deg,#040d18 0%,#071525 60%,#0a1e30 100%);
    color: #cfe4f7;
}
[data-testid="stSidebar"] {
    background: linear-gradient(180deg,#06121f 0%,#081a2e 100%);
    border-right: 1px solid #1a3550;
}
.kpi-card {
    background: rgba(255,255,255,0.035);
    border: 1px solid rgba(0,200,255,0.18);
    border-radius: 14px;
    padding: 18px 20px 14px;
    margin: 6px 0;
    box-shadow: 0 8px 30px rgba(0,0,0,.45), inset 0 1px 0 rgba(255,255,255,.06);
    backdrop-filter: blur(16px);
    transition: transform .2s;
}
.kpi-card:hover { transform: translateY(-3px); }
.kpi-label { color:#7fa8c8; font-size:.78rem; letter-spacing:.12em; text-transform:uppercase; margin:0 0 4px; }
.kpi-value { color:#00e5ff; font-size:2rem; font-weight:700; margin:0; line-height:1.1; }
.kpi-sub   { color:#4da8da; font-size:.78rem; margin:4px 0 0; }
.sec-hdr {
    color:#00e5ff; font-size:1.05rem; font-weight:600; letter-spacing:.14em;
    text-transform:uppercase; padding:8px 0 10px;
    border-bottom:1px solid rgba(0,200,255,.22); margin:4px 0 16px;
}
.badge-critical {
    display:inline-block; background:linear-gradient(135deg,#c0392b,#e74c3c);
    color:#fff; padding:6px 18px; border-radius:30px; font-weight:700; font-size:.9rem;
    box-shadow:0 0 18px rgba(231,76,60,.45);
}
.badge-warning {
    display:inline-block; background:linear-gradient(135deg,#e67e22,#f1c40f);
    color:#1a1a1a; padding:6px 18px; border-radius:30px; font-weight:700; font-size:.9rem;
    box-shadow:0 0 18px rgba(241,196,15,.35);
}
.badge-good {
    display:inline-block; background:linear-gradient(135deg,#27ae60,#2ecc71);
    color:#fff; padding:6px 18px; border-radius:30px; font-weight:700; font-size:.9rem;
    box-shadow:0 0 18px rgba(46,204,113,.35);
}
.dot { width:10px; height:10px; border-radius:50%; display:inline-block; margin-right:6px; }
.dot-red    { background:#ff4444; box-shadow:0 0 8px #ff4444; }
.dot-yellow { background:#ffd700; box-shadow:0 0 8px #ffd700; }
.dot-green  { background:#00ff9d; box-shadow:0 0 8px #00ff9d; }
.dot-blue   { background:#4287f5; box-shadow:0 0 8px #4287f5; }
::-webkit-scrollbar { width:6px; height:6px; }
::-webkit-scrollbar-track { background:#06121f; }
::-webkit-scrollbar-thumb { background:#1a3550; border-radius:4px; }
::-webkit-scrollbar-thumb:hover { background:#2a5070; }
[data-baseweb="tab-list"] { gap:6px; }
[data-baseweb="tab"] {
    border-radius:8px 8px 0 0 !important;
    background:rgba(255,255,255,.04) !important;
    color:#7fa8c8 !important; font-weight:600; font-size:.82rem; letter-spacing:.06em;
}
[aria-selected="true"] {
    background:linear-gradient(135deg,rgba(0,229,255,.15),rgba(0,150,200,.1)) !important;
    color:#00e5ff !important; border-bottom:2px solid #00e5ff !important;
}
.rain-info {
    background:rgba(0,100,180,.12); border:1px solid rgba(0,180,255,.25);
    border-radius:10px; padding:12px 16px; margin:8px 0; font-size:.85rem; color:#90cef4;
}
.filter-tag {
    display:inline-block; background:rgba(0,229,255,.12); border:1px solid rgba(0,229,255,.3);
    color:#00e5ff; padding:3px 10px; border-radius:12px; font-size:.78rem; margin:2px 3px;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# PLOTLY THEME
# ─────────────────────────────────────────────────────────────────────────────
PLOT_BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter,Arial,sans-serif", color="#cfe4f7", size=12),
    title_font=dict(size=14, color="#00e5ff"),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="rgba(255,255,255,.1)",
                borderwidth=1, font=dict(color="#cfe4f7")),
    xaxis=dict(gridcolor="rgba(255,255,255,.06)", zerolinecolor="rgba(255,255,255,.1)",
               tickfont=dict(color="#7fa8c8"), title_font=dict(color="#7fa8c8")),
    yaxis=dict(gridcolor="rgba(255,255,255,.06)", zerolinecolor="rgba(255,255,255,.1)",
               tickfont=dict(color="#7fa8c8"), title_font=dict(color="#7fa8c8")),
    margin=dict(l=40, r=20, t=50, b=40),
)

def apply_layout(fig, **kw):
    fig.update_layout(**{**PLOT_BASE, **kw})
    return fig

def kpi_card(col, label, value, sub=""):
    col.markdown(
        f"""<div class='kpi-card'>
            <p class='kpi-label'>{label}</p>
            <p class='kpi-value'>{value}</p>
            <p class='kpi-sub'>{sub}</p>
        </div>""",
        unsafe_allow_html=True,
    )

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────
try:
    NEON_URL = st.secrets["NEON_URL"]
except:
    NEON_URL = None

@st.cache_resource
def init_connection():
    if NEON_URL is None:
        return None
    try:
        engine = create_engine(
            NEON_URL, pool_size=5, max_overflow=10,
            pool_timeout=30, pool_recycle=1800, pool_pre_ping=True,
            connect_args={"connect_timeout": 10, "keepalives": 1,
                          "keepalives_idle": 30, "keepalives_interval": 10,
                          "keepalives_count": 5, "sslmode": "require"},
        )
        with engine.connect() as c:
            with c.begin():
                c.execute(text("SELECT 1")).fetchone()
        return engine
    except Exception as e:
        st.error(f"⚠️ DB connection failed: {e}")
        return None

engine = init_connection()

def test_connection():
    if engine is None:
        return False, "No engine"
    try:
        with engine.connect() as c:
            with c.begin():
                row = c.execute(text("SELECT current_database(), current_user")).fetchone()
        return True, f"Connected to {row[0]} as {row[1]}"
    except Exception as e:
        return False, str(e)

conn_ok, conn_msg = test_connection()
with st.sidebar:
    if conn_ok:
        st.success(f"✅ {conn_msg}")
    else:
        st.error(f"❌ {conn_msg}")

# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_all_data():
    if engine is None:
        return [pd.DataFrame()] * 8
    try:
        with engine.connect() as conn:
            with conn.begin():
                tables = pd.read_sql(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema='public'",
                    conn
                )["table_name"].tolist()

                def get_df(tbl):
                    if tbl in tables:
                        try:
                            return pd.read_sql(f'SELECT * FROM "{tbl}"', conn)
                        except Exception:
                            return pd.DataFrame()
                    return pd.DataFrame()

                sources     = get_df("water_sources")
                stations    = get_df("water_monitoring_stations")
                groundwater = get_df("groundwater_levels")
                rainfall    = get_df("rainfall_history")
                alerts      = get_df("active_alerts")
                regional    = get_df("regional_stats")
                water_quality = stations.copy() if not stations.empty else pd.DataFrame()

                usage = pd.DataFrame()
                if "water_usage_history" in tables and "water_sources" in tables:
                    try:
                        usage = pd.read_sql(
                            """SELECT wu.*, ws.source_name, ws.source_type, ws.state, ws.district
                               FROM water_usage_history wu
                               LEFT JOIN water_sources ws ON wu.source_id = ws.source_id""",
                            conn
                        )
                    except Exception:
                        pass

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

# ─────────────────────────────────────────────────────────────────────────────
# DATA PROCESSING
# ─────────────────────────────────────────────────────────────────────────────
current_year = datetime.now().year
current_time = datetime.now(pytz.timezone("Asia/Kolkata"))

if not sources.empty:
    for col in ["capacity_percent", "build_year", "max_capacity_mcm"]:
        if col in sources.columns:
            sources[col] = pd.to_numeric(sources[col], errors="coerce")
    if "build_year" in sources.columns:
        sources["age"] = (current_year - sources["build_year"]).clip(0, 200)
    else:
        sources["age"] = 0
    if "capacity_percent" in sources.columns:
        sources["health_score"] = (
            sources["capacity_percent"].fillna(50) * 0.5
            + (100 - sources["age"].clip(0, 100).fillna(50)) * 0.3
            + 20
        ).clip(0, 100)
        sources["risk_level"] = pd.cut(
            sources["capacity_percent"],
            bins=[0, 30, 60, 100],
            labels=["Critical", "Moderate", "Good"],
            include_lowest=True,
        )
    else:
        sources["health_score"] = 50
        sources["risk_level"]   = "Unknown"
    np.random.seed(42)
    sources["trend"] = np.random.choice(
        ["📈 Increasing", "📉 Decreasing", "➡️ Stable"], len(sources)
    )

# ── FIXED: Simple coordinate merge without column duplication ────────────────
if not sources.empty and not stations.empty:
    # Create district coordinate lookup from stations
    station_coords = stations[stations["latitude"].notna() & stations["longitude"].notna()].copy()
    if not station_coords.empty and "district_name" in station_coords.columns:
        station_coords["district_clean"] = station_coords["district_name"].str.strip().str.lower()
        district_lookup = station_coords.groupby("district_clean").agg({
            "latitude": "first",
            "longitude": "first"
        }).reset_index()
        
        # Add coordinates to sources
        sources["district_clean"] = sources["district"].str.strip().str.lower()
        sources = sources.merge(district_lookup, on="district_clean", how="left")
        sources = sources.drop("district_clean", axis=1)
        
        # Fill missing coordinates with NaN
        if "latitude_y" in sources.columns:
            sources["latitude"] = sources["latitude_y"].fillna(sources.get("latitude_x", np.nan))
            sources["longitude"] = sources["longitude_y"].fillna(sources.get("longitude_x", np.nan))
            sources = sources.drop(["latitude_x", "latitude_y", "longitude_x", "longitude_y"], axis=1, errors="ignore")
    else:
        if "latitude" not in sources.columns:
            sources["latitude"] = np.nan
        if "longitude" not in sources.columns:
            sources["longitude"] = np.nan
else:
    if not sources.empty:
        if "latitude" not in sources.columns:
            sources["latitude"] = np.nan
        if "longitude" not in sources.columns:
            sources["longitude"] = np.nan

# Convert to float
sources["latitude"] = pd.to_numeric(sources["latitude"], errors="coerce")
sources["longitude"] = pd.to_numeric(sources["longitude"], errors="coerce")

if not groundwater.empty:
    if "avg_depth_meters" in groundwater.columns:
        groundwater["stress_level"] = pd.cut(
            groundwater["avg_depth_meters"],
            bins=[0, 20, 40, 100], labels=["Low", "Moderate", "High"]
        )
    if "assessment_year" in groundwater.columns and "avg_depth_meters" in groundwater.columns:
        groundwater = groundwater.sort_values(["district_name", "assessment_year"])
        groundwater["depth_change"]   = groundwater.groupby("district_name")["avg_depth_meters"].diff()
        groundwater["depletion_rate"] = groundwater.groupby("district_name")["depth_change"].transform("mean")

if not rainfall.empty:
    if "rainfall_cm" in rainfall.columns:
        rainfall["rainfall_category"] = pd.cut(
            rainfall["rainfall_cm"],
            bins=[0, 50, 150, 300, float("inf")],
            labels=["Low", "Moderate", "High", "Extreme"]
        )
    if "rainfall_cm" in rainfall.columns and "district_name" in rainfall.columns:
        avg_r = rainfall.groupby("district_name")["rainfall_cm"].transform("mean")
        rainfall["deviation_pct"] = ((rainfall["rainfall_cm"] - avg_r) / avg_r * 100).round(1)

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR FILTERS
# ─────────────────────────────────────────────────────────────────────────────
_cap_min = float(sources["capacity_percent"].min()) if not sources.empty and "capacity_percent" in sources.columns and not sources["capacity_percent"].isna().all() else 0.0
_cap_max = float(sources["capacity_percent"].max()) if not sources.empty and "capacity_percent" in sources.columns and not sources["capacity_percent"].isna().all() else 100.0
_by_min  = int(sources["build_year"].min()) if not sources.empty and "build_year" in sources.columns and not sources["build_year"].isna().all() else 1800
_by_max  = int(sources["build_year"].max()) if not sources.empty and "build_year" in sources.columns and not sources["build_year"].isna().all() else current_year
_r_min   = float(rainfall["rainfall_cm"].min()) if not rainfall.empty and "rainfall_cm" in rainfall.columns and not rainfall["rainfall_cm"].isna().all() else 0.0
_r_max   = float(rainfall["rainfall_cm"].max()) if not rainfall.empty and "rainfall_cm" in rainfall.columns and not rainfall["rainfall_cm"].isna().all() else 1000.0

with st.sidebar:
    st.title("🎮 AQUASTAT")
    st.caption("Command Interface v3.2")
    st.markdown("---")

    if st.button("🔄 Reset All Filters", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")

    # ── Geographic ──────────────────────────────────────────────────────────
    st.markdown("### 🌍 Geographic Filters")

    state_opts = (["All States"] + sorted(sources["state"].dropna().unique().tolist())
                  if not sources.empty and "state" in sources.columns else ["All States"])
    selected_state = st.selectbox("State", state_opts)

    dist_pool = (
        sources[sources["state"] == selected_state]["district"].dropna().unique()
        if selected_state != "All States" and not sources.empty and "district" in sources.columns
        else (sources["district"].dropna().unique() if not sources.empty and "district" in sources.columns else [])
    )
    dist_opts = ["All Districts"] + sorted(dist_pool.tolist()) if len(dist_pool) else ["All Districts"]
    selected_district = st.selectbox("District", dist_opts)

    st.markdown("---")

    # ── Source Filters ───────────────────────────────────────────────────────
    st.markdown("### 💧 Source Filters")

    type_opts = (["All Types"] + sorted(sources["source_type"].dropna().unique().tolist())
                 if not sources.empty and "source_type" in sources.columns else ["All Types"])
    selected_type = st.selectbox("Source Type", type_opts)

    capacity_range = st.slider("Capacity %", _cap_min, _cap_max, (_cap_min, _cap_max))

    risk_opts = (["All Risk Levels"] + list(sources["risk_level"].dropna().unique())
                 if not sources.empty and "risk_level" in sources.columns else ["All Risk Levels"])
    selected_risk = st.selectbox("Risk Level", risk_opts)

    st.markdown("---")

    # ── Build Year Filter ────────────────────────────────────────────────────
    st.markdown("### 📅 Build Year Filter")
    year_range = st.slider("Build Year", _by_min, _by_max, (_by_min, _by_max))

    st.markdown("---")

    # ── Rainfall Filter ──────────────────────────────────────────────────────
    st.markdown("### ☔ Rainfall Filter")
    rainfall_range   = st.slider("Avg Rainfall (cm)", _r_min, _r_max, (_r_min, _r_max),
                                 help="Filter districts whose average rainfall falls in this range")
    show_rain_on_map = st.checkbox("Show rainfall districts on map", value=False)

    st.markdown("---")

    # ── Map Settings ─────────────────────────────────────────────────────────
    st.markdown("### 🗺️ Map Settings")
    map_style = st.selectbox("Map Style",
                             ["Esri Satellite", "OpenStreetMap", "CartoDB Dark", "CartoDB Light"])
    show_heatmap      = st.checkbox("Show Heatmap", True)
    show_clusters     = st.checkbox("Show Clusters", True)
    show_stations_map = st.checkbox("Show Monitoring Stations", True)
    marker_size       = st.slider("Marker Size", 5, 20, 12)

    st.markdown("---")
    st.caption(f"📊 Total Sources: {len(sources):,}")

# ─────────────────────────────────────────────────────────────────────────────
# APPLY FILTERS
# ─────────────────────────────────────────────────────────────────────────────
def apply_source_filters():
    df = sources.copy()
    if selected_state    != "All States"    and "state"    in df.columns:
        df = df[df["state"] == selected_state]
    if selected_district != "All Districts" and "district" in df.columns:
        df = df[df["district"] == selected_district]
    if selected_type     != "All Types"     and "source_type" in df.columns:
        df = df[df["source_type"] == selected_type]
    if "capacity_percent" in df.columns:
        df = df[df["capacity_percent"].between(capacity_range[0], capacity_range[1])]
    if selected_risk != "All Risk Levels" and "risk_level" in df.columns:
        df = df[df["risk_level"] == selected_risk]
    if "build_year" in df.columns:
        df = df[df["build_year"].isna() | df["build_year"].between(year_range[0], year_range[1])]
    return df

filtered_sources = apply_source_filters()

def apply_station_filters():
    df = stations.copy()
    if selected_state    != "All States"    and "state_name"    in df.columns:
        df = df[df["state_name"] == selected_state]
    if selected_district != "All Districts" and "district_name" in df.columns:
        df = df[df["district_name"] == selected_district]
    return df

filtered_stations = apply_station_filters()

# Derived district / state sets used by secondary tables
filtered_districts = (
    filtered_sources["district"].dropna().unique().tolist()
    if not filtered_sources.empty and "district" in filtered_sources.columns else []
)

# Rainfall district lookup
def get_rainfall_districts():
    if rainfall.empty or "rainfall_cm" not in rainfall.columns or "district_name" not in rainfall.columns:
        return pd.DataFrame(), []
    avg_r = (rainfall.groupby("district_name")["rainfall_cm"].mean()
             .reset_index().rename(columns={"rainfall_cm": "avg_rain_cm"}))
    frd = avg_r[avg_r["avg_rain_cm"].between(rainfall_range[0], rainfall_range[1])]
    return frd, frd["district_name"].tolist()

rain_districts_df, rain_district_names = get_rainfall_districts()

# ─────────────────────────────────────────────────────────────────────────────
# HEADER + KPIs
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(
    "<h1 style='color:#00e5ff;letter-spacing:.04em;margin-bottom:0'>💧 AQUASTAT</h1>"
    "<p style='color:#7fa8c8;margin-top:4px;font-size:.9rem;'>"
    "National Water Command Center &nbsp;•&nbsp; "
    f"Live Intelligence &nbsp;•&nbsp; {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}"
    "</p>",
    unsafe_allow_html=True,
)

total_cap  = (filtered_sources["capacity_percent"].mean()
              if not filtered_sources.empty and "capacity_percent" in filtered_sources.columns else 0)
critical_n = (len(filtered_sources[filtered_sources["capacity_percent"] < 30])
              if not filtered_sources.empty and "capacity_percent" in filtered_sources.columns else 0)
on_map_n   = (int(filtered_sources["latitude"].notna().sum())
              if not filtered_sources.empty and "latitude" in filtered_sources.columns else 0)

k1, k2, k3, k4, k5 = st.columns(5)
kpi_card(k1, "Total Sources",    f"{len(sources):,}",    f"{len(filtered_sources)} shown")
kpi_card(k2, "Avg Capacity",     f"{total_cap:.1f}%",    "filtered sources")
kpi_card(k3, "Critical Sources", str(critical_n),        "below 30% capacity")
kpi_card(k4, "Mapped Sources",   str(on_map_n),          "with coordinates")
kpi_card(k5, "Active Alerts",    str(len(alerts)),       "system-wide")

# Filter tag pills
active_tags = []
if selected_state    != "All States":     active_tags.append(f"🏛 {selected_state}")
if selected_district != "All Districts":  active_tags.append(f"📍 {selected_district}")
if selected_type     != "All Types":      active_tags.append(f"🔧 {selected_type}")
if selected_risk     != "All Risk Levels":active_tags.append(f"⚠️ {selected_risk}")
if capacity_range != (_cap_min, _cap_max):
    active_tags.append(f"💧 {capacity_range[0]:.0f}–{capacity_range[1]:.0f}%")
if year_range != (_by_min, _by_max):
    active_tags.append(f"📅 {year_range[0]}–{year_range[1]}")
if active_tags:
    tags_html = "".join(f"<span class='filter-tag'>{t}</span>" for t in active_tags)
    st.markdown(f"<div style='margin:6px 0 10px'>{tags_html}</div>", unsafe_allow_html=True)

st.markdown("<div style='margin:4px 0'></div>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Dashboard", "🗺️ Map View", "📈 Analytics",
    "💧 Water Quality", "⚠️ Alerts", "📋 Data Tables",
])

# ===================== TAB 1: DASHBOARD =====================
with tab1:
    if filtered_sources.empty:
        st.warning("⚠️ No water sources match the current filters.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<p class="sec-hdr">📊 Storage Capacity Table</p>', unsafe_allow_html=True)
            if 'capacity_percent' in filtered_sources.columns:
                capacity_table = filtered_sources[['source_name', 'source_type', 'capacity_percent', 'state', 'district', 'risk_level']].sort_values('capacity_percent')
                capacity_table['capacity_percent'] = capacity_table['capacity_percent'].round(1).astype(str) + '%'
                st.dataframe(capacity_table, use_container_width=True, hide_index=True)
        
        with col2:
            st.markdown('<p class="sec-hdr">🏭 Source Types</p>', unsafe_allow_html=True)
            if 'source_type' in filtered_sources.columns:
                type_counts = filtered_sources['source_type'].value_counts().reset_index()
                type_counts.columns = ['Source Type', 'Count']
                fig = px.pie(type_counts, values='Count', names='Source Type', title="Water Sources by Type", 
                            template="plotly_dark", color_discrete_sequence=px.colors.sequential.Tealgrn)
                fig.update_traces(textposition='inside', textinfo='percent+label')
                fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<p class="sec-hdr">📈 Groundwater Analysis</p>', unsafe_allow_html=True)
            if not groundwater.empty:
                filtered_gw = groundwater.copy()
                if filtered_districts:
                    filtered_gw = filtered_gw[filtered_gw["district_name"].isin(filtered_districts)]
                if not filtered_gw.empty:
                    stress_counts = filtered_gw['stress_level'].value_counts().reset_index()
                    stress_counts.columns = ['Stress Level', 'Count']
                    fig = px.bar(stress_counts, x='Stress Level', y='Count', title="Groundwater Stress Distribution", 
                                template="plotly_dark", color='Stress Level', 
                                color_discrete_map={'Low':'#00ff9d','Moderate':'#ffd700','High':'#ff4444'}, text_auto=True)
                    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig, use_container_width=True)
                    
                    st.markdown("#### 📋 Groundwater Details")
                    gw_table = filtered_gw[['district_name', 'avg_depth_meters', 'extraction_pct', 'recharge_rate_mcm', 'assessment_year', 'stress_level']].sort_values('avg_depth_meters')
                    gw_table['avg_depth_meters'] = gw_table['avg_depth_meters'].round(1).astype(str) + ' m'
                    gw_table['extraction_pct'] = gw_table['extraction_pct'].round(1).astype(str) + '%'
                    gw_table['recharge_rate_mcm'] = gw_table['recharge_rate_mcm'].round(1).astype(str) + ' MCM'
                    st.dataframe(gw_table, use_container_width=True, hide_index=True)
                else:
                    st.info("No groundwater data available")
        
        with col2:
            st.markdown('<p class="sec-hdr">☔ Rainfall Analysis</p>', unsafe_allow_html=True)
            if not rainfall.empty:
                filtered_rain = rainfall.copy()
                if filtered_districts:
                    filtered_rain = filtered_rain[filtered_rain["district_name"].isin(filtered_districts)]
                if not filtered_rain.empty:
                    season_rain = filtered_rain.groupby('season')['rainfall_cm'].mean().reset_index()
                    fig = px.bar(season_rain, x='season', y='rainfall_cm', title="Average Rainfall by Season", 
                                template="plotly_dark", color='rainfall_cm', color_continuous_scale='Blues', text_auto='.1f')
                    fig.update_traces(textposition='outside')
                    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig, use_container_width=True)
                    
                    st.markdown("#### 📋 Rainfall Details")
                    rain_table = filtered_rain[['district_name', 'rainfall_cm', 'record_year', 'season', 'rainfall_category']].sort_values('rainfall_cm', ascending=False)
                    rain_table['rainfall_cm'] = rain_table['rainfall_cm'].round(1).astype(str) + ' cm'
                    st.dataframe(rain_table, use_container_width=True, hide_index=True)
                else:
                    st.info("No rainfall data available for selected filters")
        
        if 'risk_level' in filtered_sources.columns:
            st.markdown('<p class="sec-hdr">⚠️ Risk Assessment</p>', unsafe_allow_html=True)
            risk_counts = filtered_sources['risk_level'].value_counts().reset_index()
            risk_counts.columns = ['Risk Level', 'Count']
            fig = px.bar(risk_counts, x='Risk Level', y='Count', color='Risk Level', 
                        color_discrete_map={'Good':'#00ff9d','Moderate':'#ffd700','Critical':'#ff4444'}, 
                        title="Infrastructure Risk Assessment", template="plotly_dark", text_auto=True)
            fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)

# ===================== TAB 2: MAP VIEW (FIXED) =====================
with tab2:
    st.markdown('<p class="sec-hdr">🗺️ National Interactive Water Resources Map</p>', unsafe_allow_html=True)

    map_filter_parts = []
    if selected_state    != "All States":     map_filter_parts.append(f"State: **{selected_state}**")
    if selected_district != "All Districts":  map_filter_parts.append(f"District: **{selected_district}**")
    if selected_type     != "All Types":      map_filter_parts.append(f"Type: **{selected_type}**")
    if selected_risk     != "All Risk Levels":map_filter_parts.append(f"Risk: **{selected_risk}**")
    if show_rain_on_map:
        map_filter_parts.append(f"Rainfall: **{rainfall_range[0]:.0f}–{rainfall_range[1]:.0f} cm**")

    if map_filter_parts:
        st.info(f"🔍 {' | '.join(map_filter_parts)}  —  Showing **{len(filtered_sources)}** filtered sources")
    else:
        st.info(f"Showing all **{len(filtered_sources)}** sources (no geographic filters active)")

    # Sources that have valid coordinates (from the FILTERED set only)
    if not filtered_sources.empty and "latitude" in filtered_sources.columns:
        map_sources = filtered_sources[
            filtered_sources["latitude"].notna() & filtered_sources["longitude"].notna()
        ].copy()
    else:
        map_sources = pd.DataFrame()

    # Map centre from filtered+coord data
    if not map_sources.empty:
        center_lat = float(map_sources["latitude"].mean())
        center_lon = float(map_sources["longitude"].mean())
        zoom = 9 if selected_district != "All Districts" else 7 if selected_state != "All States" else 5
    else:
        center_lat, center_lon, zoom = 20.5937, 78.9629, 5

    style_map = {
        "Esri Satellite": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        "OpenStreetMap":  "OpenStreetMap",
        "CartoDB Dark":   "CartoDB dark_matter",
        "CartoDB Light":  "CartoDB positron",
    }

    m = folium.Map(location=[center_lat, center_lon], zoom_start=zoom,
                   tiles=style_map[map_style], attr="AQUASTAT")
    Fullscreen().add_to(m)

    cluster_group = (
        MarkerCluster().add_to(m) if show_clusters and len(map_sources) > 10 else m
    )

    heat_data      = []
    sources_on_map = 0

    # ── Water source markers — ONLY from filtered_sources with valid coords ──
    for _, row in map_sources.iterrows():
        cap = float(row.get("capacity_percent", 50) or 50)
        if cap < 30:   clr, risk_txt = "#ff4444", "CRITICAL"
        elif cap < 60: clr, risk_txt = "#ffd700", "MODERATE"
        else:          clr, risk_txt = "#00ff9d", "GOOD"

        lat = float(row["latitude"])
        lon = float(row["longitude"])
        heat_data.append([lat, lon])
        sources_on_map += 1

        build_yr = int(row["build_year"]) if pd.notna(row.get("build_year")) else "—"
        popup_html = f"""
        <div style="font-family:Inter,Arial;min-width:230px;background:#06121f;
                    color:#cfe4f7;border-radius:8px;padding:10px;">
            <b style="color:{clr};font-size:1rem;">{row.get('source_name','Unknown')}</b>
            <hr style="border-color:#1a3550;margin:6px 0">
            <table style="width:100%;font-size:.82rem;">
                  <tr><td style="color:#7fa8c8;padding-right:8px">Type</td>
                      <td>{row.get('source_type','—')}</td></tr>
                  <tr><td style="color:#7fa8c8">State</td>
                      <td>{row.get('state','—')}</td></tr>
                  <tr><td style="color:#7fa8c8">District</td>
                      <td>{row.get('district','—')}</td></tr>
                  <tr><td style="color:#7fa8c8">Capacity</td>
                      <td>{cap:.1f}%</td></tr>
                  <tr><td style="color:#7fa8c8">Build Year</td>
                      <td>{build_yr}</td></tr>
                  <tr><td style="color:#7fa8c8">Age</td>
                      <td>{row.get('age',0):.0f} yrs</td></tr>
                  <tr><td style="color:#7fa8c8">Risk</td>
                      <td><b style="color:{clr}">{risk_txt}</b></td></tr>
                  <tr><td style="color:#7fa8c8">Trend</td>
                      <td>{row.get('trend','—')}</td></tr>
             </table>
        </div>"""

        folium.CircleMarker(
            location=[lat, lon],
            radius=marker_size + (3 if cap < 30 else 0),
            color=clr, fill=True, fill_opacity=0.75, weight=1.5,
            popup=folium.Popup(popup_html, max_width=280),
            tooltip=f"{row.get('source_name','?')} — {cap:.0f}%",
        ).add_to(cluster_group)

    if show_heatmap and heat_data:
        HeatMap(heat_data, radius=15, blur=10,
                gradient={0.2:"blue",0.4:"cyan",0.6:"lime",0.8:"yellow",1:"red"}).add_to(m)

    # ── Monitoring stations — ONLY from filtered_stations ───────────────────
    if show_stations_map and not filtered_stations.empty:
        if "latitude" in filtered_stations.columns and "longitude" in filtered_stations.columns:
            st_geo = filtered_stations[
                filtered_stations["latitude"].notna() & filtered_stations["longitude"].notna()
            ]
            for _, row in st_geo.iterrows():
                status = row.get("status", "Unknown")
                s_clr  = "green" if status == "Active" else "orange" if status == "Maintenance" else "red"
                popup_html = f"""
                <div style="background:#06121f;color:#cfe4f7;padding:8px;border-radius:6px;font-size:.82rem;">
                    <b>{row.get('station_name','Unknown')}</b><br>
                    State: {row.get('state_name','—')}<br>
                    District: {row.get('district_name','—')}<br>
                    Status: <b style="color:{'#2ecc71' if status=='Active' else '#f1c40f' if status=='Maintenance' else '#e74c3c'}">{status}</b><br>
                    pH: {row.get('ph_level','—')} &nbsp;
                    DO: {row.get('dissolved_oxygen_mg_l','—')} mg/L &nbsp;
                    Turbidity: {row.get('turbidity_ntu','—')} NTU
                </div>"""
                folium.Marker(
                    location=[float(row["latitude"]), float(row["longitude"])],
                    icon=folium.Icon(color=s_clr, icon="info-sign"),
                    popup=folium.Popup(popup_html, max_width=260),
                    tooltip=f"⚙ {row.get('station_name','?')}",
                ).add_to(m)

    # ── Rainfall district overlay ────────────────────────────────────────────
    if show_rain_on_map and rain_district_names and not stations.empty:
        if "district_name" in stations.columns and "latitude" in stations.columns:
            rain_sta = stations[
                stations["district_name"].isin(rain_district_names) &
                stations["latitude"].notna() & stations["longitude"].notna()
            ]
            dist_locs = rain_sta.groupby("district_name").agg(
                lat=("latitude","first"), lon=("longitude","first")
            ).reset_index()
            merged_rain = dist_locs.merge(rain_districts_df, on="district_name")
            for _, row in merged_rain.iterrows():
                avg_r = row["avg_rain_cm"]
                rc_clr = "#4287f5" if avg_r < 50 else "#00ccee" if avg_r < 150 else "#00ff9d" if avg_r < 300 else "#ff4444"
                folium.CircleMarker(
                    location=[float(row["lat"]), float(row["lon"])],
                    radius=16, color=rc_clr, fill=True, fill_opacity=0.4, weight=2,
                    tooltip=f"🌧️ {row['district_name']} — Avg {avg_r:.1f} cm",
                    popup=folium.Popup(
                        f"<b>{row['district_name']}</b><br>Avg Rainfall: {avg_r:.1f} cm", max_width=200
                    ),
                ).add_to(m)

    if map_sources.empty:
        st.warning("⚠️ No sources with coordinates for current filters. Map centred on India.")

    st_folium(m, width=1300, height=620)

    mc1, mc2, mc3 = st.columns(3)
    mc1.metric("Sources on Map",      sources_on_map)
    mc2.metric("Total Filtered",      len(filtered_sources))
    mc3.metric("Coordinate Coverage",
               f"{sources_on_map / max(len(filtered_sources),1)*100:.1f}%")

    st.markdown("---")
    leg = st.columns(5)
    leg[0].markdown('<span class="dot dot-green"></span> Good (≥60%)',        unsafe_allow_html=True)
    leg[1].markdown('<span class="dot dot-yellow"></span> Moderate (30–60%)', unsafe_allow_html=True)
    leg[2].markdown('<span class="dot dot-red"></span> Critical (<30%)',      unsafe_allow_html=True)
    leg[3].markdown('<span class="dot dot-blue"></span> Monitoring Station',  unsafe_allow_html=True)
    leg[4].markdown('🔵 Rainfall District (when enabled)',                    unsafe_allow_html=True)

# ===================== TAB 3: ANALYTICS =====================
with tab3:
    st.markdown('<p class="sec-hdr">📈 Advanced Analytics</p>', unsafe_allow_html=True)
    
    atab1, atab2, atab3 = st.tabs(["📊 Trends", "📉 Comparisons", "📐 Statistics"])
    
    with atab1:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Rainfall Trend")
            if not rainfall.empty and 'record_year' in rainfall.columns and 'rainfall_cm' in rainfall.columns:
                filtered_rain = rainfall.copy()
                if filtered_districts:
                    filtered_rain = filtered_rain[filtered_rain["district_name"].isin(filtered_districts)]
                if not filtered_rain.empty:
                    rain_trend = filtered_rain.groupby('record_year')['rainfall_cm'].mean().reset_index()
                    if not rain_trend.empty:
                        fig = px.line(rain_trend, x='record_year', y='rainfall_cm', title="Average Rainfall Over Years", 
                                    template="plotly_dark", markers=True)
                        fig.update_traces(line_color='#00e5ff', line_width=3)
                        fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                        st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Groundwater Trend")
            if not groundwater.empty and 'assessment_year' in groundwater.columns and 'avg_depth_meters' in groundwater.columns:
                filtered_gw = groundwater.copy()
                if filtered_districts:
                    filtered_gw = filtered_gw[filtered_gw["district_name"].isin(filtered_districts)]
                if not filtered_gw.empty:
                    gw_trend = filtered_gw.groupby('assessment_year')['avg_depth_meters'].mean().reset_index()
                    if not gw_trend.empty:
                        fig = px.line(gw_trend, x='assessment_year', y='avg_depth_meters', title="Average Groundwater Depth", 
                                    template="plotly_dark", markers=True)
                        fig.update_traces(line_color='#ffd700', line_width=3)
                        fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                        st.plotly_chart(fig, use_container_width=True)
    
    with atab2:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Capacity by State")
            if not filtered_sources.empty and 'state' in filtered_sources.columns and 'capacity_percent' in filtered_sources.columns:
                state_cap = filtered_sources.groupby('state')['capacity_percent'].mean().sort_values(ascending=False).head(10)
                if len(state_cap) > 0:
                    fig = px.bar(x=state_cap.values, y=state_cap.index, orientation='h', title="Average Capacity by State", 
                                template="plotly_dark", color=state_cap.values, color_continuous_scale='Tealgrn', 
                                labels={'x':'Avg Capacity (%)','y':'State'})
                    fig.update_traces(texttemplate='%{x:.1f}%', textposition='outside')
                    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Extraction vs Recharge")
            if not groundwater.empty and 'recharge_rate_mcm' in groundwater.columns and 'extraction_pct' in groundwater.columns:
                filtered_gw = groundwater.copy()
                if filtered_districts:
                    filtered_gw = filtered_gw[filtered_gw["district_name"].isin(filtered_districts)]
                if not filtered_gw.empty:
                    fig = px.scatter(filtered_gw, x='recharge_rate_mcm', y='extraction_pct', 
                                    size='avg_depth_meters' if 'avg_depth_meters' in filtered_gw.columns else None,
                                    color='district_name' if 'district_name' in filtered_gw.columns else None,
                                    title="Groundwater Extraction vs Recharge", template="plotly_dark")
                    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig, use_container_width=True)
    
    with atab3:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Statistical Summary")
            if not filtered_sources.empty:
                stats_cols = [c for c in ['capacity_percent', 'age', 'health_score'] if c in filtered_sources.columns]
                if stats_cols:
                    st.dataframe(filtered_sources[stats_cols].describe().style.format("{:.2f}"), use_container_width=True)
        
        with col2:
            st.subheader("Correlation Matrix")
            if not filtered_sources.empty and not groundwater.empty and 'district' in filtered_sources.columns and 'district_name' in groundwater.columns:
                merged = filtered_sources.merge(groundwater, left_on='district', right_on='district_name', how='inner')
                if not merged.empty:
                    numeric_cols = [c for c in ['capacity_percent', 'age', 'avg_depth_meters', 'extraction_pct', 'recharge_rate_mcm'] if c in merged.columns]
                    if len(numeric_cols) >= 2:
                        corr_data = merged[numeric_cols].dropna()
                        if not corr_data.empty:
                            fig = px.imshow(corr_data.corr(), text_auto=True, aspect="auto", title="Correlation Matrix", 
                                          template="plotly_dark", color_continuous_scale='RdBu_r')
                            fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                            st.plotly_chart(fig, use_container_width=True)

# ===================== TAB 4: WATER QUALITY =====================
with tab4:
    st.markdown('<p class="sec-hdr">💧 Water Quality Monitoring</p>', unsafe_allow_html=True)
    
    fwq = water_quality.copy()
    if not fwq.empty:
        if selected_state    != "All States"    and "state_name"    in fwq.columns:
            fwq = fwq[fwq["state_name"] == selected_state]
        if selected_district != "All Districts" and "district_name" in fwq.columns:
            fwq = fwq[fwq["district_name"] == selected_district]
    
    if not fwq.empty:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            avg_ph = fwq['ph_level'].mean() if 'ph_level' in fwq.columns else 0
            st.metric("Average pH", f"{avg_ph:.2f}", delta=None)
        with col2:
            avg_do = fwq['dissolved_oxygen_mg_l'].mean() if 'dissolved_oxygen_mg_l' in fwq.columns else 0
            st.metric("Dissolved Oxygen", f"{avg_do:.1f} mg/L", delta=None)
        with col3:
            avg_turbidity = fwq['turbidity_ntu'].mean() if 'turbidity_ntu' in fwq.columns else 0
            st.metric("Turbidity", f"{avg_turbidity:.1f} NTU", delta=None)
        with col4:
            active_count = len(fwq[fwq['status'] == 'Active']) if 'status' in fwq.columns else 0
            st.metric("Active Stations", active_count, delta=None)
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        with col1:
            if 'ph_level' in fwq.columns:
                fig = px.histogram(fwq, x='ph_level', nbins=20, title="pH Distribution", 
                                  template="plotly_dark", color_discrete_sequence=['#00e5ff'])
                fig.add_vline(x=7, line_dash="dash", line_color="white")
                fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
        with col2:
            if 'dissolved_oxygen_mg_l' in fwq.columns:
                fig = px.box(fwq, y='dissolved_oxygen_mg_l', title="Dissolved Oxygen Distribution", 
                            template="plotly_dark", color_discrete_sequence=['#00ff9d'])
                fig.add_hline(y=4, line_dash="dash", line_color="red", annotation_text="Critical Level (4 mg/L)")
                fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("### 📋 Water Quality Readings")
        display_cols = ['station_name', 'district_name', 'ph_level', 'dissolved_oxygen_mg_l', 'turbidity_ntu', 'status']
        available_cols = [c for c in display_cols if c in fwq.columns]
        if available_cols:
            display_df = fwq[available_cols].copy()
            st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("No water quality data available.")

# ===================== TAB 5: ALERTS =====================
with tab5:
    st.markdown('<p class="sec-hdr">🚨 Active Alerts and Warnings</p>', unsafe_allow_html=True)
    
    if not alerts.empty:
        if not sources.empty and 'source_name' in sources.columns:
            alerts = alerts.merge(sources[['source_name', 'source_type', 'district', 'state']], on='source_name', how='left')
            for col in ['source_type', 'district', 'state']:
                if col in alerts.columns:
                    alerts[col] = alerts[col].fillna('Unknown')
        
        filtered_alerts = alerts.copy()
        if selected_state != "All States" and 'state' in filtered_alerts.columns:
            filtered_alerts = filtered_alerts[filtered_alerts['state'] == selected_state]
        if selected_district != "All Districts" and 'district' in filtered_alerts.columns:
            filtered_alerts = filtered_alerts[filtered_alerts['district'] == selected_district]
        if selected_type != "All Types" and 'source_type' in filtered_alerts.columns:
            filtered_alerts = filtered_alerts[filtered_alerts['source_type'] == selected_type]
        
        critical_alerts = filtered_alerts[filtered_alerts['alert_status'] == 'CRITICAL'] if 'alert_status' in filtered_alerts.columns else pd.DataFrame()
        warning_alerts = filtered_alerts[filtered_alerts['alert_status'] == 'WARNING'] if 'alert_status' in filtered_alerts.columns else pd.DataFrame()
        stable_alerts = filtered_alerts[filtered_alerts['alert_status'] == 'STABLE'] if 'alert_status' in filtered_alerts.columns else pd.DataFrame()
        
        col1, col2, col3 = st.columns(3)
        with col1: st.markdown(f'<div class="badge-critical">🔴 CRITICAL: {len(critical_alerts)}</div>', unsafe_allow_html=True)
        with col2: st.markdown(f'<div class="badge-warning">🟡 WARNING: {len(warning_alerts)}</div>', unsafe_allow_html=True)
        with col3: st.markdown(f'<div class="badge-good">🟢 STABLE: {len(stable_alerts)}</div>', unsafe_allow_html=True)
        
        st.markdown("---")
        
        if filtered_alerts.empty:
            st.success("✅ No alerts match the current filters")
        else:
            severity_order = {'CRITICAL': 0, 'WARNING': 1, 'STABLE': 2}
            filtered_alerts['severity'] = filtered_alerts['alert_status'].map(severity_order)
            filtered_alerts = filtered_alerts.sort_values('severity').drop('severity', axis=1)
            
            for _, alert in filtered_alerts.iterrows():
                alert_status = alert.get('alert_status', 'STABLE')
                if alert_status == 'CRITICAL':
                    status_emoji, border_color = "🔴", "#ff4444"
                elif alert_status == 'WARNING':
                    status_emoji, border_color = "🟡", "#ffd700"
                else:
                    status_emoji, border_color = "🟢", "#00ff9d"
                
                source_name = alert.get('source_name', 'Unknown')
                source_type = alert.get('source_type', 'Unknown')
                location = f"{alert.get('district', 'Unknown')}, {alert.get('state', 'Unknown')}"
                capacity = alert.get('capacity_percent', 0)
                ph = alert.get('ph_level', 'N/A')
                alert_time = alert.get('alert_time', 'N/A')
                if isinstance(alert_time, pd.Timestamp):
                    alert_time = alert_time.strftime('%Y-%m-%d %H:%M:%S')
                
                reasons = []
                if capacity < 30:
                    reasons.append(f"Critical capacity: {capacity}%")
                elif capacity < 60:
                    reasons.append(f"Low capacity: {capacity}%")
                if ph != 'N/A' and pd.notna(ph):
                    if ph < 6.5:
                        reasons.append(f"pH too low: {ph}")
                    elif ph > 8.5:
                        reasons.append(f"pH too high: {ph}")
                reason = " | ".join(reasons) if reasons else "Monitoring alert - Routine check"
                
                with st.expander(f"{status_emoji} {source_name} - {alert_status}", expanded=(alert_status == 'CRITICAL')):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**Type:** {source_type}")
                        st.markdown(f"**Location:** {location}")
                        st.markdown(f"**Capacity:** {capacity}%")
                        st.progress(capacity/100 if capacity > 0 else 0)
                    with col2:
                        st.markdown(f"**pH Level:** {ph}")
                        st.markdown(f"**Time:** {alert_time}")
                        st.markdown(f"**Status:** :{'red' if alert_status=='CRITICAL' else ('orange' if alert_status=='WARNING' else 'green')}[{alert_status}]")
                    st.markdown(f"**Alert Reason:** {reason}")
                    if alert_status == 'CRITICAL':
                        st.warning("⚠️ **Immediate Action Required** - Schedule emergency inspection")
                    elif alert_status == 'WARNING':
                        st.info("📋 **Action Required** - Schedule maintenance within 7 days")
                    else:
                        st.success("✅ **Normal Operations** - Routine monitoring only")
    else:
        st.success("✅ No active alerts - All systems normal")
        st.balloons()

# ===================== TAB 6: DATA TABLES =====================
with tab6:
    st.markdown('<p class="sec-hdr">📋 Data Explorer</p>', unsafe_allow_html=True)
    
    table_choice = st.selectbox("Select Table to View", ["Water Sources", "Monitoring Stations", "Groundwater Levels", 
                                                         "Rainfall History", "Water Usage", "Active Alerts", "Regional Statistics"])
    
    def dl_btn(df, prefix):
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Download CSV", csv,
                           f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                           "text/csv", use_container_width=True)

    def show_table(df, cols):
        avail = [c for c in cols if c in df.columns]
        out = df[avail].reset_index(drop=True) if avail else df.reset_index(drop=True)
        st.dataframe(out, use_container_width=True, hide_index=True)
        dl_btn(out, cols[0] if cols else "export")
    
    if table_choice == "Water Sources":
        df = filtered_sources if not filtered_sources.empty else sources
        if not df.empty:
            show_table(df, ["source_id","source_name","source_type","capacity_percent","max_capacity_mcm",
                           "build_year","age","state","district","origin_state","is_transboundary","risk_level","health_score"])
    
    elif table_choice == "Monitoring Stations":
        df = filtered_stations if not filtered_stations.empty else stations
        if not df.empty:
            show_table(df, ["station_id","station_name","state_name","district_name","latitude","longitude",
                           "ph_level","dissolved_oxygen_mg_l","turbidity_ntu","status"])
    
    elif table_choice == "Groundwater Levels":
        df = groundwater.copy()
        if filtered_districts and "district_name" in df.columns:
            df = df[df["district_name"].isin(filtered_districts)]
        if not df.empty:
            show_table(df, ["district_name","avg_depth_meters","extraction_pct","recharge_rate_mcm",
                           "assessment_year","stress_level"])
    
    elif table_choice == "Rainfall History":
        df = rainfall.copy()
        if filtered_districts and "district_name" in df.columns:
            df = df[df["district_name"].isin(filtered_districts)]
        if "rainfall_cm" in df.columns:
            df = df[df["rainfall_cm"].between(rainfall_range[0], rainfall_range[1])]
        if not df.empty:
            show_table(df, ["district_name","rainfall_cm","record_year","season","rainfall_category"])
    
    elif table_choice == "Water Usage":
        df = usage.copy()
        if selected_state != "All States" and "state" in df.columns:
            df = df[df["state"] == selected_state]
        if selected_district != "All Districts" and "district" in df.columns:
            df = df[df["district"] == selected_district]
        if not df.empty:
            show_table(df, ["source_name","source_type","sector","sub_sector","consumer_name",
                           "consumption_mcm","record_year","season","state","district"])
    
    elif table_choice == "Active Alerts":
        df = alerts.copy()
        if not df.empty:
            show_table(df, ["alert_id","source_name","capacity_percent","ph_level","alert_status","alert_time"])
    
    elif table_choice == "Regional Statistics":
        df = regional.copy()
        if not df.empty:
            show_table(df, ["region_name","population_count","annual_rainfall_avg_cm"])

# ===================== SIDEBAR SUMMARY =====================
with st.sidebar.expander("📊 Filter Summary", expanded=False):
    st.markdown(f"""
**Geography:**
- State: `{selected_state}`
- District: `{selected_district}`

**Source Filters:**
- Type: `{selected_type}`
- Capacity: `{capacity_range[0]:.0f}% – {capacity_range[1]:.0f}%`
- Risk: `{selected_risk}`
- Build Year: `{year_range[0]} – {year_range[1]}`

**Rainfall Range:**
- `{rainfall_range[0]:.0f} cm – {rainfall_range[1]:.0f} cm`
- Matching districts: `{len(rain_district_names)}`

**Results:**
- Sources: `{len(filtered_sources)}` of `{len(sources)}`
- On Map: `{on_map_n}`
""")

# ===================== EXPORT =====================
st.sidebar.markdown("---")
if st.sidebar.button("📦 Export All Filtered Data", use_container_width=True):
    exp_gw = groundwater.copy()
    if filtered_districts and "district_name" in exp_gw.columns:
        exp_gw = exp_gw[exp_gw["district_name"].isin(filtered_districts)]
    
    exp_rain = rainfall.copy()
    if filtered_districts and "district_name" in exp_rain.columns:
        exp_rain = exp_rain[exp_rain["district_name"].isin(filtered_districts)]
    
    exp_wq = water_quality.copy()
    if selected_state != "All States" and "state_name" in exp_wq.columns:
        exp_wq = exp_wq[exp_wq["state_name"] == selected_state]
    if selected_district != "All Districts" and "district_name" in exp_wq.columns:
        exp_wq = exp_wq[exp_wq["district_name"] == selected_district]
    
    export_dict = {
        "water_sources": filtered_sources,
        "monitoring_stations": filtered_stations,
        "groundwater": exp_gw,
        "rainfall": exp_rain,
        "water_quality": exp_wq,
        "alerts": alerts if not alerts.empty else pd.DataFrame(),
    }
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet, sdf in export_dict.items():
            if not sdf.empty:
                sdf.to_excel(writer, sheet_name=sheet[:31], index=False)
    st.sidebar.download_button(
        "📥 Download Excel Report", buf.getvalue(),
        f"aquastat_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# ===================== FOOTER =====================
st.markdown("---")
f1, f2, f3 = st.columns(3)
f1.markdown(
    "<p style='text-align:center;color:#00e5ff;font-size:1.1rem;margin:0'>💧 AQUASTAT</p>"
    "<p style='text-align:center;color:#7fa8c8;margin:0'>National Water Command Center</p>",
    unsafe_allow_html=True,
)
f2.markdown(
    f"<p style='text-align:center;color:#7fa8c8;margin:0'>Data Source: Ministry of Jal Shakti</p>"
    f"<p style='text-align:center;color:#7fa8c8;margin:0'>Updated: {current_time.strftime('%Y-%m-%d %H:%M IST')}</p>",
    unsafe_allow_html=True,
)
f3.markdown(
    "<p style='text-align:center;color:#7fa8c8;margin:0'>© 2025 All Rights Reserved</p>"
    "<p style='text-align:center;color:#7fa8c8;margin:0'>v3.2 — Official Use Only</p>",
    unsafe_allow_html=True,
)

st.markdown("""
<div style="position:fixed;bottom:12px;right:14px;background:rgba(0,229,255,.1);
            border:1px solid rgba(0,229,255,.2);padding:5px 12px;border-radius:20px;
            font-size:.75rem;color:#00e5ff;">
    🔄 Auto-refresh every 5 min &nbsp;•&nbsp; Cloud Connected
</div>""", unsafe_allow_html=True)

if st.button("🔄 Refresh Data", key="refresh_btn"):
    st.cache_data.clear()
    st.rerun()
