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
.sql-query-box {
    background:#0a1e30;
    border-left: 4px solid #00e5ff;
    padding: 12px;
    border-radius: 8px;
    font-family: 'Courier New', monospace;
    font-size: 0.85rem;
    margin: 10px 0;
    overflow-x: auto;
    white-space: pre-wrap;
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
# DATABASE CONNECTION
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

# ─────────────────────────────────────────────────────────────────────────────
# SQL QUERY FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def execute_sql_query(query, params=None):
    """Execute SQL query and return results"""
    if engine is None:
        return None, "Database not connected"
    try:
        with engine.connect() as conn:
            if params:
                result = pd.read_sql(query, conn, params=params)
            else:
                result = pd.read_sql(query, conn)
        return result, None
    except Exception as e:
        return None, str(e)

# Basic Filter Queries
def get_rainfall_filter_query(district_name=None, min_rainfall=None, max_rainfall=None, 
                               year=None, season=None):
    query = "SELECT * FROM rainfall_history WHERE 1=1"
    params = []
    if district_name and district_name != "All Districts":
        query += " AND district_name = %s"
        params.append(district_name)
    if min_rainfall is not None:
        query += " AND rainfall_cm >= %s"
        params.append(min_rainfall)
    if max_rainfall is not None:
        query += " AND rainfall_cm <= %s"
        params.append(max_rainfall)
    if year:
        query += " AND record_year = %s"
        params.append(year)
    if season and season != "All":
        query += " AND season = %s"
        params.append(season)
    query += " ORDER BY record_year DESC, rainfall_cm DESC"
    return query, params

def get_groundwater_filter_query(district_name=None, min_depth=None, max_depth=None,
                                  min_extraction=None, max_extraction=None,
                                  stress_level=None, year=None):
    query = "SELECT * FROM groundwater_levels WHERE 1=1"
    params = []
    if district_name and district_name != "All Districts":
        query += " AND district_name = %s"
        params.append(district_name)
    if min_depth is not None:
        query += " AND avg_depth_meters >= %s"
        params.append(min_depth)
    if max_depth is not None:
        query += " AND avg_depth_meters <= %s"
        params.append(max_depth)
    if min_extraction is not None:
        query += " AND extraction_pct >= %s"
        params.append(min_extraction)
    if max_extraction is not None:
        query += " AND extraction_pct <= %s"
        params.append(max_extraction)
    if stress_level and stress_level != "All":
        query += " AND stress_level = %s"
        params.append(stress_level)
    if year:
        query += " AND assessment_year = %s"
        params.append(year)
    query += " ORDER BY assessment_year DESC, avg_depth_meters DESC"
    return query, params

def get_water_sources_filter_query(source_type=None, state=None, district=None,
                                    min_capacity=None, max_capacity=None,
                                    risk_level=None, min_year=None, max_year=None):
    query = "SELECT * FROM water_sources WHERE 1=1"
    params = []
    if source_type and source_type != "All Types":
        query += " AND source_type = %s"
        params.append(source_type)
    if state and state != "All States":
        query += " AND state = %s"
        params.append(state)
    if district and district != "All Districts":
        query += " AND district = %s"
        params.append(district)
    if min_capacity is not None:
        query += " AND capacity_percent >= %s"
        params.append(min_capacity)
    if max_capacity is not None:
        query += " AND capacity_percent <= %s"
        params.append(max_capacity)
    if risk_level and risk_level != "All Risk Levels":
        query += " AND risk_level = %s"
        params.append(risk_level)
    if min_year is not None:
        query += " AND build_year >= %s"
        params.append(min_year)
    if max_year is not None:
        query += " AND build_year <= %s"
        params.append(max_year)
    query += " ORDER BY capacity_percent DESC"
    return query, params

# Advanced Analytical Queries
def get_state_wise_statistics():
    return """
    SELECT 
        state,
        COUNT(*) as total_sources,
        ROUND(AVG(capacity_percent), 2) as avg_capacity_percent,
        ROUND(MIN(capacity_percent), 2) as min_capacity,
        ROUND(MAX(capacity_percent), 2) as max_capacity,
        COUNT(CASE WHEN capacity_percent < 30 THEN 1 END) as critical_sources
    FROM water_sources
    WHERE state IS NOT NULL AND capacity_percent IS NOT NULL
    GROUP BY state
    ORDER BY avg_capacity_percent DESC
    """

def get_join_query():
    return """
    SELECT 
        ws.source_name,
        ws.source_type,
        ws.state,
        ws.capacity_percent,
        wms.station_name,
        wms.ph_level,
        wms.status as station_status
    FROM water_sources ws
    LEFT JOIN water_monitoring_stations wms 
        ON ws.district = wms.district_name
    WHERE ws.capacity_percent IS NOT NULL
    LIMIT 30
    """

def get_subquery_example():
    return """
    SELECT 
        source_name,
        source_type,
        state,
        capacity_percent
    FROM water_sources
    WHERE capacity_percent > (
        SELECT AVG(capacity_percent) 
        FROM water_sources 
        WHERE capacity_percent IS NOT NULL
    )
    ORDER BY capacity_percent DESC
    """

def get_window_function_query():
    return """
    SELECT 
        district_name,
        record_year,
        rainfall_cm,
        ROW_NUMBER() OVER (PARTITION BY district_name ORDER BY rainfall_cm DESC) as rank_in_district,
        LAG(rainfall_cm, 1) OVER (PARTITION BY district_name ORDER BY record_year) as previous_year_rainfall
    FROM rainfall_history
    WHERE rainfall_cm IS NOT NULL
    ORDER BY district_name, record_year
    LIMIT 30
    """

def get_case_statement_query():
    return """
    SELECT 
        source_name,
        capacity_percent,
        CASE 
            WHEN capacity_percent < 30 THEN 'CRITICAL'
            WHEN capacity_percent BETWEEN 30 AND 60 THEN 'WARNING'
            ELSE 'GOOD'
        END as risk_level,
        CASE
            WHEN capacity_percent < 20 THEN 'Immediate Action Required'
            WHEN capacity_percent < 40 THEN 'Schedule Maintenance'
            ELSE 'Routine Monitoring'
        END as action_required
    FROM water_sources
    WHERE capacity_percent IS NOT NULL
    ORDER BY capacity_percent
    LIMIT 20
    """

def get_having_clause_query():
    return """
    SELECT 
        district_name,
        COUNT(*) as rainfall_records,
        ROUND(AVG(rainfall_cm), 2) as avg_rainfall
    FROM rainfall_history
    WHERE rainfall_cm IS NOT NULL
    GROUP BY district_name
    HAVING COUNT(*) > 3 AND AVG(rainfall_cm) > 100
    ORDER BY avg_rainfall DESC
    """

def get_date_functions_query():
    return """
    SELECT 
        source_name,
        alert_status,
        alert_time,
        EXTRACT(YEAR FROM alert_time) as alert_year,
        EXTRACT(MONTH FROM alert_time) as alert_month,
        TO_CHAR(alert_time, 'Day') as day_of_week
    FROM active_alerts
    WHERE alert_time >= CURRENT_DATE - INTERVAL '90 days'
    ORDER BY alert_time DESC
    LIMIT 20
    """

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

                sources = get_df("water_sources")
                stations = get_df("water_monitoring_stations")
                groundwater = get_df("groundwater_levels")
                rainfall = get_df("rainfall_history")
                alerts = get_df("active_alerts")
                regional = get_df("regional_stats")
                water_quality = stations.copy() if not stations.empty else pd.DataFrame()

                usage = pd.DataFrame()
                if "water_usage_history" in tables:
                    try:
                        usage = pd.read_sql('SELECT * FROM "water_usage_history"', conn)
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
    c1.metric("Sources", len(sources))
    c1.metric("Stations", len(stations))
    c2.metric("GW Records", len(groundwater))
    c2.metric("Rainfall", len(rainfall))

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
        sources["risk_level"] = "Unknown"
    np.random.seed(42)
    sources["trend"] = np.random.choice(
        ["📈 Increasing", "📉 Decreasing", "➡️ Stable"], len(sources)
    )

# Coordinate merge
if not sources.empty and not stations.empty:
    station_coords = stations[stations["latitude"].notna() & stations["longitude"].notna()].copy()
    if not station_coords.empty and "district_name" in station_coords.columns:
        station_coords["district_clean"] = station_coords["district_name"].str.strip().str.lower()
        district_lookup = station_coords.groupby("district_clean").agg({
            "latitude": "first",
            "longitude": "first"
        }).reset_index()
        
        sources["district_clean"] = sources["district"].str.strip().str.lower()
        sources = sources.merge(district_lookup, on="district_clean", how="left")
        sources = sources.drop("district_clean", axis=1)
        
        if "latitude_y" in sources.columns:
            sources["latitude"] = sources["latitude_y"].fillna(sources.get("latitude_x", np.nan))
            sources["longitude"] = sources["longitude_y"].fillna(sources.get("longitude_x", np.nan))
            sources = sources.drop(["latitude_x", "latitude_y", "longitude_x", "longitude_y"], axis=1, errors="ignore")
    else:
        if "latitude" not in sources.columns:
            sources["latitude"] = np.nan
        if "longitude" not in sources.columns:
            sources["longitude"] = np.nan

sources["latitude"] = pd.to_numeric(sources["latitude"], errors="coerce")
sources["longitude"] = pd.to_numeric(sources["longitude"], errors="coerce")

if not groundwater.empty:
    if "avg_depth_meters" in groundwater.columns:
        groundwater["stress_level"] = pd.cut(
            groundwater["avg_depth_meters"],
            bins=[0, 20, 40, 100], labels=["Low", "Moderate", "High"]
        )

if not rainfall.empty:
    if "rainfall_cm" in rainfall.columns:
        rainfall["rainfall_category"] = pd.cut(
            rainfall["rainfall_cm"],
            bins=[0, 50, 150, 300, float("inf")],
            labels=["Low", "Moderate", "High", "Extreme"]
        )

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR FILTERS
# ─────────────────────────────────────────────────────────────────────────────
_cap_min = float(sources["capacity_percent"].min()) if not sources.empty and "capacity_percent" in sources.columns and not sources["capacity_percent"].isna().all() else 0.0
_cap_max = float(sources["capacity_percent"].max()) if not sources.empty and "capacity_percent" in sources.columns and not sources["capacity_percent"].isna().all() else 100.0
_by_min = int(sources["build_year"].min()) if not sources.empty and "build_year" in sources.columns and not sources["build_year"].isna().all() else 1800
_by_max = int(sources["build_year"].max()) if not sources.empty and "build_year" in sources.columns and not sources["build_year"].isna().all() else current_year
_r_min = float(rainfall["rainfall_cm"].min()) if not rainfall.empty and "rainfall_cm" in rainfall.columns and not rainfall["rainfall_cm"].isna().all() else 0.0
_r_max = float(rainfall["rainfall_cm"].max()) if not rainfall.empty and "rainfall_cm" in rainfall.columns and not rainfall["rainfall_cm"].isna().all() else 1000.0

with st.sidebar:
    st.title("🎮 AQUASTAT")
    st.caption("Command Interface v3.2 - SQL Enhanced")
    st.markdown("---")

    if st.button("🔄 Reset All Filters", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")

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
    st.markdown("### 💧 Source Filters")

    type_opts = (["All Types"] + sorted(sources["source_type"].dropna().unique().tolist())
                 if not sources.empty and "source_type" in sources.columns else ["All Types"])
    selected_type = st.selectbox("Source Type", type_opts)

    capacity_range = st.slider("Capacity %", _cap_min, _cap_max, (_cap_min, _cap_max))

    risk_opts = (["All Risk Levels"] + list(sources["risk_level"].dropna().unique())
                 if not sources.empty and "risk_level" in sources.columns else ["All Risk Levels"])
    selected_risk = st.selectbox("Risk Level", risk_opts)

    st.markdown("---")
    st.markdown("### 📅 Build Year Filter")
    year_range = st.slider("Build Year", _by_min, _by_max, (_by_min, _by_max))

    st.markdown("---")
    st.markdown("### ☔ Rainfall Filter")
    rainfall_range = st.slider("Avg Rainfall (cm)", _r_min, _r_max, (_r_min, _r_max))
    show_rain_on_map = st.checkbox("Show rainfall districts on map", value=False)

    st.markdown("---")
    st.markdown("### 🗺️ Map Settings")
    map_style = st.selectbox("Map Style", ["Esri Satellite", "OpenStreetMap", "CartoDB Dark", "CartoDB Light"])
    show_heatmap = st.checkbox("Show Heatmap", True)
    show_clusters = st.checkbox("Show Clusters", True)
    show_stations_map = st.checkbox("Show Monitoring Stations", True)
    marker_size = st.slider("Marker Size", 5, 20, 12)

    st.markdown("---")
    st.caption(f"📊 Total Sources: {len(sources):,}")

# ─────────────────────────────────────────────────────────────────────────────
# APPLY FILTERS
# ─────────────────────────────────────────────────────────────────────────────
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
    if "build_year" in df.columns:
        df = df[df["build_year"].isna() | df["build_year"].between(year_range[0], year_range[1])]
    return df

filtered_sources = apply_source_filters()

def apply_station_filters():
    df = stations.copy()
    if selected_state != "All States" and "state_name" in df.columns:
        df = df[df["state_name"] == selected_state]
    if selected_district != "All Districts" and "district_name" in df.columns:
        df = df[df["district_name"] == selected_district]
    return df

filtered_stations = apply_station_filters()

filtered_districts = (
    filtered_sources["district"].dropna().unique().tolist()
    if not filtered_sources.empty and "district" in filtered_sources.columns else []
)

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

total_cap = (filtered_sources["capacity_percent"].mean()
              if not filtered_sources.empty and "capacity_percent" in filtered_sources.columns else 0)
critical_n = (len(filtered_sources[filtered_sources["capacity_percent"] < 30])
              if not filtered_sources.empty and "capacity_percent" in filtered_sources.columns else 0)
on_map_n = (int(filtered_sources["latitude"].notna().sum())
              if not filtered_sources.empty and "latitude" in filtered_sources.columns else 0)

k1, k2, k3, k4, k5 = st.columns(5)
kpi_card(k1, "Total Sources", f"{len(sources):,}", f"{len(filtered_sources)} shown")
kpi_card(k2, "Avg Capacity", f"{total_cap:.1f}%", "filtered sources")
kpi_card(k3, "Critical Sources", str(critical_n), "below 30% capacity")
kpi_card(k4, "Mapped Sources", str(on_map_n), "with coordinates")
kpi_card(k5, "Active Alerts", str(len(alerts)), "system-wide")

# ─────────────────────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📊 Dashboard", "🗺️ Map View", "📈 Analytics",
    "💧 Water Quality", "⚠️ Alerts", "📋 Data Tables", "🗄️ SQL Queries"
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

# ===================== TAB 2: MAP VIEW =====================
with tab2:
    st.markdown('<p class="sec-hdr">🗺️ National Interactive Water Resources Map</p>', unsafe_allow_html=True)

    if not filtered_sources.empty and "latitude" in filtered_sources.columns:
        map_sources = filtered_sources[
            filtered_sources["latitude"].notna() & filtered_sources["longitude"].notna()
        ].copy()
    else:
        map_sources = pd.DataFrame()

    if not map_sources.empty:
        center_lat = float(map_sources["latitude"].mean())
        center_lon = float(map_sources["longitude"].mean())
        zoom = 9 if selected_district != "All Districts" else 7 if selected_state != "All States" else 5
    else:
        center_lat, center_lon, zoom = 20.5937, 78.9629, 5

    style_map = {
        "Esri Satellite": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        "OpenStreetMap": "OpenStreetMap",
        "CartoDB Dark": "CartoDB dark_matter",
        "CartoDB Light": "CartoDB positron",
    }

    m = folium.Map(location=[center_lat, center_lon], zoom_start=zoom,
                   tiles=style_map[map_style], attr="AQUASTAT")
    Fullscreen().add_to(m)

    cluster_group = MarkerCluster().add_to(m) if show_clusters and len(map_sources) > 10 else m
    heat_data = []
    sources_on_map = 0

    for _, row in map_sources.iterrows():
        cap = float(row.get("capacity_percent", 50) or 50)
        if cap < 30: clr = "#ff4444"
        elif cap < 60: clr = "#ffd700"
        else: clr = "#00ff9d"

        lat = float(row["latitude"])
        lon = float(row["longitude"])
        heat_data.append([lat, lon])
        sources_on_map += 1

        folium.CircleMarker(
            location=[lat, lon],
            radius=marker_size,
            color=clr, fill=True, fill_opacity=0.75, weight=1.5,
            tooltip=f"{row.get('source_name','?')} — {cap:.0f}%",
        ).add_to(cluster_group)

    if show_heatmap and heat_data:
        HeatMap(heat_data, radius=15, blur=10).add_to(m)

    if show_stations_map and not filtered_stations.empty:
        if "latitude" in filtered_stations.columns and "longitude" in filtered_stations.columns:
            st_geo = filtered_stations[
                filtered_stations["latitude"].notna() & filtered_stations["longitude"].notna()
            ]
            for _, row in st_geo.iterrows():
                folium.Marker(
                    location=[float(row["latitude"]), float(row["longitude"])],
                    icon=folium.Icon(color="blue", icon="info-sign"),
                    tooltip=f"⚙ {row.get('station_name','?')}",
                ).add_to(m)

    if map_sources.empty:
        st.warning("⚠️ No sources with coordinates for current filters.")

    st_folium(m, width=1300, height=620)

# ===================== TAB 3: ANALYTICS =====================
with tab3:
    st.markdown('<p class="sec-hdr">📈 Advanced Analytics</p>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Rainfall Trend")
        if not rainfall.empty and 'record_year' in rainfall.columns:
            rain_trend = rainfall.groupby('record_year')['rainfall_cm'].mean().reset_index()
            if not rain_trend.empty:
                fig = px.line(rain_trend, x='record_year', y='rainfall_cm', title="Average Rainfall Over Years", 
                            template="plotly_dark", markers=True)
                fig.update_traces(line_color='#00e5ff', line_width=3)
                st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Groundwater Trend")
        if not groundwater.empty and 'assessment_year' in groundwater.columns:
            gw_trend = groundwater.groupby('assessment_year')['avg_depth_meters'].mean().reset_index()
            if not gw_trend.empty:
                fig = px.line(gw_trend, x='assessment_year', y='avg_depth_meters', title="Average Groundwater Depth", 
                            template="plotly_dark", markers=True)
                fig.update_traces(line_color='#ffd700', line_width=3)
                st.plotly_chart(fig, use_container_width=True)

# ===================== TAB 4: WATER QUALITY =====================
with tab4:
    st.markdown('<p class="sec-hdr">💧 Water Quality Monitoring</p>', unsafe_allow_html=True)
    
    if not water_quality.empty:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            avg_ph = water_quality['ph_level'].mean() if 'ph_level' in water_quality.columns else 0
            st.metric("Average pH", f"{avg_ph:.2f}")
        with col2:
            avg_do = water_quality['dissolved_oxygen_mg_l'].mean() if 'dissolved_oxygen_mg_l' in water_quality.columns else 0
            st.metric("Dissolved Oxygen", f"{avg_do:.1f} mg/L")
        with col3:
            avg_turbidity = water_quality['turbidity_ntu'].mean() if 'turbidity_ntu' in water_quality.columns else 0
            st.metric("Turbidity", f"{avg_turbidity:.1f} NTU")
        with col4:
            active_count = len(water_quality[water_quality['status'] == 'Active']) if 'status' in water_quality.columns else 0
            st.metric("Active Stations", active_count)
        
        st.dataframe(water_quality.head(20), use_container_width=True)
    else:
        st.info("No water quality data available.")

# ===================== TAB 5: ALERTS =====================
with tab5:
    st.markdown('<p class="sec-hdr">🚨 Active Alerts and Warnings</p>', unsafe_allow_html=True)
    
    if not alerts.empty:
        critical_alerts = alerts[alerts['alert_status'] == 'CRITICAL'] if 'alert_status' in alerts.columns else pd.DataFrame()
        warning_alerts = alerts[alerts['alert_status'] == 'WARNING'] if 'alert_status' in alerts.columns else pd.DataFrame()
        
        col1, col2, col3 = st.columns(3)
        with col1: st.markdown(f'<div class="badge-critical">🔴 CRITICAL: {len(critical_alerts)}</div>', unsafe_allow_html=True)
        with col2: st.markdown(f'<div class="badge-warning">🟡 WARNING: {len(warning_alerts)}</div>', unsafe_allow_html=True)
        with col3: st.markdown(f'<div class="badge-good">🟢 TOTAL: {len(alerts)}</div>', unsafe_allow_html=True)
        
        st.dataframe(alerts, use_container_width=True)
    else:
        st.success("✅ No active alerts - All systems normal")

# ===================== TAB 6: DATA TABLES =====================
with tab6:
    st.markdown('<p class="sec-hdr">📋 Data Explorer</p>', unsafe_allow_html=True)
    
    table_choice = st.selectbox("Select Table to View", ["Water Sources", "Monitoring Stations", "Groundwater Levels", "Rainfall History", "Active Alerts"])
    
    if table_choice == "Water Sources":
        st.dataframe(filtered_sources if not filtered_sources.empty else sources, use_container_width=True)
    elif table_choice == "Monitoring Stations":
        st.dataframe(filtered_stations if not filtered_stations.empty else stations, use_container_width=True)
    elif table_choice == "Groundwater Levels":
        st.dataframe(groundwater, use_container_width=True)
    elif table_choice == "Rainfall History":
        st.dataframe(rainfall, use_container_width=True)
    elif table_choice == "Active Alerts":
        st.dataframe(alerts, use_container_width=True)

# ===================== TAB 7: SQL QUERIES =====================
with tab7:
    st.markdown('<p class="sec-hdr">🗄️ SQL Query Workspace</p>', unsafe_allow_html=True)
    
    query_type = st.selectbox("Select Query Type", [
        "Basic WHERE Filter",
        "JOIN Query",
        "GROUP BY with Aggregates",
        "Subquery Example",
        "Window Functions",
        "CASE Statement",
        "HAVING Clause",
        "Date Functions",
        "Custom Query"
    ])
    
    if query_type == "Basic WHERE Filter":
        col1, col2 = st.columns(2)
        with col1:
            filter_state = st.text_input("State (e.g., Maharashtra)", "")
            min_cap = st.number_input("Min Capacity %", 0, 100, 0)
        with col2:
            filter_type_sql = st.selectbox("Source Type", ["All", "Dam", "Reservoir", "River", "Lake"])
            max_cap = st.number_input("Max Capacity %", 0, 100, 100)
        
        if st.button("Execute Query"):
            query, params = get_water_sources_filter_query(
                state=filter_state if filter_state else None,
                min_capacity=min_cap if min_cap > 0 else None,
                max_capacity=max_cap if max_cap < 100 else None,
                source_type=filter_type_sql if filter_type_sql != "All" else None
            )
            results, error = execute_sql_query(query, params)
            if error:
                st.error(f"Error: {error}")
            else:
                st.markdown(f"<div class='sql-query-box'>{query}</div>", unsafe_allow_html=True)
                st.dataframe(results, use_container_width=True)
    
    elif query_type == "JOIN Query":
        if st.button("Execute JOIN Query"):
            query = get_join_query()
            results, error = execute_sql_query(query)
            if error:
                st.error(f"Error: {error}")
            else:
                st.markdown(f"<div class='sql-query-box'>{query}</div>", unsafe_allow_html=True)
                st.dataframe(results, use_container_width=True)
    
    elif query_type == "GROUP BY with Aggregates":
        if st.button("Execute GROUP BY Query"):
            query = get_state_wise_statistics()
            results, error = execute_sql_query(query)
            if error:
                st.error(f"Error: {error}")
            else:
                st.markdown(f"<div class='sql-query-box'>{query}</div>", unsafe_allow_html=True)
                st.dataframe(results, use_container_width=True)
                
                # Bar chart visualization
                if not results.empty:
                    fig = px.bar(results, x='state', y='avg_capacity_percent', title="Average Capacity by State",
                                color='avg_capacity_percent', color_continuous_scale='RdYlGn')
                    st.plotly_chart(fig, use_container_width=True)
    
    elif query_type == "Subquery Example":
        if st.button("Execute Subquery"):
            query = get_subquery_example()
            results, error = execute_sql_query(query)
            if error:
                st.error(f"Error: {error}")
            else:
                st.markdown(f"<div class='sql-query-box'>{query}</div>", unsafe_allow_html=True)
                st.dataframe(results, use_container_width=True)
    
    elif query_type == "Window Functions":
        if st.button("Execute Window Function Query"):
            query = get_window_function_query()
            results, error = execute_sql_query(query)
            if error:
                st.error(f"Error: {error}")
            else:
                st.markdown(f"<div class='sql-query-box'>{query}</div>", unsafe_allow_html=True)
                st.dataframe(results, use_container_width=True)
    
    elif query_type == "CASE Statement":
        if st.button("Execute CASE Query"):
            query = get_case_statement_query()
            results, error = execute_sql_query(query)
            if error:
                st.error(f"Error: {error}")
            else:
                st.markdown(f"<div class='sql-query-box'>{query}</div>", unsafe_allow_html=True)
                st.dataframe(results, use_container_width=True)
    
    elif query_type == "HAVING Clause":
        if st.button("Execute HAVING Query"):
            query = get_having_clause_query()
            results, error = execute_sql_query(query)
            if error:
                st.error(f"Error: {error}")
            else:
                st.markdown(f"<div class='sql-query-box'>{query}</div>", unsafe_allow_html=True)
                st.dataframe(results, use_container_width=True)
    
    elif query_type == "Date Functions":
        if st.button("Execute Date Functions Query"):
            query = get_date_functions_query()
            results, error = execute_sql_query(query)
            if error:
                st.error(f"Error: {error}")
            else:
                st.markdown(f"<div class='sql-query-box'>{query}</div>", unsafe_allow_html=True)
                st.dataframe(results, use_container_width=True)
    
    elif query_type == "Custom Query":
        custom_query = st.text_area("Enter your SQL query:", height=150)
        if st.button("Run Custom Query"):
            if custom_query.strip():
                results, error = execute_sql_query(custom_query)
                if error:
                    st.error(f"Error: {error}")
                else:
                    st.markdown(f"<div class='sql-query-box'>{custom_query}</div>", unsafe_allow_html=True)
                    st.dataframe(results, use_container_width=True)
                    
                    # Download button
                    csv = results.to_csv(index=False).encode('utf-8')
                    st.download_button("📥 Download Results", csv, "query_results.csv", "text/csv")
            else:
                st.warning("Please enter a SQL query")

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
    "<p style='text-align:center;color:#7fa8c8;margin:0'>v3.2 — SQL Enhanced</p>",
    unsafe_allow_html=True,
)

if st.button("🔄 Refresh Data", key="refresh_btn"):
    st.cache_data.clear()
    st.rerun()
