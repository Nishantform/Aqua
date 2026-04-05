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
# CSS STYLING
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
.filter-container {
    background: rgba(10,30,48,0.6);
    border: 1px solid rgba(0,200,255,0.2);
    border-radius: 12px;
    padding: 20px;
    margin-bottom: 20px;
    backdrop-filter: blur(10px);
}
</style>
""", unsafe_allow_html=True)

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
        engine = create_engine(NEON_URL, pool_size=5, max_overflow=10)
        with engine.connect() as c:
            c.execute(text("SELECT 1")).fetchone()
        return engine
    except Exception as e:
        st.error(f"⚠️ DB connection failed: {e}")
        return None

engine = init_connection()

def execute_sql_query(query, params=None):
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

# ─────────────────────────────────────────────────────────────────────────────
# SQL QUERY FUNCTIONS - RAINFALL
# ─────────────────────────────────────────────────────────────────────────────
def get_rainfall_filter_query(district_name=None, min_rainfall=None, max_rainfall=None, 
                               year=None, season=None, rainfall_category=None):
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
    if year and year != "All":
        query += " AND record_year = %s"
        params.append(year)
    if season and season != "All":
        query += " AND season = %s"
        params.append(season)
    if rainfall_category and rainfall_category != "All":
        query += " AND rainfall_category = %s"
        params.append(rainfall_category)
    query += " ORDER BY record_year DESC, rainfall_cm DESC"
    return query, params

def get_advanced_rainfall_analysis():
    return """
    SELECT 
        district_name,
        COUNT(*) as total_records,
        ROUND(AVG(rainfall_cm), 2) as avg_rainfall,
        ROUND(MIN(rainfall_cm), 2) as min_rainfall,
        ROUND(MAX(rainfall_cm), 2) as max_rainfall,
        ROUND(STDDEV(rainfall_cm), 2) as std_deviation
    FROM rainfall_history
    WHERE rainfall_cm IS NOT NULL
    GROUP BY district_name
    HAVING COUNT(*) > 5
    ORDER BY avg_rainfall DESC
    """

# ─────────────────────────────────────────────────────────────────────────────
# SQL QUERY FUNCTIONS - GROUNDWATER
# ─────────────────────────────────────────────────────────────────────────────
def get_groundwater_filter_query(district_name=None, min_depth=None, max_depth=None,
                                  min_extraction=None, max_extraction=None,
                                  stress_level=None, year=None, min_recharge=None, max_recharge=None):
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
    if year and year != "All":
        query += " AND assessment_year = %s"
        params.append(year)
    if min_recharge is not None:
        query += " AND recharge_rate_mcm >= %s"
        params.append(min_recharge)
    if max_recharge is not None:
        query += " AND recharge_rate_mcm <= %s"
        params.append(max_recharge)
    query += " ORDER BY assessment_year DESC, avg_depth_meters DESC"
    return query, params

def get_groundwater_depletion_analysis():
    return """
    SELECT 
        district_name,
        MAX(assessment_year) as latest_year,
        ROUND(AVG(avg_depth_meters), 2) as current_avg_depth,
        ROUND(AVG(extraction_pct), 2) as avg_extraction_pct,
        ROUND(AVG(recharge_rate_mcm), 2) as avg_recharge_rate,
        CASE 
            WHEN AVG(extraction_pct) > AVG(recharge_rate_mcm) * 1.5 THEN 'CRITICAL_DEPLETION'
            WHEN AVG(extraction_pct) > AVG(recharge_rate_mcm) THEN 'MODERATE_DEPLETION'
            ELSE 'STABLE'
        END as depletion_status
    FROM groundwater_levels
    GROUP BY district_name
    ORDER BY current_avg_depth DESC
    """

# ─────────────────────────────────────────────────────────────────────────────
# SQL QUERY FUNCTIONS - WATER QUALITY
# ─────────────────────────────────────────────────────────────────────────────
def get_water_quality_filter_query(state=None, district=None, min_ph=None, max_ph=None,
                                    min_do=None, max_do=None, status=None, 
                                    min_turbidity=None, max_turbidity=None):
    query = "SELECT * FROM water_monitoring_stations WHERE 1=1"
    params = []
    if state and state != "All States":
        query += " AND state_name = %s"
        params.append(state)
    if district and district != "All Districts":
        query += " AND district_name = %s"
        params.append(district)
    if min_ph is not None:
        query += " AND ph_level >= %s"
        params.append(min_ph)
    if max_ph is not None:
        query += " AND ph_level <= %s"
        params.append(max_ph)
    if min_do is not None:
        query += " AND dissolved_oxygen_mg_l >= %s"
        params.append(min_do)
    if max_do is not None:
        query += " AND dissolved_oxygen_mg_l <= %s"
        params.append(max_do)
    if status and status != "All":
        query += " AND status = %s"
        params.append(status)
    if min_turbidity is not None:
        query += " AND turbidity_ntu >= %s"
        params.append(min_turbidity)
    if max_turbidity is not None:
        query += " AND turbidity_ntu <= %s"
        params.append(max_turbidity)
    query += " ORDER BY station_name"
    return query, params

def get_water_quality_assessment():
    return """
    SELECT 
        district_name,
        COUNT(*) as total_stations,
        ROUND(AVG(ph_level), 2) as avg_ph,
        ROUND(AVG(dissolved_oxygen_mg_l), 2) as avg_do,
        ROUND(AVG(turbidity_ntu), 2) as avg_turbidity,
        CASE 
            WHEN AVG(ph_level) BETWEEN 6.5 AND 8.5 
                 AND AVG(dissolved_oxygen_mg_l) > 6 
                 AND AVG(turbidity_ntu) < 5 THEN 'EXCELLENT'
            WHEN AVG(ph_level) BETWEEN 6 AND 9 
                 AND AVG(dissolved_oxygen_mg_l) > 4 
                 AND AVG(turbidity_ntu) < 10 THEN 'GOOD'
            ELSE 'MODERATE'
        END as overall_water_quality
    FROM water_monitoring_stations
    WHERE ph_level IS NOT NULL AND dissolved_oxygen_mg_l IS NOT NULL
    GROUP BY district_name
    ORDER BY overall_water_quality, avg_do DESC
    """

# ─────────────────────────────────────────────────────────────────────────────
# SQL QUERY FUNCTIONS - WATER USAGE
# ─────────────────────────────────────────────────────────────────────────────
def get_water_usage_filter_query(sector=None, source_type=None, year=None, season=None,
                                  min_consumption=None, max_consumption=None):
    query = """
    SELECT wu.*, ws.source_name, ws.source_type, ws.state, ws.district
    FROM water_usage_history wu
    LEFT JOIN water_sources ws ON wu.source_id = ws.source_id
    WHERE 1=1
    """
    params = []
    if sector and sector != "All":
        query += " AND wu.sector = %s"
        params.append(sector)
    if source_type and source_type != "All Types":
        query += " AND ws.source_type = %s"
        params.append(source_type)
    if year and year != "All":
        query += " AND wu.record_year = %s"
        params.append(year)
    if season and season != "All":
        query += " AND wu.season = %s"
        params.append(season)
    if min_consumption is not None:
        query += " AND wu.consumption_mcm >= %s"
        params.append(min_consumption)
    if max_consumption is not None:
        query += " AND wu.consumption_mcm <= %s"
        params.append(max_consumption)
    query += " ORDER BY wu.consumption_mcm DESC"
    return query, params

def get_sector_wise_water_consumption():
    return """
    SELECT 
        sector,
        COUNT(*) as usage_records,
        ROUND(SUM(consumption_mcm), 2) as total_consumption,
        ROUND(AVG(consumption_mcm), 2) as avg_consumption
    FROM water_usage_history
    WHERE consumption_mcm IS NOT NULL AND sector IS NOT NULL
    GROUP BY sector
    ORDER BY total_consumption DESC
    """

def get_top_water_consumers():
    return """
    SELECT 
        ws.source_name,
        ws.source_type,
        ws.state,
        wu.sector,
        wu.consumption_mcm,
        wu.record_year
    FROM water_usage_history wu
    LEFT JOIN water_sources ws ON wu.source_id = ws.source_id
    WHERE wu.consumption_mcm IS NOT NULL
    ORDER BY wu.consumption_mcm DESC
    LIMIT 20
    """

# ─────────────────────────────────────────────────────────────────────────────
# ADVANCED ANALYTICS QUERIES
# ─────────────────────────────────────────────────────────────────────────────
def get_cross_parameter_correlation():
    return """
    SELECT 
        ws.district,
        ROUND(AVG(ws.capacity_percent), 2) as avg_source_capacity,
        ROUND(AVG(rh.rainfall_cm), 2) as avg_rainfall,
        ROUND(AVG(gl.avg_depth_meters), 2) as avg_groundwater_depth
    FROM water_sources ws
    LEFT JOIN rainfall_history rh ON ws.district = rh.district_name
    LEFT JOIN groundwater_levels gl ON ws.district = gl.district_name
    WHERE ws.capacity_percent IS NOT NULL
    GROUP BY ws.district
    ORDER BY avg_source_capacity DESC
    LIMIT 20
    """

def get_alert_prediction_query():
    return """
    SELECT 
        ws.source_name,
        ws.source_type,
        ws.capacity_percent,
        ws.state,
        CASE 
            WHEN ws.capacity_percent < 30 THEN 'HIGH_ALERT'
            WHEN ws.capacity_percent < 50 THEN 'MEDIUM_ALERT'
            ELSE 'NORMAL'
        END as predicted_alert_level
    FROM water_sources ws
    WHERE ws.capacity_percent IS NOT NULL
    ORDER BY predicted_alert_level, ws.capacity_percent
    LIMIT 25
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
            tables = pd.read_sql("SELECT table_name FROM information_schema.tables WHERE table_schema='public'", conn)["table_name"].tolist()
            
            def get_df(tbl):
                if tbl in tables:
                    try:
                        return pd.read_sql(f'SELECT * FROM "{tbl}"', conn)
                    except:
                        return pd.DataFrame()
                return pd.DataFrame()
            
            sources = get_df("water_sources")
            stations = get_df("water_monitoring_stations")
            groundwater = get_df("groundwater_levels")
            rainfall = get_df("rainfall_history")
            alerts = get_df("active_alerts")
            regional = get_df("regional_stats")
            water_quality = stations.copy()
            
            usage = pd.DataFrame()
            if "water_usage_history" in tables:
                usage = get_df("water_usage_history")
            
            return sources, stations, groundwater, rainfall, alerts, usage, regional, water_quality
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return [pd.DataFrame()] * 8

with st.spinner("🚀 Connecting to AQUASTAT Cloud…"):
    sources, stations, groundwater, rainfall, alerts, usage, regional, water_quality = load_all_data()

# ─────────────────────────────────────────────────────────────────────────────
# DATA PROCESSING
# ─────────────────────────────────────────────────────────────────────────────
current_year = datetime.now().year
current_time = datetime.now(pytz.timezone("Asia/Kolkata"))

if not sources.empty:
    if "capacity_percent" in sources.columns:
        sources["capacity_percent"] = pd.to_numeric(sources["capacity_percent"], errors="coerce")
        sources["risk_level"] = pd.cut(sources["capacity_percent"], bins=[0, 30, 60, 100], labels=["Critical", "Moderate", "Good"], include_lowest=True)

if not groundwater.empty and "avg_depth_meters" in groundwater.columns:
    groundwater["stress_level"] = pd.cut(groundwater["avg_depth_meters"], bins=[0, 20, 40, 100], labels=["Low", "Moderate", "High"])

if not rainfall.empty and "rainfall_cm" in rainfall.columns:
    rainfall["rainfall_category"] = pd.cut(rainfall["rainfall_cm"], bins=[0, 50, 150, 300, float("inf")], labels=["Low", "Moderate", "High", "Extreme"])

# ─────────────────────────────────────────────────────────────────────────────
# UNIFIED FILTER SYSTEM
# ─────────────────────────────────────────────────────────────────────────────
def apply_unified_filters():
    """Apply filters based on selected filter type"""
    global filtered_rainfall, filtered_groundwater, filtered_water_quality, filtered_sources_capacity
    
    filter_type = st.session_state.get("filter_type", "🌧️ Rainfall Filter")
    
    if filter_type == "🌧️ Rainfall Filter":
        query, params = get_rainfall_filter_query(
            district_name=st.session_state.get("rain_district", None) if st.session_state.get("rain_district") != "All Districts" else None,
            min_rainfall=st.session_state.get("min_rainfall", None) if st.session_state.get("min_rainfall", 0) > 0 else None,
            max_rainfall=st.session_state.get("max_rainfall", None) if st.session_state.get("max_rainfall", 500) < 500 else None,
            year=st.session_state.get("rain_year", None) if st.session_state.get("rain_year") != "All" else None,
            season=st.session_state.get("season", None) if st.session_state.get("season") != "All" else None,
            rainfall_category=st.session_state.get("rain_category", None) if st.session_state.get("rain_category") != "All" else None
        )
        filtered_rainfall, error = execute_sql_query(query, params)
        if error:
            st.error(f"Rainfall filter error: {error}")
            filtered_rainfall = pd.DataFrame()
    
    elif filter_type == "🌊 Groundwater Filter":
        query, params = get_groundwater_filter_query(
            district_name=st.session_state.get("gw_district", None) if st.session_state.get("gw_district") != "All Districts" else None,
            min_depth=st.session_state.get("min_depth", None) if st.session_state.get("min_depth", 0) > 0 else None,
            max_depth=st.session_state.get("max_depth", None) if st.session_state.get("max_depth", 50) < 50 else None,
            stress_level=st.session_state.get("stress_level", None) if st.session_state.get("stress_level") != "All" else None,
            year=st.session_state.get("gw_year", None) if st.session_state.get("gw_year") != "All" else None
        )
        filtered_groundwater, error = execute_sql_query(query, params)
        if error:
            st.error(f"Groundwater filter error: {error}")
            filtered_groundwater = pd.DataFrame()
    
    elif filter_type == "💧 Water Quality (pH) Filter":
        query, params = get_water_quality_filter_query(
            state=st.session_state.get("wq_state", None) if st.session_state.get("wq_state") != "All States" else None,
            district=st.session_state.get("wq_district", None) if st.session_state.get("wq_district") != "All Districts" else None,
            min_ph=st.session_state.get("min_ph", None) if st.session_state.get("min_ph", 0) > 0 else None,
            max_ph=st.session_state.get("max_ph", None) if st.session_state.get("max_ph", 14) < 14 else None,
            status=st.session_state.get("wq_status", None) if st.session_state.get("wq_status") != "All" else None
        )
        filtered_water_quality, error = execute_sql_query(query, params)
        if error:
            st.error(f"Water quality filter error: {error}")
            filtered_water_quality = pd.DataFrame()
    
    elif filter_type == "🏭 Source Capacity Filter":
        if not sources.empty:
            filtered_sources_capacity = sources.copy()
            if st.session_state.get("cap_state") != "All States":
                filtered_sources_capacity = filtered_sources_capacity[filtered_sources_capacity["state"] == st.session_state.get("cap_state")]
            if st.session_state.get("cap_district") != "All Districts":
                filtered_sources_capacity = filtered_sources_capacity[filtered_sources_capacity["district"] == st.session_state.get("cap_district")]
            if st.session_state.get("min_capacity", 0) > 0:
                filtered_sources_capacity = filtered_sources_capacity[filtered_sources_capacity["capacity_percent"] >= st.session_state.get("min_capacity")]
            if st.session_state.get("max_capacity", 100) < 100:
                filtered_sources_capacity = filtered_sources_capacity[filtered_sources_capacity["capacity_percent"] <= st.session_state.get("max_capacity")]
            if st.session_state.get("risk_level_filter") != "All Risk Levels":
                filtered_sources_capacity = filtered_sources_capacity[filtered_sources_capacity["risk_level"] == st.session_state.get("risk_level_filter")]
        else:
            filtered_sources_capacity = pd.DataFrame()

# Initialize session state for filters
if "filter_type" not in st.session_state:
    st.session_state.filter_type = "🌧️ Rainfall Filter"

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR FILTERS (Original filters moved to unified section)
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🎮 AQUASTAT")
    st.caption("Advanced Water Management System")
    st.markdown("---")
    
    if st.button("🔄 Reset All Filters", use_container_width=True):
        st.cache_data.clear()
        # Reset all filter session states
        for key in st.session_state.keys():
            if key not in ["filter_type"]:
                del st.session_state[key]
        st.rerun()
    
    st.markdown("---")
    st.markdown("### 🗺️ Map Settings")
    show_heatmap = st.checkbox("Show Heatmap", True)
    show_clusters = st.checkbox("Show Clusters", True)
    marker_size = st.slider("Marker Size", 5, 20, 12)

# ─────────────────────────────────────────────────────────────────────────────
# MAIN FILTER SECTION - UNIFIED LOCATION
# ─────────────────────────────────────────────────────────────────────────────
st.markdown('<div class="filter-container">', unsafe_allow_html=True)
st.markdown("### 🔍 Advanced Filter System")
st.markdown("Select a filter type from the dropdown below to apply specific filters:")

# Filter Type Selection
filter_type = st.selectbox(
    "📋 Select Filter Type",
    options=["🌧️ Rainfall Filter", "🌊 Groundwater Filter", "💧 Water Quality (pH) Filter", "🏭 Source Capacity Filter"],
    key="filter_type"
)

st.markdown("---")

# Dynamic Filter Controls based on selection
if filter_type == "🌧️ Rainfall Filter":
    st.markdown("#### 🌧️ Rainfall Data Filter")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        districts = rainfall['district_name'].unique().tolist() if 'district_name' in rainfall.columns else []
        st.selectbox("District", ["All Districts"] + list(districts), key="rain_district")
        st.number_input("Min Rainfall (cm)", 0.0, 1000.0, 0.0, key="min_rainfall")
    
    with col2:
        st.number_input("Max Rainfall (cm)", 0.0, 1000.0, 500.0, key="max_rainfall")
        years = rainfall['record_year'].unique().tolist() if 'record_year' in rainfall.columns else []
        st.selectbox("Year", ["All"] + sorted(years), key="rain_year")
    
    with col3:
        st.selectbox("Season", ["All", "Winter", "Summer", "Monsoon", "Post-Monsoon"], key="season")
        st.selectbox("Rainfall Category", ["All", "Low", "Moderate", "High", "Extreme"], key="rain_category")
    
    if st.button("🔍 Apply Rainfall Filter", use_container_width=True, type="primary"):
        apply_unified_filters()
        st.success("✅ Rainfall filter applied!")

elif filter_type == "🌊 Groundwater Filter":
    st.markdown("#### 🌊 Groundwater Data Filter")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        districts = groundwater['district_name'].unique().tolist() if 'district_name' in groundwater.columns else []
        st.selectbox("District", ["All Districts"] + list(districts), key="gw_district")
        st.number_input("Min Depth (m)", 0.0, 100.0, 0.0, key="min_depth")
    
    with col2:
        st.number_input("Max Depth (m)", 0.0, 100.0, 50.0, key="max_depth")
        st.selectbox("Stress Level", ["All", "Low", "Moderate", "High"], key="stress_level")
    
    with col3:
        years = groundwater['assessment_year'].unique().tolist() if 'assessment_year' in groundwater.columns else []
        st.selectbox("Assessment Year", ["All"] + sorted(years), key="gw_year")
    
    if st.button("🔍 Apply Groundwater Filter", use_container_width=True, type="primary"):
        apply_unified_filters()
        st.success("✅ Groundwater filter applied!")

elif filter_type == "💧 Water Quality (pH) Filter":
    st.markdown("#### 💧 Water Quality & pH Filter")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        states = water_quality['state_name'].unique().tolist() if 'state_name' in water_quality.columns else []
        st.selectbox("State", ["All States"] + list(states), key="wq_state")
        st.number_input("Min pH Level", 0.0, 14.0, 0.0, key="min_ph")
    
    with col2:
        districts = water_quality['district_name'].unique().tolist() if 'district_name' in water_quality.columns else []
        st.selectbox("District", ["All Districts"] + list(districts), key="wq_district")
        st.number_input("Max pH Level", 0.0, 14.0, 14.0, key="max_ph")
    
    with col3:
        st.selectbox("Station Status", ["All", "Active", "Maintenance", "Inactive"], key="wq_status")
        st.markdown("**pH Guide:** 6.5-8.5 (Ideal) | 6-9 (Acceptable)")
    
    if st.button("🔍 Apply Water Quality Filter", use_container_width=True, type="primary"):
        apply_unified_filters()
        st.success("✅ Water quality filter applied!")

elif filter_type == "🏭 Source Capacity Filter":
    st.markdown("#### 🏭 Water Source Capacity Filter")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        states = sources['state'].unique().tolist() if not sources.empty and 'state' in sources.columns else []
        st.selectbox("State", ["All States"] + list(states), key="cap_state")
        st.number_input("Min Capacity (%)", 0, 100, 0, key="min_capacity")
    
    with col2:
        districts = sources['district'].unique().tolist() if not sources.empty and 'district' in sources.columns else []
        st.selectbox("District", ["All Districts"] + list(districts), key="cap_district")
        st.number_input("Max Capacity (%)", 0, 100, 100, key="max_capacity")
    
    with col3:
        st.selectbox("Risk Level", ["All Risk Levels", "Critical", "Moderate", "Good"], key="risk_level_filter")
        st.markdown("**Risk Guide:** <30% Critical | 30-60% Moderate | >60% Good")
    
    if st.button("🔍 Apply Source Capacity Filter", use_container_width=True, type="primary"):
        apply_unified_filters()
        st.success("✅ Source capacity filter applied!")

st.markdown('</div>', unsafe_allow_html=True)

# Display filter results
if filter_type == "🌧️ Rainfall Filter" and 'filtered_rainfall' in globals() and not filtered_rainfall.empty:
    st.markdown("### 📊 Filter Results")
    st.dataframe(filtered_rainfall, use_container_width=True)
    st.caption(f"📈 Total records found: {len(filtered_rainfall)}")
    
    # Download button
    csv = filtered_rainfall.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Download Results", csv, "rainfall_filtered.csv", "text/csv")

elif filter_type == "🌊 Groundwater Filter" and 'filtered_groundwater' in globals() and not filtered_groundwater.empty:
    st.markdown("### 📊 Filter Results")
    st.dataframe(filtered_groundwater, use_container_width=True)
    st.caption(f"📈 Total records found: {len(filtered_groundwater)}")
    
    csv = filtered_groundwater.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Download Results", csv, "groundwater_filtered.csv", "text/csv")

elif filter_type == "💧 Water Quality (pH) Filter" and 'filtered_water_quality' in globals() and not filtered_water_quality.empty:
    st.markdown("### 📊 Filter Results")
    st.dataframe(filtered_water_quality, use_container_width=True)
    st.caption(f"📈 Total records found: {len(filtered_water_quality)}")
    
    csv = filtered_water_quality.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Download Results", csv, "water_quality_filtered.csv", "text/csv")

elif filter_type == "🏭 Source Capacity Filter" and 'filtered_sources_capacity' in globals() and not filtered_sources_capacity.empty:
    st.markdown("### 📊 Filter Results")
    st.dataframe(filtered_sources_capacity, use_container_width=True)
    st.caption(f"📈 Total sources found: {len(filtered_sources_capacity)}")
    
    csv = filtered_sources_capacity.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Download Results", csv, "sources_filtered.csv", "text/csv")

# ─────────────────────────────────────────────────────────────────────────────
# APPLY ORIGINAL FILTERS FOR MAP AND DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────
def apply_source_filters():
    df = sources.copy()
    # Keep original sidebar filters for map
    if 'selected_state' in st.session_state:
        if st.session_state.selected_state != "All States" and "state" in df.columns:
            df = df[df["state"] == st.session_state.selected_state]
    if 'selected_district' in st.session_state:
        if st.session_state.selected_district != "All Districts" and "district" in df.columns:
            df = df[df["district"] == st.session_state.selected_district]
    if 'selected_type' in st.session_state:
        if st.session_state.selected_type != "All Types" and "source_type" in df.columns:
            df = df[df["source_type"] == st.session_state.selected_type]
    if 'selected_risk' in st.session_state:
        if st.session_state.selected_risk != "All Risk Levels" and "risk_level" in df.columns:
            df = df[df["risk_level"] == st.session_state.selected_risk]
    return df

# Initialize sidebar filters if not present
if "selected_state" not in st.session_state:
    state_opts = ["All States"] + sorted(sources["state"].dropna().unique().tolist()) if not sources.empty and "state" in sources.columns else ["All States"]
    st.session_state.selected_state = "All States"
    st.session_state.selected_district = "All Districts"
    st.session_state.selected_type = "All Types"
    st.session_state.selected_risk = "All Risk Levels"

filtered_sources = apply_source_filters()
filtered_districts = filtered_sources["district"].dropna().unique().tolist() if not filtered_sources.empty and "district" in filtered_sources.columns else []

# ─────────────────────────────────────────────────────────────────────────────
# HEADER + KPIs
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(f"<h1 style='color:#00e5ff'>💧 AQUASTAT</h1>", unsafe_allow_html=True)
st.markdown(f"<p>National Water Command Center • Live Intelligence • {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}</p>", unsafe_allow_html=True)

total_cap = filtered_sources["capacity_percent"].mean() if not filtered_sources.empty and "capacity_percent" in filtered_sources.columns else 0
critical_n = len(filtered_sources[filtered_sources["capacity_percent"] < 30]) if not filtered_sources.empty and "capacity_percent" in filtered_sources.columns else 0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Sources", len(sources))
k2.metric("Avg Capacity", f"{total_cap:.1f}%")
k3.metric("Critical Sources", critical_n)
k4.metric("Active Alerts", len(alerts))
k5.metric("GW Records", len(groundwater))

# ─────────────────────────────────────────────────────────────────────────────
# MAIN TABS
# ─────────────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📊 Dashboard", "🗺️ Map View", "📈 Analytics", "💧 Water Quality", "⚠️ Alerts", "🗄️ SQL Queries"])

# TAB 1: DASHBOARD
with tab1:
    if filtered_sources.empty:
        st.warning("No data available")
    else:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Water Sources by Type")
            type_counts = filtered_sources['source_type'].value_counts()
            fig = px.pie(values=type_counts.values, names=type_counts.index, title="Source Distribution")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Risk Level Distribution")
            risk_counts = filtered_sources['risk_level'].value_counts()
            fig = px.bar(x=risk_counts.index, y=risk_counts.values, title="Risk Assessment", color=risk_counts.index)
            st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Water Sources Data")
        st.dataframe(filtered_sources, use_container_width=True)

# TAB 2: MAP VIEW
with tab2:
    st.subheader("Interactive Water Resources Map")
    map_sources = filtered_sources[filtered_sources["latitude"].notna() & filtered_sources["longitude"].notna()].copy()
    
    if not map_sources.empty:
        center_lat = map_sources["latitude"].mean()
        center_lon = map_sources["longitude"].mean()
        m = folium.Map(location=[center_lat, center_lon], zoom_start=6)
        
        cluster = MarkerCluster().add_to(m) if show_clusters else m
        for _, row in map_sources.iterrows():
            color = "red" if row.get("capacity_percent", 100) < 30 else "orange" if row.get("capacity_percent", 100) < 60 else "green"
            folium.CircleMarker([row["latitude"], row["longitude"]], radius=marker_size, color=color, fill=True, popup=row.get("source_name", "")).add_to(cluster)
        
        st_folium(m, width=1200, height=600)
    else:
        st.warning("No coordinates available for selected filters")

# TAB 3: ANALYTICS
with tab3:
    st.subheader("Analytics Dashboard")
    col1, col2 = st.columns(2)
    with col1:
        if not rainfall.empty:
            rain_trend = rainfall.groupby('record_year')['rainfall_cm'].mean()
            fig = px.line(x=rain_trend.index, y=rain_trend.values, title="Rainfall Trend")
            st.plotly_chart(fig, use_container_width=True)
    with col2:
        if not groundwater.empty:
            gw_trend = groundwater.groupby('assessment_year')['avg_depth_meters'].mean()
            fig = px.line(x=gw_trend.index, y=gw_trend.values, title="Groundwater Depth Trend")
            st.plotly_chart(fig, use_container_width=True)

# TAB 4: WATER QUALITY
with tab4:
    st.subheader("Water Quality Monitoring")
    if not water_quality.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Avg pH", f"{water_quality['ph_level'].mean():.2f}" if 'ph_level' in water_quality else "N/A")
        col2.metric("Avg Dissolved Oxygen", f"{water_quality['dissolved_oxygen_mg_l'].mean():.1f} mg/L" if 'dissolved_oxygen_mg_l' in water_quality else "N/A")
        col3.metric("Active Stations", len(water_quality[water_quality['status'] == 'Active']) if 'status' in water_quality else 0)
        st.dataframe(water_quality, use_container_width=True)
    else:
        st.info("No water quality data available")

# TAB 5: ALERTS
with tab5:
    st.subheader("Active Alerts")
    if not alerts.empty:
        critical = len(alerts[alerts['alert_status'] == 'CRITICAL']) if 'alert_status' in alerts else 0
        warning = len(alerts[alerts['alert_status'] == 'WARNING']) if 'alert_status' in alerts else 0
        col1, col2 = st.columns(2)
        col1.markdown(f"<div class='badge-critical'>🔴 CRITICAL: {critical}</div>", unsafe_allow_html=True)
        col2.markdown(f"<div class='badge-warning'>🟡 WARNING: {warning}</div>", unsafe_allow_html=True)
        st.dataframe(alerts, use_container_width=True)
    else:
        st.success("No active alerts")

# TAB 6: SQL QUERIES
with tab6:
    st.subheader("SQL Query Workspace - Complete Filtering System")
    
    filter_category = st.selectbox("Select Filter Category", [
        "🌧️ Rainfall Filters",
        "🌊 Groundwater Filters",
        "💧 Water Quality Filters",
        "📊 Water Usage Filters",
        "🔬 Advanced Analytics",
        "📝 Custom SQL Query"
    ])
    
    # RAINFALL FILTERS
    if filter_category == "🌧️ Rainfall Filters":
        st.markdown("### Rainfall Data Filters")
        col1, col2 = st.columns(2)
        with col1:
            districts = rainfall['district_name'].unique().tolist() if 'district_name' in rainfall.columns else []
            district_filter = st.selectbox("District", ["All Districts"] + list(districts))
            min_rain = st.number_input("Min Rainfall (cm)", 0.0, 1000.0, 0.0)
        with col2:
            max_rain = st.number_input("Max Rainfall (cm)", 0.0, 1000.0, 500.0)
            year_filter = st.selectbox("Year", ["All"] + sorted(rainfall['record_year'].unique().tolist()) if 'record_year' in rainfall.columns else ["All"])
        
        if st.button("Execute Rainfall Filter"):
            query, params = get_rainfall_filter_query(
                district_name=district_filter if district_filter != "All Districts" else None,
                min_rainfall=min_rain if min_rain > 0 else None,
                max_rainfall=max_rain if max_rain < 500 else None,
                year=year_filter if year_filter != "All" else None
            )
            results, error = execute_sql_query(query, params)
            if error:
                st.error(f"Error: {error}")
            else:
                st.code(query, language="sql")
                st.success(f"Found {len(results)} records")
                st.dataframe(results, use_container_width=True)
    
    # GROUNDWATER FILTERS
    elif filter_category == "🌊 Groundwater Filters":
        st.markdown("### Groundwater Data Filters")
        col1, col2 = st.columns(2)
        with col1:
            districts = groundwater['district_name'].unique().tolist() if 'district_name' in groundwater.columns else []
            gw_district = st.selectbox("District", ["All Districts"] + list(districts))
            min_depth = st.number_input("Min Depth (m)", 0.0, 100.0, 0.0)
        with col2:
            max_depth = st.number_input("Max Depth (m)", 0.0, 100.0, 50.0)
            stress_filter = st.selectbox("Stress Level", ["All", "Low", "Moderate", "High"])
        
        if st.button("Execute Groundwater Filter"):
            query, params = get_groundwater_filter_query(
                district_name=gw_district if gw_district != "All Districts" else None,
                min_depth=min_depth if min_depth > 0 else None,
                max_depth=max_depth if max_depth < 50 else None,
                stress_level=stress_filter if stress_filter != "All" else None
            )
            results, error = execute_sql_query(query, params)
            if error:
                st.error(f"Error: {error}")
            else:
                st.code(query, language="sql")
                st.success(f"Found {len(results)} records")
                st.dataframe(results, use_container_width=True)
        
        st.markdown("---")
        if st.button("Run Depletion Analysis"):
            query = get_groundwater_depletion_analysis()
            results, error = execute_sql_query(query)
            if error:
                st.error(f"Error: {error}")
            else:
                st.code(query, language="sql")
                st.dataframe(results, use_container_width=True)
    
    # WATER QUALITY FILTERS
    elif filter_category == "💧 Water Quality Filters":
        st.markdown("### Water Quality Data Filters")
        col1, col2 = st.columns(2)
        with col1:
            states = water_quality['state_name'].unique().tolist() if 'state_name' in water_quality.columns else []
            wq_state = st.selectbox("State", ["All States"] + list(states))
            min_ph = st.number_input("Min pH", 0.0, 14.0, 0.0)
        with col2:
            max_ph = st.number_input("Max pH", 0.0, 14.0, 14.0)
            status_filter = st.selectbox("Status", ["All", "Active", "Maintenance", "Inactive"])
        
        if st.button("Execute Water Quality Filter"):
            query, params = get_water_quality_filter_query(
                state=wq_state if wq_state != "All States" else None,
                min_ph=min_ph if min_ph > 0 else None,
                max_ph=max_ph if max_ph < 14 else None,
                status=status_filter if status_filter != "All" else None
            )
            results, error = execute_sql_query(query, params)
            if error:
                st.error(f"Error: {error}")
            else:
                st.code(query, language="sql")
                st.success(f"Found {len(results)} records")
                st.dataframe(results, use_container_width=True)
        
        st.markdown("---")
        if st.button("Run Quality Assessment"):
            query = get_water_quality_assessment()
            results, error = execute_sql_query(query)
            if error:
                st.error(f"Error: {error}")
            else:
                st.code(query, language="sql")
                st.dataframe(results, use_container_width=True)
    
    # WATER USAGE FILTERS
    elif filter_category == "📊 Water Usage Filters":
        st.markdown("### Water Usage Data Filters")
        col1, col2 = st.columns(2)
        with col1:
            sectors = ["All", "Agriculture", "Industrial", "Domestic", "Power Generation"]
            usage_sector = st.selectbox("Sector", sectors)
            min_consumption = st.number_input("Min Consumption (MCM)", 0.0, 10000.0, 0.0)
        with col2:
            usage_year = st.selectbox("Year", ["All"] + list(range(2020, 2026)))
            max_consumption = st.number_input("Max Consumption (MCM)", 0.0, 10000.0, 5000.0)
        
        if st.button("Execute Water Usage Filter"):
            query, params = get_water_usage_filter_query(
                sector=usage_sector if usage_sector != "All" else None,
                year=usage_year if usage_year != "All" else None,
                min_consumption=min_consumption if min_consumption > 0 else None,
                max_consumption=max_consumption if max_consumption < 5000 else None
            )
            results, error = execute_sql_query(query, params)
            if error:
                st.error(f"Error: {error}")
            else:
                st.code(query, language="sql")
                st.success(f"Found {len(results)} records")
                st.dataframe(results, use_container_width=True)
        
        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Run Sector Analysis"):
                query = get_sector_wise_water_consumption()
                results, error = execute_sql_query(query)
                if error:
                    st.error(f"Error: {error}")
                else:
                    st.code(query, language="sql")
                    st.dataframe(results, use_container_width=True)
        with col2:
            if st.button("Find Top Consumers"):
                query = get_top_water_consumers()
                results, error = execute_sql_query(query)
                if error:
                    st.error(f"Error: {error}")
                else:
                    st.code(query, language="sql")
                    st.dataframe(results, use_container_width=True)
    
    # ADVANCED ANALYTICS
    elif filter_category == "🔬 Advanced Analytics":
        st.markdown("### Advanced Analytics")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Cross-Parameter Correlation"):
                query = get_cross_parameter_correlation()
                results, error = execute_sql_query(query)
                if error:
                    st.error(f"Error: {error}")
                else:
                    st.code(query, language="sql")
                    st.dataframe(results, use_container_width=True)
        with col2:
            if st.button("Alert Prediction"):
                query = get_alert_prediction_query()
                results, error = execute_sql_query(query)
                if error:
                    st.error(f"Error: {error}")
                else:
                    st.code(query, language="sql")
                    st.dataframe(results, use_container_width=True)
    
    # CUSTOM SQL QUERY
    elif filter_category == "📝 Custom SQL Query":
        st.markdown("### Write Your Own SQL Query")
        custom_query = st.text_area("Enter SQL Query:", height=150, placeholder="SELECT * FROM water_sources WHERE capacity_percent < 50")
        if st.button("Execute Custom Query"):
            if custom_query.strip():
                results, error = execute_sql_query(custom_query)
                if error:
                    st.error(f"Error: {error}")
                else:
                    st.code(custom_query, language="sql")
                    st.success(f"Found {len(results)} records")
                    st.dataframe(results, use_container_width=True)
                    
                    csv = results.to_csv(index=False).encode('utf-8')
                    st.download_button("Download Results", csv, "query_results.csv", "text/csv")
            else:
                st.warning("Please enter a query")

# ─────────────────────────────────────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(f"<p style='text-align:center'>AQUASTAT v3.2 • Complete SQL Filtering System • Updated: {current_time.strftime('%Y-%m-%d %H:%M IST')}</p>", unsafe_allow_html=True)

if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()
