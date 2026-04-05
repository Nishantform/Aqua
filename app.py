"""
AQUASTAT - National Water Command Center
Complete Water Management System with 6 Filter Sections
Each filter has its own SQL query function
Filter Summary Dashboard shows all active filters
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import folium
from folium.plugins import MarkerCluster, HeatMap, Fullscreen
from streamlit_folium import st_folium
from datetime import datetime
import warnings
from sqlalchemy import create_engine, text
import pytz

warnings.filterwarnings('ignore')

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="AQUASTAT - National Water Command Center",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# CUSTOM CSS STYLING
# ============================================================================

st.markdown("""
<style>
.main { background: linear-gradient(135deg, #0a0f1e 0%, #0d1a2b 100%); color: #e6f1ff; }
.metric-card { background: rgba(10, 25, 47, 0.7); border: 1px solid #1e3a5f; border-radius: 10px; padding: 20px; margin: 10px 0; backdrop-filter: blur(10px); }
.alert-critical { background: linear-gradient(135deg, #ff416c, #ff4b2b); color: white; padding: 5px 15px; border-radius: 20px; display: inline-block; animation: pulse 2s infinite; }
.alert-warning { background: linear-gradient(135deg, #f7971e, #ffd200); color: black; padding: 5px 15px; border-radius: 20px; display: inline-block; }
@keyframes pulse { 0% { opacity: 1; transform: scale(1); } 50% { opacity: 0.8; transform: scale(1.05); } 100% { opacity: 1; transform: scale(1); } }
.section-header { color: #00e5ff; font-size: 1.5rem; font-weight: 600; margin: 20px 0; padding-bottom: 10px; border-bottom: 2px solid #1e3a5f; }
.filter-section { background: rgba(10, 30, 48, 0.5); border: 1px solid #1e3a5f; border-radius: 10px; padding: 15px; margin-bottom: 15px; }
.filter-header { color: #00e5ff; font-size: 1rem; font-weight: 600; margin-bottom: 10px; padding-bottom: 5px; border-bottom: 1px solid #1e3a5f; }
.viewer-badge { background: linear-gradient(135deg, #00b09b, #96c93d); color: white; padding: 5px 15px; border-radius: 20px; display: inline-block; }
.filter-summary { background: rgba(0, 229, 255, 0.1); border-left: 4px solid #00e5ff; border-radius: 8px; padding: 15px; margin: 15px 0; }
.filter-tag { background: #1e3a5f; padding: 4px 12px; border-radius: 20px; font-size: 0.8rem; margin: 3px; display: inline-block; }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

@st.cache_resource
def init_database_connection():
    """Initialize Neon PostgreSQL database connection"""
    try:
        NEON_URL = st.secrets["NEON_URL"]
        engine = create_engine(
            NEON_URL,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True,
            connect_args={'connect_timeout': 10, 'keepalives': 1, 'keepalives_idle': 30, 'sslmode': 'require'}
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).fetchone()
        return engine
    except Exception as e:
        st.error(f"⚠️ Database connection failed: {e}")
        return None

engine = init_database_connection()

def execute_sql(query, params=None):
    """Execute SQL query and return dataframe"""
    if engine is None:
        return pd.DataFrame()
    try:
        with engine.connect() as conn:
            if params:
                return pd.read_sql(query, conn, params=params)
            else:
                return pd.read_sql(query, conn)
    except Exception as e:
        return pd.DataFrame()

# ============================================================================
# DATA LOADING
# ============================================================================

@st.cache_data(ttl=300)
def load_all_data():
    """Load all data from database"""
    if engine is None:
        return [pd.DataFrame()] * 5
    
    try:
        sources = execute_sql("SELECT * FROM water_sources")
        groundwater = execute_sql("SELECT * FROM groundwater_levels")
        rainfall = execute_sql("SELECT * FROM rainfall_history")
        water_quality = execute_sql("SELECT * FROM water_monitoring_stations")
        alerts = execute_sql("SELECT * FROM active_alerts")
        
        return sources, groundwater, rainfall, water_quality, alerts
    except Exception as e:
        return [pd.DataFrame()] * 5

with st.spinner("🚀 Connecting to AQUASTAT Cloud Database..."):
    sources, groundwater, rainfall, water_quality, alerts = load_all_data()

# ============================================================================
# DATA PROCESSING
# ============================================================================

current_time = datetime.now(pytz.timezone('Asia/Kolkata'))

if not sources.empty and 'capacity_percent' in sources.columns:
    sources['capacity_percent'] = pd.to_numeric(sources['capacity_percent'], errors='coerce')
    sources['risk_level'] = pd.cut(sources['capacity_percent'], bins=[0, 30, 60, 100], labels=['Critical', 'Moderate', 'Good'], include_lowest=True)

if not groundwater.empty and 'avg_depth_meters' in groundwater.columns:
    groundwater['avg_depth_meters'] = pd.to_numeric(groundwater['avg_depth_meters'], errors='coerce')
    groundwater['stress_level'] = pd.cut(groundwater['avg_depth_meters'], bins=[0, 20, 40, 100], labels=['Low', 'Moderate', 'High'])

if not rainfall.empty and 'rainfall_cm' in rainfall.columns:
    rainfall['rainfall_cm'] = pd.to_numeric(rainfall['rainfall_cm'], errors='coerce')
    rainfall['rainfall_category'] = pd.cut(rainfall['rainfall_cm'], bins=[0, 50, 150, 300, float('inf')], labels=['Low', 'Moderate', 'High', 'Extreme'])

if not water_quality.empty and 'ph_level' in water_quality.columns:
    water_quality['ph_level'] = pd.to_numeric(water_quality['ph_level'], errors='coerce')
    water_quality['dissolved_oxygen_mg_l'] = pd.to_numeric(water_quality['dissolved_oxygen_mg_l'], errors='coerce')

# ============================================================================
# SQL QUERY FUNCTIONS FOR EACH FILTER
# ============================================================================

def get_water_sources_query(state=None, district=None, source_type=None, min_cap=0, max_cap=100, risk=None):
    query = "SELECT * FROM water_sources WHERE 1=1"
    params = []
    if state and state != "All States":
        query += " AND state = %s"
        params.append(state)
    if district and district != "All Districts":
        query += " AND district = %s"
        params.append(district)
    if source_type and source_type != "All Types":
        query += " AND source_type = %s"
        params.append(source_type)
    if min_cap > 0 or max_cap < 100:
        query += " AND capacity_percent >= %s AND capacity_percent <= %s"
        params.extend([min_cap, max_cap])
    if risk and risk != "All Risk Levels":
        query += " AND risk_level = %s"
        params.append(risk)
    query += " ORDER BY capacity_percent DESC"
    return query, params

def get_rainfall_query(district=None, year=None, season=None, min_rain=0, max_rain=500, category=None):
    query = "SELECT * FROM rainfall_history WHERE 1=1"
    params = []
    if district and district != "All Districts":
        query += " AND district_name = %s"
        params.append(district)
    if year and year != "All Years" and year is not None:
        query += " AND record_year = %s"
        params.append(int(year))
    if season and season != "All Seasons":
        query += " AND season = %s"
        params.append(season)
    if min_rain > 0 or max_rain < 500:
        query += " AND rainfall_cm >= %s AND rainfall_cm <= %s"
        params.extend([float(min_rain), float(max_rain)])
    if category and category != "All Categories":
        query += " AND rainfall_category = %s"
        params.append(category)
    query += " ORDER BY record_year DESC, rainfall_cm DESC"
    return query, params

def get_groundwater_query(district=None, year=None, min_depth=0, max_depth=100, stress=None, min_ext=0, max_ext=100, min_recharge=0, max_recharge=1000):
    query = "SELECT * FROM groundwater_levels WHERE 1=1"
    params = []
    if district and district != "All Districts":
        query += " AND district_name = %s"
        params.append(district)
    if year and year != "All Years" and year is not None:
        query += " AND assessment_year = %s"
        params.append(int(year))
    if min_depth > 0 or max_depth < 100:
        query += " AND avg_depth_meters >= %s AND avg_depth_meters <= %s"
        params.extend([float(min_depth), float(max_depth)])
    if stress and stress != "All Levels":
        query += " AND stress_level = %s"
        params.append(stress)
    if min_ext > 0 or max_ext < 100:
        query += " AND extraction_pct >= %s AND extraction_pct <= %s"
        params.extend([float(min_ext), float(max_ext)])
    if min_recharge > 0 or max_recharge < 1000:
        query += " AND recharge_rate_mcm >= %s AND recharge_rate_mcm <= %s"
        params.extend([float(min_recharge), float(max_recharge)])
    query += " ORDER BY assessment_year DESC, avg_depth_meters DESC"
    return query, params

def get_water_quality_query(state=None, district=None, min_ph=0, max_ph=14, min_do=0, max_do=15, min_turb=0, max_turb=100, status=None):
    query = "SELECT * FROM water_monitoring_stations WHERE 1=1"
    params = []
    if state and state != "All States":
        query += " AND state_name = %s"
        params.append(state)
    if district and district != "All Districts":
        query += " AND district_name = %s"
        params.append(district)
    if min_ph > 0 or max_ph < 14:
        query += " AND ph_level >= %s AND ph_level <= %s"
        params.extend([float(min_ph), float(max_ph)])
    if min_do > 0 or max_do < 15:
        query += " AND dissolved_oxygen_mg_l >= %s AND dissolved_oxygen_mg_l <= %s"
        params.extend([float(min_do), float(max_do)])
    if min_turb > 0 or max_turb < 100:
        query += " AND turbidity_ntu >= %s AND turbidity_ntu <= %s"
        params.extend([float(min_turb), float(max_turb)])
    if status and status != "All Status":
        query += " AND status = %s"
        params.append(status)
    query += " ORDER BY station_name"
    return query, params

# ============================================================================
# SIDEBAR - ALL FILTERS
# ============================================================================

with st.sidebar:
    st.title("🎮 AQUASTAT")
    st.caption("Command Interface v3.0")
    
    if engine is not None:
        st.success("✅ Cloud Connected")
    else:
        st.error("❌ Cloud Disconnected")
    
    st.markdown(f'<span class="viewer-badge">👤 VIEWER ACCESS</span>', unsafe_allow_html=True)
    st.markdown("---")
    
    if st.button("🔄 Reset All Filters", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    st.markdown("---")
    
    # SECTION 1: WATER SOURCE FILTERS
    with st.expander("🏭 WATER SOURCE FILTERS", expanded=True):
        states = ['All States'] + sorted(sources['state'].dropna().unique().tolist()) if not sources.empty else ['All States']
        selected_state = st.selectbox("State", states, key="state")
        
        if selected_state != "All States" and not sources.empty:
            districts_list = sources[sources['state'] == selected_state]['district'].dropna().unique().tolist()
        else:
            districts_list = sources['district'].dropna().unique().tolist() if not sources.empty else []
        districts = ['All Districts'] + sorted(districts_list)
        selected_district = st.selectbox("District", districts, key="district")
        
        types = ['All Types'] + sorted(sources['source_type'].dropna().unique().tolist()) if not sources.empty else ['All Types']
        selected_type = st.selectbox("Source Type", types, key="type")
        
        min_cap = st.slider("Min Capacity %", 0, 100, 0, key="min_cap")
        max_cap = st.slider("Max Capacity %", 0, 100, 100, key="max_cap")
        
        risks = ['All Risk Levels', 'Critical', 'Moderate', 'Good']
        selected_risk = st.selectbox("Risk Level", risks, key="risk")
    
    # SECTION 2: RAINFALL FILTERS
    with st.expander("🌧️ RAINFALL FILTERS", expanded=True):
        rain_districts = ['All Districts'] + sorted(rainfall['district_name'].dropna().unique().tolist()) if not rainfall.empty else ['All Districts']
        selected_rain_district = st.selectbox("Rainfall District", rain_districts, key="rain_district")
        
        rain_years = ['All Years'] + sorted(rainfall['record_year'].dropna().unique().tolist(), reverse=True) if not rainfall.empty else ['All Years']
        selected_rain_year = st.selectbox("Year", rain_years, key="rain_year")
        
        seasons = ['All Seasons', 'Winter', 'Summer', 'Monsoon', 'Post-Monsoon']
        selected_season = st.selectbox("Season", seasons, key="season")
        
        col1, col2 = st.columns(2)
        with col1:
            min_rain = st.number_input("Min Rainfall (cm)", 0, 500, 0, key="min_rain")
        with col2:
            max_rain = st.number_input("Max Rainfall (cm)", 0, 500, 500, key="max_rain")
        
        categories = ['All Categories', 'Low', 'Moderate', 'High', 'Extreme']
        selected_category = st.selectbox("Rainfall Category", categories, key="category")
    
    # SECTION 3: GROUNDWATER FILTERS
    with st.expander("🌊 GROUNDWATER FILTERS", expanded=True):
        gw_districts = ['All Districts'] + sorted(groundwater['district_name'].dropna().unique().tolist()) if not groundwater.empty else ['All Districts']
        selected_gw_district = st.selectbox("GW District", gw_districts, key="gw_district")
        
        gw_years = ['All Years'] + sorted(groundwater['assessment_year'].dropna().unique().tolist(), reverse=True) if not groundwater.empty else ['All Years']
        selected_gw_year = st.selectbox("Assessment Year", gw_years, key="gw_year")
        
        col1, col2 = st.columns(2)
        with col1:
            min_depth = st.number_input("Min Depth (m)", 0, 100, 0, key="min_depth")
        with col2:
            max_depth = st.number_input("Max Depth (m)", 0, 100, 100, key="max_depth")
        
        stress_levels = ['All Levels', 'Low', 'Moderate', 'High']
        selected_stress = st.selectbox("Stress Level", stress_levels, key="stress")
        
        col1, col2 = st.columns(2)
        with col1:
            min_ext = st.number_input("Min Extraction %", 0, 100, 0, key="min_ext")
        with col2:
            max_ext = st.number_input("Max Extraction %", 0, 100, 100, key="max_ext")
        
        col1, col2 = st.columns(2)
        with col1:
            min_recharge = st.number_input("Min Recharge (MCM)", 0, 1000, 0, key="min_recharge")
        with col2:
            max_recharge = st.number_input("Max Recharge (MCM)", 0, 1000, 1000, key="max_recharge")
    
    # SECTION 4: WATER QUALITY FILTERS
    with st.expander("💧 WATER QUALITY FILTERS", expanded=True):
        wq_states = ['All States'] + sorted(water_quality['state_name'].dropna().unique().tolist()) if not water_quality.empty else ['All States']
        selected_wq_state = st.selectbox("WQ State", wq_states, key="wq_state")
        
        if selected_wq_state != "All States" and not water_quality.empty:
            wq_districts_list = water_quality[water_quality['state_name'] == selected_wq_state]['district_name'].dropna().unique().tolist()
        else:
            wq_districts_list = water_quality['district_name'].dropna().unique().tolist() if not water_quality.empty else []
        wq_districts = ['All Districts'] + sorted(wq_districts_list)
        selected_wq_district = st.selectbox("WQ District", wq_districts, key="wq_district")
        
        col1, col2 = st.columns(2)
        with col1:
            min_ph = st.slider("Min pH", 0.0, 14.0, 0.0, 0.1, key="min_ph")
        with col2:
            max_ph = st.slider("Max pH", 0.0, 14.0, 14.0, 0.1, key="max_ph")
        
        col1, col2 = st.columns(2)
        with col1:
            min_do = st.slider("Min DO (mg/L)", 0.0, 15.0, 0.0, 0.1, key="min_do")
        with col2:
            max_do = st.slider("Max DO (mg/L)", 0.0, 15.0, 15.0, 0.1, key="max_do")
        
        col1, col2 = st.columns(2)
        with col1:
            min_turb = st.number_input("Min Turbidity (NTU)", 0, 100, 0, key="min_turb")
        with col2:
            max_turb = st.number_input("Max Turbidity (NTU)", 0, 100, 100, key="max_turb")
        
        statuses = ['All Status', 'Active', 'Maintenance', 'Inactive']
        selected_status = st.selectbox("Station Status", statuses, key="status")
    
    # SECTION 5: MAP SETTINGS
    with st.expander("🗺️ MAP SETTINGS", expanded=True):
        show_heatmap = st.checkbox("Show Heatmap", True, key="heatmap")
        show_clusters = st.checkbox("Show Clusters", True, key="clusters")
        show_stations = st.checkbox("Show Stations", True, key="show_stations")
        marker_size = st.slider("Marker Size", 5, 20, 12, key="marker_size")
        map_zoom = st.slider("Map Zoom", 4, 12, 6, key="map_zoom")
    
    st.markdown("---")
    st.caption(f"📊 Total Sources: {len(sources):,}")

# ============================================================================
# APPLY FILTERS
# ============================================================================

def apply_filters():
    df = sources.copy()
    if df.empty:
        return df
    
    if selected_state != "All States" and 'state' in df.columns:
        df = df[df['state'] == selected_state]
    if selected_district != "All Districts" and 'district' in df.columns:
        df = df[df['district'] == selected_district]
    if selected_type != "All Types" and 'source_type' in df.columns:
        df = df[df['source_type'] == selected_type]
    if 'capacity_percent' in df.columns:
        df = df[(df['capacity_percent'] >= min_cap) & (df['capacity_percent'] <= max_cap)]
    if selected_risk != "All Risk Levels" and 'risk_level' in df.columns:
        df = df[df['risk_level'] == selected_risk]
    return df

def apply_rainfall_filters():
    df = rainfall.copy()
    if df.empty:
        return df
    if selected_rain_district != "All Districts" and 'district_name' in df.columns:
        df = df[df['district_name'] == selected_rain_district]
    if selected_rain_year != "All Years" and 'record_year' in df.columns:
        df = df[df['record_year'] == selected_rain_year]
    if selected_season != "All Seasons" and 'season' in df.columns:
        df = df[df['season'] == selected_season]
    if 'rainfall_cm' in df.columns:
        df = df[(df['rainfall_cm'] >= min_rain) & (df['rainfall_cm'] <= max_rain)]
    if selected_category != "All Categories" and 'rainfall_category' in df.columns:
        df = df[df['rainfall_category'] == selected_category]
    return df

def apply_groundwater_filters():
    df = groundwater.copy()
    if df.empty:
        return df
    if selected_gw_district != "All Districts" and 'district_name' in df.columns:
        df = df[df['district_name'] == selected_gw_district]
    if selected_gw_year != "All Years" and 'assessment_year' in df.columns:
        df = df[df['assessment_year'] == selected_gw_year]
    if 'avg_depth_meters' in df.columns:
        df = df[(df['avg_depth_meters'] >= min_depth) & (df['avg_depth_meters'] <= max_depth)]
    if selected_stress != "All Levels" and 'stress_level' in df.columns:
        df = df[df['stress_level'] == selected_stress]
    if 'extraction_pct' in df.columns:
        df = df[(df['extraction_pct'] >= min_ext) & (df['extraction_pct'] <= max_ext)]
    if 'recharge_rate_mcm' in df.columns:
        df = df[(df['recharge_rate_mcm'] >= min_recharge) & (df['recharge_rate_mcm'] <= max_recharge)]
    return df

def apply_water_quality_filters():
    df = water_quality.copy()
    if df.empty:
        return df
    if selected_wq_state != "All States" and 'state_name' in df.columns:
        df = df[df['state_name'] == selected_wq_state]
    if selected_wq_district != "All Districts" and 'district_name' in df.columns:
        df = df[df['district_name'] == selected_wq_district]
    if 'ph_level' in df.columns:
        df = df[(df['ph_level'] >= min_ph) & (df['ph_level'] <= max_ph)]
    if 'dissolved_oxygen_mg_l' in df.columns:
        df = df[(df['dissolved_oxygen_mg_l'] >= min_do) & (df['dissolved_oxygen_mg_l'] <= max_do)]
    if 'turbidity_ntu' in df.columns:
        df = df[(df['turbidity_ntu'] >= min_turb) & (df['turbidity_ntu'] <= max_turb)]
    if selected_status != "All Status" and 'status' in df.columns:
        df = df[df['status'] == selected_status]
    return df

filtered_sources = apply_filters()
filtered_rainfall = apply_rainfall_filters()
filtered_groundwater = apply_groundwater_filters()
filtered_water_quality = apply_water_quality_filters()

# ============================================================================
# FILTER SUMMARY DASHBOARD
# ============================================================================

def display_filter_summary():
    """Display all active filters in a summary dashboard"""
    active_filters = []
    
    if selected_state != "All States":
        active_filters.append(f"🏭 State: {selected_state}")
    if selected_district != "All Districts":
        active_filters.append(f"🏭 District: {selected_district}")
    if selected_type != "All Types":
        active_filters.append(f"🏭 Source Type: {selected_type}")
    if min_cap > 0 or max_cap < 100:
        active_filters.append(f"🏭 Capacity: {min_cap}% - {max_cap}%")
    if selected_risk != "All Risk Levels":
        active_filters.append(f"🏭 Risk: {selected_risk}")
    if selected_rain_district != "All Districts":
        active_filters.append(f"🌧️ Rain District: {selected_rain_district}")
    if selected_rain_year != "All Years":
        active_filters.append(f"🌧️ Year: {selected_rain_year}")
    if selected_season != "All Seasons":
        active_filters.append(f"🌧️ Season: {selected_season}")
    if min_rain > 0 or max_rain < 500:
        active_filters.append(f"🌧️ Rainfall: {min_rain}-{max_rain} cm")
    if selected_gw_district != "All Districts":
        active_filters.append(f"🌊 GW District: {selected_gw_district}")
    if selected_gw_year != "All Years":
        active_filters.append(f"🌊 GW Year: {selected_gw_year}")
    if min_depth > 0 or max_depth < 100:
        active_filters.append(f"🌊 Depth: {min_depth}-{max_depth} m")
    if selected_wq_state != "All States":
        active_filters.append(f"💧 WQ State: {selected_wq_state}")
    if min_ph > 0 or max_ph < 14:
        active_filters.append(f"💧 pH: {min_ph}-{max_ph}")
    
    return active_filters

# ============================================================================
# MAIN DASHBOARD
# ============================================================================

st.title("💧 AQUASTAT National Water Command Center")
st.caption(f"**Live Intelligence** • Last Updated: {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}")

# KPI Row
col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.markdown(f"<div class='metric-card'><h3 style='color:#8892b0;'>Total Sources</h3><h1 style='color:#00e5ff;'>{len(filtered_sources):,}</h1></div>", unsafe_allow_html=True)
with col2:
    avg_cap = filtered_sources['capacity_percent'].mean() if not filtered_sources.empty else 0
    st.markdown(f"<div class='metric-card'><h3 style='color:#8892b0;'>Avg Capacity</h3><h1 style='color:#00e5ff;'>{avg_cap:.1f}%</h1></div>", unsafe_allow_html=True)
with col3:
    critical = len(filtered_sources[filtered_sources['capacity_percent'] < 30]) if not filtered_sources.empty else 0
    st.markdown(f"<div class='metric-card'><h3 style='color:#8892b0;'>Critical Sources</h3><h1 style='color:#ff4444;'>{critical}</h1></div>", unsafe_allow_html=True)
with col4:
    total_rain = filtered_rainfall['rainfall_cm'].sum() if not filtered_rainfall.empty else 0
    st.markdown(f"<div class='metric-card'><h3 style='color:#8892b0;'>Total Rainfall</h3><h1 style='color:#00e5ff;'>{total_rain:.0f} cm</h1></div>", unsafe_allow_html=True)
with col5:
    avg_gw = filtered_groundwater['avg_depth_meters'].mean() if not filtered_groundwater.empty else 0
    st.markdown(f"<div class='metric-card'><h3 style='color:#8892b0;'>Avg GW Depth</h3><h1 style='color:#00e5ff;'>{avg_gw:.1f} m</h1></div>", unsafe_allow_html=True)

st.markdown("---")

# FILTER SUMMARY SECTION
st.markdown('<p class="section-header">🔍 Active Filters Summary</p>', unsafe_allow_html=True)
active_filters = display_filter_summary()
if active_filters:
    filters_html = ''.join([f'<span class="filter-tag">{f}</span>' for f in active_filters])
    st.markdown(f'<div class="filter-summary">{filters_html}</div>', unsafe_allow_html=True)
    st.caption(f"📌 {len(active_filters)} active filters | Total records: {len(filtered_sources)} sources")
else:
    st.info("ℹ️ No active filters - showing all data")

st.markdown("---")

# ============================================================================
# MAIN TABS
# ============================================================================

tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 DASHBOARD", "🗺️ MAP VIEW", "📈 ANALYTICS", "💧 WATER QUALITY", "⚠️ ALERTS"])

# TAB 1: DASHBOARD
with tab1:
    if filtered_sources.empty:
        st.warning("⚠️ No water sources match the current filters. Try clearing some filters.")
    else:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Capacity Distribution")
            fig = px.histogram(filtered_sources, x='capacity_percent', nbins=20, title="Capacity Distribution")
            fig.add_vline(x=30, line_dash="dash", line_color="red")
            fig.add_vline(x=60, line_dash="dash", line_color="orange")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("🏭 Source Types")
            type_counts = filtered_sources['source_type'].value_counts()
            fig = px.pie(values=type_counts.values, names=type_counts.index, title="Source Types")
            st.plotly_chart(fig, use_container_width=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📈 Groundwater Stress")
            if not filtered_groundwater.empty:
                stress_counts = filtered_groundwater['stress_level'].value_counts()
                fig = px.bar(x=stress_counts.index, y=stress_counts.values, title="Groundwater Stress")
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("☔ Rainfall Analysis")
            if not filtered_rainfall.empty:
                season_rain = filtered_rainfall.groupby('season')['rainfall_cm'].mean()
                fig = px.bar(x=season_rain.index, y=season_rain.values, title="Avg Rainfall by Season")
                st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("⚠️ Risk Assessment")
        risk_counts = filtered_sources['risk_level'].value_counts()
        fig = px.bar(x=risk_counts.index, y=risk_counts.values, title="Risk Distribution", color=risk_counts.index)
        st.plotly_chart(fig, use_container_width=True)

# TAB 2: MAP VIEW (FIXED - No attribution error)
with tab2:
    st.subheader("🗺️ Interactive Water Resources Map")
    
    # Use valid map tiles with proper attribution
    map_sources = filtered_sources[filtered_sources['latitude'].notna() & filtered_sources['longitude'].notna()].copy()
    
    if not map_sources.empty:
        center_lat = map_sources['latitude'].mean()
        center_lon = map_sources['longitude'].mean()
        
        # Use valid map tile (OpenStreetMap has proper attribution)
        m = folium.Map(location=[center_lat, center_lon], zoom_start=map_zoom, tiles='OpenStreetMap')
        Fullscreen().add_to(m)
        
        if show_clusters:
            marker_cluster = MarkerCluster().add_to(m)
        else:
            marker_cluster = m
        
        heat_data = []
        for _, row in map_sources.iterrows():
            capacity = row.get('capacity_percent', 50)
            if capacity < 30:
                color = 'red'
            elif capacity < 60:
                color = 'orange'
            else:
                color = 'green'
            
            heat_data.append([row['latitude'], row['longitude']])
            
            popup_text = f"""
            <b>{row.get('source_name', 'Unknown')}</b><br>
            Type: {row.get('source_type', 'Unknown')}<br>
            Location: {row.get('district', 'Unknown')}<br>
            Capacity: {capacity:.1f}%
            """
            
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=marker_size,
                color=color,
                fill=True,
                fill_opacity=0.7,
                popup=popup_text
            ).add_to(marker_cluster)
        
        if show_heatmap and heat_data:
            HeatMap(heat_data, radius=15, blur=10).add_to(m)
        
        st_folium(m, width=1200, height=600)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Sources on Map", len(map_sources))
        with col2:
            st.metric("Total Filtered", len(filtered_sources))
        with col3:
            coverage = (len(map_sources)/len(filtered_sources)*100) if len(filtered_sources) > 0 else 0
            st.metric("Coverage", f"{coverage:.1f}%")
    else:
        st.warning("No sources with coordinates available for selected filters")

# TAB 3: ANALYTICS
with tab3:
    st.subheader("📈 Analytics Dashboard")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Rainfall Trend")
        if not filtered_rainfall.empty:
            rain_trend = filtered_rainfall.groupby('record_year')['rainfall_cm'].mean().reset_index()
            fig = px.line(rain_trend, x='record_year', y='rainfall_cm', title="Rainfall Over Years", markers=True)
            st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Capacity by State")
        if not filtered_sources.empty:
            state_cap = filtered_sources.groupby('state')['capacity_percent'].mean().sort_values(ascending=False).head(10)
            fig = px.bar(x=state_cap.values, y=state_cap.index, orientation='h', title="Avg Capacity by State")
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Groundwater Trend")
        if not filtered_groundwater.empty:
            gw_trend = filtered_groundwater.groupby('assessment_year')['avg_depth_meters'].mean().reset_index()
            fig = px.line(gw_trend, x='assessment_year', y='avg_depth_meters', title="Groundwater Depth Trend", markers=True)
            st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Extraction vs Recharge")
        if not filtered_groundwater.empty:
            fig = px.scatter(filtered_groundwater, x='recharge_rate_mcm', y='extraction_pct', 
                           title="Extraction vs Recharge", color='district_name' if 'district_name' in filtered_groundwater.columns else None)
            st.plotly_chart(fig, use_container_width=True)

# TAB 4: WATER QUALITY
with tab4:
    st.subheader("💧 Water Quality Monitoring")
    
    if not filtered_water_quality.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Avg pH", f"{filtered_water_quality['ph_level'].mean():.2f}" if 'ph_level' in filtered_water_quality else "N/A")
        col2.metric("Avg DO", f"{filtered_water_quality['dissolved_oxygen_mg_l'].mean():.1f} mg/L" if 'dissolved_oxygen_mg_l' in filtered_water_quality else "N/A")
        col3.metric("Active Stations", len(filtered_water_quality[filtered_water_quality['status'] == 'Active']) if 'status' in filtered_water_quality else 0)
        
        st.subheader("pH Distribution")
        if 'ph_level' in filtered_water_quality.columns:
            fig = px.histogram(filtered_water_quality, x='ph_level', nbins=20, title="pH Distribution")
            fig.add_vline(x=6.5, line_dash="dash", line_color="green")
            fig.add_vline(x=8.5, line_dash="dash", line_color="green")
            st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(filtered_water_quality.head(50), use_container_width=True)
    else:
        st.info("No water quality data available")

# TAB 5: ALERTS
with tab5:
    st.subheader("⚠️ Active Alerts")
    
    if not alerts.empty:
        critical = len(alerts[alerts['alert_status'] == 'CRITICAL']) if 'alert_status' in alerts else 0
        warning = len(alerts[alerts['alert_status'] == 'WARNING']) if 'alert_status' in alerts else 0
        
        col1, col2 = st.columns(2)
        col1.markdown(f"<div class='alert-critical'>🔴 CRITICAL: {critical}</div>", unsafe_allow_html=True)
        col2.markdown(f"<div class='alert-warning'>🟡 WARNING: {warning}</div>", unsafe_allow_html=True)
        
        st.dataframe(alerts, use_container_width=True)
    else:
        st.success("✅ No active alerts")

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown(f"<p style='text-align:center'>AQUASTAT v3.0 • {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}</p>", unsafe_allow_html=True)

if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()
