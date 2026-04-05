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
import time

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# -------------------------
# PAGE CONFIG
# -------------------------
st.set_page_config(
    page_title="AQUASTAT - National Water Command Center",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# -------------------------
# CUSTOM CSS
# -------------------------
st.markdown("""
<style>
    /* Main container styling */
    .main {
        background: linear-gradient(135deg, #0a0f1e 0%, #0d1a2b 100%);
        color: #e6f1ff;
    }
    
    /* Custom metric cards */
    .metric-card {
        background: rgba(10, 25, 47, 0.7);
        border: 1px solid #1e3a5f;
        border-radius: 10px;
        padding: 20px;
        margin: 10px 0;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
        backdrop-filter: blur(10px);
    }
    
    /* Alert badges */
    .alert-critical {
        background: linear-gradient(135deg, #ff416c, #ff4b2b);
        color: white;
        padding: 5px 15px;
        border-radius: 20px;
        font-weight: bold;
        text-align: center;
        animation: pulse 2s infinite;
        display: inline-block;
    }
    
    .alert-warning {
        background: linear-gradient(135deg, #f7971e, #ffd200);
        color: black;
        padding: 5px 15px;
        border-radius: 20px;
        font-weight: bold;
        text-align: center;
        display: inline-block;
    }
    
    .alert-good {
        background: linear-gradient(135deg, #00b09b, #96c93d);
        color: white;
        padding: 5px 15px;
        border-radius: 20px;
        font-weight: bold;
        text-align: center;
        display: inline-block;
    }
    
    @keyframes pulse {
        0% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.8; transform: scale(1.05); }
        100% { opacity: 1; transform: scale(1); }
    }
    
    /* Status indicators */
    .status-dot {
        height: 10px;
        width: 10px;
        border-radius: 50%;
        display: inline-block;
        margin-right: 5px;
    }
    
    .status-critical { background-color: #ff4444; box-shadow: 0 0 10px #ff4444; }
    .status-warning { background-color: #ffd700; box-shadow: 0 0 10px #ffd700; }
    .status-good { background-color: #00ff9d; box-shadow: 0 0 10px #00ff9d; }
    
    /* Custom headers */
    .section-header {
        color: #00e5ff;
        font-size: 1.5rem;
        font-weight: 600;
        margin: 20px 0;
        padding-bottom: 10px;
        border-bottom: 2px solid #1e3a5f;
        text-transform: uppercase;
        letter-spacing: 2px;
    }
    
    /* Filter section styling */
    .filter-section {
        background: rgba(10, 30, 48, 0.5);
        border: 1px solid #1e3a5f;
        border-radius: 10px;
        padding: 15px;
        margin-bottom: 15px;
    }
    
    .filter-header {
        color: #00e5ff;
        font-size: 1rem;
        font-weight: 600;
        margin-bottom: 10px;
        padding-bottom: 5px;
        border-bottom: 1px solid #1e3a5f;
    }
    
    /* Viewer badge */
    .viewer-badge {
        background: linear-gradient(135deg, #00b09b, #96c93d);
        color: white;
        padding: 5px 15px;
        border-radius: 20px;
        font-weight: bold;
        text-align: center;
        display: inline-block;
    }
</style>
""", unsafe_allow_html=True)

# -------------------------
# NEON CLOUD DATABASE CONNECTION
# -------------------------

NEON_URL = st.secrets["NEON_URL"]

@st.cache_resource
def init_connection():
    """Initialize Neon PostgreSQL database connection"""
    try:
        engine = create_engine(
            NEON_URL,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True,
            connect_args={
                'connect_timeout': 10,
                'keepalives': 1,
                'keepalives_idle': 30,
                'keepalives_interval': 10,
                'keepalives_count': 5,
                'sslmode': 'require'
            }
        )
        with engine.connect() as conn:
            with conn.begin():
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
        return engine
    except Exception as e:
        st.error(f"⚠️ Cloud Database connection failed: {e}")
        return None

engine = init_connection()

def execute_query(query, params=None):
    """Execute a SQL query with parameters"""
    if engine is None:
        st.error("Database connection not available")
        return None
    
    try:
        with engine.connect() as conn:
            with conn.begin():
                if params:
                    result = pd.read_sql(query, conn, params=params)
                else:
                    result = pd.read_sql(query, conn)
                return result
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

def test_connection():
    """Test database connection"""
    if engine is None:
        return False, "No engine"
    
    try:
        with engine.connect() as conn:
            with conn.begin():
                result = conn.execute(text("SELECT current_database(), current_user"))
                db_name, db_user = result.fetchone()
                return True, f"Connected to: {db_name} as {db_user}"
    except Exception as e:
        return False, str(e)

# -------------------------
# DATA LOADING FUNCTIONS
# -------------------------

@st.cache_data(ttl=300)
def load_all_data():
    """Load all data from Neon Cloud database"""
    if engine is None:
        st.warning("⚠️ Database connection not available.")
        return [pd.DataFrame()] * 8
    
    try:
        with engine.connect() as conn:
            with conn.begin():
                # Get list of tables
                query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
                tables_df = pd.read_sql(query, conn)
                tables = tables_df['table_name'].tolist() if not tables_df.empty else []
                
                def get_df(table_name):
                    if table_name in tables:
                        try:
                            return pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
                        except Exception as e:
                            st.warning(f"Could not load {table_name}: {e}")
                            return pd.DataFrame()
                    return pd.DataFrame()
                
                sources = get_df('water_sources')
                stations = get_df('water_monitoring_stations')
                groundwater = get_df('groundwater_levels')
                rainfall = get_df('rainfall_history')
                alerts = get_df('active_alerts')
                regional = get_df('regional_stats')
                water_quality = stations.copy()
                
                if 'water_usage_history' in tables and 'water_sources' in tables:
                    usage_query = """
                        SELECT wu.*, ws.source_name, ws.source_type, ws.state, ws.district 
                        FROM water_usage_history wu
                        LEFT JOIN water_sources ws ON wu.source_id = ws.source_id
                    """
                    try:
                        usage = pd.read_sql(usage_query, conn)
                    except Exception as e:
                        usage = pd.DataFrame()
                else:
                    usage = pd.DataFrame()
                
                return sources, stations, groundwater, rainfall, alerts, usage, regional, water_quality
                
    except Exception as e:
        st.error(f"Error loading cloud data: {e}")
        return [pd.DataFrame()] * 8

# Load the data
with st.spinner("🚀 Connecting to AQUASTAT Cloud Database..."):
    sources, stations, groundwater, rainfall, alerts, usage, regional, water_quality = load_all_data()

# -------------------------
# DATA PROCESSING
# -------------------------

current_year = datetime.now().year
current_time = datetime.now(pytz.timezone('Asia/Kolkata'))

# Process Sources
if not sources.empty:
    numeric_cols = ['capacity_percent', 'build_year', 'max_capacity_mcm', 'current_capacity_mcm']
    for col in numeric_cols:
        if col in sources.columns:
            sources[col] = pd.to_numeric(sources[col], errors='coerce')
    
    if 'build_year' in sources.columns:
        sources['age'] = current_year - sources['build_year']
        sources['age'] = sources['age'].clip(0, 200)
    else:
        sources['age'] = 0
    
    if 'capacity_percent' in sources.columns:
        sources['health_score'] = (
            sources['capacity_percent'].fillna(50) * 0.5 +
            (100 - sources['age'].clip(0, 100).fillna(50)) * 0.3 +
            20
        ).clip(0, 100)
    else:
        sources['health_score'] = 50
    
    if 'capacity_percent' in sources.columns:
        sources['risk_level'] = pd.cut(
            sources['capacity_percent'],
            bins=[0, 30, 60, 100],
            labels=['Critical', 'Moderate', 'Good'],
            include_lowest=True
        )
    else:
        sources['risk_level'] = 'Unknown'
    
    np.random.seed(42)
    sources['trend'] = np.random.choice(['📈 Increasing', '📉 Decreasing', '➡️ Stable'], len(sources))

# Process Groundwater
if not groundwater.empty:
    if 'avg_depth_meters' in groundwater.columns:
        groundwater['stress_level'] = pd.cut(
            groundwater['avg_depth_meters'],
            bins=[0, 20, 40, 100],
            labels=['Low', 'Moderate', 'High']
        )
    
    if 'assessment_year' in groundwater.columns and 'avg_depth_meters' in groundwater.columns:
        groundwater = groundwater.sort_values(['district_name', 'assessment_year'])
        groundwater['depth_change'] = groundwater.groupby('district_name')['avg_depth_meters'].diff()
        groundwater['depletion_rate'] = groundwater.groupby('district_name')['depth_change'].transform('mean')

# Process Rainfall
if not rainfall.empty:
    if 'rainfall_cm' in rainfall.columns:
        rainfall['rainfall_category'] = pd.cut(
            rainfall['rainfall_cm'],
            bins=[0, 50, 150, 300, float('inf')],
            labels=['Low', 'Moderate', 'High', 'Extreme']
        )
    
    if 'rainfall_cm' in rainfall.columns and 'record_year' in rainfall.columns and 'district_name' in rainfall.columns:
        avg_by_region = rainfall.groupby('district_name')['rainfall_cm'].transform('mean')
        rainfall['deviation_pct'] = ((rainfall['rainfall_cm'] - avg_by_region) / avg_by_region * 100).round(1)

# Add coordinates to sources from stations
def add_coordinates_to_sources(sources_df, stations_df):
    if sources_df.empty or stations_df.empty:
        return sources_df
    
    df = sources_df.copy()
    
    if 'district' in df.columns and 'district_name' in stations_df.columns:
        if 'latitude' in stations_df.columns and 'longitude' in stations_df.columns:
            stations_df['district_clean'] = stations_df['district_name'].str.strip().str.lower()
            district_coords = stations_df.groupby('district_clean').agg({
                'latitude': 'first',
                'longitude': 'first'
            }).reset_index()
            
            df['district_clean'] = df['district'].str.strip().str.lower()
            df = df.merge(district_coords, on='district_clean', how='left')
            df = df.drop('district_clean', axis=1)
    
    return df

if not sources.empty and not stations.empty:
    sources = add_coordinates_to_sources(sources, stations)

# -------------------------
# SIDEBAR - ALL 6 FILTER SECTIONS
# -------------------------

# Test connection
conn_status, conn_message = test_connection()

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
    
    # ========== SECTION 1: WATER SOURCE FILTERS ==========
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    st.markdown('<div class="filter-header">🏭 WATER SOURCE FILTERS</div>', unsafe_allow_html=True)
    
    if not sources.empty and 'state' in sources.columns:
        states = ['All States'] + sorted(sources['state'].dropna().unique().tolist())
        selected_state = st.selectbox("State", states, key="source_state")
    else:
        selected_state = "All States"
    
    if not sources.empty and 'district' in sources.columns:
        if selected_state != "All States":
            districts = sources[sources['state'] == selected_state]['district'].dropna().unique()
        else:
            districts = sources['district'].dropna().unique()
        districts = ['All Districts'] + sorted(districts.tolist()) if len(districts) > 0 else ['All Districts']
        selected_district = st.selectbox("District", districts, key="source_district")
    else:
        selected_district = "All Districts"
    
    if not sources.empty and 'source_type' in sources.columns:
        source_types = ['All Types'] + sorted(sources['source_type'].dropna().unique().tolist())
        selected_type = st.selectbox("Source Type", source_types, key="source_type")
    else:
        selected_type = "All Types"
    
    if not sources.empty and 'capacity_percent' in sources.columns:
        min_cap_val = float(sources['capacity_percent'].min())
        max_cap_val = float(sources['capacity_percent'].max())
        capacity_range = st.slider("Capacity %", min_value=min_cap_val, max_value=max_cap_val, value=(min_cap_val, max_cap_val), key="capacity_range")
    else:
        capacity_range = (0, 100)
    
    if not sources.empty and 'risk_level' in sources.columns:
        risk_options = ['All Risk Levels'] + list(sources['risk_level'].unique())
        selected_risk = st.selectbox("Risk Level", risk_options, key="risk_level")
    else:
        selected_risk = "All Risk Levels"
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # ========== SECTION 2: RAINFALL FILTERS ==========
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    st.markdown('<div class="filter-header">🌧️ RAINFALL FILTERS</div>', unsafe_allow_html=True)
    
    if not rainfall.empty and 'district_name' in rainfall.columns:
        rain_districts = ['All Districts'] + sorted(rainfall['district_name'].dropna().unique().tolist())
        selected_rain_district = st.selectbox("Rainfall District", rain_districts, key="rain_district")
    else:
        selected_rain_district = "All Districts"
    
    if not rainfall.empty and 'record_year' in rainfall.columns:
        years = ['All Years'] + sorted(rainfall['record_year'].dropna().unique().tolist(), reverse=True)
        selected_rain_year = st.selectbox("Year", years, key="rain_year")
    else:
        selected_rain_year = "All Years"
    
    seasons = ["All Seasons", "Winter", "Summer", "Monsoon", "Post-Monsoon"]
    selected_season = st.selectbox("Season", seasons, key="rain_season")
    
    col1, col2 = st.columns(2)
    with col1:
        min_rainfall = st.number_input("Min Rainfall (cm)", 0, 500, 0, key="min_rainfall")
    with col2:
        max_rainfall = st.number_input("Max Rainfall (cm)", 0, 500, 500, key="max_rainfall")
    
    categories = ["All Categories", "Low", "Moderate", "High", "Extreme"]
    selected_category = st.selectbox("Rainfall Category", categories, key="rain_category")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # ========== SECTION 3: GROUNDWATER FILTERS ==========
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    st.markdown('<div class="filter-header">🌊 GROUNDWATER FILTERS</div>', unsafe_allow_html=True)
    
    if not groundwater.empty and 'district_name' in groundwater.columns:
        gw_districts = ['All Districts'] + sorted(groundwater['district_name'].dropna().unique().tolist())
        selected_gw_district = st.selectbox("GW District", gw_districts, key="gw_district")
    else:
        selected_gw_district = "All Districts"
    
    if not groundwater.empty and 'assessment_year' in groundwater.columns:
        gw_years = ['All Years'] + sorted(groundwater['assessment_year'].dropna().unique().tolist(), reverse=True)
        selected_gw_year = st.selectbox("Assessment Year", gw_years, key="gw_year")
    else:
        selected_gw_year = "All Years"
    
    col1, col2 = st.columns(2)
    with col1:
        min_depth = st.number_input("Min Depth (m)", 0, 100, 0, key="min_depth")
    with col2:
        max_depth = st.number_input("Max Depth (m)", 0, 100, 100, key="max_depth")
    
    stress_levels = ["All Levels", "Low", "Moderate", "High", "Critical"]
    selected_stress = st.selectbox("Stress Level", stress_levels, key="stress_level")
    
    col1, col2 = st.columns(2)
    with col1:
        min_extraction = st.number_input("Min Extraction %", 0, 100, 0, key="min_extraction")
    with col2:
        max_extraction = st.number_input("Max Extraction %", 0, 100, 100, key="max_extraction")
    
    col1, col2 = st.columns(2)
    with col1:
        min_recharge = st.number_input("Min Recharge (MCM)", 0, 1000, 0, key="min_recharge")
    with col2:
        max_recharge = st.number_input("Max Recharge (MCM)", 0, 1000, 1000, key="max_recharge")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # ========== SECTION 4: WATER QUALITY FILTERS ==========
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    st.markdown('<div class="filter-header">💧 WATER QUALITY FILTERS</div>', unsafe_allow_html=True)
    
    if not water_quality.empty and 'state_name' in water_quality.columns:
        wq_states = ['All States'] + sorted(water_quality['state_name'].dropna().unique().tolist())
        selected_wq_state = st.selectbox("WQ State", wq_states, key="wq_state")
    else:
        selected_wq_state = "All States"
    
    if not water_quality.empty and 'district_name' in water_quality.columns:
        if selected_wq_state != "All States":
            wq_districts_list = water_quality[water_quality['state_name'] == selected_wq_state]['district_name'].dropna().unique()
        else:
            wq_districts_list = water_quality['district_name'].dropna().unique()
        wq_districts = ['All Districts'] + sorted(wq_districts_list.tolist()) if len(wq_districts_list) > 0 else ['All Districts']
        selected_wq_district = st.selectbox("WQ District", wq_districts, key="wq_district")
    else:
        selected_wq_district = "All Districts"
    
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
        min_turbidity = st.number_input("Min Turbidity (NTU)", 0, 100, 0, key="min_turbidity")
    with col2:
        max_turbidity = st.number_input("Max Turbidity (NTU)", 0, 100, 100, key="max_turbidity")
    
    status_options = ["All Status", "Active", "Maintenance", "Inactive"]
    selected_status = st.selectbox("Station Status", status_options, key="wq_status")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # ========== SECTION 5: ADVANCED ANALYTICS FILTERS ==========
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    st.markdown('<div class="filter-header">📊 ADVANCED ANALYTICS FILTERS</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    with col1:
        start_year_filter = st.selectbox("Start Year", [2020, 2021, 2022, 2023, 2024], index=4, key="start_year")
    with col2:
        end_year_filter = st.selectbox("End Year", [2020, 2021, 2022, 2023, 2024], index=4, key="end_year")
    
    min_data_points = st.slider("Min Data Points per District", 3, 20, 5, key="min_data_points")
    
    corr_options = ["Rainfall vs Groundwater", "Capacity vs Extraction", "pH vs DO", "All Parameters"]
    selected_correlation = st.selectbox("Correlation Type", corr_options, key="correlation_type")
    
    alert_sensitivity = st.select_slider("Alert Sensitivity", options=["Low", "Medium", "High", "Critical"], value="Medium", key="alert_sensitivity")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # ========== SECTION 6: MAP SETTINGS ==========
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    st.markdown('<div class="filter-header">🗺️ MAP SETTINGS</div>', unsafe_allow_html=True)
    
    map_style = st.selectbox("Map Style", ["Esri Satellite", "OpenStreetMap", "CartoDB Dark", "CartoDB Light"], index=0, key="map_style")
    show_heatmap = st.checkbox("Show Heatmap", True, key="show_heatmap")
    show_clusters = st.checkbox("Show Clusters", True, key="show_clusters")
    show_stations = st.checkbox("Show Monitoring Stations", True, key="show_stations")
    marker_size = st.slider("Marker Size", 5, 20, 12, key="marker_size")
    map_zoom = st.slider("Map Zoom Level", 4, 12, 6, key="map_zoom")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    st.caption(f"📊 Total Sources: {len(sources):,}")

# -------------------------
# APPLY ALL FILTERS TO DATAFRAMES
# -------------------------

def apply_water_sources_filters():
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
        df = df[(df['capacity_percent'] >= capacity_range[0]) & (df['capacity_percent'] <= capacity_range[1])]
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
        df = df[(df['rainfall_cm'] >= min_rainfall) & (df['rainfall_cm'] <= max_rainfall)]
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
        df = df[(df['extraction_pct'] >= min_extraction) & (df['extraction_pct'] <= max_extraction)]
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
        df = df[(df['turbidity_ntu'] >= min_turbidity) & (df['turbidity_ntu'] <= max_turbidity)]
    if selected_status != "All Status" and 'status' in df.columns:
        df = df[df['status'] == selected_status]
    
    return df

# Apply all filters
filtered_sources = apply_water_sources_filters()
filtered_rainfall = apply_rainfall_filters()
filtered_groundwater = apply_groundwater_filters()
filtered_water_quality = apply_water_quality_filters()

# -------------------------
# MAIN DASHBOARD
# -------------------------

st.title("💧 AQUASTAT National Water Command Center")
st.caption(f"**Live Intelligence** • Last Updated: {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}")

# KPI Row
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.markdown(f"""
    <div class="metric-card">
        <h3 style="color: #8892b0; margin:0;">Total Sources</h3>
        <h1 style="color: #00e5ff; margin:0;">{len(filtered_sources):,}</h1>
    </div>
    """, unsafe_allow_html=True)

with col2:
    avg_cap = filtered_sources['capacity_percent'].mean() if not filtered_sources.empty and 'capacity_percent' in filtered_sources.columns else 0
    st.markdown(f"""
    <div class="metric-card">
        <h3 style="color: #8892b0; margin:0;">Avg Capacity</h3>
        <h1 style="color: #00e5ff; margin:0;">{avg_cap:.1f}%</h1>
    </div>
    """, unsafe_allow_html=True)

with col3:
    critical = len(filtered_sources[filtered_sources['capacity_percent'] < 30]) if not filtered_sources.empty and 'capacity_percent' in filtered_sources.columns else 0
    st.markdown(f"""
    <div class="metric-card">
        <h3 style="color: #8892b0; margin:0;">Critical Sources</h3>
        <h1 style="color: #ff4444; margin:0;">{critical}</h1>
    </div>
    """, unsafe_allow_html=True)

with col4:
    total_rainfall = filtered_rainfall['rainfall_cm'].sum() if not filtered_rainfall.empty and 'rainfall_cm' in filtered_rainfall.columns else 0
    st.markdown(f"""
    <div class="metric-card">
        <h3 style="color: #8892b0; margin:0;">Total Rainfall</h3>
        <h1 style="color: #00e5ff; margin:0;">{total_rainfall:.0f} cm</h1>
    </div>
    """, unsafe_allow_html=True)

with col5:
    avg_gw_depth = filtered_groundwater['avg_depth_meters'].mean() if not filtered_groundwater.empty and 'avg_depth_meters' in filtered_groundwater.columns else 0
    st.markdown(f"""
    <div class="metric-card">
        <h3 style="color: #8892b0; margin:0;">Avg GW Depth</h3>
        <h1 style="color: #00e5ff; margin:0;">{avg_gw_depth:.1f} m</h1>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# Main Tabs (Admin panel removed)
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 DASHBOARD", "🗺️ MAP VIEW", "📈 ANALYTICS", "💧 WATER QUALITY", 
    "⚠️ ALERTS", "📋 DATA TABLES"
])

# =====================
# TAB 1: DASHBOARD
# =====================

with tab1:
    if filtered_sources.empty:
        st.warning("⚠️ No water sources match the current filters. Try clearing some filters.")
    else:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<p class="section-header">📊 Capacity Distribution</p>', unsafe_allow_html=True)
            if 'capacity_percent' in filtered_sources.columns:
                fig = px.histogram(
                    filtered_sources, x='capacity_percent', nbins=20,
                    title=f"Storage Capacity Distribution ({len(filtered_sources)} sources)",
                    template="plotly_dark", color_discrete_sequence=['#00e5ff']
                )
                fig.add_vline(x=30, line_dash="dash", line_color="red")
                fig.add_vline(x=60, line_dash="dash", line_color="orange")
                fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown('<p class="section-header">🏭 Source Types</p>', unsafe_allow_html=True)
            if 'source_type' in filtered_sources.columns:
                type_counts = filtered_sources['source_type'].value_counts().reset_index()
                type_counts.columns = ['Source Type', 'Count']
                fig = px.pie(type_counts, values='Count', names='Source Type', 
                            title="Water Sources by Type", template="plotly_dark")
                fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<p class="section-header">📈 Groundwater Stress</p>', unsafe_allow_html=True)
            if not filtered_groundwater.empty and 'stress_level' in filtered_groundwater.columns:
                stress_counts = filtered_groundwater['stress_level'].value_counts().reset_index()
                stress_counts.columns = ['Stress Level', 'Count']
                fig = px.bar(stress_counts, x='Stress Level', y='Count', 
                            title="Groundwater Stress Distribution", template="plotly_dark",
                            color='Stress Level', text_auto=True)
                fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown('<p class="section-header">☔ Rainfall Analysis</p>', unsafe_allow_html=True)
            if not filtered_rainfall.empty and 'season' in filtered_rainfall.columns:
                season_rain = filtered_rainfall.groupby('season')['rainfall_cm'].mean().reset_index()
                fig = px.bar(season_rain, x='season', y='rainfall_cm', 
                            title="Average Rainfall by Season", template="plotly_dark",
                            color='rainfall_cm', color_continuous_scale='Blues', text_auto='.1f')
                fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
        
        st.markdown('<p class="section-header">⚠️ Risk Assessment</p>', unsafe_allow_html=True)
        if 'risk_level' in filtered_sources.columns:
            risk_counts = filtered_sources['risk_level'].value_counts().reset_index()
            risk_counts.columns = ['Risk Level', 'Count']
            fig = px.bar(risk_counts, x='Risk Level', y='Count', color='Risk Level',
                        title="Infrastructure Risk Assessment", template="plotly_dark", text_auto=True)
            fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)

# =====================
# TAB 2: MAP VIEW
# =====================

with tab2:
    st.markdown('<p class="section-header">🗺️ National Interactive Water Resources Map</p>', unsafe_allow_html=True)
    
    # Map style mapping
    style_map = {
        "Esri Satellite": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        "OpenStreetMap": "OpenStreetMap",
        "CartoDB Dark": "CartoDB dark_matter",
        "CartoDB Light": "CartoDB positron"
    }
    
    center_lat, center_lon, zoom = 20.5937, 78.9629, map_zoom
    
    if not filtered_sources.empty and 'latitude' in filtered_sources.columns and 'longitude' in filtered_sources.columns:
        sources_with_coords = filtered_sources[filtered_sources['latitude'].notna() & filtered_sources['longitude'].notna()]
        if not sources_with_coords.empty:
            if selected_district != "All Districts":
                center_lat = sources_with_coords['latitude'].mean()
                center_lon = sources_with_coords['longitude'].mean()
            elif selected_state != "All States":
                center_lat = sources_with_coords['latitude'].mean()
                center_lon = sources_with_coords['longitude'].mean()
    
    m = folium.Map(location=[center_lat, center_lon], zoom_start=zoom, tiles=style_map[map_style])
    Fullscreen().add_to(m)
    
    if show_clusters and len(filtered_sources) > 10:
        marker_cluster = MarkerCluster().add_to(m)
    else:
        marker_cluster = m
    
    heat_data = []
    sources_on_map = 0
    
    if not filtered_sources.empty and 'latitude' in filtered_sources.columns and 'longitude' in filtered_sources.columns:
        sources_with_coords = filtered_sources[filtered_sources['latitude'].notna() & filtered_sources['longitude'].notna()]
        
        for _, source in sources_with_coords.iterrows():
            capacity = source.get('capacity_percent', 50)
            if capacity < 30:
                color = '#ff4444'
                risk_text = "CRITICAL"
            elif capacity < 60:
                color = '#ffd700'
                risk_text = "MODERATE"
            else:
                color = '#00ff9d'
                risk_text = "GOOD"
            
            heat_data.append([source['latitude'], source['longitude']])
            sources_on_map += 1
            
            popup_html = f"""
            <div style="font-family: Arial; min-width: 200px; background: #0a192f; color: #e6f1ff;">
                <b>{source.get('source_name', 'Unknown')}</b><br>
                Type: {source.get('source_type', 'Unknown')}<br>
                Location: {source.get('district', 'Unknown')}, {source.get('state', 'Unknown')}<br>
                Capacity: {capacity:.1f}%<br>
                Risk: <span style="color:{color};">{risk_text}</span>
            </div>
            """
            
            folium.CircleMarker(
                location=[source['latitude'], source['longitude']],
                radius=marker_size,
                color=color,
                fill=True,
                fillOpacity=0.7,
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"{source.get('source_name', 'Unknown')} - {capacity:.0f}%"
            ).add_to(marker_cluster)
        
        if show_heatmap and heat_data:
            HeatMap(heat_data, radius=15, blur=10).add_to(m)
    
    if show_stations and not filtered_water_quality.empty:
        stations_with_coords = filtered_water_quality[
            filtered_water_quality['latitude'].notna() & filtered_water_quality['longitude'].notna()
        ]
        for _, station in stations_with_coords.iterrows():
            status = station.get('status', 'Unknown')
            station_color = 'green' if status == 'Active' else 'orange' if status == 'Maintenance' else 'red'
            
            station_popup = f"""
            <div style="background: #0a192f; color: #e6f1ff;">
                <b>{station.get('station_name', 'Unknown')}</b><br>
                Location: {station.get('district_name', 'Unknown')}<br>
                Status: {status}<br>
                pH: {station.get('ph_level', 'N/A')}<br>
                DO: {station.get('dissolved_oxygen_mg_l', 'N/A')} mg/L
            </div>
            """
            
            folium.Marker(
                location=[station['latitude'], station['longitude']],
                icon=folium.Icon(color=station_color, icon='info-sign'),
                popup=folium.Popup(station_popup, max_width=300)
            ).add_to(m)
    
    st_folium(m, width=1300, height=600)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Sources on Map", sources_on_map)
    with col2:
        st.metric("Total Filtered Sources", len(filtered_sources))
    with col3:
        coverage = (sources_on_map/len(filtered_sources)*100) if len(filtered_sources) > 0 else 0
        st.metric("Coordinate Coverage", f"{coverage:.1f}%")

# =====================
# TAB 3: ANALYTICS
# =====================

with tab3:
    st.markdown('<p class="section-header">📈 Advanced Analytics</p>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Rainfall Trend")
        if not filtered_rainfall.empty and 'record_year' in filtered_rainfall.columns:
            rain_trend = filtered_rainfall.groupby('record_year')['rainfall_cm'].mean().reset_index()
            fig = px.line(rain_trend, x='record_year', y='rainfall_cm', 
                         title="Average Rainfall Over Years", template="plotly_dark", markers=True)
            fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Groundwater Trend")
        if not filtered_groundwater.empty and 'assessment_year' in filtered_groundwater.columns:
            gw_trend = filtered_groundwater.groupby('assessment_year')['avg_depth_meters'].mean().reset_index()
            fig = px.line(gw_trend, x='assessment_year', y='avg_depth_meters', 
                         title="Average Groundwater Depth", template="plotly_dark", markers=True)
            fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Capacity by State")
        if not filtered_sources.empty and 'state' in filtered_sources.columns:
            state_cap = filtered_sources.groupby('state')['capacity_percent'].mean().sort_values(ascending=False).head(10)
            fig = px.bar(x=state_cap.values, y=state_cap.index, orientation='h',
                        title="Average Capacity by State", template="plotly_dark")
            fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Extraction vs Recharge")
        if not filtered_groundwater.empty:
            fig = px.scatter(filtered_groundwater, x='recharge_rate_mcm', y='extraction_pct',
                           size='avg_depth_meters' if 'avg_depth_meters' in filtered_groundwater.columns else None,
                           color='district_name' if 'district_name' in filtered_groundwater.columns else None,
                           title="Groundwater Extraction vs Recharge", template="plotly_dark")
            fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)

# =====================
# TAB 4: WATER QUALITY
# =====================

with tab4:
    st.markdown('<p class="section-header">💧 Water Quality Monitoring</p>', unsafe_allow_html=True)
    
    if not filtered_water_quality.empty:
        col1, col2, col3 = st.columns(3)
        avg_ph = filtered_water_quality['ph_level'].mean() if 'ph_level' in filtered_water_quality.columns else 0
        avg_do = filtered_water_quality['dissolved_oxygen_mg_l'].mean() if 'dissolved_oxygen_mg_l' in filtered_water_quality.columns else 0
        active = len(filtered_water_quality[filtered_water_quality['status'] == 'Active']) if 'status' in filtered_water_quality.columns else 0
        
        col1.metric("Average pH", f"{avg_ph:.2f}")
        col2.metric("Dissolved Oxygen", f"{avg_do:.1f} mg/L")
        col3.metric("Active Stations", active)
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if 'ph_level' in filtered_water_quality.columns:
                fig = px.histogram(filtered_water_quality, x='ph_level', nbins=20,
                                  title="pH Distribution", template="plotly_dark")
                fig.add_vline(x=6.5, line_dash="dash", line_color="green")
                fig.add_vline(x=8.5, line_dash="dash", line_color="green")
                fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if 'dissolved_oxygen_mg_l' in filtered_water_quality.columns:
                fig = px.box(filtered_water_quality, y='dissolved_oxygen_mg_l',
                            title="Dissolved Oxygen Distribution", template="plotly_dark")
                fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("### Recent Water Quality Readings")
        display_cols = ['station_name', 'district_name', 'ph_level', 'dissolved_oxygen_mg_l', 'turbidity_ntu', 'status']
        available_cols = [col for col in display_cols if col in filtered_water_quality.columns]
        st.dataframe(filtered_water_quality[available_cols].head(50), use_container_width=True)
    else:
        st.info("No water quality data available")

# =====================
# TAB 5: ALERTS
# =====================

with tab5:
    st.markdown('<p class="section-header">🚨 Active Alerts and Warnings</p>', unsafe_allow_html=True)
    
    if not alerts.empty:
        critical_count = len(alerts[alerts['alert_status'] == 'CRITICAL']) if 'alert_status' in alerts.columns else 0
        warning_count = len(alerts[alerts['alert_status'] == 'WARNING']) if 'alert_status' in alerts.columns else 0
        
        col1, col2 = st.columns(2)
        col1.markdown(f'<div class="alert-critical">🔴 CRITICAL: {critical_count}</div>', unsafe_allow_html=True)
        col2.markdown(f'<div class="alert-warning">🟡 WARNING: {warning_count}</div>', unsafe_allow_html=True)
        
        st.markdown("---")
        st.dataframe(alerts, use_container_width=True)
    else:
        st.success("✅ No active alerts - All systems normal")

# =====================
# TAB 6: DATA TABLES
# =====================

with tab6:
    st.markdown('<p class="section-header">📋 Data Explorer</p>', unsafe_allow_html=True)
    
    table_choice = st.selectbox("Select Table to View", 
                                ["Water Sources", "Monitoring Stations", "Groundwater Levels", 
                                 "Rainfall History", "Water Quality", "Active Alerts"])
    
    if table_choice == "Water Sources":
        display_df = filtered_sources if not filtered_sources.empty else sources
        if not display_df.empty:
            st.dataframe(display_df, use_container_width=True)
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"water_sources_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
    
    elif table_choice == "Monitoring Stations":
        display_df = filtered_water_quality if not filtered_water_quality.empty else stations
        if not display_df.empty:
            st.dataframe(display_df, use_container_width=True)
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"stations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
    
    elif table_choice == "Groundwater Levels":
        display_df = filtered_groundwater if not filtered_groundwater.empty else groundwater
        if not display_df.empty:
            st.dataframe(display_df, use_container_width=True)
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"groundwater_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
    
    elif table_choice == "Rainfall History":
        display_df = filtered_rainfall if not filtered_rainfall.empty else rainfall
        if not display_df.empty:
            st.dataframe(display_df, use_container_width=True)
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"rainfall_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
    
    elif table_choice == "Water Quality":
        display_df = filtered_water_quality if not filtered_water_quality.empty else water_quality
        if not display_df.empty:
            st.dataframe(display_df, use_container_width=True)
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"water_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
    
    elif table_choice == "Active Alerts":
        if not alerts.empty:
            st.dataframe(alerts, use_container_width=True)
            csv = alerts.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")

# =====================
# FOOTER
# =====================

st.markdown("---")
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
    <div style="text-align: center;">
        <p style="color: #00e5ff; font-size: 1.2rem;">💧 AQUASTAT</p>
        <p style="color: #8892b0;">National Water Command Center</p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div style="text-align: center;">
        <p style="color: #8892b0;">Data Source: Ministry of Jal Shakti</p>
        <p style="color: #8892b0;">Last Updated: {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}</p>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
    <div style="text-align: center;">
        <p style="color: #8892b0;">© 2025 All Rights Reserved</p>
        <p style="color: #8892b0;">Version 3.0 | Viewer Mode</p>
    </div>
    """, unsafe_allow_html=True)

if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()
