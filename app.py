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
from sqlalchemy import create_engine, text, exc
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
.filter-section {
    background: rgba(10,30,48,0.4);
    border: 1px solid rgba(0,200,255,0.15);
    border-radius: 10px;
    padding: 15px;
    margin-bottom: 15px;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE CONNECTION WITH ERROR HANDLING
# ─────────────────────────────────────────────────────────────────────────────
try:
    NEON_URL = st.secrets["NEON_URL"]
except:
    NEON_URL = None

@st.cache_resource
def init_connection():
    if NEON_URL is None:
        st.warning("⚠️ Database URL not found. Using demo mode.")
        return None
    try:
        engine = create_engine(NEON_URL, pool_size=5, max_overflow=10, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).fetchone()
        return engine
    except Exception as e:
        st.warning(f"⚠️ Database connection failed. Using demo mode.")
        return None

engine = init_connection()

def execute_sql_query(query, params=None):
    if engine is None:
        return get_sample_data_for_query(query), None
    try:
        with engine.connect() as conn:
            conn.rollback()
            if params:
                result = pd.read_sql(query, conn, params=params)
            else:
                result = pd.read_sql(query, conn)
        return result, None
    except Exception as e:
        return None, str(e)

def get_sample_data_for_query(query):
    """Return sample data for demo mode"""
    if "rainfall" in query.lower():
        return pd.DataFrame({
            'district_name': ['North District', 'South District', 'East District', 'West District', 'Central District'],
            'rainfall_cm': [145.6, 98.3, 210.4, 78.9, 167.2],
            'record_year': [2024, 2024, 2024, 2024, 2024],
            'season': ['Monsoon', 'Monsoon', 'Monsoon', 'Monsoon', 'Monsoon'],
            'rainfall_category': ['High', 'Moderate', 'Extreme', 'Moderate', 'High']
        })
    elif "groundwater" in query.lower():
        return pd.DataFrame({
            'district_name': ['North District', 'South District', 'East District', 'West District', 'Central District'],
            'avg_depth_meters': [35.6, 28.2, 42.1, 22.5, 31.8],
            'extraction_pct': [72.4, 58.3, 81.2, 45.6, 63.5],
            'stress_level': ['High', 'Moderate', 'Critical', 'Low', 'Moderate'],
            'assessment_year': [2024, 2024, 2024, 2024, 2024],
            'recharge_rate_mcm': [250, 320, 180, 400, 290]
        })
    elif "water_monitoring" in query.lower():
        return pd.DataFrame({
            'station_name': ['Station A', 'Station B', 'Station C', 'Station D', 'Station E'],
            'ph_level': [7.2, 7.5, 6.8, 7.8, 7.1],
            'dissolved_oxygen_mg_l': [6.8, 7.2, 5.9, 7.5, 6.5],
            'status': ['Active', 'Active', 'Maintenance', 'Active', 'Active'],
            'state_name': ['State 1', 'State 1', 'State 2', 'State 2', 'State 1'],
            'district_name': ['North District', 'South District', 'East District', 'West District', 'Central District'],
            'turbidity_ntu': [4.2, 3.8, 12.5, 2.9, 5.6]
        })
    elif "water_sources" in query.lower():
        return pd.DataFrame({
            'source_name': ['River A', 'Reservoir B', 'Lake C', 'Well D', 'Canal E'],
            'source_type': ['River', 'Reservoir', 'Lake', 'Well', 'Canal'],
            'state': ['State 1', 'State 1', 'State 2', 'State 2', 'State 1'],
            'district': ['North District', 'South District', 'East District', 'West District', 'Central District'],
            'capacity_percent': [85.5, 45.2, 28.6, 72.3, 54.8],
            'latitude': [28.6139, 22.5726, 19.0760, 12.9716, 26.9124],
            'longitude': [77.2090, 88.3639, 72.8777, 77.5946, 75.7873]
        })
    else:
        return pd.DataFrame({'message': ['Demo data - Connect to database for live data']})

# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_all_data():
    if engine is None:
        return create_sample_data()
    
    try:
        with engine.connect() as conn:
            conn.rollback()
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
        st.warning(f"Error loading data. Using demo data.")
        return create_sample_data()

def create_sample_data():
    """Create comprehensive sample data for demo mode"""
    np.random.seed(42)
    
    # Water Sources
    sources = pd.DataFrame({
        'source_id': range(1, 21),
        'source_name': [f'Source_{i}' for i in range(1, 21)],
        'source_type': np.random.choice(['River', 'Reservoir', 'Lake', 'Well', 'Canal'], 20),
        'state': np.random.choice(['State A', 'State B', 'State C', 'State D'], 20),
        'district': [f'District_{i}' for i in range(1, 21)],
        'capacity_percent': np.random.randint(20, 100, 20),
        'latitude': np.random.uniform(8, 37, 20),
        'longitude': np.random.uniform(68, 97, 20),
        'assessment_date': pd.date_range('2024-01-01', periods=20)
    })
    sources['risk_level'] = pd.cut(sources['capacity_percent'], bins=[0, 30, 60, 100], labels=['Critical', 'Moderate', 'Good'])
    
    # Water Monitoring Stations
    stations = pd.DataFrame({
        'station_id': range(1, 16),
        'station_name': [f'Station_{i}' for i in range(1, 16)],
        'state_name': np.random.choice(['State A', 'State B', 'State C', 'State D'], 15),
        'district_name': [f'District_{i}' for i in range(1, 16)],
        'ph_level': np.random.uniform(6.2, 8.8, 15),
        'dissolved_oxygen_mg_l': np.random.uniform(3.5, 8.5, 15),
        'turbidity_ntu': np.random.uniform(1, 25, 15),
        'status': np.random.choice(['Active', 'Active', 'Active', 'Maintenance', 'Inactive'], 15, p=[0.7, 0.1, 0.1, 0.05, 0.05])
    })
    
    # Groundwater Levels
    groundwater = pd.DataFrame({
        'record_id': range(1, 41),
        'district_name': [f'District_{i}' for i in range(1, 9)] * 5,
        'avg_depth_meters': np.random.uniform(15, 55, 40),
        'extraction_pct': np.random.uniform(35, 85, 40),
        'recharge_rate_mcm': np.random.uniform(150, 450, 40),
        'assessment_year': np.random.choice([2020, 2021, 2022, 2023, 2024], 40),
        'stress_level': np.random.choice(['Low', 'Moderate', 'High', 'Critical'], 40)
    })
    
    # Rainfall History
    rainfall = pd.DataFrame({
        'record_id': range(1, 61),
        'district_name': [f'District_{i}' for i in range(1, 9)] * 7 + [f'District_{i}' for i in range(1, 5)],
        'rainfall_cm': np.random.uniform(20, 350, 60),
        'record_year': np.random.choice([2020, 2021, 2022, 2023, 2024], 60),
        'season': np.random.choice(['Winter', 'Summer', 'Monsoon', 'Post-Monsoon'], 60),
        'rainfall_category': np.random.choice(['Low', 'Moderate', 'High', 'Extreme'], 60)
    })
    
    # Alerts
    alerts = pd.DataFrame({
        'alert_id': range(1, 8),
        'source_name': [f'Source_{i}' for i in range(1, 8)],
        'alert_status': np.random.choice(['CRITICAL', 'WARNING', 'INFO'], 7, p=[0.3, 0.4, 0.3]),
        'alert_message': ['Low capacity', 'High extraction', 'Poor quality', 'Drought risk', 'Flood watch', 'Maintenance due', 'Over usage'],
        'timestamp': pd.date_range('2024-01-01', periods=7)
    })
    
    return sources, stations, groundwater, rainfall, alerts, pd.DataFrame(), pd.DataFrame(), stations

with st.spinner("🚀 Loading AQUASTAT Data..."):
    sources, stations, groundwater, rainfall, alerts, usage, regional, water_quality = load_all_data()

# ─────────────────────────────────────────────────────────────────────────────
# DATA PROCESSING
# ─────────────────────────────────────────────────────────────────────────────
current_time = datetime.now(pytz.timezone("Asia/Kolkata"))

if not sources.empty and "capacity_percent" in sources.columns:
    sources["capacity_percent"] = pd.to_numeric(sources["capacity_percent"], errors="coerce")
    if "risk_level" not in sources.columns:
        sources["risk_level"] = pd.cut(sources["capacity_percent"], bins=[0, 30, 60, 100], labels=["Critical", "Moderate", "Good"], include_lowest=True)

if not groundwater.empty and "avg_depth_meters" in groundwater.columns:
    if "stress_level" not in groundwater.columns:
        groundwater["stress_level"] = pd.cut(groundwater["avg_depth_meters"], bins=[0, 20, 40, 100], labels=["Low", "Moderate", "High"])

if not rainfall.empty and "rainfall_cm" in rainfall.columns:
    if "rainfall_category" not in rainfall.columns:
        rainfall["rainfall_category"] = pd.cut(rainfall["rainfall_cm"], bins=[0, 50, 150, 300, float("inf")], labels=["Low", "Moderate", "High", "Extreme"])

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR FILTERS - ALL FILTERS RESTORED HERE
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🎮 AQUASTAT")
    st.caption("Complete Water Management System")
    st.markdown("---")
    
    # Reset button
    if st.button("🔄 Reset All Filters", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    st.markdown("---")
    
    # Filter Type Selection Dropdown
    st.markdown("### 📋 Select Filter Category")
    filter_category = st.selectbox(
        "Choose Filter Type",
        options=["🏭 Source Capacity", "🌧️ Rainfall", "🌊 Groundwater", "💧 Water Quality", "📊 Combined Analytics"],
        key="filter_category_sidebar",
        help="Select which dataset to filter"
    )
    
    st.markdown("---")
    
    # Dynamic filters based on selection
    if filter_category == "🏭 Source Capacity":
        st.markdown("### 🏭 Source Capacity Filters")
        
        # State Filter
        if not sources.empty and "state" in sources.columns:
            state_options = ["All States"] + sorted(sources["state"].dropna().unique().tolist())
            selected_state = st.selectbox("📍 Select State", state_options, key="source_state")
        else:
            selected_state = "All States"
            state_options = ["All States"]
        
        # District Filter (dependent on state)
        if not sources.empty and "district" in sources.columns:
            if selected_state != "All States":
                district_options = ["All Districts"] + sorted(sources[sources["state"] == selected_state]["district"].dropna().unique().tolist())
            else:
                district_options = ["All Districts"] + sorted(sources["district"].dropna().unique().tolist())
            selected_district = st.selectbox("📍 Select District", district_options, key="source_district")
        else:
            selected_district = "All Districts"
        
        # Source Type Filter
        if not sources.empty and "source_type" in sources.columns:
            type_options = ["All Types"] + sorted(sources["source_type"].dropna().unique().tolist())
            selected_type = st.selectbox("💧 Source Type", type_options, key="source_type")
        else:
            selected_type = "All Types"
        
        # Capacity Range
        st.markdown("### 📊 Capacity Range")
        capacity_range = st.slider("Capacity Percentage (%)", 0, 100, (0, 100), key="capacity_range")
        
        # Risk Level Filter
        risk_options = ["All Risk Levels", "Critical", "Moderate", "Good"]
        selected_risk = st.selectbox("⚠️ Risk Level", risk_options, key="source_risk")
        
        # Apply button for source filter
        if st.button("🔍 Apply Source Filters", use_container_width=True, type="primary"):
            st.session_state.source_filters_applied = True
            st.success("✅ Filters applied to dashboard!")
    
    elif filter_category == "🌧️ Rainfall":
        st.markdown("### 🌧️ Rainfall Filters")
        
        # District Filter
        if not rainfall.empty and "district_name" in rainfall.columns:
            rain_districts = ["All Districts"] + sorted(rainfall["district_name"].dropna().unique().tolist())
            selected_rain_district = st.selectbox("📍 Select District", rain_districts, key="rain_district")
        else:
            selected_rain_district = "All Districts"
        
        # Year Filter
        if not rainfall.empty and "record_year" in rainfall.columns:
            year_options = ["All Years"] + sorted(rainfall["record_year"].dropna().unique().tolist())
            selected_year = st.selectbox("📅 Select Year", year_options, key="rain_year")
        else:
            selected_year = "All Years"
        
        # Season Filter
        season_options = ["All Seasons", "Winter", "Summer", "Monsoon", "Post-Monsoon"]
        selected_season = st.selectbox("🌤️ Season", season_options, key="rain_season")
        
        # Rainfall Range
        st.markdown("### 📊 Rainfall Range (cm)")
        rain_range = st.slider("Rainfall (cm)", 0, 500, (0, 500), key="rain_range")
        
        # Category Filter
        category_options = ["All Categories", "Low", "Moderate", "High", "Extreme"]
        selected_category = st.selectbox("🏷️ Rainfall Category", category_options, key="rain_category")
        
        if st.button("🔍 Apply Rainfall Filters", use_container_width=True, type="primary"):
            st.session_state.rain_filters_applied = True
            st.success("✅ Rainfall filters applied!")
    
    elif filter_category == "🌊 Groundwater":
        st.markdown("### 🌊 Groundwater Filters")
        
        # District Filter
        if not groundwater.empty and "district_name" in groundwater.columns:
            gw_districts = ["All Districts"] + sorted(groundwater["district_name"].dropna().unique().tolist())
            selected_gw_district = st.selectbox("📍 Select District", gw_districts, key="gw_district")
        else:
            selected_gw_district = "All Districts"
        
        # Year Filter
        if not groundwater.empty and "assessment_year" in groundwater.columns:
            gw_year_options = ["All Years"] + sorted(groundwater["assessment_year"].dropna().unique().tolist())
            selected_gw_year = st.selectbox("📅 Assessment Year", gw_year_options, key="gw_year")
        else:
            selected_gw_year = "All Years"
        
        # Depth Range
        st.markdown("### 📊 Depth Range (meters)")
        depth_range = st.slider("Groundwater Depth (m)", 0, 100, (0, 100), key="depth_range")
        
        # Stress Level
        stress_options = ["All Levels", "Low", "Moderate", "High", "Critical"]
        selected_stress = st.selectbox("⚠️ Stress Level", stress_options, key="gw_stress")
        
        # Extraction Range
        st.markdown("### 💧 Extraction Rate (%)")
        extraction_range = st.slider("Extraction Percentage (%)", 0, 100, (0, 100), key="extraction_range")
        
        if st.button("🔍 Apply Groundwater Filters", use_container_width=True, type="primary"):
            st.session_state.gw_filters_applied = True
            st.success("✅ Groundwater filters applied!")
    
    elif filter_category == "💧 Water Quality":
        st.markdown("### 💧 Water Quality Filters")
        
        # State Filter
        if not water_quality.empty and "state_name" in water_quality.columns:
            wq_state_options = ["All States"] + sorted(water_quality["state_name"].dropna().unique().tolist())
            selected_wq_state = st.selectbox("📍 Select State", wq_state_options, key="wq_state")
        else:
            selected_wq_state = "All States"
        
        # District Filter
        if not water_quality.empty and "district_name" in water_quality.columns:
            if selected_wq_state != "All States":
                wq_district_options = ["All Districts"] + sorted(water_quality[water_quality["state_name"] == selected_wq_state]["district_name"].dropna().unique().tolist())
            else:
                wq_district_options = ["All Districts"] + sorted(water_quality["district_name"].dropna().unique().tolist())
            selected_wq_district = st.selectbox("📍 Select District", wq_district_options, key="wq_district")
        else:
            selected_wq_district = "All Districts"
        
        # pH Range
        st.markdown("### 🧪 pH Level Range")
        ph_range = st.slider("pH Level", 0.0, 14.0, (6.0, 8.5), 0.1, key="ph_range")
        
        # DO Range
        st.markdown("### 💨 Dissolved Oxygen (mg/L)")
        do_range = st.slider("Dissolved Oxygen (mg/L)", 0.0, 15.0, (4.0, 8.0), 0.1, key="do_range")
        
        # Station Status
        status_options = ["All Status", "Active", "Maintenance", "Inactive"]
        selected_status = st.selectbox("🏭 Station Status", status_options, key="wq_status")
        
        if st.button("🔍 Apply Water Quality Filters", use_container_width=True, type="primary"):
            st.session_state.wq_filters_applied = True
            st.success("✅ Water quality filters applied!")
    
    elif filter_category == "📊 Combined Analytics":
        st.markdown("### 📊 Combined Analytics Filters")
        st.info("Apply multiple filters across all datasets")
        
        # Year range for combined analysis
        year_range = st.slider("Year Range", 2020, 2024, (2022, 2024), key="year_range")
        
        # Minimum data quality
        min_data_points = st.slider("Minimum Data Points per District", 3, 20, 5, key="min_data")
        
        if st.button("🔍 Apply Combined Filters", use_container_width=True, type="primary"):
            st.session_state.combined_filters_applied = True
            st.success("✅ Combined filters applied!")
    
    st.markdown("---")
    st.markdown("### 🗺️ Map Visualization Settings")
    show_clusters = st.checkbox("Show Marker Clusters", True, key="show_clusters")
    marker_size = st.slider("Marker Size", 5, 20, 10, key="marker_size")

# ─────────────────────────────────────────────────────────────────────────────
# APPLY FILTERS TO DATAFRAMES
# ─────────────────────────────────────────────────────────────────────────────
def filter_sources_data():
    df = sources.copy()
    if df.empty:
        return df
    
    # Apply state filter
    if 'selected_state' in st.session_state and st.session_state.selected_state != "All States":
        if "state" in df.columns:
            df = df[df["state"] == st.session_state.selected_state]
    
    # Apply district filter
    if 'selected_district' in st.session_state and st.session_state.selected_district != "All Districts":
        if "district" in df.columns:
            df = df[df["district"] == st.session_state.selected_district]
    
    # Apply source type filter
    if 'selected_type' in st.session_state and st.session_state.selected_type != "All Types":
        if "source_type" in df.columns:
            df = df[df["source_type"] == st.session_state.selected_type]
    
    # Apply capacity range filter
    if 'capacity_range' in st.session_state:
        min_cap, max_cap = st.session_state.capacity_range
        if "capacity_percent" in df.columns:
            df = df[(df["capacity_percent"] >= min_cap) & (df["capacity_percent"] <= max_cap)]
    
    # Apply risk filter
    if 'selected_risk' in st.session_state and st.session_state.selected_risk != "All Risk Levels":
        if "risk_level" in df.columns:
            df = df[df["risk_level"] == st.session_state.selected_risk]
    
    return df

def filter_rainfall_data():
    df = rainfall.copy()
    if df.empty:
        return df
    
    # Apply district filter
    if 'selected_rain_district' in st.session_state and st.session_state.selected_rain_district != "All Districts":
        if "district_name" in df.columns:
            df = df[df["district_name"] == st.session_state.selected_rain_district]
    
    # Apply year filter
    if 'selected_year' in st.session_state and st.session_state.selected_year != "All Years":
        if "record_year" in df.columns:
            df = df[df["record_year"] == st.session_state.selected_year]
    
    # Apply season filter
    if 'selected_season' in st.session_state and st.session_state.selected_season != "All Seasons":
        if "season" in df.columns:
            df = df[df["season"] == st.session_state.selected_season]
    
    # Apply rainfall range filter
    if 'rain_range' in st.session_state:
        min_rain, max_rain = st.session_state.rain_range
        if "rainfall_cm" in df.columns:
            df = df[(df["rainfall_cm"] >= min_rain) & (df["rainfall_cm"] <= max_rain)]
    
    # Apply category filter
    if 'selected_category' in st.session_state and st.session_state.selected_category != "All Categories":
        if "rainfall_category" in df.columns:
            df = df[df["rainfall_category"] == st.session_state.selected_category]
    
    return df

def filter_groundwater_data():
    df = groundwater.copy()
    if df.empty:
        return df
    
    # Apply district filter
    if 'selected_gw_district' in st.session_state and st.session_state.selected_gw_district != "All Districts":
        if "district_name" in df.columns:
            df = df[df["district_name"] == st.session_state.selected_gw_district]
    
    # Apply year filter
    if 'selected_gw_year' in st.session_state and st.session_state.selected_gw_year != "All Years":
        if "assessment_year" in df.columns:
            df = df[df["assessment_year"] == st.session_state.selected_gw_year]
    
    # Apply depth range filter
    if 'depth_range' in st.session_state:
        min_depth, max_depth = st.session_state.depth_range
        if "avg_depth_meters" in df.columns:
            df = df[(df["avg_depth_meters"] >= min_depth) & (df["avg_depth_meters"] <= max_depth)]
    
    # Apply stress filter
    if 'selected_stress' in st.session_state and st.session_state.selected_stress != "All Levels":
        if "stress_level" in df.columns:
            df = df[df["stress_level"] == st.session_state.selected_stress]
    
    # Apply extraction range filter
    if 'extraction_range' in st.session_state:
        min_ext, max_ext = st.session_state.extraction_range
        if "extraction_pct" in df.columns:
            df = df[(df["extraction_pct"] >= min_ext) & (df["extraction_pct"] <= max_ext)]
    
    return df

def filter_water_quality_data():
    df = water_quality.copy()
    if df.empty:
        return df
    
    # Apply state filter
    if 'selected_wq_state' in st.session_state and st.session_state.selected_wq_state != "All States":
        if "state_name" in df.columns:
            df = df[df["state_name"] == st.session_state.selected_wq_state]
    
    # Apply district filter
    if 'selected_wq_district' in st.session_state and st.session_state.selected_wq_district != "All Districts":
        if "district_name" in df.columns:
            df = df[df["district_name"] == st.session_state.selected_wq_district]
    
    # Apply pH range filter
    if 'ph_range' in st.session_state:
        min_ph, max_ph = st.session_state.ph_range
        if "ph_level" in df.columns:
            df = df[(df["ph_level"] >= min_ph) & (df["ph_level"] <= max_ph)]
    
    # Apply DO range filter
    if 'do_range' in st.session_state:
        min_do, max_do = st.session_state.do_range
        if "dissolved_oxygen_mg_l" in df.columns:
            df = df[(df["dissolved_oxygen_mg_l"] >= min_do) & (df["dissolved_oxygen_mg_l"] <= max_do)]
    
    # Apply status filter
    if 'selected_status' in st.session_state and st.session_state.selected_status != "All Status":
        if "status" in df.columns:
            df = df[df["status"] == st.session_state.selected_status]
    
    return df

# Apply all filters
filtered_sources = filter_sources_data()
filtered_rainfall = filter_rainfall_data()
filtered_groundwater = filter_groundwater_data()
filtered_water_quality = filter_water_quality_data()

# ─────────────────────────────────────────────────────────────────────────────
# HEADER + KPIs
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(f"<h1 style='color:#00e5ff'>💧 AQUASTAT</h1>", unsafe_allow_html=True)
st.markdown(f"<p>National Water Command Center • Live Intelligence • {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}</p>", unsafe_allow_html=True)

# Calculate KPIs based on filtered data
total_sources = len(filtered_sources)
avg_capacity = filtered_sources["capacity_percent"].mean() if not filtered_sources.empty and "capacity_percent" in filtered_sources.columns else 0
critical_sources = len(filtered_sources[filtered_sources["capacity_percent"] < 30]) if not filtered_sources.empty and "capacity_percent" in filtered_sources.columns else 0
active_alerts = len(alerts[alerts["alert_status"] == "CRITICAL"]) if not alerts.empty and "alert_status" in alerts.columns else 0
gw_records = len(filtered_groundwater)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Sources", total_sources)
k2.metric("Avg Capacity", f"{avg_capacity:.1f}%" if avg_capacity else "0%")
k3.metric("Critical Sources", critical_sources)
k4.metric("Critical Alerts", active_alerts)
k5.metric("GW Records", gw_records)

# ─────────────────────────────────────────────────────────────────────────────
# MAIN TABS
# ─────────────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📊 Dashboard", "🗺️ Map View", "📈 Analytics", "💧 Water Quality", "⚠️ Alerts", "🗄️ SQL Queries"])

# TAB 1: DASHBOARD
with tab1:
    st.markdown("### 📊 Water Resources Dashboard")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Source Distribution by Type
        st.markdown("#### 💧 Water Sources by Type")
        if not filtered_sources.empty and "source_type" in filtered_sources.columns:
            type_counts = filtered_sources['source_type'].value_counts()
            if not type_counts.empty:
                fig = px.pie(
                    values=type_counts.values, 
                    names=type_counts.index, 
                    title="Source Distribution",
                    color_discrete_sequence=px.colors.sequential.Teal
                )
                fig.update_layout(bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#cfe4f7')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data available")
        else:
            st.info("No source type data available")
        
        # Capacity Distribution Histogram
        st.markdown("#### 📈 Capacity Distribution")
        if not filtered_sources.empty and "capacity_percent" in filtered_sources.columns:
            fig = px.histogram(
                filtered_sources, 
                x="capacity_percent", 
                nbins=20,
                title="Water Source Capacity Distribution",
                color_discrete_sequence=['#00e5ff']
            )
            fig.update_layout(bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#cfe4f7')
            fig.add_vline(x=30, line_dash="dash", line_color="red", annotation_text="Critical")
            fig.add_vline(x=60, line_dash="dash", line_color="orange", annotation_text="Moderate")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No capacity data available")
    
    with col2:
        # Risk Level Distribution
        st.markdown("#### ⚠️ Risk Assessment")
        if not filtered_sources.empty and "risk_level" in filtered_sources.columns:
            risk_counts = filtered_sources['risk_level'].value_counts()
            if not risk_counts.empty:
                colors = {'Critical': '#ff4444', 'Moderate': '#ffd700', 'Good': '#00ff9d'}
                fig = px.bar(
                    x=risk_counts.index, 
                    y=risk_counts.values, 
                    title="Risk Level Distribution",
                    color=risk_counts.index,
                    color_discrete_map=colors
                )
                fig.update_layout(bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#cfe4f7')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No risk data available")
        else:
            st.info("No risk level data available")
        
        # Top 10 Sources by Capacity
        st.markdown("#### 🏆 Top 10 Sources by Capacity")
        if not filtered_sources.empty and "capacity_percent" in filtered_sources.columns and "source_name" in filtered_sources.columns:
            top_sources = filtered_sources.nlargest(10, "capacity_percent")[["source_name", "source_type", "capacity_percent"]]
            fig = px.bar(
                top_sources,
                x="capacity_percent",
                y="source_name",
                orientation='h',
                title="Top Performing Sources",
                color="capacity_percent",
                color_continuous_scale="Viridis"
            )
            fig.update_layout(bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#cfe4f7')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No source data available")
    
    # Recent Data Table
    st.markdown("#### 📋 Filtered Water Sources Data")
    if not filtered_sources.empty:
        st.dataframe(filtered_sources.head(50), use_container_width=True)
    else:
        st.warning("No data available for selected filters")

# TAB 2: MAP VIEW
with tab2:
    st.subheader("🗺️ Interactive Water Resources Map")
    
    if not filtered_sources.empty and "latitude" in filtered_sources.columns and "longitude" in filtered_sources.columns:
        map_sources = filtered_sources[filtered_sources["latitude"].notna() & filtered_sources["longitude"].notna()].copy()
        
        if not map_sources.empty:
            center_lat = map_sources["latitude"].mean()
            center_lon = map_sources["longitude"].mean()
            m = folium.Map(location=[center_lat, center_lon], zoom_start=6, tiles="CartoDB dark_matter")
            
            # Add heatmap layer if enabled
            if st.session_state.get("show_heatmap", False):
                heat_data = [[row["latitude"], row["longitude"], row.get("capacity_percent", 50)] 
                           for _, row in map_sources.iterrows()]
                HeatMap(heat_data).add_to(m)
            
            # Add marker cluster if enabled
            if show_clusters:
                marker_cluster = MarkerCluster().add_to(m)
                for _, row in map_sources.iterrows():
                    capacity = row.get("capacity_percent", 100)
                    if pd.isna(capacity):
                        capacity = 100
                    color = "red" if capacity < 30 else "orange" if capacity < 60 else "green"
                    
                    popup_text = f"""
                    <b>{row.get('source_name', 'Unknown')}</b><br>
                    Type: {row.get('source_type', 'N/A')}<br>
                    Capacity: {capacity:.1f}%<br>
                    District: {row.get('district', 'N/A')}
                    """
                    
                    folium.CircleMarker(
                        [row["latitude"], row["longitude"]], 
                        radius=marker_size, 
                        color=color, 
                        fill=True,
                        fill_opacity=0.7,
                        popup=popup_text
                    ).add_to(marker_cluster)
            else:
                for _, row in map_sources.iterrows():
                    capacity = row.get("capacity_percent", 100)
                    if pd.isna(capacity):
                        capacity = 100
                    color = "red" if capacity < 30 else "orange" if capacity < 60 else "green"
                    
                    popup_text = f"""
                    <b>{row.get('source_name', 'Unknown')}</b><br>
                    Type: {row.get('source_type', 'N/A')}<br>
                    Capacity: {capacity:.1f}%
                    """
                    
                    folium.CircleMarker(
                        [row["latitude"], row["longitude"]], 
                        radius=marker_size, 
                        color=color, 
                        fill=True,
                        popup=popup_text
                    ).add_to(m)
            
            # Add fullscreen button
            Fullscreen().add_to(m)
            
            st_folium(m, width=1200, height=600)
        else:
            st.warning("No coordinates available for selected filters")
    else:
        st.warning("Latitude/Longitude data not available")

# TAB 3: ANALYTICS
with tab3:
    st.subheader("📈 Analytics Dashboard")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Rainfall Trend
        st.markdown("#### 🌧️ Rainfall Trend")
        if not filtered_rainfall.empty and "record_year" in filtered_rainfall.columns and "rainfall_cm" in filtered_rainfall.columns:
            rain_trend = filtered_rainfall.groupby('record_year')['rainfall_cm'].agg(['mean', 'min', 'max']).reset_index()
            if not rain_trend.empty:
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=rain_trend['record_year'], y=rain_trend['mean'], 
                                        mode='lines+markers', name='Average', line=dict(color='#00e5ff', width=2)))
                fig.add_trace(go.Scatter(x=rain_trend['record_year'], y=rain_trend['max'], 
                                        mode='lines', name='Maximum', line=dict(color='#ff4444', width=1, dash='dash')))
                fig.add_trace(go.Scatter(x=rain_trend['record_year'], y=rain_trend['min'], 
                                        mode='lines', name='Minimum', line=dict(color='#00ff9d', width=1, dash='dash')))
                fig.update_layout(title="Rainfall Trend Over Years", bgcolor='rgba(0,0,0,0)', 
                                 paper_bgcolor='rgba(0,0,0,0)', font_color='#cfe4f7')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No rainfall trend data")
        else:
            st.info("No rainfall data available")
        
        # Seasonal Rainfall Pattern
        st.markdown("#### 🌤️ Seasonal Rainfall Pattern")
        if not filtered_rainfall.empty and "season" in filtered_rainfall.columns and "rainfall_cm" in filtered_rainfall.columns:
            seasonal = filtered_rainfall.groupby('season')['rainfall_cm'].mean().reset_index()
            if not seasonal.empty:
                fig = px.bar(seasonal, x='season', y='rainfall_cm', title="Average Rainfall by Season",
                            color='rainfall_cm', color_continuous_scale='Blues')
                fig.update_layout(bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#cfe4f7')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No seasonal data")
    
    with col2:
        # Groundwater Trend
        st.markdown("#### 🌊 Groundwater Depth Trend")
        if not filtered_groundwater.empty and "assessment_year" in filtered_groundwater.columns and "avg_depth_meters" in filtered_groundwater.columns:
            gw_trend = filtered_groundwater.groupby('assessment_year')['avg_depth_meters'].mean().reset_index()
            if not gw_trend.empty:
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=gw_trend['assessment_year'], y=gw_trend['avg_depth_meters'], 
                                        mode='lines+markers', name='Average Depth', 
                                        line=dict(color='#00e5ff', width=2),
                                        marker=dict(size=8, color='#ff4444')))
                fig.update_layout(title="Groundwater Depth Trend (Deeper = Worse)",
                                 yaxis_title="Depth (meters)", bgcolor='rgba(0,0,0,0)',
                                 paper_bgcolor='rgba(0,0,0,0)', font_color='#cfe4f7')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No groundwater trend data")
        else:
            st.info("No groundwater data available")
        
        # Extraction vs Recharge
        st.markdown("#### 💧 Extraction vs Recharge Analysis")
        if not filtered_groundwater.empty and "extraction_pct" in filtered_groundwater.columns and "recharge_rate_mcm" in filtered_groundwater.columns:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=filtered_groundwater['district_name'].head(10), 
                                y=filtered_groundwater['extraction_pct'].head(10), 
                                name='Extraction %', marker_color='#ff4444'))
            fig.add_trace(go.Bar(x=filtered_groundwater['district_name'].head(10), 
                                y=filtered_groundwater['recharge_rate_mcm'].head(10) / 5, 
                                name='Recharge Rate (scaled)', marker_color='#00ff9d'))
            fig.update_layout(title="Extraction vs Recharge by District", barmode='group',
                             bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#cfe4f7')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No extraction/recharge data")
    
    # Correlation Heatmap
    st.markdown("#### 🔥 Parameter Correlation Heatmap")
    if not filtered_sources.empty and not filtered_groundwater.empty:
        # Create correlation data
        corr_data = pd.DataFrame()
        if "capacity_percent" in filtered_sources.columns:
            corr_data['capacity'] = filtered_sources['capacity_percent']
        if not filtered_rainfall.empty and "rainfall_cm" in filtered_rainfall.columns:
            corr_data['rainfall'] = filtered_rainfall['rainfall_cm'].head(len(corr_data)) if len(corr_data) > 0 else []
        if not filtered_groundwater.empty and "avg_depth_meters" in filtered_groundwater.columns:
            corr_data['gw_depth'] = filtered_groundwater['avg_depth_meters'].head(len(corr_data)) if len(corr_data) > 0 else []
        
        if len(corr_data.columns) > 1:
            corr_matrix = corr_data.corr()
            fig = px.imshow(corr_matrix, text_auto=True, aspect="auto", 
                           title="Correlation Between Water Parameters",
                           color_continuous_scale='RdBu')
            fig.update_layout(bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#cfe4f7')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Insufficient data for correlation analysis")
    else:
        st.info("Need multiple parameters for correlation analysis")

# TAB 4: WATER QUALITY
with tab4:
    st.subheader("💧 Water Quality Monitoring")
    
    if not filtered_water_quality.empty:
        col1, col2, col3 = st.columns(3)
        
        avg_ph = filtered_water_quality['ph_level'].mean() if 'ph_level' in filtered_water_quality else 0
        avg_do = filtered_water_quality['dissolved_oxygen_mg_l'].mean() if 'dissolved_oxygen_mg_l' in filtered_water_quality else 0
        active_stations = len(filtered_water_quality[filtered_water_quality['status'] == 'Active']) if 'status' in filtered_water_quality else 0
        
        col1.metric("Avg pH", f"{avg_ph:.2f}", delta="Ideal: 6.5-8.5")
        col2.metric("Avg Dissolved Oxygen", f"{avg_do:.1f} mg/L", delta="Good: >5 mg/L")
        col3.metric("Active Stations", active_stations)
        
        # pH Distribution
        st.markdown("#### 🧪 pH Level Distribution")
        if 'ph_level' in filtered_water_quality.columns:
            fig = px.histogram(filtered_water_quality, x='ph_level', nbins=20, 
                              title="pH Level Distribution",
                              color_discrete_sequence=['#00e5ff'])
            fig.add_vline(x=6.5, line_dash="dash", line_color="green", annotation_text="Ideal Min")
            fig.add_vline(x=8.5, line_dash="dash", line_color="green", annotation_text="Ideal Max")
            fig.update_layout(bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#cfe4f7')
            st.plotly_chart(fig, use_container_width=True)
        
        # DO vs pH Scatter
        st.markdown("#### 📊 Dissolved Oxygen vs pH Analysis")
        if 'dissolved_oxygen_mg_l' in filtered_water_quality.columns and 'ph_level' in filtered_water_quality.columns:
            fig = px.scatter(filtered_water_quality, x='ph_level', y='dissolved_oxygen_mg_l',
                            color='status', size='turbidity_ntu' if 'turbidity_ntu' in filtered_water_quality.columns else None,
                            title="DO vs pH Relationship",
                            labels={'ph_level': 'pH Level', 'dissolved_oxygen_mg_l': 'Dissolved Oxygen (mg/L)'})
            fig.update_layout(bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#cfe4f7')
            st.plotly_chart(fig, use_container_width=True)
        
        # Water Quality Data Table
        st.markdown("#### 📋 Water Quality Data")
        st.dataframe(filtered_water_quality, use_container_width=True)
    else:
        st.info("No water quality data available for selected filters")

# TAB 5: ALERTS
with tab5:
    st.subheader("⚠️ Active Alerts and Warnings")
    
    if not alerts.empty:
        critical = len(alerts[alerts['alert_status'] == 'CRITICAL']) if 'alert_status' in alerts else 0
        warning = len(alerts[alerts['alert_status'] == 'WARNING']) if 'alert_status' in alerts else 0
        info = len(alerts[alerts['alert_status'] == 'INFO']) if 'alert_status' in alerts else 0
        
        col1, col2, col3 = st.columns(3)
        col1.markdown(f"<div class='badge-critical'>🔴 CRITICAL: {critical}</div>", unsafe_allow_html=True)
        col2.markdown(f"<div class='badge-warning'>🟡 WARNING: {warning}</div>", unsafe_allow_html=True)
        col3.markdown(f"<div class='badge-good'>🔵 INFO: {info}</div>", unsafe_allow_html=True)
        
        # Alert Timeline
        if 'timestamp' in alerts.columns:
            alerts['timestamp'] = pd.to_datetime(alerts['timestamp'])
            alert_timeline = alerts.groupby([alerts['timestamp'].dt.date, 'alert_status']).size().reset_index(name='count')
            fig = px.line(alert_timeline, x='timestamp', y='count', color='alert_status',
                         title="Alert Timeline")
            fig.update_layout(bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#cfe4f7')
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("#### 📋 Alert Details")
        st.dataframe(alerts, use_container_width=True)
    else:
        st.success("✅ No active alerts at this time")

# TAB 6: SQL QUERIES
with tab6:
    st.subheader("🗄️ SQL Query Workspace")
    
    # Quick SQL Templates
    st.markdown("### 📋 Quick SQL Templates")
    template_queries = {
        "Top 10 Water Sources": "SELECT source_name, source_type, state, district, capacity_percent FROM water_sources ORDER BY capacity_percent DESC LIMIT 10",
        "Critical Sources (<30%)": "SELECT source_name, state, district, capacity_percent FROM water_sources WHERE capacity_percent < 30 ORDER BY capacity_percent",
        "Average Rainfall by District": "SELECT district_name, ROUND(AVG(rainfall_cm), 2) as avg_rainfall FROM rainfall_history GROUP BY district_name ORDER BY avg_rainfall DESC",
        "Groundwater Stress Analysis": "SELECT district_name, ROUND(AVG(avg_depth_meters), 2) as avg_depth, ROUND(AVG(extraction_pct), 2) as avg_extraction FROM groundwater_levels GROUP BY district_name ORDER BY avg_depth DESC",
        "Water Quality Summary": "SELECT state_name, ROUND(AVG(ph_level), 2) as avg_ph, ROUND(AVG(dissolved_oxygen_mg_l), 2) as avg_do FROM water_monitoring_stations GROUP BY state_name",
        "High Risk Districts": """
            SELECT DISTINCT gw.district_name, 
                   ROUND(AVG(gw.avg_depth_meters), 2) as avg_depth,
                   ROUND(AVG(ws.capacity_percent), 2) as avg_capacity
            FROM groundwater_levels gw
            JOIN water_sources ws ON gw.district_name = ws.district
            WHERE gw.avg_depth_meters > 30 OR ws.capacity_percent < 40
            GROUP BY gw.district_name
            ORDER BY avg_depth DESC
        """
    }
    
    selected_template = st.selectbox("Load SQL Template", ["-- Select a template --"] + list(template_queries.keys()))
    if selected_template != "-- Select a template --":
        st.session_state.custom_query = template_queries[selected_template]
    
    st.markdown("### ✍️ Custom SQL Query")
    custom_query = st.text_area("Enter SQL Query:", height=150, 
                                value=st.session_state.get("custom_query", "SELECT * FROM water_sources LIMIT 50"), 
                                key="custom_query")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("▶️ Execute Query", use_container_width=True, type="primary"):
            if custom_query.strip():
                results, error = execute_sql_query(custom_query)
                if error:
                    st.error(f"Error: {error}")
                else:
                    st.session_state.custom_results = results
                    st.success(f"✅ Query executed successfully! Found {len(results)} records")
                    st.code(custom_query, language="sql")
                    st.dataframe(results, use_container_width=True)
                    
                    if not results.empty:
                        csv = results.to_csv(index=False).encode('utf-8')
                        st.download_button("📥 Download Results as CSV", csv, "query_results.csv", "text/csv")
            else:
                st.warning("Please enter a SQL query")
    
    with col2:
        if st.button("📊 Show Table Schema", use_container_width=True):
            st.markdown("### Available Tables:")
            tables_info = """
            - `water_sources` - Water source information (capacity, location, type)
            - `water_monitoring_stations` - Water quality monitoring data (pH, DO, turbidity)
            - `groundwater_levels` - Groundwater depth, extraction, recharge rates
            - `rainfall_history` - Historical rainfall data by district and season
            - `active_alerts` - Current alerts and warnings
            - `water_usage_history` - Water consumption by sector
            """
            st.info(tables_info)

# ─────────────────────────────────────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(f"<p style='text-align:center'>AQUASTAT v4.0 • Advanced Water Management System • Updated: {current_time.strftime('%Y-%m-%d %H:%M IST')}</p>", unsafe_allow_html=True)

if st.button("🔄 Refresh All Data"):
    st.cache_data.clear()
    st.rerun()
