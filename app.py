import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import folium
from folium.plugins import MarkerCluster, HeatMap
from streamlit_folium import st_folium
from datetime import datetime
import warnings
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
.filter-section {
    background: rgba(10,30,48,0.4);
    border: 1px solid rgba(0,200,255,0.15);
    border-radius: 10px;
    padding: 15px;
    margin-bottom: 20px;
}
.filter-header {
    color: #00e5ff;
    font-size: 1rem;
    font-weight: 600;
    margin-bottom: 10px;
    border-bottom: 1px solid rgba(0,200,255,0.3);
    padding-bottom: 5px;
}
.kpi-card {
    background: rgba(255,255,255,0.035);
    border: 1px solid rgba(0,200,255,0.18);
    border-radius: 14px;
    padding: 15px;
    text-align: center;
}
.kpi-value { color:#00e5ff; font-size: 1.8rem; font-weight: 700; }
.badge-critical { background: #c0392b; color:#fff; padding:4px 12px; border-radius:20px; display:inline-block; }
.badge-warning { background: #e67e22; color:#fff; padding:4px 12px; border-radius:20px; display:inline-block; }
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
        engine = create_engine(NEON_URL, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).fetchone()
        return engine
    except Exception as e:
        st.warning(f"Database connection failed. Using demo data.")
        return None

engine = init_connection()

def execute_query(query, params=None):
    """Execute SQL query and return dataframe"""
    if engine is None:
        return get_demo_data(query)
    try:
        with engine.connect() as conn:
            if params:
                # Convert params to tuple if it's a list
                if isinstance(params, list):
                    params = tuple(params)
                return pd.read_sql(query, conn, params=params)
            else:
                return pd.read_sql(query, conn)
    except Exception as e:
        st.warning(f"Query error: {str(e)[:100]}. Using demo data.")
        return get_demo_data(query)

def get_demo_data(query):
    """Return demo data based on query type"""
    np.random.seed(42)
    
    if "water_sources" in query.lower():
        return pd.DataFrame({
            'source_name': [f'Source_{i}' for i in range(1, 21)],
            'source_type': np.random.choice(['River', 'Reservoir', 'Lake', 'Well', 'Canal'], 20),
            'state': np.random.choice(['State A', 'State B', 'State C', 'State D'], 20),
            'district': np.random.choice(['North', 'South', 'East', 'West', 'Central'], 20),
            'capacity_percent': np.random.randint(20, 100, 20),
            'latitude': np.random.uniform(8, 37, 20),
            'longitude': np.random.uniform(68, 97, 20),
            'risk_level': np.random.choice(['Critical', 'Moderate', 'Good'], 20)
        })
    elif "rainfall" in query.lower():
        return pd.DataFrame({
            'district_name': np.random.choice(['North', 'South', 'East', 'West', 'Central'], 50),
            'rainfall_cm': np.random.uniform(20, 350, 50),
            'record_year': np.random.choice([2020, 2021, 2022, 2023, 2024], 50),
            'season': np.random.choice(['Winter', 'Summer', 'Monsoon', 'Post-Monsoon'], 50),
            'rainfall_category': np.random.choice(['Low', 'Moderate', 'High', 'Extreme'], 50)
        })
    elif "groundwater" in query.lower():
        return pd.DataFrame({
            'district_name': np.random.choice(['North', 'South', 'East', 'West', 'Central'], 40),
            'avg_depth_meters': np.random.uniform(15, 55, 40),
            'extraction_pct': np.random.uniform(35, 85, 40),
            'recharge_rate_mcm': np.random.uniform(150, 450, 40),
            'assessment_year': np.random.choice([2020, 2021, 2022, 2023, 2024], 40),
            'stress_level': np.random.choice(['Low', 'Moderate', 'High', 'Critical'], 40)
        })
    elif "water_monitoring" in query.lower():
        return pd.DataFrame({
            'station_name': [f'Station_{i}' for i in range(1, 16)],
            'state_name': np.random.choice(['State A', 'State B', 'State C', 'State D'], 15),
            'district_name': np.random.choice(['North', 'South', 'East', 'West', 'Central'], 15),
            'ph_level': np.random.uniform(6.2, 8.8, 15),
            'dissolved_oxygen_mg_l': np.random.uniform(3.5, 8.5, 15),
            'turbidity_ntu': np.random.uniform(1, 25, 15),
            'status': np.random.choice(['Active', 'Maintenance', 'Inactive'], 15, p=[0.7, 0.2, 0.1])
        })
    else:
        return pd.DataFrame({'message': ['Demo data - Connect to database for live data']})

# ─────────────────────────────────────────────────────────────────────────────
# SQL QUERIES FOR EACH FILTER TYPE (AUTOMATIC)
# ─────────────────────────────────────────────────────────────────────────────

def get_water_sources_query(state=None, district=None, source_type=None, min_cap=0, max_cap=100, risk=None):
    """Build SQL query for water sources with filters"""
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
        query += " AND capacity_percent BETWEEN %s AND %s"
        params.extend([min_cap, max_cap])
    if risk and risk != "All Risk Levels":
        query += " AND risk_level = %s"
        params.append(risk)
    
    query += " ORDER BY capacity_percent DESC"
    return query, params

def get_rainfall_query(district=None, year=None, season=None, min_rain=0, max_rain=500, category=None):
    """Build SQL query for rainfall with filters"""
    query = "SELECT * FROM rainfall_history WHERE 1=1"
    params = []
    
    if district and district != "All Districts":
        query += " AND district_name = %s"
        params.append(district)
    if year and year != "All Years":
        query += " AND record_year = %s"
        params.append(year)
    if season and season != "All Seasons":
        query += " AND season = %s"
        params.append(season)
    if min_rain > 0 or max_rain < 500:
        query += " AND rainfall_cm BETWEEN %s AND %s"
        params.extend([min_rain, max_rain])
    if category and category != "All Categories":
        query += " AND rainfall_category = %s"
        params.append(category)
    
    query += " ORDER BY record_year DESC, rainfall_cm DESC"
    return query, params

def get_groundwater_query(district=None, year=None, min_depth=0, max_depth=100, stress=None, min_ext=0, max_ext=100, min_recharge=0, max_recharge=1000):
    """Build SQL query for groundwater with filters"""
    query = "SELECT * FROM groundwater_levels WHERE 1=1"
    params = []
    
    if district and district != "All Districts":
        query += " AND district_name = %s"
        params.append(district)
    if year and year != "All Years":
        query += " AND assessment_year = %s"
        params.append(year)
    if min_depth > 0 or max_depth < 100:
        query += " AND avg_depth_meters BETWEEN %s AND %s"
        params.extend([min_depth, max_depth])
    if stress and stress != "All Levels":
        query += " AND stress_level = %s"
        params.append(stress)
    if min_ext > 0 or max_ext < 100:
        query += " AND extraction_pct BETWEEN %s AND %s"
        params.extend([min_ext, max_ext])
    if min_recharge > 0 or max_recharge < 1000:
        query += " AND recharge_rate_mcm BETWEEN %s AND %s"
        params.extend([min_recharge, max_recharge])
    
    query += " ORDER BY assessment_year DESC, avg_depth_meters DESC"
    return query, params

def get_water_quality_query(state=None, district=None, min_ph=0, max_ph=14, min_do=0, max_do=15, min_turb=0, max_turb=100, status=None):
    """Build SQL query for water quality with filters"""
    query = "SELECT * FROM water_monitoring_stations WHERE 1=1"
    params = []
    
    if state and state != "All States":
        query += " AND state_name = %s"
        params.append(state)
    if district and district != "All Districts":
        query += " AND district_name = %s"
        params.append(district)
    if min_ph > 0 or max_ph < 14:
        query += " AND ph_level BETWEEN %s AND %s"
        params.extend([min_ph, max_ph])
    if min_do > 0 or max_do < 15:
        query += " AND dissolved_oxygen_mg_l BETWEEN %s AND %s"
        params.extend([min_do, max_do])
    if min_turb > 0 or max_turb < 100:
        query += " AND turbidity_ntu BETWEEN %s AND %s"
        params.extend([min_turb, max_turb])
    if status and status != "All Status":
        query += " AND status = %s"
        params.append(status)
    
    query += " ORDER BY station_name"
    return query, params

def get_rainfall_trend_query(district=None, start_year=2020, end_year=2024):
    """Get rainfall trend analysis"""
    query = """
        SELECT 
            record_year,
            COUNT(*) as total_records,
            ROUND(AVG(rainfall_cm), 2) as avg_rainfall,
            ROUND(MIN(rainfall_cm), 2) as min_rainfall,
            ROUND(MAX(rainfall_cm), 2) as max_rainfall
        FROM rainfall_history
        WHERE record_year BETWEEN %s AND %s
    """
    params = [start_year, end_year]
    
    if district and district != "All Districts":
        query += " AND district_name = %s"
        params.append(district)
    
    query += " GROUP BY record_year ORDER BY record_year"
    return query, params

def get_groundwater_trend_query(district=None, start_year=2020, end_year=2024):
    """Get groundwater trend analysis"""
    query = """
        SELECT 
            assessment_year,
            COUNT(*) as total_records,
            ROUND(AVG(avg_depth_meters), 2) as avg_depth,
            ROUND(AVG(extraction_pct), 2) as avg_extraction
        FROM groundwater_levels
        WHERE assessment_year BETWEEN %s AND %s
    """
    params = [start_year, end_year]
    
    if district and district != "All Districts":
        query += " AND district_name = %s"
        params.append(district)
    
    query += " GROUP BY assessment_year ORDER BY assessment_year"
    return query, params

def get_alert_query(sensitivity='Medium'):
    """Get alerts based on sensitivity"""
    threshold = {'Low': 50, 'Medium': 60, 'High': 70, 'Critical': 80}
    thresh = threshold.get(sensitivity, 60)
    
    return f"""
        SELECT 
            source_name,
            source_type,
            state,
            capacity_percent,
            CASE 
                WHEN capacity_percent < 30 THEN 'CRITICAL_ALERT'
                WHEN capacity_percent < {thresh} THEN 'WARNING'
                ELSE 'NORMAL'
            END as alert_level
        FROM water_sources
        WHERE capacity_percent < {thresh}
        ORDER BY capacity_percent
        LIMIT 20
    """, []

# ─────────────────────────────────────────────────────────────────────────────
# INITIALIZE SESSION STATE FOR FILTERS
# ─────────────────────────────────────────────────────────────────────────────
if 'sources_data' not in st.session_state:
    st.session_state.sources_data = None
if 'rainfall_data' not in st.session_state:
    st.session_state.rainfall_data = None
if 'groundwater_data' not in st.session_state:
    st.session_state.groundwater_data = None
if 'water_quality_data' not in st.session_state:
    st.session_state.water_quality_data = None

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR - ALL 6 FILTER SECTIONS
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🎮 AQUASTAT")
    st.caption("Complete Water Management System")
    st.markdown("---")
    
    if st.button("🔄 Reset All Filters", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    st.markdown("---")
    
    # ========== SECTION 1: WATER SOURCE FILTERS ==========
    with st.expander("🏭 WATER SOURCE FILTERS", expanded=True):
        state_filter = st.selectbox("State", ["All States", "State A", "State B", "State C", "State D"], key="state")
        district_filter = st.selectbox("District", ["All Districts", "North", "South", "East", "West", "Central"], key="district")
        source_type_filter = st.selectbox("Source Type", ["All Types", "River", "Reservoir", "Lake", "Well", "Canal"], key="source_type")
        
        col1, col2 = st.columns(2)
        with col1:
            min_capacity = st.number_input("Min Capacity %", 0, 100, 0, key="min_cap")
        with col2:
            max_capacity = st.number_input("Max Capacity %", 0, 100, 100, key="max_cap")
        
        risk_filter = st.selectbox("Risk Level", ["All Risk Levels", "Critical", "Moderate", "Good"], key="risk")
    
    # ========== SECTION 2: RAINFALL FILTERS ==========
    with st.expander("🌧️ RAINFALL FILTERS", expanded=True):
        rain_district = st.selectbox("Rainfall District", ["All Districts", "North", "South", "East", "West", "Central"], key="rain_dist")
        rain_year = st.selectbox("Year", ["All Years", 2020, 2021, 2022, 2023, 2024], key="rain_year")
        rain_season = st.selectbox("Season", ["All Seasons", "Winter", "Summer", "Monsoon", "Post-Monsoon"], key="rain_season")
        
        col1, col2 = st.columns(2)
        with col1:
            min_rainfall = st.number_input("Min Rainfall (cm)", 0, 500, 0, key="min_rain")
        with col2:
            max_rainfall = st.number_input("Max Rainfall (cm)", 0, 500, 500, key="max_rain")
        
        rain_category = st.selectbox("Rainfall Category", ["All Categories", "Low", "Moderate", "High", "Extreme"], key="rain_cat")
    
    # ========== SECTION 3: GROUNDWATER FILTERS ==========
    with st.expander("🌊 GROUNDWATER FILTERS", expanded=True):
        gw_district = st.selectbox("GW District", ["All Districts", "North", "South", "East", "West", "Central"], key="gw_dist")
        gw_year = st.selectbox("Assessment Year", ["All Years", 2020, 2021, 2022, 2023, 2024], key="gw_year")
        
        col1, col2 = st.columns(2)
        with col1:
            min_depth = st.number_input("Min Depth (m)", 0, 100, 0, key="min_depth")
        with col2:
            max_depth = st.number_input("Max Depth (m)", 0, 100, 100, key="max_depth")
        
        stress_level = st.selectbox("Stress Level", ["All Levels", "Low", "Moderate", "High", "Critical"], key="stress")
        
        col1, col2 = st.columns(2)
        with col1:
            min_extraction = st.number_input("Min Extraction %", 0, 100, 0, key="min_ext")
        with col2:
            max_extraction = st.number_input("Max Extraction %", 0, 100, 100, key="max_ext")
        
        col1, col2 = st.columns(2)
        with col1:
            min_recharge = st.number_input("Min Recharge (MCM)", 0, 1000, 0, key="min_rech")
        with col2:
            max_recharge = st.number_input("Max Recharge (MCM)", 0, 1000, 1000, key="max_rech")
    
    # ========== SECTION 4: WATER QUALITY FILTERS ==========
    with st.expander("💧 WATER QUALITY FILTERS", expanded=True):
        wq_state = st.selectbox("WQ State", ["All States", "State A", "State B", "State C", "State D"], key="wq_state")
        wq_district = st.selectbox("WQ District", ["All Districts", "North", "South", "East", "West", "Central"], key="wq_dist")
        
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
            min_turbidity = st.number_input("Min Turbidity (NTU)", 0, 100, 0, key="min_turb")
        with col2:
            max_turbidity = st.number_input("Max Turbidity (NTU)", 0, 100, 100, key="max_turb")
        
        station_status = st.selectbox("Station Status", ["All Status", "Active", "Maintenance", "Inactive"], key="status")
    
    # ========== SECTION 5: ADVANCED ANALYTICS FILTERS ==========
    with st.expander("📊 ADVANCED ANALYTICS FILTERS", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            start_year = st.selectbox("Start Year", [2020, 2021, 2022, 2023, 2024], index=0, key="start_year")
        with col2:
            end_year = st.selectbox("End Year", [2020, 2021, 2022, 2023, 2024], index=4, key="end_year")
        
        alert_sensitivity = st.select_slider("Alert Sensitivity", options=["Low", "Medium", "High", "Critical"], value="Medium", key="alert_sens")
    
    # ========== SECTION 6: MAP SETTINGS ==========
    with st.expander("🗺️ MAP SETTINGS", expanded=True):
        show_heatmap = st.checkbox("Show Heatmap", False, key="heatmap")
        show_clusters = st.checkbox("Show Marker Clusters", True, key="clusters")
        marker_size = st.slider("Marker Size", 5, 20, 10, key="marker_size")
        map_zoom = st.slider("Map Zoom Level", 4, 12, 6, key="map_zoom")

# ─────────────────────────────────────────────────────────────────────────────
# AUTO-EXECUTE SQL QUERIES BASED ON FILTERS
# ─────────────────────────────────────────────────────────────────────────────

# Execute Water Sources Query
sources_query, sources_params = get_water_sources_query(
    state_filter, district_filter, source_type_filter, min_capacity, max_capacity, risk_filter
)
st.session_state.sources_data = execute_query(sources_query, sources_params if sources_params else None)

# Execute Rainfall Query
rainfall_query, rainfall_params = get_rainfall_query(
    rain_district, rain_year if rain_year != "All Years" else None, 
    rain_season if rain_season != "All Seasons" else None,
    min_rainfall, max_rainfall, rain_category if rain_category != "All Categories" else None
)
st.session_state.rainfall_data = execute_query(rainfall_query, rainfall_params if rainfall_params else None)

# Execute Groundwater Query
groundwater_query, gw_params = get_groundwater_query(
    gw_district, gw_year if gw_year != "All Years" else None,
    min_depth, max_depth, stress_level if stress_level != "All Levels" else None,
    min_extraction, max_extraction, min_recharge, max_recharge
)
st.session_state.groundwater_data = execute_query(groundwater_query, gw_params if gw_params else None)

# Execute Water Quality Query
wq_query, wq_params = get_water_quality_query(
    wq_state, wq_district, min_ph, max_ph, min_do, max_do, min_turbidity, max_turbidity,
    station_status if station_status != "All Status" else None
)
st.session_state.water_quality_data = execute_query(wq_query, wq_params if wq_params else None)

# Execute Trend Queries for Analytics
rain_trend_query, rain_trend_params = get_rainfall_trend_query(rain_district, start_year, end_year)
st.session_state.rain_trend = execute_query(rain_trend_query, rain_trend_params if rain_trend_params else None)

gw_trend_query, gw_trend_params = get_groundwater_trend_query(gw_district, start_year, end_year)
st.session_state.gw_trend = execute_query(gw_trend_query, gw_trend_params if gw_trend_params else None)

# Execute Alert Query
alert_query, alert_params = get_alert_query(alert_sensitivity)
st.session_state.alerts = execute_query(alert_query, alert_params if alert_params else None)

# ─────────────────────────────────────────────────────────────────────────────
# MAIN CONTENT - HEADER & KPIs
# ─────────────────────────────────────────────────────────────────────────────
current_time = datetime.now(pytz.timezone("Asia/Kolkata"))

st.markdown(f"<h1 style='color:#00e5ff'>💧 AQUASTAT</h1>", unsafe_allow_html=True)
st.markdown(f"<p>National Water Command Center • Live Intelligence • {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}</p>", unsafe_allow_html=True)

# KPIs
sources_df = st.session_state.sources_data
rainfall_df = st.session_state.rainfall_data
gw_df = st.session_state.groundwater_data

total_sources = len(sources_df) if sources_df is not None else 0
avg_capacity = sources_df['capacity_percent'].mean() if sources_df is not None and not sources_df.empty else 0
critical_sources = len(sources_df[sources_df['capacity_percent'] < 30]) if sources_df is not None and not sources_df.empty else 0
total_rainfall = rainfall_df['rainfall_cm'].sum() if rainfall_df is not None and not rainfall_df.empty else 0
avg_gw_depth = gw_df['avg_depth_meters'].mean() if gw_df is not None and not gw_df.empty else 0

col1, col2, col3, col4, col5 = st.columns(5)
col1.markdown(f"<div class='kpi-card'><div class='kpi-value'>{total_sources}</div><div>Total Sources</div></div>", unsafe_allow_html=True)
col2.markdown(f"<div class='kpi-card'><div class='kpi-value'>{avg_capacity:.1f}%</div><div>Avg Capacity</div></div>", unsafe_allow_html=True)
col3.markdown(f"<div class='kpi-card'><div class='kpi-value'>{critical_sources}</div><div>Critical Sources</div></div>", unsafe_allow_html=True)
col4.markdown(f"<div class='kpi-card'><div class='kpi-value'>{total_rainfall:.0f} cm</div><div>Total Rainfall</div></div>", unsafe_allow_html=True)
col5.markdown(f"<div class='kpi-card'><div class='kpi-value'>{avg_gw_depth:.1f} m</div><div>Avg GW Depth</div></div>", unsafe_allow_html=True)

st.markdown("---")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN TABS
# ─────────────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Dashboard", "🗺️ Map View", "📈 Analytics", "💧 Water Quality", "⚠️ Alerts"])

# ========== TAB 1: DASHBOARD ==========
with tab1:
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🏭 Water Sources by Type")
        if sources_df is not None and not sources_df.empty and 'source_type' in sources_df.columns:
            type_counts = sources_df['source_type'].value_counts()
            if not type_counts.empty:
                fig = px.pie(values=type_counts.values, names=type_counts.index, title="Source Distribution")
                fig.update_layout(bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                fig.update_layout(font=dict(color='#cfe4f7'))
                st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("### 📈 Capacity Distribution")
        if sources_df is not None and not sources_df.empty and 'capacity_percent' in sources_df.columns:
            fig = px.histogram(sources_df, x='capacity_percent', nbins=20, title="Capacity Distribution")
            fig.add_vline(x=30, line_dash="dash", line_color="red")
            fig.add_vline(x=60, line_dash="dash", line_color="orange")
            fig.update_layout(bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            fig.update_layout(font=dict(color='#cfe4f7'))
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### ⚠️ Risk Assessment")
        if sources_df is not None and not sources_df.empty and 'risk_level' in sources_df.columns:
            risk_counts = sources_df['risk_level'].value_counts()
            if not risk_counts.empty:
                colors = {'Critical': '#ff4444', 'Moderate': '#ffd700', 'Good': '#00ff9d'}
                fig = px.bar(x=risk_counts.index, y=risk_counts.values, title="Risk Distribution", color=risk_counts.index, color_discrete_map=colors)
                fig.update_layout(bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                fig.update_layout(font=dict(color='#cfe4f7'))
                st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("### 🏆 Top Sources by Capacity")
        if sources_df is not None and not sources_df.empty and 'capacity_percent' in sources_df.columns:
            top_sources = sources_df.nlargest(5, 'capacity_percent')[['source_name', 'capacity_percent']]
            if not top_sources.empty:
                fig = px.bar(top_sources, x='capacity_percent', y='source_name', orientation='h', title="Top 5 Sources")
                fig.update_layout(bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                fig.update_layout(font=dict(color='#cfe4f7'))
                st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("### 📋 Filtered Data")
    if sources_df is not None and not sources_df.empty:
        st.dataframe(sources_df, use_container_width=True)

# ========== TAB 2: MAP VIEW ==========
with tab2:
    st.subheader("🗺️ Interactive Water Resources Map")
    
    if sources_df is not None and not sources_df.empty and 'latitude' in sources_df.columns and 'longitude' in sources_df.columns:
        map_data = sources_df[sources_df['latitude'].notna() & sources_df['longitude'].notna()]
        
        if not map_data.empty:
            center_lat = map_data['latitude'].mean()
            center_lon = map_data['longitude'].mean()
            m = folium.Map(location=[center_lat, center_lon], zoom_start=map_zoom, tiles="CartoDB dark_matter")
            
            if show_heatmap:
                heat_data = [[row['latitude'], row['longitude'], row.get('capacity_percent', 50)] for _, row in map_data.iterrows()]
                HeatMap(heat_data).add_to(m)
            
            if show_clusters:
                marker_cluster = MarkerCluster().add_to(m)
                for _, row in map_data.iterrows():
                    capacity = row.get('capacity_percent', 100)
                    color = 'red' if capacity < 30 else 'orange' if capacity < 60 else 'green'
                    popup = f"<b>{row.get('source_name', 'Unknown')}</b><br>Capacity: {capacity:.1f}%"
                    folium.CircleMarker([row['latitude'], row['longitude']], radius=marker_size, color=color, fill=True, popup=popup).add_to(marker_cluster)
            else:
                for _, row in map_data.iterrows():
                    capacity = row.get('capacity_percent', 100)
                    color = 'red' if capacity < 30 else 'orange' if capacity < 60 else 'green'
                    folium.CircleMarker([row['latitude'], row['longitude']], radius=marker_size, color=color, fill=True).add_to(m)
            
            st_folium(m, width=1200, height=600)
        else:
            st.warning("No map data available")
    else:
        st.warning("Latitude/Longitude data not available")

# ========== TAB 3: ANALYTICS ==========
with tab3:
    st.subheader("📈 Analytics Dashboard")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🌧️ Rainfall Trend")
        if st.session_state.rain_trend is not None and not st.session_state.rain_trend.empty:
            fig = px.line(st.session_state.rain_trend, x='record_year', y='avg_rainfall', title="Average Rainfall Over Years", markers=True)
            fig.update_layout(bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            fig.update_layout(font=dict(color='#cfe4f7'))
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("### 🌊 Groundwater Trend")
        if st.session_state.gw_trend is not None and not st.session_state.gw_trend.empty:
            fig = px.line(st.session_state.gw_trend, x='assessment_year', y='avg_depth', title="Groundwater Depth Trend", markers=True)
            fig.update_layout(bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            fig.update_layout(font=dict(color='#cfe4f7'))
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### 💧 Seasonal Rainfall Pattern")
        if rainfall_df is not None and not rainfall_df.empty and 'season' in rainfall_df.columns:
            seasonal = rainfall_df.groupby('season')['rainfall_cm'].mean().reset_index()
            if not seasonal.empty:
                fig = px.bar(seasonal, x='season', y='rainfall_cm', title="Average Rainfall by Season")
                fig.update_layout(bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                fig.update_layout(font=dict(color='#cfe4f7'))
                st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("### 🔄 Extraction vs Recharge")
        if gw_df is not None and not gw_df.empty and 'district_name' in gw_df.columns:
            top_gw = gw_df.head(10)
            if not top_gw.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(name='Extraction %', x=top_gw['district_name'], y=top_gw['extraction_pct'], marker_color='#ff4444'))
                fig.add_trace(go.Bar(name='Recharge Rate (scaled)', x=top_gw['district_name'], y=top_gw['recharge_rate_mcm']/5, marker_color='#00ff9d'))
                fig.update_layout(title="Extraction vs Recharge by District", barmode='group')
                fig.update_layout(bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                fig.update_layout(font=dict(color='#cfe4f7'))
                st.plotly_chart(fig, use_container_width=True)

# ========== TAB 4: WATER QUALITY ==========
with tab4:
    st.subheader("💧 Water Quality Monitoring")
    
    wq_df = st.session_state.water_quality_data
    
    if wq_df is not None and not wq_df.empty:
        col1, col2, col3 = st.columns(3)
        avg_ph = wq_df['ph_level'].mean() if 'ph_level' in wq_df.columns else 0
        avg_do = wq_df['dissolved_oxygen_mg_l'].mean() if 'dissolved_oxygen_mg_l' in wq_df.columns else 0
        active = len(wq_df[wq_df['status'] == 'Active']) if 'status' in wq_df.columns else 0
        
        col1.metric("Avg pH", f"{avg_ph:.2f}", delta="Ideal: 6.5-8.5")
        col2.metric("Avg Dissolved Oxygen", f"{avg_do:.1f} mg/L", delta="Good: >5")
        col3.metric("Active Stations", active)
        
        st.markdown("### 🧪 pH Level Distribution")
        if 'ph_level' in wq_df.columns:
            fig = px.histogram(wq_df, x='ph_level', nbins=15, title="pH Distribution")
            fig.add_vline(x=6.5, line_dash="dash", line_color="green")
            fig.add_vline(x=8.5, line_dash="dash", line_color="green")
            fig.update_layout(bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            fig.update_layout(font=dict(color='#cfe4f7'))
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("### 📋 Water Quality Data")
        st.dataframe(wq_df, use_container_width=True)
    else:
        st.info("No water quality data available")

# ========== TAB 5: ALERTS ==========
with tab5:
    st.subheader("⚠️ Active Alerts")
    
    alerts_df = st.session_state.alerts
    
    if alerts_df is not None and not alerts_df.empty:
        critical = len(alerts_df[alerts_df['alert_level'] == 'CRITICAL_ALERT']) if 'alert_level' in alerts_df.columns else 0
        warning = len(alerts_df[alerts_df['alert_level'] == 'WARNING']) if 'alert_level' in alerts_df.columns else 0
        
        col1, col2 = st.columns(2)
        col1.markdown(f"<div class='badge-critical'>🔴 CRITICAL: {critical}</div>", unsafe_allow_html=True)
        col2.markdown(f"<div class='badge-warning'>🟡 WARNING: {warning}</div>", unsafe_allow_html=True)
        
        st.markdown("### 📋 Alert Details")
        st.dataframe(alerts_df, use_container_width=True)
    else:
        st.success("✅ No active alerts at this time")

# ─────────────────────────────────────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(f"<p style='text-align:center'>AQUASTAT v5.0 • Automatic SQL Filtering • {current_time.strftime('%Y-%m-%d %H:%M IST')}</p>", unsafe_allow_html=True)
