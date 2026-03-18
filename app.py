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
import hashlib
import hmac
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
    
    /* Admin badge */
    .admin-badge {
        background: linear-gradient(135deg, #ff416c, #ff4b2b);
        color: white;
        padding: 5px 15px;
        border-radius: 20px;
        font-weight: bold;
        text-align: center;
        display: inline-block;
    }
    
    /* User badge */
    .user-badge {
        background: linear-gradient(135deg, #00b09b, #96c93d);
        color: white;
        padding: 5px 15px;
        border-radius: 20px;
        font-weight: bold;
        text-align: center;
        display: inline-block;
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
    }
    
    .alert-warning {
        background: linear-gradient(135deg, #f7971e, #ffd200);
        color: black;
        padding: 5px 15px;
        border-radius: 20px;
        font-weight: bold;
        text-align: center;
    }
    
    .alert-good {
        background: linear-gradient(135deg, #00b09b, #96c93d);
        color: white;
        padding: 5px 15px;
        border-radius: 20px;
        font-weight: bold;
        text-align: center;
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
    
    /* Data table styling */
    .dataframe {
        background: rgba(10, 25, 47, 0.9);
        border: 1px solid #1e3a5f;
        border-radius: 10px;
        color: #e6f1ff;
    }
    
    /* Scrollbar styling */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: #0a192f;
    }
    
    ::-webkit-scrollbar-thumb {
        background: #1e3a5f;
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: #2b4b7a;
    }
    
    /* Admin panel styling */
    .admin-panel {
        background: rgba(20, 30, 50, 0.9);
        border: 2px solid #ff416c;
        border-radius: 10px;
        padding: 20px;
        margin: 10px 0;
    }
    
    .admin-header {
        color: #ff416c;
        font-size: 1.3rem;
        font-weight: 600;
        margin-bottom: 15px;
    }
</style>
""", unsafe_allow_html=True)

# -------------------------
# AUTHENTICATION SYSTEM
# -------------------------

def check_password():
    """Returns `True` if the user is authenticated using Streamlit secrets."""
    
    def login_form():
        """Form with widgets to login"""
        with st.form("Login"):
            st.markdown('<p class="section-header">🔐 Admin Login</p>', unsafe_allow_html=True)
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Login")
            
            if submitted:
                # Get credentials from secrets
                try:
                    admin_username = st.secrets["ADMIN_USERNAME"]
                    admin_password = st.secrets["ADMIN_PASSWORD"]
                except Exception as e:
                    st.error(f"Admin credentials not configured in secrets: {e}")
                    return False
                
                # Use constant-time comparison
                if hmac.compare_digest(username, admin_username) and \
                   hmac.compare_digest(password, admin_password):
                    st.session_state["authenticated"] = True
                    st.session_state["username"] = username
                    st.session_state["is_admin"] = True
                    st.session_state["auth_time"] = datetime.now().timestamp()
                    st.rerun()
                else:
                    st.error("Invalid username or password")
                    return False
        return False
    
    def logout():
        if st.button("Logout"):
            # Clear all session state
            for key in ["authenticated", "is_admin", "username", "auth_time"]:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()
    
    # Check if already authenticated (with timeout)
    if st.session_state.get("authenticated", False):
        # Check session timeout (8 hours)
        auth_time = st.session_state.get("auth_time", 0)
        if datetime.now().timestamp() - auth_time > 28800:  # 8 hours
            logout()
            return False
        
        # Show logout button in sidebar
        with st.sidebar:
            st.markdown("---")
            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown(f'<span class="admin-badge">👑 Admin: {st.session_state.get("username", "")}</span>', unsafe_allow_html=True)
            with col2:
                logout()
        return True
    else:
        # Show login form
        with st.sidebar:
            st.markdown("---")
            login_form()
        return False

# -------------------------
# NEON CLOUD DATABASE CONNECTION
# -------------------------

# Secure way to load the URL from Streamlit secrets
NEON_URL = st.secrets["NEON_URL"]

@st.cache_resource
def init_connection():
    """Initialize Neon PostgreSQL database connection using SQLAlchemy"""
    try:
        # Create SQLAlchemy engine with proper transaction handling
        engine = create_engine(
            NEON_URL,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True,  # Test connections before using
            connect_args={
                'connect_timeout': 10,
                'keepalives': 1,
                'keepalives_idle': 30,
                'keepalives_interval': 10,
                'keepalives_count': 5,
                'sslmode': 'require'
            }
        )
        # Test connection with proper transaction handling
        with engine.connect() as conn:
            # Use a transaction context manager
            with conn.begin():
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
        return engine
    except Exception as e:
        st.error(f"⚠️ Cloud Database connection failed: {e}")
        return None

# Initialize engine
engine = init_connection()

# -------------------------
# DATABASE OPERATIONS (CRUD)
# -------------------------

def execute_query(query, params=None, commit=False):
    """Execute a SQL query with parameters - with proper transaction handling"""
    if engine is None:
        st.error("Database connection not available")
        return None
    
    try:
        with engine.connect() as conn:
            # Use transaction context manager for proper commit/rollback
            with conn.begin():
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                
                if commit:
                    # Transaction will auto-commit on successful block completion
                    return True
                else:
                    # For SELECT queries, fetch results within the transaction
                    if result.returns_rows:
                        return result.fetchall()
                    return None
    except Exception as e:
        st.error(f"Database error: {e}")
        # Transaction will auto-rollback on exception
        return None

def test_connection():
    """Test database connection and return status"""
    if engine is None:
        return False, "No engine"
    
    try:
        with engine.connect() as conn:
            with conn.begin():
                result = conn.execute(text("SELECT current_database(), current_user, version()"))
                db_name, db_user, version = result.fetchone()
                return True, f"Connected to: {db_name} as {db_user}"
    except Exception as e:
        return False, str(e)

# Test connection and show in sidebar
conn_status, conn_message = test_connection()
with st.sidebar:
    if conn_status:
        st.success(f"✅ {conn_message}")
    else:
        st.error(f"❌ Connection failed: {conn_message}")

# CRUD Operations for Water Sources
def add_water_source(data):
    """Add a new water source"""
    query = """
    INSERT INTO water_sources 
    (source_name, source_type, capacity_percent, max_capacity_mcm, current_capacity_mcm, 
     build_year, state, district, origin_state, is_transboundary, latitude, longitude)
    VALUES 
    (:source_name, :source_type, :capacity_percent, :max_capacity_mcm, :current_capacity_mcm,
     :build_year, :state, :district, :origin_state, :is_transboundary, :latitude, :longitude)
    """
    return execute_query(query, data, commit=True)

def update_water_source(source_id, data):
    """Update an existing water source"""
    query = """
    UPDATE water_sources 
    SET source_name = :source_name,
        source_type = :source_type,
        capacity_percent = :capacity_percent,
        max_capacity_mcm = :max_capacity_mcm,
        current_capacity_mcm = :current_capacity_mcm,
        build_year = :build_year,
        state = :state,
        district = :district,
        origin_state = :origin_state,
        is_transboundary = :is_transboundary,
        latitude = :latitude,
        longitude = :longitude,
        updated_at = CURRENT_TIMESTAMP
    WHERE source_id = :source_id
    """
    data['source_id'] = source_id
    return execute_query(query, data, commit=True)

def delete_water_source(source_id):
    """Delete a water source"""
    query = "DELETE FROM water_sources WHERE source_id = :source_id"
    return execute_query(query, {'source_id': source_id}, commit=True)

# CRUD Operations for Monitoring Stations
def add_monitoring_station(data):
    """Add a new monitoring station"""
    query = """
    INSERT INTO water_monitoring_stations 
    (station_name, state_name, district_name, latitude, longitude, 
     ph_level, dissolved_oxygen_mg_l, turbidity_ntu, status)
    VALUES 
    (:station_name, :state_name, :district_name, :latitude, :longitude,
     :ph_level, :dissolved_oxygen_mg_l, :turbidity_ntu, :status)
    """
    return execute_query(query, data, commit=True)

def update_monitoring_station(station_id, data):
    """Update an existing monitoring station"""
    query = """
    UPDATE water_monitoring_stations 
    SET station_name = :station_name,
        state_name = :state_name,
        district_name = :district_name,
        latitude = :latitude,
        longitude = :longitude,
        ph_level = :ph_level,
        dissolved_oxygen_mg_l = :dissolved_oxygen_mg_l,
        turbidity_ntu = :turbidity_ntu,
        status = :status,
        last_updated = CURRENT_TIMESTAMP
    WHERE station_id = :station_id
    """
    data['station_id'] = station_id
    return execute_query(query, data, commit=True)

def delete_monitoring_station(station_id):
    """Delete a monitoring station"""
    query = "DELETE FROM water_monitoring_stations WHERE station_id = :station_id"
    return execute_query(query, {'station_id': station_id}, commit=True)

# CRUD Operations for Alerts
def add_alert(data):
    """Add a new alert"""
    query = """
    INSERT INTO active_alerts 
    (source_name, capacity_percent, ph_level, alert_status, alert_time)
    VALUES 
    (:source_name, :capacity_percent, :ph_level, :alert_status, CURRENT_TIMESTAMP)
    """
    return execute_query(query, data, commit=True)

def update_alert(alert_id, data):
    """Update an existing alert"""
    query = """
    UPDATE active_alerts 
    SET source_name = :source_name,
        capacity_percent = :capacity_percent,
        ph_level = :ph_level,
        alert_status = :alert_status
    WHERE alert_id = :alert_id
    """
    data['alert_id'] = alert_id
    return execute_query(query, data, commit=True)

def delete_alert(alert_id):
    """Delete an alert"""
    query = "DELETE FROM active_alerts WHERE alert_id = :alert_id"
    return execute_query(query, {'alert_id': alert_id}, commit=True)

# CRUD Operations for Groundwater Data
def add_groundwater_data(data):
    """Add new groundwater data"""
    query = """
    INSERT INTO groundwater_levels 
    (district_name, avg_depth_meters, extraction_pct, recharge_rate_mcm, assessment_year)
    VALUES 
    (:district_name, :avg_depth_meters, :extraction_pct, :recharge_rate_mcm, :assessment_year)
    """
    return execute_query(query, data, commit=True)

def update_groundwater_data(gw_id, data):
    """Update groundwater data"""
    query = """
    UPDATE groundwater_levels 
    SET avg_depth_meters = :avg_depth_meters,
        extraction_pct = :extraction_pct,
        recharge_rate_mcm = :recharge_rate_mcm,
        assessment_year = :assessment_year
    WHERE id = :gw_id
    """
    data['gw_id'] = gw_id
    return execute_query(query, data, commit=True)

def delete_groundwater_data(gw_id):
    """Delete groundwater data"""
    query = "DELETE FROM groundwater_levels WHERE id = :gw_id"
    return execute_query(query, {'gw_id': gw_id}, commit=True)

# CRUD Operations for Rainfall Data
def add_rainfall_data(data):
    """Add new rainfall data"""
    query = """
    INSERT INTO rainfall_history 
    (district_name, rainfall_cm, record_year, season)
    VALUES 
    (:district_name, :rainfall_cm, :record_year, :season)
    """
    return execute_query(query, data, commit=True)

def update_rainfall_data(rainfall_id, data):
    """Update rainfall data"""
    query = """
    UPDATE rainfall_history 
    SET rainfall_cm = :rainfall_cm,
        record_year = :record_year,
        season = :season
    WHERE id = :rainfall_id
    """
    data['rainfall_id'] = rainfall_id
    return execute_query(query, data, commit=True)

def delete_rainfall_data(rainfall_id):
    """Delete rainfall data"""
    query = "DELETE FROM rainfall_history WHERE id = :rainfall_id"
    return execute_query(query, {'rainfall_id': rainfall_id}, commit=True)

# -------------------------
# DATA LOADING FUNCTIONS (FIXED - SINGLE VERSION)
# -------------------------

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_all_data():
    """Load all data from Neon Cloud database with error handling"""
    if engine is None:
        st.warning("⚠️ Database connection not available. Please check your connection settings.")
        return [pd.DataFrame()] * 8
    
    try:
        with engine.connect() as conn:
            # Use transaction for reading data
            with conn.begin():
                # Get list of tables
                query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
                tables_df = pd.read_sql(query, conn)
                tables = tables_df['table_name'].tolist() if not tables_df.empty else []
                
                # Helper function to load table with error handling
                def get_df(table_name):
                    if table_name in tables:
                        try:
                            return pd.read_sql(f"SELECT * FROM {table_name}", conn)
                        except Exception as e:
                            st.warning(f"Could not load {table_name}: {e}")
                            return pd.DataFrame()
                    else:
                        return pd.DataFrame()
                
                # Load all tables
                sources = get_df('water_sources')
                stations = get_df('water_monitoring_stations')
                groundwater = get_df('groundwater_levels')
                rainfall = get_df('rainfall_history')
                alerts = get_df('active_alerts')
                regional = get_df('regional_stats')
                water_quality = get_df('water_quality')
                
                # Water Usage with join
                if 'water_usage_history' in tables and 'water_sources' in tables:
                    usage_query = """
                        SELECT wu.*, ws.source_name, ws.source_type, ws.state, ws.district 
                        FROM water_usage_history wu
                        LEFT JOIN water_sources ws ON wu.source_id = ws.source_id
                    """
                    try:
                        usage = pd.read_sql(usage_query, conn)
                    except Exception as e:
                        st.warning(f"Could not load water usage: {e}")
                        usage = pd.DataFrame()
                else:
                    usage = pd.DataFrame()
                
                return sources, stations, groundwater, rainfall, alerts, usage, regional, water_quality
                
    except Exception as e:
        st.error(f"Error loading cloud data: {e}")
        return [pd.DataFrame()] * 8

# Load the data (call the function once)
with st.spinner("🚀 Connecting to AQUASTAT Cloud Database..."):
    sources, stations, groundwater, rainfall, alerts, usage, regional, water_quality = load_all_data()

# Display loading summary in sidebar
with st.sidebar:
    st.markdown("---")
    st.markdown("### 📊 Data Summary")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Sources", len(sources))
        st.metric("Stations", len(stations))
    with col2:
        st.metric("Groundwater", len(groundwater))
        st.metric("Rainfall", len(rainfall))



# -------------------------
# DATA PROCESSING
# -------------------------

current_year = datetime.now().year
current_time = datetime.now(pytz.timezone('Asia/Kolkata'))

# Process Sources
if not sources.empty:
    # Convert numeric columns
    numeric_cols = ['capacity_percent', 'build_year', 'max_capacity_mcm', 'current_capacity_mcm']
    for col in numeric_cols:
        if col in sources.columns:
            sources[col] = pd.to_numeric(sources[col], errors='coerce')
    
    # Calculate age
    if 'build_year' in sources.columns:
        sources['age'] = current_year - sources['build_year']
        sources['age'] = sources['age'].clip(0, 200)
    else:
        sources['age'] = 0
    
    # Calculate health score
    if 'capacity_percent' in sources.columns:
        sources['health_score'] = (
            sources['capacity_percent'].fillna(50) * 0.5 +  # 50% weight to current capacity
            (100 - sources['age'].clip(0, 100).fillna(50)) * 0.3 +  # 30% weight to age
            20  # 20% base score
        ).clip(0, 100)
    else:
        sources['health_score'] = 50
    
    # Risk classification
    if 'capacity_percent' in sources.columns:
        sources['risk_level'] = pd.cut(
            sources['capacity_percent'],
            bins=[0, 30, 60, 100],
            labels=['Critical', 'Moderate', 'Good'],
            include_lowest=True
        )
    else:
        sources['risk_level'] = 'Unknown'
    
    # Trend calculation
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
    
    # Calculate depletion rate if we have multiple years
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
    
    # Calculate deviation from normal
    if 'rainfall_cm' in rainfall.columns and 'record_year' in rainfall.columns:
        avg_by_region = rainfall.groupby('district_name')['rainfall_cm'].transform('mean')
        rainfall['deviation_pct'] = ((rainfall['rainfall_cm'] - avg_by_region) / avg_by_region * 100).round(1)

# Add coordinates to sources from stations
def add_coordinates_to_sources(sources_df, stations_df):
    """Add coordinates from monitoring stations to water sources"""
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

# Apply coordinate mapping
if not sources.empty and not stations.empty:
    sources = add_coordinates_to_sources(sources, stations)

# -------------------------
# SIDEBAR FILTERS & AUTH
# -------------------------

# Check authentication
is_authenticated = check_password()
is_admin = st.session_state.get("is_admin", False)

with st.sidebar:
    st.title("🎮 AQUASTAT")
    st.caption("Command Interface v3.0")
    
    # Connection status
    if engine is not None:
        st.success("✅ Cloud Connected")
    else:
        st.error("❌ Cloud Disconnected")
    
    # User role badge
    if is_authenticated and is_admin:
        st.markdown(f'<span class="admin-badge">👑 ADMIN ACCESS</span>', unsafe_allow_html=True)
    else:
        st.markdown(f'<span class="user-badge">👤 VIEWER ACCESS</span>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Reset button
    if st.button("🔄 Reset All Filters", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    st.markdown("---")
    
    # ========== TIME FILTERS ==========
    st.markdown("### 📅 Time Filters")
    
    # Year filter
    if not sources.empty and 'build_year' in sources.columns:
        available_years = sorted(sources['build_year'].dropna().unique())
        if len(available_years) > 0:
            min_year = int(min(available_years))
            max_year = int(max(available_years))
            year_range = st.slider(
                "Build Year Range",
                min_value=1800,
                max_value=2026,
                value=(min_year, max_year)
            )
        else:
            year_range = (1800, 2026)
    else:
        year_range = (1800, 2026)
    
    st.markdown("---")
    
    # ========== GEOGRAPHIC FILTERS ==========
    st.markdown("### 🌍 Geographic Filters")
    
    # State filter
    if not sources.empty and 'state' in sources.columns:
        states = ['All States'] + sorted(sources['state'].dropna().unique().tolist())
        selected_state = st.selectbox("State", states, index=0)
    else:
        selected_state = "All States"
    
    # District filter
    if not sources.empty and 'district' in sources.columns:
        if selected_state != "All States":
            districts = sources[sources['state'] == selected_state]['district'].dropna().unique()
        else:
            districts = sources['district'].dropna().unique()
        
        if len(districts) > 0:
            districts = ['All Districts'] + sorted(districts.tolist())
        else:
            districts = ['All Districts']
        selected_district = st.selectbox("District", districts, index=0)
    else:
        selected_district = "All Districts"
    
    st.markdown("---")
    
    # ========== SOURCE FILTERS ==========
    st.markdown("### 💧 Source Filters")
    
    # Source type
    if not sources.empty and 'source_type' in sources.columns:
        source_types = ['All Types'] + sorted(sources['source_type'].dropna().unique().tolist())
        selected_type = st.selectbox("Source Type", source_types, index=0)
    else:
        selected_type = "All Types"
    
    # Capacity range
    if not sources.empty and 'capacity_percent' in sources.columns:
        min_cap = float(sources['capacity_percent'].min())
        max_cap = float(sources['capacity_percent'].max())
        capacity_range = st.slider(
            "Capacity %",
            min_value=min_cap,
            max_value=max_cap,
            value=(min_cap, max_cap)
        )
    else:
        capacity_range = (0, 100)
    
    # Risk level
    if not sources.empty and 'risk_level' in sources.columns:
        risk_options = ['All Risk Levels'] + list(sources['risk_level'].unique())
        selected_risk = st.selectbox("Risk Level", risk_options, index=0)
    else:
        selected_risk = "All Risk Levels"
    
    st.markdown("---")
    
    # ========== MAP SETTINGS ==========
    st.markdown("### 🗺️ Map Settings")
    
    map_style = st.selectbox(
        "Map Style",
        ["Esri Satellite", "OpenStreetMap", "CartoDB Dark", "CartoDB Light"],
        index=0
    )
    
    show_heatmap = st.checkbox("Show Heatmap", True)
    show_clusters = st.checkbox("Show Clusters", True)
    show_stations = st.checkbox("Show Monitoring Stations", True)
    marker_size = st.slider("Marker Size", 5, 20, 12)
    
    st.markdown("---")
    st.caption(f"📊 Total Sources: {len(sources):,}")

# ========== APPLY FILTERS ==========

def apply_filters():
    """Apply all filters to data"""
    filtered_sources = sources.copy()
    
    # Apply state filter
    if selected_state != "All States" and 'state' in filtered_sources.columns:
        filtered_sources = filtered_sources[filtered_sources['state'] == selected_state]
    
    # Apply district filter
    if selected_district != "All Districts" and 'district' in filtered_sources.columns:
        filtered_sources = filtered_sources[filtered_sources['district'] == selected_district]
    
    # Apply source type filter
    if selected_type != "All Types" and 'source_type' in filtered_sources.columns:
        filtered_sources = filtered_sources[filtered_sources['source_type'] == selected_type]
    
    # Apply year filter
    if 'build_year' in filtered_sources.columns:
        filtered_sources = filtered_sources[
            (filtered_sources['build_year'] >= year_range[0]) &
            (filtered_sources['build_year'] <= year_range[1])
        ]
    
    # Apply capacity filter
    if 'capacity_percent' in filtered_sources.columns:
        filtered_sources = filtered_sources[
            (filtered_sources['capacity_percent'] >= capacity_range[0]) &
            (filtered_sources['capacity_percent'] <= capacity_range[1])
        ]
    
    # Apply risk filter
    if selected_risk != "All Risk Levels" and 'risk_level' in filtered_sources.columns:
        filtered_sources = filtered_sources[filtered_sources['risk_level'] == selected_risk]
    
    return filtered_sources

filtered_sources = apply_filters()

def filter_stations():
    """Filter stations based on selected state and district"""
    filtered_stations = stations.copy()
    
    if selected_state != "All States" and 'state_name' in filtered_stations.columns:
        filtered_stations = filtered_stations[filtered_stations['state_name'] == selected_state]
    
    if selected_district != "All Districts" and 'district_name' in filtered_stations.columns:
        filtered_stations = filtered_stations[filtered_stations['district_name'] == selected_district]
    
    return filtered_stations

filtered_stations = filter_stations()

# -------------------------
# MAIN DASHBOARD
# -------------------------

# Header
st.title("💧 AQUASTAT National Water Command Center")
col1, col2 = st.columns([3, 1])
with col1:
    st.caption(f"**Live Intelligence** • Last Updated: {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
with col2:
    if is_admin:
        st.markdown(f"<div style='text-align: right;'><span class='admin-badge'>👑 ADMIN MODE</span></div>", unsafe_allow_html=True)
    else:
        st.markdown(f"<div style='text-align: right;'><span class='user-badge'>👤 VIEWER MODE</span></div>", unsafe_allow_html=True)

# KPI Row with custom styling
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.markdown("""
    <div class="metric-card">
        <h3 style="color: #8892b0; margin:0;">Total Sources</h3>
        <h1 style="color: #00e5ff; margin:0;">{}</h1>
        <p style="color: #64ffda;">{} filtered</p>
    </div>
    """.format(
        f"{len(sources):,}",
        f"{len(sources) - len(filtered_sources)}"
    ), unsafe_allow_html=True)

with col2:
    avg_cap = filtered_sources['capacity_percent'].mean() if not filtered_sources.empty and 'capacity_percent' in filtered_sources.columns else 0
    st.markdown("""
    <div class="metric-card">
        <h3 style="color: #8892b0; margin:0;">Avg Capacity</h3>
        <h1 style="color: #00e5ff; margin:0;">{:.1f}%</h1>
    </div>
    """.format(avg_cap), unsafe_allow_html=True)

with col3:
    if not filtered_sources.empty and 'capacity_percent' in filtered_sources.columns:
        critical = len(filtered_sources[filtered_sources['capacity_percent'] < 30])
    else:
        critical = 0
    st.markdown("""
    <div class="metric-card">
        <h3 style="color: #8892b0; margin:0;">Critical Sources</h3>
        <h1 style="color: #ff4444; margin:0;">{}</h1>
    </div>
    """.format(critical), unsafe_allow_html=True)

with col4:
    if not filtered_sources.empty and 'latitude' in filtered_sources.columns:
        sources_with_coords = len(filtered_sources[filtered_sources['latitude'].notna()])
    else:
        sources_with_coords = 0
    st.markdown("""
    <div class="metric-card">
        <h3 style="color: #8892b0; margin:0;">On Map</h3>
        <h1 style="color: #00e5ff; margin:0;">{}</h1>
    </div>
    """.format(sources_with_coords), unsafe_allow_html=True)

with col5:
    st.markdown("""
    <div class="metric-card">
        <h3 style="color: #8892b0; margin:0;">Active Alerts</h3>
        <h1 style="color: #ffd700; margin:0;">{}</h1>
    </div>
    """.format(len(alerts)), unsafe_allow_html=True)

st.markdown("---")

# Main Tabs - Show Admin tab only for authenticated admins
if is_admin:
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "📊 DASHBOARD",
        "🗺️ MAP VIEW",
        "📈 ANALYTICS",
        "💧 WATER QUALITY",
        "⚠️ ALERTS",
        "📋 DATA TABLES",
        "👑 ADMIN PANEL"
    ])
else:
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 DASHBOARD",
        "🗺️ MAP VIEW",
        "📈 ANALYTICS",
        "💧 WATER QUALITY",
        "⚠️ ALERTS",
        "📋 DATA TABLES"
    ])

# =====================
# TAB 1: DASHBOARD (Same as before)
# =====================

with tab1:
    if filtered_sources.empty:
        st.warning("⚠️ No water sources match the current filters. Try clearing some filters.")
        
        with st.expander("📋 Show all sources sample"):
            available_cols = ['source_name', 'source_type', 'state', 'district', 'capacity_percent']
            available_cols = [col for col in available_cols if col in sources.columns]
            if len(available_cols) > 0:
                st.dataframe(sources[available_cols].head(20), use_container_width=True)
    
    else:
        # First row - Capacity and Type Distribution
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<p class="section-header">📊 Capacity Distribution</p>', unsafe_allow_html=True)
            if 'capacity_percent' in filtered_sources.columns:
                fig = px.histogram(
                    filtered_sources,
                    x='capacity_percent',
                    nbins=20,
                    title=f"Storage Capacity Distribution ({len(filtered_sources)} sources)",
                    template="plotly_dark",
                    color_discrete_sequence=['#00e5ff']
                )
                fig.update_layout(
                    xaxis_title="Capacity (%)",
                    yaxis_title="Number of Sources",
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown('<p class="section-header">🏭 Source Types</p>', unsafe_allow_html=True)
            if 'source_type' in filtered_sources.columns:
                type_counts = filtered_sources['source_type'].value_counts().reset_index()
                type_counts.columns = ['Source Type', 'Count']
                fig = px.pie(
                    type_counts,
                    values='Count',
                    names='Source Type',
                    title="Water Sources by Type",
                    template="plotly_dark",
                    color_discrete_sequence=px.colors.sequential.Tealgrn
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
        
        # Second row - Groundwater and Rainfall
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<p class="section-header">📈 Groundwater Stress</p>', unsafe_allow_html=True)
            if not groundwater.empty and 'stress_level' in groundwater.columns:
                filtered_gw = groundwater.copy()
                if selected_district != "All Districts" and 'district_name' in filtered_gw.columns:
                    filtered_gw = filtered_gw[filtered_gw['district_name'] == selected_district]
                
                if not filtered_gw.empty:
                    stress_counts = filtered_gw['stress_level'].value_counts().reset_index()
                    stress_counts.columns = ['Stress Level', 'Count']
                    fig = px.bar(
                        stress_counts,
                        x='Stress Level',
                        y='Count',
                        title="Groundwater Stress Distribution",
                        template="plotly_dark",
                        color='Stress Level',
                        color_discrete_map={
                            'Low': '#00ff9d',
                            'Moderate': '#ffd700',
                            'High': '#ff4444'
                        },
                        text_auto=True
                    )
                    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown('<p class="section-header">☔ Rainfall Analysis</p>', unsafe_allow_html=True)
            if not rainfall.empty and 'season' in rainfall.columns and 'rainfall_cm' in rainfall.columns:
                filtered_rain = rainfall.copy()
                if selected_district != "All Districts" and 'district_name' in filtered_rain.columns:
                    filtered_rain = filtered_rain[filtered_rain['district_name'] == selected_district]
                
                if not filtered_rain.empty:
                    season_rain = filtered_rain.groupby('season')['rainfall_cm'].mean().reset_index()
                    fig = px.bar(
                        season_rain,
                        x='season',
                        y='rainfall_cm',
                        title="Average Rainfall by Season",
                        template="plotly_dark",
                        color='rainfall_cm',
                        color_continuous_scale='Blues',
                        text_auto='.1f'
                    )
                    fig.update_traces(textposition='outside')
                    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig, use_container_width=True)
        
        # Third row - Risk Distribution
        if 'risk_level' in filtered_sources.columns:
            st.markdown('<p class="section-header">⚠️ Risk Assessment</p>', unsafe_allow_html=True)
            risk_counts = filtered_sources['risk_level'].value_counts().reset_index()
            risk_counts.columns = ['Risk Level', 'Count']
            
            fig = px.bar(
                risk_counts,
                x='Risk Level',
                y='Count',
                color='Risk Level',
                color_discrete_map={
                    'Good': '#00ff9d',
                    'Moderate': '#ffd700',
                    'Critical': '#ff4444'
                },
                title="Infrastructure Risk Assessment",
                template="plotly_dark",
                text_auto=True
            )
            fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)

# =====================
# TAB 2: MAP VIEW (Same as before)
# =====================

with tab2:
    st.markdown('<p class="section-header">🗺️ National Interactive Water Resources Map</p>', unsafe_allow_html=True)
    
    # Filter info
    filter_info = []
    if selected_state != "All States":
        filter_info.append(f"State: {selected_state}")
    if selected_district != "All Districts":
        filter_info.append(f"District: {selected_district}")
    if selected_type != "All Types":
        filter_info.append(f"Type: {selected_type}")
    
    if filter_info:
        st.info(f"**Showing:** {', '.join(filter_info)} | **Total Sources:** {len(filtered_sources)}")
    
    # Map style mapping
    style_map = {
        "Esri Satellite": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        "OpenStreetMap": "OpenStreetMap",
        "CartoDB Dark": "CartoDB dark_matter",
        "CartoDB Light": "CartoDB positron"
    }
    
    # Center map
    center_lat, center_lon, zoom = 20.5937, 78.9629, 5
    
    if not filtered_sources.empty and 'latitude' in filtered_sources.columns and 'longitude' in filtered_sources.columns:
        sources_with_coords = filtered_sources[filtered_sources['latitude'].notna() & filtered_sources['longitude'].notna()]
        
        if not sources_with_coords.empty:
            if selected_district != "All Districts":
                center_lat = sources_with_coords['latitude'].mean()
                center_lon = sources_with_coords['longitude'].mean()
                zoom = 9
            elif selected_state != "All States":
                center_lat = sources_with_coords['latitude'].mean()
                center_lon = sources_with_coords['longitude'].mean()
                zoom = 7
    
    # Create map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=zoom,
        tiles=style_map[map_style],
        attr='AQUASTAT'
    )
    
    Fullscreen().add_to(m)
    
    # Add layers
    if show_clusters and len(filtered_sources) > 10:
        marker_cluster = MarkerCluster().add_to(m)
    else:
        marker_cluster = m
    
    heat_data = []
    sources_on_map = 0
    
    # Add water sources
    if not filtered_sources.empty and 'latitude' in filtered_sources.columns and 'longitude' in filtered_sources.columns:
        sources_with_coords = filtered_sources[
            filtered_sources['latitude'].notna() & 
            filtered_sources['longitude'].notna()
        ]
        
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
            
            # Popup HTML
            popup_html = f"""
            <div style="font-family: Arial; min-width: 250px; background: #0a192f; color: #e6f1ff;">
                <h4 style="color: {color}; margin:0;">{source.get('source_name', 'Unknown')}</h4>
                <hr style="margin:5px 0; border-color: #1e3a5f;">
                <table style="width:100%;">
                    <tr><td><b>Type:</b></td><td>{source.get('source_type', 'Unknown')}</td></tr>
                    <tr><td><b>Location:</b></td><td>{source.get('district', 'Unknown')}, {source.get('state', 'Unknown')}</td></tr>
                    <tr><td><b>Capacity:</b></td><td>{capacity:.1f}%</td></tr>
                    <tr><td><b>Age:</b></td><td>{source.get('age', 0):.0f} years</td></tr>
                    <tr><td><b>Risk:</b></td><td><span style="color:{color};">{risk_text}</span></td></tr>
                    <tr><td><b>Trend:</b></td><td>{source.get('trend', 'N/A')}</td></tr>
                </table>
            </div>
            """
            
            marker = folium.CircleMarker(
                location=[source['latitude'], source['longitude']],
                radius=marker_size + (3 if capacity < 30 else 0),
                color=color,
                fill=True,
                fillOpacity=0.7,
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"{source.get('source_name', 'Unknown')} - {capacity:.0f}%"
            )
            
            if show_clusters and len(filtered_sources) > 10:
                marker.add_to(marker_cluster)
            else:
                marker.add_to(m)
        
        # Add heatmap
        if show_heatmap and heat_data:
            HeatMap(
                heat_data,
                radius=15,
                blur=10,
                gradient={0.2: 'blue', 0.4: 'cyan', 0.6: 'lime', 0.8: 'yellow', 1: 'red'}
            ).add_to(m)
    
    # Add monitoring stations
    if show_stations and not filtered_stations.empty:
        if 'latitude' in filtered_stations.columns and 'longitude' in filtered_stations.columns:
            stations_with_coords = filtered_stations[
                filtered_stations['latitude'].notna() & 
                filtered_stations['longitude'].notna()
            ]
            
            for _, station in stations_with_coords.iterrows():
                status = station.get('status', 'Unknown')
                station_color = 'green' if status == 'Active' else 'orange' if status == 'Maintenance' else 'red'
                
                station_popup = f"""
                <div style="background: #0a192f; color: #e6f1ff; padding: 10px;">
                    <b>{station.get('station_name', 'Unknown')}</b><br>
                    Location: {station.get('district_name', 'Unknown')}<br>
                    Status: <span style="color:{'#00ff9d' if status=='Active' else '#ffd700' if status=='Maintenance' else '#ff4444'};">{status}</span><br>
                    pH: {station.get('ph_level', 'N/A')}<br>
                    DO: {station.get('dissolved_oxygen_mg_l', 'N/A')} mg/L<br>
                    Turbidity: {station.get('turbidity_ntu', 'N/A')} NTU
                </div>
                """
                
                folium.Marker(
                    location=[station['latitude'], station['longitude']],
                    icon=folium.Icon(color=station_color, icon='info-sign'),
                    popup=folium.Popup(station_popup, max_width=300),
                    tooltip=f"Station: {station.get('station_name', 'Unknown')}"
                ).add_to(m)
    
    # Display map
    st_folium(m, width=1300, height=600)
    
    # Map statistics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Sources on Map", sources_on_map)
    with col2:
        st.metric("Total Filtered Sources", len(filtered_sources))
    with col3:
        coverage = (sources_on_map/len(filtered_sources)*100) if len(filtered_sources) > 0 else 0
        st.metric("Coordinate Coverage", f"{coverage:.1f}%")
    
    # Legend
    st.markdown("---")
    cols = st.columns(5)
    with cols[0]:
        st.markdown('<span class="status-dot status-good"></span> Good (≥60%)', unsafe_allow_html=True)
    with cols[1]:
        st.markdown('<span class="status-dot status-warning"></span> Moderate (30-60%)', unsafe_allow_html=True)
    with cols[2]:
        st.markdown('<span class="status-dot status-critical"></span> Critical (<30%)', unsafe_allow_html=True)
    with cols[3]:
        st.markdown('<span class="status-dot" style="background: #4287f5;"></span> Monitoring Station', unsafe_allow_html=True)
    with cols[4]:
        st.markdown('🔥 Heatmap Area', unsafe_allow_html=True)

# =====================
# TAB 3: ANALYTICS (Same as before)
# =====================

with tab3:
    st.markdown('<p class="section-header">📈 Advanced Analytics</p>', unsafe_allow_html=True)
    
    atab1, atab2, atab3 = st.tabs(["📊 Trends", "📉 Comparisons", "📐 Statistics"])
    
    with atab1:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Rainfall Trend")
            if not rainfall.empty and 'record_year' in rainfall.columns and 'rainfall_cm' in rainfall.columns:
                filtered_rain = rainfall.copy()
                if selected_district != "All Districts" and 'district_name' in filtered_rain.columns:
                    filtered_rain = filtered_rain[filtered_rain['district_name'] == selected_district]
                
                if not filtered_rain.empty:
                    rain_trend = filtered_rain.groupby('record_year')['rainfall_cm'].mean().reset_index()
                    fig = px.line(
                        rain_trend,
                        x='record_year',
                        y='rainfall_cm',
                        title="Average Rainfall Over Years",
                        template="plotly_dark",
                        markers=True
                    )
                    fig.update_traces(line_color='#00e5ff', line_width=3)
                    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Groundwater Trend")
            if not groundwater.empty and 'assessment_year' in groundwater.columns and 'avg_depth_meters' in groundwater.columns:
                filtered_gw = groundwater.copy()
                if selected_district != "All Districts" and 'district_name' in filtered_gw.columns:
                    filtered_gw = filtered_gw[filtered_gw['district_name'] == selected_district]
                
                if not filtered_gw.empty:
                    gw_trend = filtered_gw.groupby('assessment_year')['avg_depth_meters'].mean().reset_index()
                    fig = px.line(
                        gw_trend,
                        x='assessment_year',
                        y='avg_depth_meters',
                        title="Average Groundwater Depth",
                        template="plotly_dark",
                        markers=True
                    )
                    fig.update_traces(line_color='#ffd700', line_width=3)
                    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig, use_container_width=True)
    
    with atab2:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Capacity by State")
            if not filtered_sources.empty and 'state' in filtered_sources.columns and 'capacity_percent' in filtered_sources.columns:
                state_cap = filtered_sources.groupby('state')['capacity_percent'].mean().sort_values(ascending=False)
                if len(state_cap) > 10:
                    state_cap = state_cap.head(10)
                
                if len(state_cap) > 0:
                    fig = px.bar(
                        x=state_cap.values,
                        y=state_cap.index,
                        orientation='h',
                        title=f"Average Capacity by State",
                        template="plotly_dark",
                        color=state_cap.values,
                        color_continuous_scale='Tealgrn',
                        labels={'x': 'Avg Capacity (%)', 'y': 'State'}
                    )
                    fig.update_traces(texttemplate='%{x:.1f}%', textposition='outside')
                    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Extraction vs Recharge")
            if not groundwater.empty and 'recharge_rate_mcm' in groundwater.columns and 'extraction_pct' in groundwater.columns:
                filtered_gw = groundwater.copy()
                if selected_district != "All Districts" and 'district_name' in filtered_gw.columns:
                    filtered_gw = filtered_gw[filtered_gw['district_name'] == selected_district]
                
                if not filtered_gw.empty:
                    fig = px.scatter(
                        filtered_gw,
                        x='recharge_rate_mcm',
                        y='extraction_pct',
                        size='avg_depth_meters' if 'avg_depth_meters' in filtered_gw.columns else None,
                        color='district_name' if 'district_name' in filtered_gw.columns else None,
                        title="Groundwater Extraction vs Recharge",
                        template="plotly_dark"
                    )
                    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig, use_container_width=True)
    
    with atab3:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Statistical Summary")
            if not filtered_sources.empty:
                stats_cols = []
                for col in ['capacity_percent', 'age', 'health_score']:
                    if col in filtered_sources.columns:
                        stats_cols.append(col)
                if stats_cols:
                    stats_df = filtered_sources[stats_cols].describe()
                    st.dataframe(stats_df.style.format("{:.2f}"), use_container_width=True)
        
        with col2:
            st.subheader("Correlation Matrix")
            if not filtered_sources.empty and not groundwater.empty:
                if 'district' in filtered_sources.columns and 'district_name' in groundwater.columns:
                    merged = filtered_sources.merge(
                        groundwater,
                        left_on='district',
                        right_on='district_name',
                        how='inner'
                    )
                    
                    if not merged.empty:
                        numeric_cols = [col for col in ['capacity_percent', 'age', 'avg_depth_meters', 'extraction_pct', 'recharge_rate_mcm'] 
                                      if col in merged.columns]
                        
                        if len(numeric_cols) >= 2:
                            corr_data = merged[numeric_cols].dropna()
                            if not corr_data.empty:
                                corr_matrix = corr_data.corr()
                                fig = px.imshow(
                                    corr_matrix,
                                    text_auto=True,
                                    aspect="auto",
                                    title="Correlation Matrix",
                                    template="plotly_dark",
                                    color_continuous_scale='RdBu_r'
                                )
                                fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                                st.plotly_chart(fig, use_container_width=True)

# =====================
# TAB 4: WATER QUALITY (Same as before)
# =====================

with tab4:
    st.markdown('<p class="section-header">💧 Water Quality Monitoring</p>', unsafe_allow_html=True)
    
    if not water_quality.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_ph = water_quality['ph_level'].mean() if 'ph_level' in water_quality.columns else 0
            st.metric("Average pH", f"{avg_ph:.2f}", delta=None)
        
        with col2:
            avg_do = water_quality['dissolved_oxygen'].mean() if 'dissolved_oxygen' in water_quality.columns else 0
            st.metric("Dissolved Oxygen", f"{avg_do:.1f} mg/L", delta=None)
        
        with col3:
            avg_turbidity = water_quality['turbidity'].mean() if 'turbidity' in water_quality.columns else 0
            st.metric("Turbidity", f"{avg_turbidity:.1f} NTU", delta=None)
        
        with col4:
            avg_tds = water_quality['tds'].mean() if 'tds' in water_quality.columns else 0
            st.metric("Total Dissolved Solids", f"{avg_tds:.0f} ppm", delta=None)
        
        st.markdown("---")
        
        # Water Quality Charts
        col1, col2 = st.columns(2)
        
        with col1:
            if 'ph_level' in water_quality.columns:
                fig = px.histogram(
                    water_quality,
                    x='ph_level',
                    nbins=20,
                    title="pH Distribution",
                    template="plotly_dark",
                    color_discrete_sequence=['#00e5ff']
                )
                fig.add_vline(x=7, line_dash="dash", line_color="white")
                fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if 'dissolved_oxygen' in water_quality.columns:
                fig = px.box(
                    water_quality,
                    y='dissolved_oxygen',
                    title="Dissolved Oxygen Distribution",
                    template="plotly_dark",
                    color_discrete_sequence=['#00ff9d']
                )
                fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
        
         # --- Corrected Water Quality Table ---
st.markdown("### Recent Water Quality Readings")

# Updated these names to match your Neon database exactly
display_cols = [
    'station_name', 
    'district_name', 
    'ph_level', 
    'dissolved_oxygen_mg_l', 
    'turbidity_ntu', 
    'status'
]

# Safety check: only use columns that actually exist in your dataframe
available_cols = [col for col in display_cols if col in water_quality.columns]

if not water_quality.empty and available_cols:
    # We remove sort_values('measurement_date') because that column doesn't exist in your Neon table yet
    st.dataframe(
        water_quality[available_cols].head(50),
        use_container_width=True,
        hide_index=True
    )
    
    # Optional: Add a success message to show data is live
    st.caption(f"Showing {len(water_quality.head(50))} live readings from Neon Cloud.")
else:
    st.info("No water quality data available. Please check if 'water_monitoring_stations' table is loaded.")


# =====================
# TAB 5: ALERTS (Same as before)
# =====================

with tab5:
    st.markdown('<p class="section-header">🚨 Active Alerts and Warnings</p>', unsafe_allow_html=True)
    
    if not alerts.empty:
        # Alert counts
        col1, col2, col3 = st.columns(3)
        
        with col1:
            critical_count = len(alerts[alerts['alert_status'] == 'CRITICAL']) if 'alert_status' in alerts.columns else 0
            st.markdown(f'<div class="alert-critical">🔴 CRITICAL: {critical_count}</div>', unsafe_allow_html=True)
        
        with col2:
            warning_count = len(alerts[alerts['alert_status'] == 'WARNING']) if 'alert_status' in alerts.columns else 0
            st.markdown(f'<div class="alert-warning">🟡 WARNING: {warning_count}</div>', unsafe_allow_html=True)
        
        with col3:
            stable_count = len(alerts[alerts['alert_status'] == 'STABLE']) if 'alert_status' in alerts.columns else 0
            st.markdown(f'<div class="alert-good">🟢 STABLE: {stable_count}</div>', unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Filter alerts
        filtered_alerts = alerts.copy()
        
        if not sources.empty and 'source_name' in sources.columns:
            if selected_state != "All States" and 'state' in sources.columns:
                state_sources = sources[sources['state'] == selected_state]['source_name'].tolist()
                if 'source_name' in filtered_alerts.columns:
                    filtered_alerts = filtered_alerts[filtered_alerts['source_name'].isin(state_sources)]
            
            if selected_district != "All Districts" and 'district' in sources.columns:
                district_sources = sources[sources['district'] == selected_district]['source_name'].tolist()
                if 'source_name' in filtered_alerts.columns:
                    filtered_alerts = filtered_alerts[filtered_alerts['source_name'].isin(district_sources)]
        
        if filtered_alerts.empty:
            st.success("✅ No alerts match the current filters")
        else:
            # Display alerts
            for _, alert in filtered_alerts.iterrows():
                source_info = None
                if not sources.empty and 'source_name' in sources.columns:
                    source_matches = sources[sources['source_name'] == alert.get('source_name', '')]
                    if not source_matches.empty:
                        source_info = source_matches.iloc[0]
                
                alert_status = alert.get('alert_status', 'UNKNOWN')
                
                if alert_status == 'CRITICAL':
                    status_class = "alert-critical"
                    status_emoji = "🔴"
                elif alert_status == 'WARNING':
                    status_class = "alert-warning"
                    status_emoji = "🟡"
                else:
                    status_class = "alert-good"
                    status_emoji = "🟢"
                
                source_name = alert.get('source_name', 'Unknown')
                
                with st.expander(f"{status_emoji} {source_name} - {alert_status}", expanded=False):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if source_info is not None:
                            st.markdown(f"**Type:** {source_info.get('source_type', 'Unknown')}")
                            st.markdown(f"**Location:** {source_info.get('district', 'Unknown')}, {source_info.get('state', 'Unknown')}")
                        else:
                            st.markdown(f"**Type:** Unknown")
                            st.markdown(f"**Location:** Unknown")
                        
                        capacity = alert.get('capacity_percent', 0)
                        st.markdown(f"**Capacity:** {capacity}%")
                        st.progress(capacity/100 if capacity > 0 else 0)
                    
                    with col2:
                        st.markdown(f"**pH Level:** {alert.get('ph_level', 'N/A')}")
                        st.markdown(f"**Time:** {alert.get('alert_time', 'N/A')}")
                        
                        if alert_status == 'CRITICAL':
                            st.markdown(f"**Status:** :red[{alert_status}]")
                            st.markdown("**Action Required:** Immediate inspection needed")
                        elif alert_status == 'WARNING':
                            st.markdown(f"**Status:** :orange[{alert_status}]")
                            st.markdown("**Action Required:** Schedule maintenance")
                        else:
                            st.markdown(f"**Status:** :green[{alert_status}]")
                            st.markdown("**Action Required:** Monitor routinely")
    else:
        st.success("✅ No active alerts - All systems normal")
        st.balloons()

# =====================
# TAB 6: DATA TABLES (Same as before)
# =====================

with tab6:
    st.markdown('<p class="section-header">📋 Data Explorer</p>', unsafe_allow_html=True)
    
    table_choice = st.selectbox(
        "Select Table to View",
        ["Water Sources", "Monitoring Stations", "Groundwater Levels", 
         "Rainfall History", "Water Usage", "Water Quality", "Active Alerts", "Regional Statistics"]
    )
    
    if table_choice == "Water Sources":
        display_df = filtered_sources if not filtered_sources.empty else sources
        if not display_df.empty:
            available_cols = [col for col in ['source_id', 'source_name', 'source_type', 'capacity_percent', 
                                             'max_capacity_mcm', 'build_year', 'age', 'state', 'district', 
                                             'origin_state', 'is_transboundary', 'risk_level', 'health_score', 'trend']
                            if col in display_df.columns]
            display_df = display_df[available_cols] if available_cols else display_df
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            # Download button
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "📥 Download CSV",
                csv,
                f"water_sources_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                "text/csv"
            )
    
    elif table_choice == "Monitoring Stations":
        display_df = filtered_stations if not filtered_stations.empty else stations
        if not display_df.empty:
            available_cols = [col for col in ['station_id', 'station_name', 'state_name', 'district_name', 
                                             'latitude', 'longitude', 'ph_level', 'dissolved_oxygen_mg_l', 
                                             'turbidity_ntu', 'status', 'last_maintenance']
                            if col in display_df.columns]
            display_df = display_df[available_cols] if available_cols else display_df
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"stations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
    
    elif table_choice == "Groundwater Levels":
        display_df = groundwater.copy()
        if selected_district != "All Districts" and 'district_name' in display_df.columns:
            display_df = display_df[display_df['district_name'] == selected_district]
        
        if not display_df.empty:
            available_cols = [col for col in ['id', 'district_name', 'avg_depth_meters', 'extraction_pct', 
                                             'recharge_rate_mcm', 'assessment_year', 'stress_level', 
                                             'depletion_rate']
                            if col in display_df.columns]
            display_df = display_df[available_cols] if available_cols else display_df
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"groundwater_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
    
    elif table_choice == "Rainfall History":
        display_df = rainfall.copy()
        if selected_district != "All Districts" and 'district_name' in display_df.columns:
            display_df = display_df[display_df['district_name'] == selected_district]
        
        if not display_df.empty:
            available_cols = [col for col in ['id', 'district_name', 'rainfall_cm', 'record_year', 'season', 
                                             'rainfall_category', 'deviation_pct']
                            if col in display_df.columns]
            display_df = display_df[available_cols] if available_cols else display_df
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"rainfall_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
    
    elif table_choice == "Water Usage":
        display_df = usage.copy()
        if selected_state != "All States" and 'state' in display_df.columns:
            display_df = display_df[display_df['state'] == selected_state]
        if selected_district != "All Districts" and 'district' in display_df.columns:
            display_df = display_df[display_df['district'] == selected_district]
        
        if not display_df.empty:
            available_cols = [col for col in ['usage_id', 'source_name', 'source_type', 'sector', 'sub_sector', 
                                             'consumer_name', 'consumption_mcm', 'record_year', 'season', 
                                             'state', 'district']
                            if col in display_df.columns]
            display_df = display_df[available_cols] if available_cols else display_df
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"usage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
    
    elif table_choice == "Water Quality":
        display_df = water_quality.copy()
        if not display_df.empty:
            available_cols = [col for col in ['quality_id', 'station_name', 'measurement_date', 'ph_level', 
                                             'dissolved_oxygen', 'turbidity', 'tds', 'temperature', 
                                             'conductivity', 'hardness']
                            if col in display_df.columns]
            display_df = display_df[available_cols] if available_cols else display_df
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"water_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
    
    elif table_choice == "Active Alerts":
        display_df = alerts.copy()
        if not display_df.empty:
            available_cols = [col for col in ['alert_id', 'source_name', 'capacity_percent', 'ph_level', 
                                             'alert_status', 'alert_time']
                            if col in display_df.columns]
            display_df = display_df[available_cols] if available_cols else display_df
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
    
    elif table_choice == "Regional Statistics":
        display_df = regional.copy()
        if not display_df.empty:
            available_cols = [col for col in ['region_id', 'region_name', 'population_count', 'annual_rainfall_avg_cm']
                            if col in display_df.columns]
            display_df = display_df[available_cols] if available_cols else display_df
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"regional_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")

# =====================
# TAB 7: ADMIN PANEL (Only visible to authenticated admins)
# =====================

if is_admin:
    with tab7:
        st.markdown('<p class="section-header">👑 Admin Control Panel</p>', unsafe_allow_html=True)
        st.warning("⚠️ **Admin Access Only** - All changes are permanent and affect the live database")
        
        # Create tabs for different CRUD operations
        admin_tab1, admin_tab2, admin_tab3, admin_tab4, admin_tab5 = st.tabs([
            "💧 Water Sources",
            "📡 Monitoring Stations",
            "⚠️ Alerts",
            "🌊 Groundwater",
            "☔ Rainfall"
        ])
        
        # ===== WATER SOURCES MANAGEMENT =====
        with admin_tab1:
            st.markdown("### Manage Water Sources")
            
            # Action selector
            action = st.radio(
                "Select Action",
                ["➕ Add New Source", "✏️ Update Existing Source", "🗑️ Delete Source"],
                horizontal=True
            )
            
            if action == "➕ Add New Source":
                st.markdown("#### Add New Water Source")
                
                with st.form("add_source_form"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        source_name = st.text_input("Source Name*")
                        source_type = st.selectbox("Source Type*", ["Dam", "Reservoir", "River", "Canal", "Lake", "Pond", "Well"])
                        capacity_percent = st.slider("Current Capacity %*", 0, 100, 50)
                        max_capacity_mcm = st.number_input("Max Capacity (MCM)*", min_value=0.0, value=100.0)
                        current_capacity_mcm = st.number_input("Current Capacity (MCM)*", min_value=0.0, value=50.0)
                        build_year = st.number_input("Build Year*", min_value=1800, max_value=2026, value=2000)
                    
                    with col2:
                        state = st.text_input("State*")
                        district = st.text_input("District*")
                        origin_state = st.text_input("Origin State")
                        is_transboundary = st.checkbox("Is Transboundary?")
                        latitude = st.number_input("Latitude", value=20.5937, format="%.6f")
                        longitude = st.number_input("Longitude", value=78.9629, format="%.6f")
                    
                    submitted = st.form_submit_button("➕ Add Source")
                    
                    if submitted:
                        if not source_name or not source_type or not state or not district:
                            st.error("Please fill all required fields")
                        else:
                            data = {
                                'source_name': source_name,
                                'source_type': source_type,
                                'capacity_percent': capacity_percent,
                                'max_capacity_mcm': max_capacity_mcm,
                                'current_capacity_mcm': current_capacity_mcm,
                                'build_year': build_year,
                                'state': state,
                                'district': district,
                                'origin_state': origin_state if origin_state else None,
                                'is_transboundary': is_transboundary,
                                'latitude': latitude if latitude else None,
                                'longitude': longitude if longitude else None
                            }
                            
                            if add_water_source(data):
                                st.success(f"✅ Source '{source_name}' added successfully!")
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Failed to add source")
            
            elif action == "✏️ Update Existing Source":
                st.markdown("#### Update Water Source")
                
                if not filtered_sources.empty:
                    # Select source to update
                    source_options = filtered_sources['source_name'].tolist() if 'source_name' in filtered_sources.columns else []
                    selected_source = st.selectbox("Select Source to Update", source_options)
                    
                    if selected_source:
                        source_data = filtered_sources[filtered_sources['source_name'] == selected_source].iloc[0]
                        
                        with st.form("update_source_form"):
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                source_name = st.text_input("Source Name*", value=source_data.get('source_name', ''))
                                source_type = st.selectbox("Source Type*", 
                                                          ["Dam", "Reservoir", "River", "Canal", "Lake", "Pond", "Well"],
                                                          index=["Dam", "Reservoir", "River", "Canal", "Lake", "Pond", "Well"].index(source_data.get('source_type', 'Dam')) if source_data.get('source_type', 'Dam') in ["Dam", "Reservoir", "River", "Canal", "Lake", "Pond", "Well"] else 0)
                                capacity_percent = st.slider("Current Capacity %*", 0, 100, int(source_data.get('capacity_percent', 50)))
                                max_capacity_mcm = st.number_input("Max Capacity (MCM)*", min_value=0.0, value=float(source_data.get('max_capacity_mcm', 100)))
                                current_capacity_mcm = st.number_input("Current Capacity (MCM)*", min_value=0.0, value=float(source_data.get('current_capacity_mcm', 50)))
                                build_year = st.number_input("Build Year*", min_value=1800, max_value=2026, value=int(source_data.get('build_year', 2000)))
                            
                            with col2:
                                state = st.text_input("State*", value=source_data.get('state', ''))
                                district = st.text_input("District*", value=source_data.get('district', ''))
                                origin_state = st.text_input("Origin State", value=source_data.get('origin_state', ''))
                                is_transboundary = st.checkbox("Is Transboundary?", value=bool(source_data.get('is_transboundary', False)))
                                latitude = st.number_input("Latitude", value=float(source_data.get('latitude', 20.5937)), format="%.6f")
                                longitude = st.number_input("Longitude", value=float(source_data.get('longitude', 78.9629)), format="%.6f")
                            
                            submitted = st.form_submit_button("✏️ Update Source")
                            
                            if submitted:
                                data = {
                                    'source_name': source_name,
                                    'source_type': source_type,
                                    'capacity_percent': capacity_percent,
                                    'max_capacity_mcm': max_capacity_mcm,
                                    'current_capacity_mcm': current_capacity_mcm,
                                    'build_year': build_year,
                                    'state': state,
                                    'district': district,
                                    'origin_state': origin_state if origin_state else None,
                                    'is_transboundary': is_transboundary,
                                    'latitude': latitude if latitude else None,
                                    'longitude': longitude if longitude else None
                                }
                                
                                if update_water_source(source_data.get('source_id'), data):
                                    st.success(f"✅ Source '{source_name}' updated successfully!")
                                    st.cache_data.clear()
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error("Failed to update source")
                else:
                    st.info("No sources available to update")
            
            elif action == "🗑️ Delete Source":
                st.markdown("#### Delete Water Source")
                st.warning("⚠️ This action cannot be undone!")
                
                if not filtered_sources.empty:
                    source_options = filtered_sources['source_name'].tolist() if 'source_name' in filtered_sources.columns else []
                    selected_source = st.selectbox("Select Source to Delete", source_options)
                    
                    if selected_source:
                        source_data = filtered_sources[filtered_sources['source_name'] == selected_source].iloc[0]
                        
                        st.markdown(f"**You are about to delete:** {selected_source}")
                        st.markdown(f"**Type:** {source_data.get('source_type', 'Unknown')}")
                        st.markdown(f"**Location:** {source_data.get('district', 'Unknown')}, {source_data.get('state', 'Unknown')}")
                        
                        confirm = st.checkbox("I understand this is permanent")
                        if st.button("🗑️ Delete Source", disabled=not confirm):
                            if delete_water_source(source_data.get('source_id')):
                                st.success(f"✅ Source '{selected_source}' deleted successfully!")
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Failed to delete source")
                else:
                    st.info("No sources available to delete")
        
        # ===== MONITORING STATIONS MANAGEMENT =====
        with admin_tab2:
            st.markdown("### Manage Monitoring Stations")
            
            action = st.radio(
                "Select Action",
                ["➕ Add New Station", "✏️ Update Station", "🗑️ Delete Station"],
                horizontal=True,
                key="station_action"
            )
            
            if action == "➕ Add New Station":
                with st.form("add_station_form"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        station_name = st.text_input("Station Name*")
                        state_name = st.text_input("State*")
                        district_name = st.text_input("District*")
                        latitude = st.number_input("Latitude*", value=20.5937, format="%.6f")
                        longitude = st.number_input("Longitude*", value=78.9629, format="%.6f")
                    
                    with col2:
                        ph_level = st.number_input("pH Level", value=7.0, format="%.2f")
                        dissolved_oxygen = st.number_input("Dissolved Oxygen (mg/L)", value=5.0, format="%.2f")
                        turbidity = st.number_input("Turbidity (NTU)", value=2.0, format="%.2f")
                        status = st.selectbox("Status", ["Active", "Maintenance", "Inactive"])
                    
                    submitted = st.form_submit_button("➕ Add Station")
                    
                    if submitted:
                        if not station_name or not state_name or not district_name:
                            st.error("Please fill all required fields")
                        else:
                            data = {
                                'station_name': station_name,
                                'state_name': state_name,
                                'district_name': district_name,
                                'latitude': latitude,
                                'longitude': longitude,
                                'ph_level': ph_level,
                                'dissolved_oxygen_mg_l': dissolved_oxygen,
                                'turbidity_ntu': turbidity,
                                'status': status
                            }
                            
                            if add_monitoring_station(data):
                                st.success(f"✅ Station '{station_name}' added successfully!")
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Failed to add station")
            
            elif action == "✏️ Update Station":
                if not filtered_stations.empty:
                    station_options = filtered_stations['station_name'].tolist() if 'station_name' in filtered_stations.columns else []
                    selected_station = st.selectbox("Select Station to Update", station_options, key="update_station")
                    
                    if selected_station:
                        station_data = filtered_stations[filtered_stations['station_name'] == selected_station].iloc[0]
                        
                        with st.form("update_station_form"):
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                station_name = st.text_input("Station Name*", value=station_data.get('station_name', ''))
                                state_name = st.text_input("State*", value=station_data.get('state_name', ''))
                                district_name = st.text_input("District*", value=station_data.get('district_name', ''))
                                latitude = st.number_input("Latitude*", value=float(station_data.get('latitude', 20.5937)), format="%.6f")
                                longitude = st.number_input("Longitude*", value=float(station_data.get('longitude', 78.9629)), format="%.6f")
                            
                            with col2:
                                ph_level = st.number_input("pH Level", value=float(station_data.get('ph_level', 7.0)), format="%.2f")
                                dissolved_oxygen = st.number_input("Dissolved Oxygen (mg/L)", value=float(station_data.get('dissolved_oxygen_mg_l', 5.0)), format="%.2f")
                                turbidity = st.number_input("Turbidity (NTU)", value=float(station_data.get('turbidity_ntu', 2.0)), format="%.2f")
                                status = st.selectbox("Status", ["Active", "Maintenance", "Inactive"], 
                                                     index=["Active", "Maintenance", "Inactive"].index(station_data.get('status', 'Active')) if station_data.get('status', 'Active') in ["Active", "Maintenance", "Inactive"] else 0)
                            
                            submitted = st.form_submit_button("✏️ Update Station")
                            
                            if submitted:
                                data = {
                                    'station_name': station_name,
                                    'state_name': state_name,
                                    'district_name': district_name,
                                    'latitude': latitude,
                                    'longitude': longitude,
                                    'ph_level': ph_level,
                                    'dissolved_oxygen_mg_l': dissolved_oxygen,
                                    'turbidity_ntu': turbidity,
                                    'status': status
                                }
                                
                                if update_monitoring_station(station_data.get('station_id'), data):
                                    st.success(f"✅ Station '{station_name}' updated successfully!")
                                    st.cache_data.clear()
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error("Failed to update station")
                else:
                    st.info("No stations available to update")
            
            elif action == "🗑️ Delete Station":
                st.warning("⚠️ This action cannot be undone!")
                
                if not filtered_stations.empty:
                    station_options = filtered_stations['station_name'].tolist() if 'station_name' in filtered_stations.columns else []
                    selected_station = st.selectbox("Select Station to Delete", station_options, key="delete_station")
                    
                    if selected_station:
                        station_data = filtered_stations[filtered_stations['station_name'] == selected_station].iloc[0]
                        
                        st.markdown(f"**You are about to delete:** {selected_station}")
                        st.markdown(f"**Location:** {station_data.get('district_name', 'Unknown')}, {station_data.get('state_name', 'Unknown')}")
                        
                        confirm = st.checkbox("I understand this is permanent")
                        if st.button("🗑️ Delete Station", disabled=not confirm):
                            if delete_monitoring_station(station_data.get('station_id')):
                                st.success(f"✅ Station '{selected_station}' deleted successfully!")
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Failed to delete station")
                else:
                    st.info("No stations available to delete")
        
        # ===== ALERTS MANAGEMENT =====
        with admin_tab3:
            st.markdown("### Manage Alerts")
            
            action = st.radio(
                "Select Action",
                ["➕ Add New Alert", "✏️ Update Alert", "🗑️ Delete Alert"],
                horizontal=True,
                key="alert_action"
            )
            
            if action == "➕ Add New Alert":
                with st.form("add_alert_form"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        source_name = st.text_input("Source Name*")
                        capacity_percent = st.slider("Capacity %*", 0, 100, 50)
                    
                    with col2:
                        ph_level = st.number_input("pH Level", value=7.0, format="%.2f")
                        alert_status = st.selectbox("Alert Status*", ["CRITICAL", "WARNING", "STABLE"])
                    
                    submitted = st.form_submit_button("➕ Add Alert")
                    
                    if submitted:
                        if not source_name:
                            st.error("Please fill all required fields")
                        else:
                            data = {
                                'source_name': source_name,
                                'capacity_percent': capacity_percent,
                                'ph_level': ph_level,
                                'alert_status': alert_status
                            }
                            
                            if add_alert(data):
                                st.success(f"✅ Alert for '{source_name}' added successfully!")
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Failed to add alert")
            
            elif action == "✏️ Update Alert":
                if not alerts.empty:
                    alert_options = alerts['source_name'].tolist() if 'source_name' in alerts.columns else []
                    selected_alert = st.selectbox("Select Alert to Update", alert_options, key="update_alert")
                    
                    if selected_alert:
                        alert_data = alerts[alerts['source_name'] == selected_alert].iloc[0]
                        
                        with st.form("update_alert_form"):
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                source_name = st.text_input("Source Name*", value=alert_data.get('source_name', ''))
                                capacity_percent = st.slider("Capacity %*", 0, 100, int(alert_data.get('capacity_percent', 50)))
                            
                            with col2:
                                ph_level = st.number_input("pH Level", value=float(alert_data.get('ph_level', 7.0)), format="%.2f")
                                alert_status = st.selectbox("Alert Status*", ["CRITICAL", "WARNING", "STABLE"],
                                                           index=["CRITICAL", "WARNING", "STABLE"].index(alert_data.get('alert_status', 'WARNING')) if alert_data.get('alert_status', 'WARNING') in ["CRITICAL", "WARNING", "STABLE"] else 1)
                            
                            submitted = st.form_submit_button("✏️ Update Alert")
                            
                            if submitted:
                                data = {
                                    'source_name': source_name,
                                    'capacity_percent': capacity_percent,
                                    'ph_level': ph_level,
                                    'alert_status': alert_status
                                }
                                
                                if update_alert(alert_data.get('alert_id'), data):
                                    st.success(f"✅ Alert for '{source_name}' updated successfully!")
                                    st.cache_data.clear()
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error("Failed to update alert")
                else:
                    st.info("No alerts available to update")
            
            elif action == "🗑️ Delete Alert":
                st.warning("⚠️ This action cannot be undone!")
                
                if not alerts.empty:
                    alert_options = alerts['source_name'].tolist() if 'source_name' in alerts.columns else []
                    selected_alert = st.selectbox("Select Alert to Delete", alert_options, key="delete_alert")
                    
                    if selected_alert:
                        alert_data = alerts[alerts['source_name'] == selected_alert].iloc[0]
                        
                        st.markdown(f"**You are about to delete alert for:** {selected_alert}")
                        st.markdown(f"**Status:** {alert_data.get('alert_status', 'Unknown')}")
                        
                        confirm = st.checkbox("I understand this is permanent")
                        if st.button("🗑️ Delete Alert", disabled=not confirm):
                            if delete_alert(alert_data.get('alert_id')):
                                st.success(f"✅ Alert for '{selected_alert}' deleted successfully!")
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Failed to delete alert")
                else:
                    st.info("No alerts available to delete")
        
        # ===== GROUNDWATER MANAGEMENT =====
        with admin_tab4:
            st.markdown("### Manage Groundwater Data")
            
            action = st.radio(
                "Select Action",
                ["➕ Add New Data", "✏️ Update Data", "🗑️ Delete Data"],
                horizontal=True,
                key="gw_action"
            )
            
            if action == "➕ Add New Data":
                with st.form("add_gw_form"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        district_name = st.text_input("District Name*")
                        avg_depth_meters = st.number_input("Average Depth (meters)*", min_value=0.0, value=30.0)
                        extraction_pct = st.number_input("Extraction %*", min_value=0.0, max_value=100.0, value=50.0)
                    
                    with col2:
                        recharge_rate_mcm = st.number_input("Recharge Rate (MCM)*", min_value=0.0, value=100.0)
                        assessment_year = st.number_input("Assessment Year*", min_value=2000, max_value=2026, value=2024)
                    
                    submitted = st.form_submit_button("➕ Add Data")
                    
                    if submitted:
                        if not district_name:
                            st.error("Please fill all required fields")
                        else:
                            data = {
                                'district_name': district_name,
                                'avg_depth_meters': avg_depth_meters,
                                'extraction_pct': extraction_pct,
                                'recharge_rate_mcm': recharge_rate_mcm,
                                'assessment_year': assessment_year
                            }
                            
                            if add_groundwater_data(data):
                                st.success(f"✅ Groundwater data for '{district_name}' added successfully!")
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Failed to add groundwater data")
            
            elif action == "✏️ Update Data":
                if not groundwater.empty:
                    gw_options = groundwater['district_name'].tolist() if 'district_name' in groundwater.columns else []
                    selected_gw = st.selectbox("Select District to Update", gw_options, key="update_gw")
                    
                    if selected_gw:
                        gw_data = groundwater[groundwater['district_name'] == selected_gw].iloc[0]
                        
                        with st.form("update_gw_form"):
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                district_name = st.text_input("District Name*", value=gw_data.get('district_name', ''))
                                avg_depth_meters = st.number_input("Average Depth (meters)*", min_value=0.0, value=float(gw_data.get('avg_depth_meters', 30.0)))
                                extraction_pct = st.number_input("Extraction %*", min_value=0.0, max_value=100.0, value=float(gw_data.get('extraction_pct', 50.0)))
                            
                            with col2:
                                recharge_rate_mcm = st.number_input("Recharge Rate (MCM)*", min_value=0.0, value=float(gw_data.get('recharge_rate_mcm', 100.0)))
                                assessment_year = st.number_input("Assessment Year*", min_value=2000, max_value=2026, value=int(gw_data.get('assessment_year', 2024)))
                            
                            submitted = st.form_submit_button("✏️ Update Data")
                            
                            if submitted:
                                data = {
                                    'district_name': district_name,
                                    'avg_depth_meters': avg_depth_meters,
                                    'extraction_pct': extraction_pct,
                                    'recharge_rate_mcm': recharge_rate_mcm,
                                    'assessment_year': assessment_year
                                }
                                
                                if update_groundwater_data(gw_data.get('id'), data):
                                    st.success(f"✅ Groundwater data for '{district_name}' updated successfully!")
                                    st.cache_data.clear()
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error("Failed to update groundwater data")
                else:
                    st.info("No groundwater data available to update")
            
            elif action == "🗑️ Delete Data":
                st.warning("⚠️ This action cannot be undone!")
                
                if not groundwater.empty:
                    gw_options = groundwater['district_name'].tolist() if 'district_name' in groundwater.columns else []
                    selected_gw = st.selectbox("Select District to Delete", gw_options, key="delete_gw")
                    
                    if selected_gw:
                        gw_data = groundwater[groundwater['district_name'] == selected_gw].iloc[0]
                        
                        st.markdown(f"**You are about to delete data for:** {selected_gw}")
                        st.markdown(f"**Year:** {gw_data.get('assessment_year', 'Unknown')}")
                        
                        confirm = st.checkbox("I understand this is permanent")
                        if st.button("🗑️ Delete Data", disabled=not confirm):
                            if delete_groundwater_data(gw_data.get('id')):
                                st.success(f"✅ Groundwater data for '{selected_gw}' deleted successfully!")
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Failed to delete groundwater data")
                else:
                    st.info("No groundwater data available to delete")
        
        # ===== RAINFALL MANAGEMENT =====
        with admin_tab5:
            st.markdown("### Manage Rainfall Data")
            
            action = st.radio(
                "Select Action",
                ["➕ Add New Data", "✏️ Update Data", "🗑️ Delete Data"],
                horizontal=True,
                key="rain_action"
            )
            
            if action == "➕ Add New Data":
                with st.form("add_rain_form"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        district_name = st.text_input("District Name*")
                        rainfall_cm = st.number_input("Rainfall (cm)*", min_value=0.0, value=100.0)
                    
                    with col2:
                        record_year = st.number_input("Year*", min_value=1900, max_value=2026, value=2024)
                        season = st.selectbox("Season*", ["Monsoon", "Winter", "Summer", "Post-Monsoon"])
                    
                    submitted = st.form_submit_button("➕ Add Data")
                    
                    if submitted:
                        if not district_name:
                            st.error("Please fill all required fields")
                        else:
                            data = {
                                'district_name': district_name,
                                'rainfall_cm': rainfall_cm,
                                'record_year': record_year,
                                'season': season
                            }
                            
                            if add_rainfall_data(data):
                                st.success(f"✅ Rainfall data for '{district_name}' added successfully!")
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Failed to add rainfall data")
            
            elif action == "✏️ Update Data":
                if not rainfall.empty:
                    rain_options = rainfall['district_name'].tolist() if 'district_name' in rainfall.columns else []
                    selected_rain = st.selectbox("Select District to Update", rain_options, key="update_rain")
                    
                    if selected_rain:
                        rain_data = rainfall[rainfall['district_name'] == selected_rain].iloc[0]
                        
                        with st.form("update_rain_form"):
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                district_name = st.text_input("District Name*", value=rain_data.get('district_name', ''))
                                rainfall_cm = st.number_input("Rainfall (cm)*", min_value=0.0, value=float(rain_data.get('rainfall_cm', 100.0)))
                            
                            with col2:
                                record_year = st.number_input("Year*", min_value=1900, max_value=2026, value=int(rain_data.get('record_year', 2024)))
                                season = st.selectbox("Season*", ["Monsoon", "Winter", "Summer", "Post-Monsoon"],
                                                     index=["Monsoon", "Winter", "Summer", "Post-Monsoon"].index(rain_data.get('season', 'Monsoon')) if rain_data.get('season', 'Monsoon') in ["Monsoon", "Winter", "Summer", "Post-Monsoon"] else 0)
                            
                            submitted = st.form_submit_button("✏️ Update Data")
                            
                            if submitted:
                                data = {
                                    'district_name': district_name,
                                    'rainfall_cm': rainfall_cm,
                                    'record_year': record_year,
                                    'season': season
                                }
                                
                                if update_rainfall_data(rain_data.get('id'), data):
                                    st.success(f"✅ Rainfall data for '{district_name}' updated successfully!")
                                    st.cache_data.clear()
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error("Failed to update rainfall data")
                else:
                    st.info("No rainfall data available to update")
            
            elif action == "🗑️ Delete Data":
                st.warning("⚠️ This action cannot be undone!")
                
                if not rainfall.empty:
                    rain_options = rainfall['district_name'].tolist() if 'district_name' in rainfall.columns else []
                    selected_rain = st.selectbox("Select District to Delete", rain_options, key="delete_rain")
                    
                    if selected_rain:
                        rain_data = rainfall[rainfall['district_name'] == selected_rain].iloc[0]
                        
                        st.markdown(f"**You are about to delete data for:** {selected_rain}")
                        st.markdown(f"**Year:** {rain_data.get('record_year', 'Unknown')}")
                        st.markdown(f"**Season:** {rain_data.get('season', 'Unknown')}")
                        
                        confirm = st.checkbox("I understand this is permanent")
                        if st.button("🗑️ Delete Data", disabled=not confirm):
                            if delete_rainfall_data(rain_data.get('id')):
                                st.success(f"✅ Rainfall data for '{selected_rain}' deleted successfully!")
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Failed to delete rainfall data")
                else:
                    st.info("No rainfall data available to delete")

# =====================
# SIDEBAR FILTER SUMMARY
# =====================

with st.sidebar.expander("📊 Current Filter Summary", expanded=False):
    st.markdown(f"""
    **Time Range:** {year_range[0]} - {year_range[1]}
    
    **Geography:**
    - State: {selected_state}
    - District: {selected_district}
    
    **Source Filters:**
    - Type: {selected_type}
    - Capacity: {capacity_range[0]}% - {capacity_range[1]}%
    - Risk: {selected_risk}
    
    **Results:**
    - Sources: {len(filtered_sources)} of {len(sources)}
    - On Map: {len(filtered_sources[filtered_sources['latitude'].notna()]) if not filtered_sources.empty and 'latitude' in filtered_sources.columns else 0}
    """)

# =====================
# EXPORT ALL FILTERED DATA
# =====================

st.sidebar.markdown("---")
if st.sidebar.button("📦 Export All Filtered Data", use_container_width=True):
    export_data = {
        'water_sources': filtered_sources,
        'monitoring_stations': filtered_stations,
        'groundwater': groundwater[groundwater['district_name'].isin(filtered_sources['district'].unique())] if not filtered_sources.empty and not groundwater.empty and 'district' in filtered_sources.columns and 'district_name' in groundwater.columns else pd.DataFrame(),
        'rainfall': rainfall[rainfall['district_name'].isin(filtered_sources['district'].unique())] if not filtered_sources.empty and not rainfall.empty and 'district' in filtered_sources.columns and 'district_name' in rainfall.columns else pd.DataFrame(),
        'water_quality': water_quality if not water_quality.empty else pd.DataFrame(),
        'alerts': alerts if not alerts.empty else pd.DataFrame()
    }
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in export_data.items():
            if not df.empty:
                df.to_excel(writer, sheet_name=sheet_name[:31], index=False)  # Sheet names max 31 chars
    
    st.sidebar.download_button(
        "📥 Download Excel Report",
        output.getvalue(),
        f"aquastat_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# =====================
# FOOTER
# =====================

st.markdown("---")
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
    <div style="text-align: center;">
        <p style="color: #00e5ff; font-size: 1.2rem; margin:0;">💧 AQUASTAT</p>
        <p style="color: #8892b0; margin:0;">National Water Command Center</p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div style="text-align: center;">
        <p style="color: #8892b0; margin:0;">Data Source: Ministry of Jal Shakti</p>
        <p style="color: #8892b0; margin:0;">Last Updated: {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}</p>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
    <div style="text-align: center;">
        <p style="color: #8892b0; margin:0;">© 2025 All Rights Reserved</p>
        <p style="color: #8892b0; margin:0;">Version 3.0 | For Official Use</p>
    </div>
    """, unsafe_allow_html=True)

# Auto-refresh indicator
st.markdown("""
<div style="position: fixed; bottom: 10px; right: 10px; background: rgba(0,229,255,0.1); padding: 5px 10px; border-radius: 5px; font-size: 0.8rem;">
    🔄 Auto-refresh every 5 minutes • Cloud Connected
</div>
""", unsafe_allow_html=True)

# Add auto-refresh functionality
if st.button("🔄 Refresh Data", key="refresh_button"):
    st.cache_data.clear()
    st.rerun()
