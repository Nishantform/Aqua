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
import json
import warnings
import os
from io import BytesIO
import sqlite3

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
    /* Main background */
    .stApp {
        background: linear-gradient(135deg, #0a0f1e 0%, #0f1425 100%);
    }
    
    /* Metric containers */
    [data-testid="metric-container"] {
        background: rgba(17, 25, 40, 0.95);
        border: 1px solid #1f2937;
        border-radius: 15px;
        padding: 20px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.5);
        transition: transform 0.3s ease;
    }
    
    [data-testid="metric-container"]:hover {
        transform: translateY(-5px);
        border-color: #00e5ff;
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #00e5ff;
        font-weight: 700;
    }
    
    h1 {
        font-size: 2.5rem;
        border-bottom: 2px solid rgba(0, 229, 255, 0.3);
        padding-bottom: 10px;
    }
    
    /* Cards */
    .info-card {
        background: rgba(17, 25, 40, 0.95);
        border: 1px solid #1f2937;
        border-radius: 15px;
        padding: 20px;
        margin: 10px 0;
        transition: all 0.3s ease;
    }
    
    .info-card:hover {
        border-color: #00e5ff;
        box-shadow: 0 8px 30px rgba(0, 229, 255, 0.2);
    }
    
    /* Status indicators */
    .status-critical {
        color: #ff4444;
        font-weight: 600;
        animation: pulse 2s infinite;
    }
    
    .status-warning {
        color: #ffd700;
        font-weight: 600;
    }
    
    .status-good {
        color: #00ff9d;
        font-weight: 600;
    }
    
    @keyframes pulse {
        0% { opacity: 1; }
        50% { opacity: 0.5; }
        100% { opacity: 1; }
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        background: rgba(17, 25, 40, 0.95);
        padding: 10px;
        border-radius: 10px;
        gap: 10px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 16px;
        background: #1f2937;
        color: white;
        font-weight: 600;
    }
    
    .stTabs [aria-selected="true"] {
        background: #00e5ff;
        color: black;
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background: rgba(10, 15, 30, 0.95);
        border-right: 1px solid #1f2937;
    }
    
    /* Dataframes */
    .stDataFrame {
        background: rgba(17, 25, 40, 0.95);
        border-radius: 10px;
        border: 1px solid #1f2937;
    }
    
    /* Progress bars */
    .stProgress > div > div {
        background: linear-gradient(90deg, #00e5ff, #00b8ff);
    }
</style>
""", unsafe_allow_html=True)

# -------------------------
# SQLITE3 DATABASE CONNECTION
# -------------------------

@st.cache_resource
def init_connection():
    """Initialize SQLite3 database connection"""
    try:
        # Connect to your SQLite database file
        # 'check_same_thread=False' allows the connection to work with web apps like Streamlit
        conn = sqlite3.connect('aqua_stat.db', check_same_thread=False)
        print("Successfully connected to Aqua Stat database!")
        return conn
    except sqlite3.Error as e:
        st.error(f"Database connection failed: {e}")
        return None

# Initialize connection
conn = init_connection()

# -------------------------
# DATA LOADING FUNCTIONS
# -------------------------

@st.cache_data(ttl=300)
def load_all_data():
    """Load all data from SQLite database"""
    if conn is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    try:
        # Get list of tables to check what's available
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in cursor.fetchall()]
        print(f"Available tables: {tables}")
        
        # Water Sources
        if 'water_sources' in tables:
            sources = pd.read_sql_query("SELECT * FROM water_sources", conn)
        else:
            sources = pd.DataFrame()
            st.warning("Table 'water_sources' not found in database")
        
        # Monitoring Stations
        if 'water_monitoring_stations' in tables:
            stations = pd.read_sql_query("SELECT * FROM water_monitoring_stations", conn)
        else:
            stations = pd.DataFrame()
            st.warning("Table 'water_monitoring_stations' not found in database")
        
        # Groundwater Levels
        if 'groundwater_levels' in tables:
            groundwater = pd.read_sql_query("SELECT * FROM groundwater_levels", conn)
        else:
            groundwater = pd.DataFrame()
            st.warning("Table 'groundwater_levels' not found in database")
        
        # Rainfall History
        if 'rainfall_history' in tables:
            rainfall = pd.read_sql_query("SELECT * FROM rainfall_history", conn)
        else:
            rainfall = pd.DataFrame()
            st.warning("Table 'rainfall_history' not found in database")
        
        # Active Alerts
        if 'active_alerts' in tables:
            alerts = pd.read_sql_query("SELECT * FROM active_alerts", conn)
        else:
            alerts = pd.DataFrame()
            st.warning("Table 'active_alerts' not found in database")
        
        # Water Usage
        if 'water_usage_history' in tables:
            usage = pd.read_sql_query("""
                SELECT wu.*, ws.source_name, ws.source_type, ws.state, ws.district 
                FROM water_usage_history wu
                LEFT JOIN water_sources ws ON wu.source_id = ws.source_id
            """, conn)
        else:
            usage = pd.DataFrame()
            st.warning("Table 'water_usage_history' not found in database")
        
        # Regional Stats
        if 'regional_stats' in tables:
            regional = pd.read_sql_query("SELECT * FROM regional_stats", conn)
        else:
            regional = pd.DataFrame()
            st.warning("Table 'regional_stats' not found in database")
        
        return sources, stations, groundwater, rainfall, alerts, usage, regional
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# -------------------------
# LOAD DATA
# -------------------------

with st.spinner("🚀 Loading AQUASTAT Command Center..."):
    sources, stations, groundwater, rainfall, alerts, usage, regional = load_all_data()

# Display column info for debugging
with st.sidebar.expander("🔍 Database Info", expanded=False):
    if not sources.empty:
        st.write("Sources columns:", list(sources.columns))
    if not stations.empty:
        st.write("Stations columns:", list(stations.columns))
    if not groundwater.empty:
        st.write("Groundwater columns:", list(groundwater.columns))

# -------------------------
# DATA PROCESSING
# -------------------------

current_year = datetime.now().year

# Process Sources - with column validation
if not sources.empty:
    # Convert numeric columns if they exist
    for col in ['capacity_percent', 'build_year']:
        if col in sources.columns:
            sources[col] = pd.to_numeric(sources[col], errors='coerce')
    
    # Calculate age if build_year exists
    if 'build_year' in sources.columns:
        sources['age'] = current_year - sources['build_year']
        sources['age'] = sources['age'].clip(0, 200)
    else:
        sources['age'] = 0
    
    # Calculate health score
    if 'capacity_percent' in sources.columns:
        sources['health_score'] = (
            sources['capacity_percent'].fillna(50) * 0.4 + 
            (100 - sources['age'].clip(0, 100).fillna(50)) * 0.3 + 
            30
        ).clip(0, 100)
    else:
        sources['health_score'] = 50
    
    # Risk classification if capacity_percent exists
    if 'capacity_percent' in sources.columns:
        sources['risk_level'] = pd.cut(
            sources['capacity_percent'],
            bins=[0, 30, 60, 100],
            labels=['Critical', 'Moderate', 'Good'],
            include_lowest=True
        )
    else:
        sources['risk_level'] = 'Unknown'

# Add coordinates from monitoring stations to sources
def add_coordinates_to_sources(sources_df, stations_df):
    """Add coordinates from monitoring stations to water sources"""
    
    if sources_df.empty or stations_df.empty:
        return sources_df
    
    df = sources_df.copy()
    
    # Check if required columns exist
    if 'district' not in df.columns or 'district_name' not in stations_df.columns:
        return df
    
    # Create a mapping of district to first available coordinates
    if 'latitude' in stations_df.columns and 'longitude' in stations_df.columns:
        stations_df['district_clean'] = stations_df['district_name'].str.strip().str.lower()
        
        # Get first coordinates per district
        district_coords = stations_df.groupby('district_clean').agg({
            'latitude': 'first',
            'longitude': 'first'
        }).reset_index()
        
        # Clean source districts for matching
        df['district_clean'] = df['district'].str.strip().str.lower()
        
        # Merge
        df = df.merge(district_coords, on='district_clean', how='left')
        
        # Drop temporary column
        df = df.drop('district_clean', axis=1)
    
    return df

# Apply coordinate mapping
if not sources.empty and not stations.empty:
    sources = add_coordinates_to_sources(sources, stations)

# Process Groundwater
if not groundwater.empty and 'avg_depth_meters' in groundwater.columns:
    groundwater['stress_level'] = pd.cut(
        groundwater['avg_depth_meters'],
        bins=[0, 20, 40, 100],
        labels=['Low', 'Moderate', 'High']
    )

# Process Rainfall
if not rainfall.empty and 'rainfall_cm' in rainfall.columns:
    rainfall['rainfall_category'] = pd.cut(
        rainfall['rainfall_cm'],
        bins=[0, 50, 150, 300, float('inf')],
        labels=['Low', 'Moderate', 'High', 'Extreme']
    )

# -------------------------
# SIDEBAR FILTERS
# -------------------------

st.sidebar.title("🎮 AQUASTAT")
st.sidebar.caption("Command Interface v2.0")

# Reset button
if st.sidebar.button("🔄 Reset All Filters", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")

# ========== TIME FILTERS ==========
st.sidebar.markdown("### 📅 Time Filters")

# Year filter - extended timeline (1800-2026)
if not sources.empty and 'build_year' in sources.columns:
    available_years = sorted(sources['build_year'].dropna().unique())
    if len(available_years) > 0:
        min_year = int(min(available_years))
        max_year = int(max(available_years))
        # Extended range from 1800 to 2026
        year_range = st.sidebar.slider(
            "Build Year Range",
            min_value=1800,
            max_value=2026,
            value=(min_year, max_year)
        )
    else:
        year_range = (1800, 2026)
else:
    year_range = (1800, 2026)

st.sidebar.markdown("---")

# ========== GEOGRAPHIC FILTERS ==========
st.sidebar.markdown("### 🌍 Geographic Filters")

# State filter - default to "All States"
if not sources.empty and 'state' in sources.columns:
    states = ['All States'] + sorted(sources['state'].dropna().unique().tolist())
    selected_state = st.sidebar.selectbox("State", states, index=0)
else:
    selected_state = "All States"

# District filter based on state
if not sources.empty and 'district' in sources.columns:
    if selected_state != "All States":
        districts = sources[sources['state'] == selected_state]['district'].dropna().unique()
    else:
        districts = sources['district'].dropna().unique()
    
    if len(districts) > 0:
        districts = ['All Districts'] + sorted(districts.tolist())
    else:
        districts = ['All Districts']
    selected_district = st.sidebar.selectbox("District", districts, index=0)
else:
    selected_district = "All Districts"

st.sidebar.markdown("---")

# ========== SOURCE FILTERS ==========
st.sidebar.markdown("### 💧 Source Filters")

# Source type - default to "All Types"
if not sources.empty and 'source_type' in sources.columns:
    source_types = ['All Types'] + sorted(sources['source_type'].dropna().unique().tolist())
    selected_type = st.sidebar.selectbox("Source Type", source_types, index=0)
else:
    selected_type = "All Types"

# Capacity range
if not sources.empty and 'capacity_percent' in sources.columns:
    min_cap = float(sources['capacity_percent'].min())
    max_cap = float(sources['capacity_percent'].max())
    capacity_range = st.sidebar.slider(
        "Capacity %",
        min_value=min_cap,
        max_value=max_cap,
        value=(min_cap, max_cap)
    )
else:
    capacity_range = (0, 100)

# Risk level filter
if not sources.empty and 'risk_level' in sources.columns:
    risk_options = ['All Risk Levels'] + list(sources['risk_level'].unique())
    selected_risk = st.sidebar.selectbox("Risk Level", risk_options, index=0)
else:
    selected_risk = "All Risk Levels"

st.sidebar.markdown("---")

# ========== MAP SETTINGS ==========
st.sidebar.markdown("### 🗺️ Map Settings")

map_style = st.sidebar.selectbox(
    "Map Style",
    ["Esri Satellite (Official)", "Dark Matter", "Light Matter"],
    index=0
)

show_heatmap = st.sidebar.checkbox("Show Heatmap", True)
show_clusters = st.sidebar.checkbox("Show Clusters", True)
show_stations = st.sidebar.checkbox("Show Monitoring Stations", True)
marker_size = st.sidebar.slider("Marker Size", 5, 20, 10)

# Show filter stats
st.sidebar.markdown("---")
st.sidebar.caption(f"Total Sources in DB: {len(sources)}")

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

# Also filter stations based on geographic filters for consistency
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
st.caption(f"**Live Intelligence** • Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# KPI Row
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(
        "Total Sources",
        f"{len(sources):,}",
        f"{len(sources) - len(filtered_sources)} filtered"
    )

with col2:
    avg_cap = filtered_sources['capacity_percent'].mean() if not filtered_sources.empty and 'capacity_percent' in filtered_sources.columns else 0
    st.metric("Avg Capacity", f"{avg_cap:.1f}%")

with col3:
    if not filtered_sources.empty and 'capacity_percent' in filtered_sources.columns:
        critical = len(filtered_sources[filtered_sources['capacity_percent'] < 30])
    else:
        critical = 0
    st.metric("Critical Sources", f"{critical}", delta_color="inverse")

with col4:
    if not filtered_sources.empty and 'latitude' in filtered_sources.columns:
        sources_with_coords = len(filtered_sources[filtered_sources['latitude'].notna()])
    else:
        sources_with_coords = 0
    st.metric("Sources on Map", f"{sources_with_coords}")

with col5:
    st.metric("Active Alerts", f"{len(alerts)}", delta_color="inverse")

st.markdown("---")

# Main Tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 DASHBOARD",
    "🗺️ MAP VIEW",
    "📈 ANALYTICS",
    "⚠️ ALERTS",
    "📋 DATA TABLES"
])

# =====================
# TAB 1: DASHBOARD
# =====================

with tab1:
    if filtered_sources.empty:
        st.warning("⚠️ No water sources match the current filters. Try clearing some filters or selecting 'All States'.")
        
        # Show sample of all data with column validation
        with st.expander("📋 Show all sources sample"):
            available_cols = []
            for col in ['source_name', 'source_type', 'state', 'district', 'capacity_percent']:
                if col in sources.columns:
                    available_cols.append(col)
            if len(available_cols) > 0:
                st.dataframe(sources[available_cols].head(20))
            else:
                st.dataframe(sources.head(20))
    
    else:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Capacity Distribution")
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
                    yaxis_title="Number of Sources"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Capacity data not available")
        
        with col2:
            st.subheader("🏭 Source Types")
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
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Source type data not available")
        
        # Second row
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📈 Groundwater Stress Levels")
            if not groundwater.empty and 'stress_level' in groundwater.columns:
                # Filter groundwater based on selected district if applicable
                filtered_gw = groundwater.copy()
                if selected_district != "All Districts" and 'district_name' in filtered_gw.columns:
                    filtered_gw = filtered_gw[filtered_gw['district_name'] == selected_district]
                
                if not filtered_gw.empty and 'stress_level' in filtered_gw.columns:
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
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No groundwater stress data available")
            else:
                st.info("ℹ️ No groundwater data available")
        
        with col2:
            st.subheader("☔ Rainfall by Season")
            if not rainfall.empty and 'season' in rainfall.columns and 'rainfall_cm' in rainfall.columns:
                # Filter rainfall based on selected district if applicable
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
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No rainfall data available")
            else:
                st.info("ℹ️ No rainfall data available")
        
        # Third row - Risk distribution
        if 'risk_level' in filtered_sources.columns:
            st.subheader("⚠️ Risk Distribution")
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
            st.plotly_chart(fig, use_container_width=True)

# =====================
# TAB 2: MAP VIEW
# =====================

with tab2:
    st.subheader("🗺️ National Interactive Water Resources Map")
    
    # Display current filter info
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
        "Esri Satellite (Official)": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        "Dark Matter": "https://cartodb-basemaps-a.global.ssl.fastly.net/dark_all/{z}/{x}/{y}.png",
        "Light Matter": "https://cartodb-basemaps-a.global.ssl.fastly.net/light_all/{z}/{x}/{y}.png"
    }
    
    # Create map centered on India or selected region
    center_lat, center_lon, zoom = 20.5937, 78.9629, 5
    
    if not filtered_sources.empty and 'latitude' in filtered_sources.columns and 'longitude' in filtered_sources.columns:
        sources_with_coords = filtered_sources[filtered_sources['latitude'].notna() & filtered_sources['longitude'].notna()]
        
        if selected_district != "All Districts" and not sources_with_coords.empty:
            center_lat = sources_with_coords['latitude'].mean()
            center_lon = sources_with_coords['longitude'].mean()
            zoom = 9
        elif selected_state != "All States" and not sources_with_coords.empty:
            center_lat = sources_with_coords['latitude'].mean()
            center_lon = sources_with_coords['longitude'].mean()
            zoom = 7
    
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=zoom,
        tiles=style_map[map_style],
        attr='AQUASTAT | Data: Esri, OSM, Survey of India'
    )
    
    Fullscreen().add_to(m)
    
    # Initialize Marker Layer
    if show_clusters and len(filtered_sources) > 10:
        marker_cluster = MarkerCluster().add_to(m)
    else:
        marker_cluster = m
    
    heat_data = []
    sources_on_map = 0
    
    # Process ONLY filtered water sources with coordinates
    if not filtered_sources.empty and 'latitude' in filtered_sources.columns and 'longitude' in filtered_sources.columns:
        sources_with_coords = filtered_sources[
            filtered_sources['latitude'].notna() & 
            filtered_sources['longitude'].notna()
        ]
        
        if not sources_with_coords.empty:
            for _, source in sources_with_coords.iterrows():
                # Determine color and risk text based on capacity
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
                
                # Build popup HTML safely
                source_name = source.get('source_name', 'Unknown')
                source_type = source.get('source_type', 'Unknown')
                district = source.get('district', 'Unknown')
                state = source.get('state', 'Unknown')
                age = source.get('age', 0)
                
                popup_html = f"""
                <div style="font-family: Arial; min-width: 250px;">
                    <h4 style="color: {color}; margin:0;">{source_name}</h4>
                    <hr style="margin:5px 0;">
                    <table style="width:100%;">
                        <tr><td><b>Type:</b></td><td>{source_type}</td></tr>
                        <tr><td><b>District:</b></td><td>{district}</td></tr>
                        <tr><td><b>State:</b></td><td>{state}</td></tr>
                        <tr><td><b>Capacity:</b></td><td>{capacity:.1f}%</td></tr>
                        <tr><td><b>Age:</b></td><td>{age:.0f} years</td></tr>
                        <tr><td><b>Risk Level:</b></td><td><span style="color:{color}; font-weight:bold;">{risk_text}</span></td></tr>
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
                    tooltip=f"{source_name} - {capacity:.0f}%"
                )
                
                if show_clusters and len(filtered_sources) > 10:
                    marker.add_to(marker_cluster)
                else:
                    marker.add_to(m)
            
            # Add heatmap if enabled
            if show_heatmap and heat_data:
                HeatMap(
                    heat_data,
                    radius=15,
                    blur=10,
                    gradient={0.2: 'blue', 0.4: 'cyan', 0.6: 'lime', 0.8: 'yellow', 1: 'red'}
                ).add_to(m)
    
    # Add monitoring stations if enabled
    if show_stations and not filtered_stations.empty:
        if 'latitude' in filtered_stations.columns and 'longitude' in filtered_stations.columns:
            stations_with_coords = filtered_stations[
                filtered_stations['latitude'].notna() & 
                filtered_stations['longitude'].notna()
            ]
            for _, station in stations_with_coords.iterrows():
                status = station.get('status', 'Unknown')
                if status == 'Active':
                    station_color = 'green'
                elif status == 'Maintenance':
                    station_color = 'orange'
                else:
                    station_color = 'red'
                
                station_name = station.get('station_name', 'Unknown')
                district = station.get('district_name', 'Unknown')
                ph = station.get('ph_level', 'N/A')
                do = station.get('dissolved_oxygen_mg_l', 'N/A')
                turbidity = station.get('turbidity_ntu', 'N/A')
                
                station_popup = f"""
                <b>{station_name}</b><br>
                District: {district}<br>
                Status: {status}<br>
                pH: {ph}<br>
                DO: {do} mg/L<br>
                Turbidity: {turbidity} NTU
                """
                
                folium.Marker(
                    location=[station['latitude'], station['longitude']],
                    icon=folium.Icon(color=station_color, icon='info-sign'),
                    popup=folium.Popup(station_popup, max_width=300),
                    tooltip=f"Station: {station_name}"
                ).add_to(m)
    
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
        st.markdown("🟢 **Good** (≥60%)")
    with cols[1]:
        st.markdown("🟡 **Moderate** (30-60%)")
    with cols[2]:
        st.markdown("🔴 **Critical** (<30%)")
    with cols[3]:
        st.markdown("🔵 **Monitoring Station**")
    with cols[4]:
        st.markdown("🔥 **Heatmap Area**")

# =====================
# TAB 3: ANALYTICS
# =====================

with tab3:
    st.subheader("📈 Advanced Analytics")
    
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
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No rainfall data available")
            else:
                st.info("No rainfall data available")
        
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
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No groundwater data available")
            else:
                st.info("No groundwater data available")
    
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
                        title=f"Average Capacity by State ({len(state_cap)} states)",
                        template="plotly_dark",
                        color=state_cap.values,
                        color_continuous_scale='Tealgrn',
                        labels={'x': 'Avg Capacity (%)', 'y': 'State'}
                    )
                    fig.update_traces(texttemplate='%{x:.1f}%', textposition='outside')
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No state data available")
            else:
                st.info("No state data available")
        
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
                        title="Groundwater Extraction vs Recharge Rate",
                        template="plotly_dark"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No groundwater data available")
            else:
                st.info("No groundwater data available")
    
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
                else:
                    st.info("No statistical data available")
            else:
                st.info("No source data available")
        
        with col2:
            st.subheader("Correlation Matrix")
            if not filtered_sources.empty and not groundwater.empty:
                # Check if merge columns exist
                if 'district' in filtered_sources.columns and 'district_name' in groundwater.columns:
                    merged = filtered_sources.merge(
                        groundwater,
                        left_on='district',
                        right_on='district_name',
                        how='inner'
                    )
                    
                    if not merged.empty:
                        numeric_cols = []
                        for col in ['capacity_percent', 'age', 'avg_depth_meters', 'extraction_pct', 'recharge_rate_mcm']:
                            if col in merged.columns:
                                numeric_cols.append(col)
                        
                        if len(numeric_cols) >= 2:
                            corr_data = merged[numeric_cols].dropna()
                            
                            if not corr_data.empty:
                                corr_matrix = corr_data.corr()
                                
                                fig = px.imshow(
                                    corr_matrix,
                                    text_auto=True,
                                    aspect="auto",
                                    title="Feature Correlation Matrix",
                                    template="plotly_dark",
                                    color_continuous_scale='RdBu_r'
                                )
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.info("Insufficient data for correlation")
                        else:
                            st.info("Not enough numeric columns for correlation")
                    else:
                        st.info("No matching data for correlation")
                else:
                    st.info("Cannot merge sources and groundwater data")
            else:
                st.info("Insufficient data for correlation")

# =====================
# TAB 4: ALERTS (Fixed - No index error)
# =====================

with tab4:
    st.subheader("🚨 Active Alerts and Warnings")
    
    if not alerts.empty:
        # Count alerts by status if column exists
        if 'alert_status' in alerts.columns:
            alert_counts = alerts['alert_status'].value_counts()
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("CRITICAL", alert_counts.get('CRITICAL', 0), delta=None, delta_color="off")
            with col2:
                st.metric("WARNING", alert_counts.get('WARNING', 0), delta=None, delta_color="off")
            with col3:
                st.metric("STABLE", alert_counts.get('STABLE', 0), delta=None, delta_color="off")
            
            st.markdown("---")
            
            # Filter alerts based on selected filters
            filtered_alerts = alerts.copy()
            
            # Apply geographic filters
            if selected_state != "All States" and not sources.empty and 'source_name' in sources.columns and 'state' in sources.columns:
                state_sources = sources[sources['state'] == selected_state]['source_name'].tolist()
                if 'source_name' in filtered_alerts.columns:
                    filtered_alerts = filtered_alerts[filtered_alerts['source_name'].isin(state_sources)]
            
            if selected_district != "All Districts" and not sources.empty and 'source_name' in sources.columns and 'district' in sources.columns:
                district_sources = sources[sources['district'] == selected_district]['source_name'].tolist()
                if 'source_name' in filtered_alerts.columns:
                    filtered_alerts = filtered_alerts[filtered_alerts['source_name'].isin(district_sources)]
            
            if selected_type != "All Types" and not sources.empty and 'source_name' in sources.columns and 'source_type' in sources.columns:
                type_sources = sources[sources['source_type'] == selected_type]['source_name'].tolist()
                if 'source_name' in filtered_alerts.columns:
                    filtered_alerts = filtered_alerts[filtered_alerts['source_name'].isin(type_sources)]
            
            if filtered_alerts.empty:
                st.info("ℹ️ No alerts match the current filters")
            else:
                # Create a DataFrame for alerts display
                alert_display = filtered_alerts.copy()
                
                # Add source information if possible
                if not sources.empty and 'source_name' in alert_display.columns:
                    source_cols = []
                    for col in ['source_name', 'source_type', 'district', 'state']:
                        if col in sources.columns:
                            source_cols.append(col)
                    if source_cols:
                        alert_display = alert_display.merge(
                            sources[source_cols],
                            on='source_name',
                            how='left'
                        )
                
                # Create status color column
                def get_status_color(status):
                    if status == 'CRITICAL':
                        return '🔴'
                    elif status == 'WARNING':
                        return '🟡'
                    else:
                        return '🟢'
                
                if 'alert_status' in alert_display.columns:
                    alert_display['icon'] = alert_display['alert_status'].apply(get_status_color)
                
                # Prepare display columns
                display_columns = ['icon', 'source_name', 'alert_status', 'alert_time']
                if 'capacity_percent' in alert_display.columns:
                    display_columns.insert(3, 'capacity_percent')
                if 'ph_level' in alert_display.columns:
                    display_columns.insert(4, 'ph_level')
                if 'source_type' in alert_display.columns:
                    display_columns.insert(2, 'source_type')
                if 'district' in alert_display.columns:
                    display_columns.insert(3, 'district')
                if 'state' in alert_display.columns:
                    display_columns.insert(4, 'state')
                
                # Filter to only existing columns
                display_columns = [col for col in display_columns if col in alert_display.columns]
                
                # Rename columns for display
                rename_map = {
                    'icon': '',
                    'source_name': 'Source',
                    'source_type': 'Type',
                    'district': 'District',
                    'state': 'State',
                    'capacity_percent': 'Capacity %',
                    'ph_level': 'pH',
                    'alert_status': 'Status',
                    'alert_time': 'Time'
                }
                rename_map = {k: v for k, v in rename_map.items() if k in display_columns}
                
                # Display alerts using st.dataframe
                column_config = {
                    '': st.column_config.TextColumn('', width='small'),
                }
                if 'capacity_percent' in display_columns:
                    column_config['capacity_percent'] = st.column_config.ProgressColumn(
                        'Capacity %',
                        format='%d%%',
                        min_value=0,
                        max_value=100
                    )
                
                st.dataframe(
                    alert_display[display_columns].rename(columns=rename_map),
                    use_container_width=True,
                    hide_index=True,
                    column_config=column_config
                )
                
                # Alternative: Use expanders for each alert
                st.markdown("### Detailed Alert List")
                for idx, alert in filtered_alerts.iterrows():
                    source_info = None
                    if not sources.empty and 'source_name' in sources.columns:
                        source_matches = sources[sources['source_name'] == alert.get('source_name', '')]
                        if not source_matches.empty:
                            source_info = source_matches.iloc[0]
                    
                    alert_status = alert.get('alert_status', 'UNKNOWN')
                    
                    if alert_status == 'CRITICAL':
                        status_color = "#ff4444"
                        status_emoji = "🔴"
                    elif alert_status == 'WARNING':
                        status_color = "#ffd700"
                        status_emoji = "🟡"
                    else:
                        status_color = "#00ff9d"
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
                            
                            # Fixed status display - no index error
                            if alert_status == 'CRITICAL':
                                st.markdown(f"**Status:** :red[{alert_status}]")
                            elif alert_status == 'WARNING':
                                st.markdown(f"**Status:** :orange[{alert_status}]")
                            else:
                                st.markdown(f"**Status:** :green[{alert_status}]")
        else:
            st.info("Alert status information not available")
    else:
        st.success("✅ No active alerts - All systems normal")
        st.balloons()

# =====================
# TAB 5: DATA TABLES
# =====================

with tab5:
    st.subheader("📋 Data Explorer")
    
    table_choice = st.selectbox(
        "Select Table to View",
        ["Water Sources", "Monitoring Stations", "Groundwater Levels", 
         "Rainfall History", "Water Usage", "Active Alerts", "Regional Statistics"]
    )
    
    if table_choice == "Water Sources":
        if not filtered_sources.empty:
            available_cols = []
            for col in ['source_name', 'source_type', 'capacity_percent', 'max_capacity_mcm', 
                       'build_year', 'age', 'state', 'district', 'origin_state', 
                       'is_transboundary', 'risk_level']:
                if col in filtered_sources.columns:
                    available_cols.append(col)
            display_df = filtered_sources[available_cols].copy() if available_cols else filtered_sources.copy()
        else:
            display_df = pd.DataFrame()
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Sources", len(display_df))
        with col2:
            avg_cap = display_df['capacity_percent'].mean() if not display_df.empty and 'capacity_percent' in display_df.columns else 0
            st.metric("Avg Capacity", f"{avg_cap:.1f}%" if not display_df.empty else "0%")
        with col3:
            transboundary = len(display_df[display_df['is_transboundary'] == 1]) if not display_df.empty and 'is_transboundary' in display_df.columns else 0
            st.metric("Transboundary", transboundary)
        
        if not display_df.empty:
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"water_sources_{selected_state}_{selected_district}_{selected_type}.csv", "text/csv")
    
    elif table_choice == "Monitoring Stations":
        display_df = stations.copy()
        if selected_state != "All States" and 'state_name' in display_df.columns:
            display_df = display_df[display_df['state_name'] == selected_state]
        if selected_district != "All Districts" and 'district_name' in display_df.columns:
            display_df = display_df[display_df['district_name'] == selected_district]
        
        if not display_df.empty:
            available_cols = []
            for col in ['station_name', 'state_name', 'district_name', 'latitude', 'longitude', 
                       'ph_level', 'dissolved_oxygen_mg_l', 'turbidity_ntu', 'status']:
                if col in display_df.columns:
                    available_cols.append(col)
            display_df = display_df[available_cols] if available_cols else display_df
        else:
            display_df = pd.DataFrame()
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Stations", len(display_df))
        with col2:
            active = len(display_df[display_df['status'] == 'Active']) if not display_df.empty and 'status' in display_df.columns else 0
            st.metric("Active Stations", active)
        with col3:
            maintenance = len(display_df[display_df['status'] == 'Maintenance']) if not display_df.empty and 'status' in display_df.columns else 0
            st.metric("Maintenance", maintenance)
        
        if not display_df.empty:
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"monitoring_stations_{selected_state}_{selected_district}.csv", "text/csv")
    
    elif table_choice == "Groundwater Levels":
        display_df = groundwater.copy()
        if selected_district != "All Districts" and 'district_name' in display_df.columns:
            display_df = display_df[display_df['district_name'] == selected_district]
        
        if not display_df.empty:
            available_cols = []
            for col in ['district_name', 'avg_depth_meters', 'extraction_pct', 'recharge_rate_mcm', 'assessment_year', 'stress_level']:
                if col in display_df.columns:
                    available_cols.append(col)
            display_df = display_df[available_cols] if available_cols else display_df
        else:
            display_df = pd.DataFrame()
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Districts", len(display_df))
        with col2:
            avg_depth = display_df['avg_depth_meters'].mean() if not display_df.empty and 'avg_depth_meters' in display_df.columns else 0
            st.metric("Avg Depth", f"{avg_depth:.1f} m")
        with col3:
            high_stress = len(display_df[display_df['stress_level'] == 'High']) if not display_df.empty and 'stress_level' in display_df.columns else 0
            st.metric("High Stress", high_stress)
        
        if not display_df.empty:
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"groundwater_{selected_district}.csv", "text/csv")
    
    elif table_choice == "Rainfall History":
        display_df = rainfall.copy()
        if selected_district != "All Districts" and 'district_name' in display_df.columns:
            display_df = display_df[display_df['district_name'] == selected_district]
        
        if not display_df.empty:
            available_cols = []
            for col in ['district_name', 'rainfall_cm', 'record_year', 'season', 'rainfall_category']:
                if col in display_df.columns:
                    available_cols.append(col)
            display_df = display_df[available_cols] if available_cols else display_df
        else:
            display_df = pd.DataFrame()
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Records", len(display_df))
        with col2:
            avg_rain = display_df['rainfall_cm'].mean() if not display_df.empty and 'rainfall_cm' in display_df.columns else 0
            st.metric("Avg Rainfall", f"{avg_rain:.1f} cm")
        with col3:
            years = display_df['record_year'].nunique() if not display_df.empty and 'record_year' in display_df.columns else 0
            st.metric("Years of Data", years)
        
        if not display_df.empty:
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"rainfall_{selected_district}.csv", "text/csv")
    
    elif table_choice == "Water Usage":
        display_df = usage.copy()
        if selected_state != "All States" and 'state' in display_df.columns:
            display_df = display_df[display_df['state'] == selected_state]
        if selected_district != "All Districts" and 'district' in display_df.columns:
            display_df = display_df[display_df['district'] == selected_district]
        if selected_type != "All Types" and 'source_type' in display_df.columns:
            display_df = display_df[display_df['source_type'] == selected_type]
        
        if not display_df.empty:
            available_cols = []
            for col in ['source_name', 'source_type', 'sector', 'sub_sector', 'consumer_name', 
                       'consumption_mcm', 'record_year', 'season', 'state', 'district']:
                if col in display_df.columns:
                    available_cols.append(col)
            display_df = display_df[available_cols] if available_cols else display_df
        else:
            display_df = pd.DataFrame()
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Records", len(display_df))
        with col2:
            total_consumption = display_df['consumption_mcm'].sum() if not display_df.empty and 'consumption_mcm' in display_df.columns else 0
            st.metric("Total Consumption", f"{total_consumption:.1f} MCM")
        with col3:
            avg_consumption = display_df['consumption_mcm'].mean() if not display_df.empty and 'consumption_mcm' in display_df.columns else 0
            st.metric("Avg Consumption", f"{avg_consumption:.1f} MCM")
        
        if not display_df.empty:
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"water_usage_{selected_state}_{selected_district}.csv", "text/csv")
    
    elif table_choice == "Active Alerts":
        display_df = alerts.copy()
        if not sources.empty and 'source_name' in sources.columns:
            if selected_state != "All States" and 'state' in sources.columns:
                state_sources = sources[sources['state'] == selected_state]['source_name'].tolist()
                if 'source_name' in display_df.columns:
                    display_df = display_df[display_df['source_name'].isin(state_sources)]
            if selected_district != "All Districts" and 'district' in sources.columns:
                district_sources = sources[sources['district'] == selected_district]['source_name'].tolist()
                if 'source_name' in display_df.columns:
                    display_df = display_df[display_df['source_name'].isin(district_sources)]
        
        if not display_df.empty:
            available_cols = []
            for col in ['source_name', 'capacity_percent', 'ph_level', 'alert_status', 'alert_time']:
                if col in display_df.columns:
                    available_cols.append(col)
            display_df = display_df[available_cols] if available_cols else display_df
        else:
            display_df = pd.DataFrame()
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Alerts", len(display_df))
        with col2:
            critical = len(display_df[display_df['alert_status'] == 'CRITICAL']) if not display_df.empty and 'alert_status' in display_df.columns else 0
            st.metric("Critical", critical)
        with col3:
            warning = len(display_df[display_df['alert_status'] == 'WARNING']) if not display_df.empty and 'alert_status' in display_df.columns else 0
            st.metric("Warning", warning)
        
        if not display_df.empty:
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"active_alerts_{selected_state}_{selected_district}.csv", "text/csv")
    
    elif table_choice == "Regional Statistics":
        display_df = regional.copy()
        if not display_df.empty:
            available_cols = []
            for col in ['region_name', 'population_count', 'annual_rainfall_avg_cm']:
                if col in display_df.columns:
                    available_cols.append(col)
            display_df = display_df[available_cols] if available_cols else display_df
        else:
            display_df = pd.DataFrame()
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Regions", len(display_df))
        with col2:
            total_pop = display_df['population_count'].sum() if not display_df.empty and 'population_count' in display_df.columns else 0
            st.metric("Total Population", f"{total_pop:,}")
        with col3:
            avg_rain = display_df['annual_rainfall_avg_cm'].mean() if not display_df.empty and 'annual_rainfall_avg_cm' in display_df.columns else 0
            st.metric("Avg Rainfall", f"{avg_rain:.1f} cm")
        
        if not display_df.empty:
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, "regional_statistics.csv", "text/csv")

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
    }
    
    # Add other data if available and related
    if not filtered_sources.empty and 'district' in filtered_sources.columns:
        if not groundwater.empty and 'district_name' in groundwater.columns:
            export_data['groundwater'] = groundwater[groundwater['district_name'].isin(filtered_sources['district'].unique())]
        if not rainfall.empty and 'district_name' in rainfall.columns:
            export_data['rainfall'] = rainfall[rainfall['district_name'].isin(filtered_sources['district'].unique())]
        if not usage.empty and 'source_id' in usage.columns and 'source_id' in filtered_sources.columns:
            export_data['usage'] = usage[usage['source_id'].isin(filtered_sources['source_id'])]
        if not alerts.empty and 'source_name' in alerts.columns and 'source_name' in filtered_sources.columns:
            export_data['alerts'] = alerts[alerts['source_name'].isin(filtered_sources['source_name'])]
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in export_data.items():
            if not df.empty:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
    
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
        <p style="color: #00e5ff; font-size: 1.2rem;">💧 AQUASTAT</p>
        <p style="color: #8892b0;">National Water Command Center</p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div style="text-align: center;">
        <p style="color: #8892b0;">Data Source: Ministry of Jal Shakti</p>
        <p style="color: #8892b0;">Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
    <div style="text-align: center;">
        <p style="color: #8892b0;">© 2025 All Rights Reserved</p>
        <p style="color: #8892b0;">Version 3.0 | For Official Use</p>
    </div>
    """, unsafe_allow_html=True)

st.markdown("""
<div style="position: fixed; bottom: 10px; right: 10px; background: rgba(0,229,255,0.1); padding: 5px 10px; border-radius: 5px; font-size: 0.8rem;">
    🔄 Data refreshes every 5 minutes
</div>
""", unsafe_allow_html=True)
