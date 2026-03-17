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
        # Water Sources
        sources = pd.read_sql_query("SELECT * FROM water_sources", conn)
        
        # Monitoring Stations
        stations = pd.read_sql_query("SELECT * FROM water_monitoring_stations", conn)
        
        # Groundwater Levels
        groundwater = pd.read_sql_query("SELECT * FROM groundwater_levels", conn)
        
        # Rainfall History
        rainfall = pd.read_sql_query("SELECT * FROM rainfall_history", conn)
        
        # Active Alerts
        alerts = pd.read_sql_query("SELECT * FROM active_alerts", conn)
        
        # Water Usage
        usage = pd.read_sql_query("""
            SELECT wu.*, ws.source_name, ws.source_type, ws.state, ws.district 
            FROM water_usage_history wu
            LEFT JOIN water_sources ws ON wu.source_id = ws.source_id
        """, conn)
        
        # Regional Stats
        regional = pd.read_sql_query("SELECT * FROM regional_stats", conn)
        
        return sources, stations, groundwater, rainfall, alerts, usage, regional
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# -------------------------
# LOAD DATA
# -------------------------

with st.spinner("🚀 Loading AQUASTAT Command Center..."):
    sources, stations, groundwater, rainfall, alerts, usage, regional = load_all_data()

# -------------------------
# DATA PROCESSING
# -------------------------

current_year = datetime.now().year

# Process Sources
if not sources.empty:
    # Convert numeric columns
    for col in ['capacity_percent', 'build_year']:
        if col in sources.columns:
            sources[col] = pd.to_numeric(sources[col], errors='coerce')
    
    # Calculate age
    sources['age'] = current_year - sources['build_year']
    sources['age'] = sources['age'].clip(0, 200)
    
    # Calculate health score
    sources['health_score'] = (
        sources['capacity_percent'].fillna(50) * 0.4 + 
        (100 - sources['age'].clip(0, 100).fillna(50)) * 0.3 + 
        30
    ).clip(0, 100)
    
    # Risk classification
    sources['risk_level'] = pd.cut(
        sources['capacity_percent'],
        bins=[0, 30, 60, 100],
        labels=['Critical', 'Moderate', 'Good'],
        include_lowest=True
    )

# Add coordinates from monitoring stations to sources
def add_coordinates_to_sources(sources_df, stations_df):
    """Add coordinates from monitoring stations to water sources"""
    
    if sources_df.empty or stations_df.empty:
        return sources_df
    
    df = sources_df.copy()
    
    # Create a mapping of district to first available coordinates
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
if not groundwater.empty:
    groundwater['stress_level'] = pd.cut(
        groundwater['avg_depth_meters'],
        bins=[0, 20, 40, 100],
        labels=['Low', 'Moderate', 'High']
    )

# Process Rainfall
if not rainfall.empty:
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
    if available_years:
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
    
    districts = ['All Districts'] + sorted(districts.tolist())
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
    if selected_state != "All States":
        filtered_sources = filtered_sources[filtered_sources['state'] == selected_state]
    
    # Apply district filter
    if selected_district != "All Districts":
        filtered_sources = filtered_sources[filtered_sources['district'] == selected_district]
    
    # Apply source type filter
    if selected_type != "All Types":
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
    
    if selected_state != "All States":
        filtered_stations = filtered_stations[filtered_stations['state_name'] == selected_state]
    
    if selected_district != "All Districts":
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
    avg_cap = filtered_sources['capacity_percent'].mean() if not filtered_sources.empty else 0
    st.metric("Avg Capacity", f"{avg_cap:.1f}%")

with col3:
    critical = len(filtered_sources[filtered_sources['capacity_percent'] < 30]) if not filtered_sources.empty else 0
    st.metric("Critical Sources", f"{critical}", delta_color="inverse")

with col4:
    sources_with_coords = len(filtered_sources[filtered_sources['latitude'].notna()]) if not filtered_sources.empty else 0
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
        
        # Show sample of all data
        with st.expander("📋 Show all sources sample"):
            st.dataframe(sources[['source_name', 'source_type', 'state', 'district', 'capacity_percent']].head(20))
    
    else:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Capacity Distribution")
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
        
        with col2:
            st.subheader("🏭 Source Types")
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
        
        # Second row
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📈 Groundwater Stress Levels")
            if not groundwater.empty:
                # Filter groundwater based on selected district if applicable
                filtered_gw = groundwater.copy()
                if selected_district != "All Districts":
                    filtered_gw = filtered_gw[filtered_gw['district_name'] == selected_district]
                
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
                st.info("ℹ️ No groundwater data available")
        
        with col2:
            st.subheader("☔ Rainfall by Season")
            if not rainfall.empty:
                # Filter rainfall based on selected district if applicable
                filtered_rain = rainfall.copy()
                if selected_district != "All Districts":
                    filtered_rain = filtered_rain[filtered_rain['district_name'] == selected_district]
                
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
                st.info("ℹ️ No rainfall data available")
        
        # Third row - Risk distribution
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
    if not filtered_sources.empty and selected_district != "All Districts":
        district_sources = filtered_sources[filtered_sources['latitude'].notna()]
        if not district_sources.empty:
            center_lat = district_sources['latitude'].mean()
            center_lon = district_sources['longitude'].mean()
            zoom = 9
        else:
            center_lat, center_lon, zoom = 20.5937, 78.9629, 5
    elif not filtered_sources.empty and selected_state != "All States":
        state_sources = filtered_sources[filtered_sources['latitude'].notna()]
        if not state_sources.empty:
            center_lat = state_sources['latitude'].mean()
            center_lon = state_sources['longitude'].mean()
            zoom = 7
        else:
            center_lat, center_lon, zoom = 20.5937, 78.9629, 5
    else:
        center_lat, center_lon, zoom = 20.5937, 78.9629, 5
    
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
    sources_with_coords = filtered_sources[
        filtered_sources['latitude'].notna() & 
        filtered_sources['longitude'].notna()
    ]
    
    if not sources_with_coords.empty:
        for _, source in sources_with_coords.iterrows():
            # Determine color and risk text based on capacity
            if source['capacity_percent'] < 30:
                color = '#ff4444'
                risk_text = "CRITICAL"
            elif source['capacity_percent'] < 60:
                color = '#ffd700'
                risk_text = "MODERATE"
            else:
                color = '#00ff9d'
                risk_text = "GOOD"
            
            heat_data.append([source['latitude'], source['longitude']])
            sources_on_map += 1
            
            popup_html = f"""
            <div style="font-family: Arial; min-width: 250px;">
                <h4 style="color: {color}; margin:0;">{source['source_name']}</h4>
                <hr style="margin:5px 0;">
                <table style="width:100%;">
                    <tr><td><b>Type:</b></td><td>{source['source_type']}</td></tr>
                    <tr><td><b>District:</b></td><td>{source['district']}</td></tr>
                    <tr><td><b>State:</b></td><td>{source['state']}</td></tr>
                    <tr><td><b>Capacity:</b></td><td>{source['capacity_percent']:.1f}%</td></tr>
                    <tr><td><b>Age:</b></td><td>{source['age']:.0f} years</td></tr>
                    <tr><td><b>Risk Level:</b></td><td><span style="color:{color}; font-weight:bold;">{risk_text}</span></td></tr>
                </table>
            </div>
            """
            
            marker = folium.CircleMarker(
                location=[source['latitude'], source['longitude']],
                radius=marker_size + (3 if source['capacity_percent'] < 30 else 0),
                color=color,
                fill=True,
                fillOpacity=0.7,
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"{source['source_name']} - {source['capacity_percent']:.0f}%"
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
        stations_with_coords = filtered_stations[
            filtered_stations['latitude'].notna() & 
            filtered_stations['longitude'].notna()
        ]
        for _, station in stations_with_coords.iterrows():
            if station['status'] == 'Active':
                station_color = 'green'
            elif station['status'] == 'Maintenance':
                station_color = 'orange'
            else:
                station_color = 'red'
            
            station_popup = f"""
            <b>{station['station_name']}</b><br>
            District: {station['district_name']}<br>
            Status: {station['status']}<br>
            pH: {station['ph_level']}<br>
            DO: {station['dissolved_oxygen_mg_l']} mg/L<br>
            Turbidity: {station['turbidity_ntu']} NTU
            """
            
            folium.Marker(
                location=[station['latitude'], station['longitude']],
                icon=folium.Icon(color=station_color, icon='info-sign'),
                popup=folium.Popup(station_popup, max_width=300),
                tooltip=f"Station: {station['station_name']}"
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
            if not rainfall.empty:
                filtered_rain = rainfall.copy()
                if selected_district != "All Districts":
                    filtered_rain = filtered_rain[filtered_rain['district_name'] == selected_district]
                
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
        
        with col2:
            st.subheader("Groundwater Trend")
            if not groundwater.empty and 'assessment_year' in groundwater.columns:
                filtered_gw = groundwater.copy()
                if selected_district != "All Districts":
                    filtered_gw = filtered_gw[filtered_gw['district_name'] == selected_district]
                
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
    
    with atab2:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Capacity by State")
            if not filtered_sources.empty and 'state' in filtered_sources.columns:
                state_cap = filtered_sources.groupby('state')['capacity_percent'].mean().sort_values(ascending=False)
                if len(state_cap) > 10:
                    state_cap = state_cap.head(10)
                
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
        
        with col2:
            st.subheader("Extraction vs Recharge")
            if not groundwater.empty:
                filtered_gw = groundwater.copy()
                if selected_district != "All Districts":
                    filtered_gw = filtered_gw[filtered_gw['district_name'] == selected_district]
                
                fig = px.scatter(
                    filtered_gw,
                    x='recharge_rate_mcm',
                    y='extraction_pct',
                    size='avg_depth_meters',
                    color='district_name',
                    title="Groundwater Extraction vs Recharge Rate",
                    template="plotly_dark"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No groundwater data available")
    
    with atab3:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Statistical Summary")
            if not filtered_sources.empty:
                stats_df = filtered_sources[['capacity_percent', 'age', 'health_score']].describe()
                st.dataframe(stats_df.style.format("{:.2f}"), use_container_width=True)
            else:
                st.info("No source data available")
        
        with col2:
            st.subheader("Correlation Matrix")
            if not filtered_sources.empty and not groundwater.empty:
                merged = filtered_sources.merge(
                    groundwater,
                    left_on='district',
                    right_on='district_name',
                    how='inner'
                )
                
                if not merged.empty:
                    numeric_cols = ['capacity_percent', 'age', 'avg_depth_meters', 'extraction_pct', 'recharge_rate_mcm']
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
                    st.info("No matching data for correlation")
            else:
                st.info("Insufficient data for correlation")

# =====================
# TAB 4: ALERTS (Fixed - No index error)
# =====================

with tab4:
    st.subheader("🚨 Active Alerts and Warnings")
    
    if not alerts.empty:
        # Count alerts by status
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
        if selected_state != "All States" and not sources.empty:
            state_sources = sources[sources['state'] == selected_state]['source_name'].tolist()
            filtered_alerts = filtered_alerts[filtered_alerts['source_name'].isin(state_sources)]
        
        if selected_district != "All Districts" and not sources.empty:
            district_sources = sources[sources['district'] == selected_district]['source_name'].tolist()
            filtered_alerts = filtered_alerts[filtered_alerts['source_name'].isin(district_sources)]
        
        if selected_type != "All Types" and not sources.empty:
            type_sources = sources[sources['source_type'] == selected_type]['source_name'].tolist()
            filtered_alerts = filtered_alerts[filtered_alerts['source_name'].isin(type_sources)]
        
        if filtered_alerts.empty:
            st.info("ℹ️ No alerts match the current filters")
        else:
            # Create a DataFrame for alerts display
            alert_display = filtered_alerts.copy()
            
            # Add source information
            if not sources.empty:
                alert_display = alert_display.merge(
                    sources[['source_name', 'source_type', 'district', 'state']],
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
            
            alert_display['icon'] = alert_display['alert_status'].apply(get_status_color)
            
            # Prepare display columns
            display_columns = ['icon', 'source_name', 'alert_status', 'capacity_percent', 'ph_level', 'alert_time']
            if 'source_type' in alert_display.columns:
                display_columns.insert(3, 'source_type')
            if 'district' in alert_display.columns:
                display_columns.insert(4, 'district')
            if 'state' in alert_display.columns:
                display_columns.insert(5, 'state')
            
            # Display alerts using st.dataframe
            st.dataframe(
                alert_display[display_columns].rename(columns={
                    'icon': '',
                    'source_name': 'Source',
                    'source_type': 'Type',
                    'district': 'District',
                    'state': 'State',
                    'capacity_percent': 'Capacity %',
                    'ph_level': 'pH',
                    'alert_status': 'Status',
                    'alert_time': 'Time'
                }),
                use_container_width=True,
                hide_index=True,
                column_config={
                    '': st.column_config.TextColumn('', width='small'),
                    'Capacity %': st.column_config.ProgressColumn(
                        'Capacity %',
                        format='%d%%',
                        min_value=0,
                        max_value=100
                    )
                }
            )
            
            # Alternative: Use expanders for each alert
            st.markdown("### Detailed Alert List")
            for idx, alert in filtered_alerts.iterrows():
                source_info = None
                if not sources.empty:
                    source_matches = sources[sources['source_name'] == alert['source_name']]
                    if not source_matches.empty:
                        source_info = source_matches.iloc[0]
                
                if alert['alert_status'] == 'CRITICAL':
                    status_color = "#ff4444"
                    status_emoji = "🔴"
                elif alert['alert_status'] == 'WARNING':
                    status_color = "#ffd700"
                    status_emoji = "🟡"
                else:
                    status_color = "#00ff9d"
                    status_emoji = "🟢"
                
                with st.expander(f"{status_emoji} {alert['source_name']} - {alert['alert_status']}", expanded=False):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**Type:** {source_info['source_type'] if source_info is not None else 'Unknown'}")
                        st.markdown(f"**Location:** {source_info['district'] if source_info is not None else 'Unknown'}, {source_info['state'] if source_info is not None else 'Unknown'}")
                        st.markdown(f"**Capacity:** {alert['capacity_percent']}%")
                        st.progress(alert['capacity_percent']/100)
                    
                    with col2:
                        st.markdown(f"**pH Level:** {alert['ph_level']}")
                        st.markdown(f"**Time:** {alert['alert_time']}")
                        
                        # Fixed status display - no index error
                        if alert['alert_status'] == 'CRITICAL':
                            st.markdown(f"**Status:** :red[{alert['alert_status']}]")
                        elif alert['alert_status'] == 'WARNING':
                            st.markdown(f"**Status:** :orange[{alert['alert_status']}]")
                        else:
                            st.markdown(f"**Status:** :green[{alert['alert_status']}]")
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
            display_df = filtered_sources[[
                'source_name', 'source_type', 'capacity_percent', 
                'max_capacity_mcm', 'build_year', 'age', 'state', 
                'district', 'origin_state', 'is_transboundary', 'risk_level'
            ]].copy()
        else:
            display_df = pd.DataFrame()
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Sources", len(display_df))
        with col2:
            st.metric("Avg Capacity", f"{display_df['capacity_percent'].mean():.1f}%" if not display_df.empty else "0%")
        with col3:
            transboundary = len(display_df[display_df['is_transboundary'] == 1]) if not display_df.empty else 0
            st.metric("Transboundary", transboundary)
        
        if not display_df.empty:
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"water_sources_{selected_state}_{selected_district}_{selected_type}.csv", "text/csv")
    
    elif table_choice == "Monitoring Stations":
        display_df = stations.copy()
        if selected_state != "All States":
            display_df = display_df[display_df['state_name'] == selected_state]
        if selected_district != "All Districts":
            display_df = display_df[display_df['district_name'] == selected_district]
        
        if not display_df.empty:
            display_df = display_df[[
                'station_name', 'state_name', 'district_name', 
                'latitude', 'longitude', 'ph_level', 
                'dissolved_oxygen_mg_l', 'turbidity_ntu', 'status'
            ]]
        else:
            display_df = pd.DataFrame()
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Stations", len(display_df))
        with col2:
            active = len(display_df[display_df['status'] == 'Active']) if not display_df.empty else 0
            st.metric("Active Stations", active)
        with col3:
            maintenance = len(display_df[display_df['status'] == 'Maintenance']) if not display_df.empty else 0
            st.metric("Maintenance", maintenance)
        
        if not display_df.empty:
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"monitoring_stations_{selected_state}_{selected_district}.csv", "text/csv")
    
    elif table_choice == "Groundwater Levels":
        display_df = groundwater.copy()
        if selected_district != "All Districts":
            display_df = display_df[display_df['district_name'] == selected_district]
        
        if not display_df.empty:
            display_df = display_df[[
                'district_name', 'avg_depth_meters', 'extraction_pct',
                'recharge_rate_mcm', 'assessment_year', 'stress_level'
            ]]
        else:
            display_df = pd.DataFrame()
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Districts", len(display_df))
        with col2:
            avg_depth = display_df['avg_depth_meters'].mean() if not display_df.empty else 0
            st.metric("Avg Depth", f"{avg_depth:.1f} m")
        with col3:
            high_stress = len(display_df[display_df['stress_level'] == 'High']) if not display_df.empty else 0
            st.metric("High Stress", high_stress)
        
        if not display_df.empty:
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"groundwater_{selected_district}.csv", "text/csv")
    
    elif table_choice == "Rainfall History":
        display_df = rainfall.copy()
        if selected_district != "All Districts":
            display_df = display_df[display_df['district_name'] == selected_district]
        
        if not display_df.empty:
            display_df = display_df[[
                'district_name', 'rainfall_cm', 'record_year', 
                'season', 'rainfall_category'
            ]]
        else:
            display_df = pd.DataFrame()
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Records", len(display_df))
        with col2:
            avg_rain = display_df['rainfall_cm'].mean() if not display_df.empty else 0
            st.metric("Avg Rainfall", f"{avg_rain:.1f} cm")
        with col3:
            years = display_df['record_year'].nunique() if not display_df.empty else 0
            st.metric("Years of Data", years)
        
        if not display_df.empty:
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"rainfall_{selected_district}.csv", "text/csv")
    
    elif table_choice == "Water Usage":
        display_df = usage.copy()
        if selected_state != "All States":
            display_df = display_df[display_df['state'] == selected_state]
        if selected_district != "All Districts":
            display_df = display_df[display_df['district'] == selected_district]
        if selected_type != "All Types":
            display_df = display_df[display_df['source_type'] == selected_type]
        
        if not display_df.empty and all(col in display_df.columns for col in ['source_name', 'source_type', 'sector', 'sub_sector', 'consumer_name', 'consumption_mcm', 'record_year', 'season', 'state', 'district']):
            display_df = display_df[[
                'source_name', 'source_type', 'sector', 'sub_sector',
                'consumer_name', 'consumption_mcm', 'record_year',
                'season', 'state', 'district'
            ]]
        else:
            display_df = pd.DataFrame()
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Records", len(display_df))
        with col2:
            total_consumption = display_df['consumption_mcm'].sum() if not display_df.empty else 0
            st.metric("Total Consumption", f"{total_consumption:.1f} MCM")
        with col3:
            avg_consumption = display_df['consumption_mcm'].mean() if not display_df.empty else 0
            st.metric("Avg Consumption", f"{avg_consumption:.1f} MCM")
        
        if not display_df.empty:
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"water_usage_{selected_state}_{selected_district}.csv", "text/csv")
    
    elif table_choice == "Active Alerts":
        display_df = alerts.copy()
        if not sources.empty:
            if selected_state != "All States":
                state_sources = sources[sources['state'] == selected_state]['source_name'].tolist()
                display_df = display_df[display_df['source_name'].isin(state_sources)]
            if selected_district != "All Districts":
                district_sources = sources[sources['district'] == selected_district]['source_name'].tolist()
                display_df = display_df[display_df['source_name'].isin(district_sources)]
        
        if not display_df.empty:
            display_df = display_df[[
                'source_name', 'capacity_percent', 'ph_level',
                'alert_status', 'alert_time'
            ]]
        else:
            display_df = pd.DataFrame()
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Alerts", len(display_df))
        with col2:
            critical = len(display_df[display_df['alert_status'] == 'CRITICAL']) if not display_df.empty else 0
            st.metric("Critical", critical)
        with col3:
            warning = len(display_df[display_df['alert_status'] == 'WARNING']) if not display_df.empty else 0
            st.metric("Warning", warning)
        
        if not display_df.empty:
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download CSV", csv, f"active_alerts_{selected_state}_{selected_district}.csv", "text/csv")
    
    elif table_choice == "Regional Statistics":
        display_df = regional.copy()
        if not display_df.empty and all(col in display_df.columns for col in ['region_name', 'population_count', 'annual_rainfall_avg_cm']):
            display_df = display_df[[
                'region_name', 'population_count', 'annual_rainfall_avg_cm'
            ]]
        else:
            display_df = pd.DataFrame()
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Regions", len(display_df))
        with col2:
            total_pop = display_df['population_count'].sum() if not display_df.empty else 0
            st.metric("Total Population", f"{total_pop:,}")
        with col3:
            avg_rain = display_df['annual_rainfall_avg_cm'].mean() if not display_df.empty else 0
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
    - On Map: {len(filtered_sources[filtered_sources['latitude'].notna()]) if not filtered_sources.empty else 0}
    """)

# =====================
# EXPORT ALL FILTERED DATA
# =====================

st.sidebar.markdown("---")
if st.sidebar.button("📦 Export All Filtered Data", use_container_width=True):
    export_data = {
        'water_sources': filtered_sources,
        'monitoring_stations': filtered_stations,
        'groundwater': groundwater[groundwater['district_name'].isin(filtered_sources['district'].unique())] if not filtered_sources.empty else pd.DataFrame(),
        'rainfall': rainfall[rainfall['district_name'].isin(filtered_sources['district'].unique())] if not filtered_sources.empty else pd.DataFrame(),
        'usage': usage[usage['source_id'].isin(filtered_sources['source_id'])] if not filtered_sources.empty else pd.DataFrame(),
        'alerts': alerts[alerts['source_name'].isin(filtered_sources['source_name'])] if not filtered_sources.empty else pd.DataFrame()
    }
    
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

# Close database connection when app is done (optional - SQLite handles this automatically)
# conn.close() if conn else None