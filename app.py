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
    .main { background: linear-gradient(135deg, #0a0f1e 0%, #0d1a2b 100%); color: #e6f1ff; }
    .metric-card { background: rgba(10, 25, 47, 0.7); border: 1px solid #1e3a5f; border-radius: 10px; padding: 20px; margin: 10px 0; box-shadow: 0 4px 6px rgba(0,0,0,0.3); backdrop-filter: blur(10px); }
    .user-badge { background: linear-gradient(135deg, #00b09b, #96c93d); color: white; padding: 5px 15px; border-radius: 20px; font-weight: bold; text-align: center; display: inline-block; }
    .alert-critical { background: linear-gradient(135deg, #ff416c, #ff4b2b); color: white; padding: 5px 15px; border-radius: 20px; font-weight: bold; text-align: center; animation: pulse 2s infinite; }
    .alert-warning { background: linear-gradient(135deg, #f7971e, #ffd200); color: black; padding: 5px 15px; border-radius: 20px; font-weight: bold; text-align: center; }
    .alert-good { background: linear-gradient(135deg, #00b09b, #96c93d); color: white; padding: 5px 15px; border-radius: 20px; font-weight: bold; text-align: center; }
    @keyframes pulse { 0% { opacity: 1; transform: scale(1); } 50% { opacity: 0.8; transform: scale(1.05); } 100% { opacity: 1; transform: scale(1); } }
    .status-dot { height: 10px; width: 10px; border-radius: 50%; display: inline-block; margin-right: 5px; }
    .status-critical { background-color: #ff4444; box-shadow: 0 0 10px #ff4444; }
    .status-warning { background-color: #ffd700; box-shadow: 0 0 10px #ffd700; }
    .status-good { background-color: #00ff9d; box-shadow: 0 0 10px #00ff9d; }
    .section-header { color: #00e5ff; font-size: 1.5rem; font-weight: 600; margin: 20px 0; padding-bottom: 10px; border-bottom: 2px solid #1e3a5f; text-transform: uppercase; letter-spacing: 2px; }
    ::-webkit-scrollbar { width: 8px; height: 8px; }
    ::-webkit-scrollbar-track { background: #0a192f; }
    ::-webkit-scrollbar-thumb { background: #1e3a5f; border-radius: 4px; }
    ::-webkit-scrollbar-thumb:hover { background: #2b4b7a; }
</style>
""", unsafe_allow_html=True)

# -------------------------
# NEON CLOUD DATABASE CONNECTION
# -------------------------

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
            NEON_URL,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True,
            connect_args={'connect_timeout': 10, 'keepalives': 1, 'keepalives_idle': 30, 'keepalives_interval': 10, 'keepalives_count': 5, 'sslmode': 'require'}
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

# -------------------------
# DATA LOADING FUNCTIONS
# -------------------------

@st.cache_data(ttl=300)
def load_all_data():
    if engine is None:
        st.warning("⚠️ Database connection not available.")
        return [pd.DataFrame()] * 7
    
    try:
        with engine.connect() as conn:
            with conn.begin():
                tables_df = pd.read_sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", conn)
                tables = tables_df['table_name'].tolist() if not tables_df.empty else []
                
                def get_df(table_name):
                    if table_name in tables:
                        try:
                            return pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
                        except:
                            return pd.DataFrame()
                    return pd.DataFrame()
                
                sources = get_df('water_sources')
                stations = get_df('water_monitoring_stations')
                groundwater = get_df('groundwater_levels')
                rainfall = get_df('rainfall_history')
                alerts = get_df('active_alerts')
                regional = get_df('regional_stats')
                
                if 'water_usage_history' in tables and 'water_sources' in tables:
                    try:
                        usage = pd.read_sql("""SELECT wu.*, ws.source_name, ws.source_type, ws.state, ws.district FROM water_usage_history wu LEFT JOIN water_sources ws ON wu.source_id = ws.source_id""", conn)
                    except:
                        usage = pd.DataFrame()
                else:
                    usage = pd.DataFrame()
                
                return sources, stations, groundwater, rainfall, alerts, usage, regional
    except Exception as e:
        st.error(f"Error loading cloud data: {e}")
        return [pd.DataFrame()] * 7

with st.spinner("🚀 Connecting to AQUASTAT Cloud Database..."):
    sources, stations, groundwater, rainfall, alerts, usage, regional = load_all_data()

# -------------------------
# DATA PROCESSING
# -------------------------

current_year = datetime.now().year
current_time = datetime.now(pytz.timezone('Asia/Kolkata'))

if not sources.empty:
    for col in ['capacity_percent', 'build_year', 'max_capacity_mcm']:
        if col in sources.columns:
            sources[col] = pd.to_numeric(sources[col], errors='coerce')
    if 'build_year' in sources.columns:
        sources['age'] = (current_year - sources['build_year']).clip(0, 200)
    else:
        sources['age'] = 0
    if 'capacity_percent' in sources.columns:
        sources['health_score'] = (sources['capacity_percent'].fillna(50) * 0.5 + (100 - sources['age'].clip(0, 100).fillna(50)) * 0.3 + 20).clip(0, 100)
        sources['risk_level'] = pd.cut(sources['capacity_percent'], bins=[0, 30, 60, 100], labels=['Critical', 'Moderate', 'Good'], include_lowest=True)
    else:
        sources['risk_level'] = 'Unknown'

if not groundwater.empty and 'avg_depth_meters' in groundwater.columns:
    groundwater['stress_level'] = pd.cut(groundwater['avg_depth_meters'], bins=[0, 20, 40, 100], labels=['Low', 'Moderate', 'High'])

if not rainfall.empty and 'rainfall_cm' in rainfall.columns:
    rainfall['rainfall_category'] = pd.cut(rainfall['rainfall_cm'], bins=[0, 50, 150, 300, float('inf')], labels=['Low', 'Moderate', 'High', 'Extreme'])

def add_coordinates_to_sources(sources_df, stations_df):
    if sources_df.empty or stations_df.empty:
        return sources_df
    df = sources_df.copy()
    if 'district' in df.columns and 'district_name' in stations_df.columns and 'latitude' in stations_df.columns:
        stations_df['district_clean'] = stations_df['district_name'].str.strip().str.lower()
        district_coords = stations_df.groupby('district_clean').agg({'latitude': 'first', 'longitude': 'first'}).reset_index()
        df['district_clean'] = df['district'].str.strip().str.lower()
        df = df.merge(district_coords, on='district_clean', how='left').drop('district_clean', axis=1)
    return df

if not sources.empty and not stations.empty:
    sources = add_coordinates_to_sources(sources, stations)

# -------------------------
# SIDEBAR FILTERS
# -------------------------

with st.sidebar:
    st.title("🎮 AQUASTAT")
    st.caption("Command Interface v3.0")
    
    if engine is not None:
        st.success("✅ Cloud Connected")
    else:
        st.error("❌ Cloud Disconnected")
    
    st.markdown(f'<span class="user-badge">👤 VIEWER ACCESS</span>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    if st.button("🔄 Reset All Filters", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    st.markdown("---")
    
    # ========== TIME FILTERS - From/To Number Inputs ==========
    st.markdown("### 📅 Time Filters")
    
    if not sources.empty and 'build_year' in sources.columns:
        build_years = sources['build_year'].dropna()
        if not build_years.empty:
            default_min_year = int(build_years.min())
            default_max_year = int(build_years.max())
        else:
            default_min_year = 1800
            default_max_year = 2026
    else:
        default_min_year = 1800
        default_max_year = 2026
    
    col1, col2 = st.columns(2)
    with col1:
        year_from = st.number_input("From Year", min_value=1800, max_value=2026, value=default_min_year, step=1)
    with col2:
        year_to = st.number_input("To Year", min_value=1800, max_value=2026, value=default_max_year, step=1)
    year_range = (year_from, year_to)
    
    st.markdown("---")
    
    # ========== GEOGRAPHIC FILTERS ==========
    st.markdown("### 🌍 Geographic Filters")
    
    selected_state = "All States"
    if not sources.empty and 'state' in sources.columns:
        states = ['All States'] + sorted(sources['state'].dropna().unique().tolist())
        selected_state = st.selectbox("State", states)
    
    selected_district = "All Districts"
    if not sources.empty and 'district' in sources.columns:
        if selected_state != "All States":
            districts = sources[sources['state'] == selected_state]['district'].dropna().unique()
        else:
            districts = sources['district'].dropna().unique()
        districts_list = ['All Districts'] + sorted(districts.tolist()) if len(districts) > 0 else ['All Districts']
        selected_district = st.selectbox("District", districts_list)
    
    st.markdown("---")
    
    # ========== SOURCE FILTERS ==========
    st.markdown("### 💧 Source Filters")
    
    selected_type = "All Types"
    if not sources.empty and 'source_type' in sources.columns:
        source_types = ['All Types'] + sorted(sources['source_type'].dropna().unique().tolist())
        selected_type = st.selectbox("Source Type", source_types)
    
    capacity_range = (0, 100)
    if not sources.empty and 'capacity_percent' in sources.columns:
        cap_vals = sources['capacity_percent'].dropna()
        if not cap_vals.empty:
            min_cap = float(cap_vals.min())
            max_cap = float(cap_vals.max())
            capacity_range = st.slider("Capacity %", min_value=min_cap, max_value=max_cap, value=(min_cap, max_cap))
    
    selected_risk = "All Risk Levels"
    if not sources.empty and 'risk_level' in sources.columns:
        risk_options = ['All Risk Levels'] + list(sources['risk_level'].dropna().unique())
        selected_risk = st.selectbox("Risk Level", risk_options)
    
    st.markdown("---")
    
    # ========== RAINFALL FILTERS ==========
    st.markdown("### ☔ Rainfall Filters")
    selected_rainfall_district = "All Districts"
    if not rainfall.empty and 'district_name' in rainfall.columns:
        rain_districts = ['All Districts'] + sorted(rainfall['district_name'].dropna().unique().tolist())
        selected_rainfall_district = st.selectbox("Rainfall District", rain_districts, key="rainfall_district_filter")
    
    if not rainfall.empty and 'record_year' in rainfall.columns:
        rain_years = rainfall['record_year'].dropna()
        if not rain_years.empty:
            rain_min_year = int(rain_years.min())
            rain_max_year = int(rain_years.max())
        else:
            rain_min_year = 2000
            rain_max_year = 2026
    else:
        rain_min_year = 2000
        rain_max_year = 2026
    
    col1, col2 = st.columns(2)
    with col1:
        rain_year_from = st.number_input("Rainfall From", min_value=1900, max_value=2026, value=rain_min_year, key="rain_from")
    with col2:
        rain_year_to = st.number_input("Rainfall To", min_value=1900, max_value=2026, value=rain_max_year, key="rain_to")
    rain_year_range = (rain_year_from, rain_year_to)
    
    selected_rainfall_category = "All Categories"
    if not rainfall.empty and 'rainfall_category' in rainfall.columns:
        rain_cats = ['All Categories'] + sorted(rainfall['rainfall_category'].dropna().unique().tolist())
        selected_rainfall_category = st.selectbox("Rainfall Category", rain_cats, key="rain_category_filter")
    
    st.markdown("---")
    
    # ========== WATER QUALITY FILTERS ==========
    st.markdown("### 💧 Water Quality Filters")
    selected_ph_min = 0.0
    selected_ph_max = 14.0
    selected_do_min = 0.0
    selected_do_max = 20.0
    
    if not stations.empty and 'ph_level' in stations.columns:
        ph_vals = stations['ph_level'].dropna()
        if not ph_vals.empty:
            ph_min, ph_max = float(ph_vals.min()), float(ph_vals.max())
        else:
            ph_min, ph_max = 0.0, 14.0
        col1, col2 = st.columns(2)
        with col1:
            selected_ph_min = st.number_input("Min pH", min_value=0.0, max_value=14.0, value=ph_min, format="%.1f", key="ph_min")
        with col2:
            selected_ph_max = st.number_input("Max pH", min_value=0.0, max_value=14.0, value=ph_max, format="%.1f", key="ph_max")
    
    if not stations.empty and 'dissolved_oxygen_mg_l' in stations.columns:
        do_vals = stations['dissolved_oxygen_mg_l'].dropna()
        if not do_vals.empty:
            do_min, do_max = float(do_vals.min()), float(do_vals.max())
        else:
            do_min, do_max = 0.0, 20.0
        col1, col2 = st.columns(2)
        with col1:
            selected_do_min = st.number_input("Min DO (mg/L)", min_value=0.0, max_value=20.0, value=do_min, format="%.1f", key="do_min")
        with col2:
            selected_do_max = st.number_input("Max DO (mg/L)", min_value=0.0, max_value=20.0, value=do_max, format="%.1f", key="do_max")
    
    selected_station_status = "All Status"
    if not stations.empty and 'status' in stations.columns:
        status_opts = ['All Status'] + sorted(stations['status'].dropna().unique().tolist())
        selected_station_status = st.selectbox("Station Status", status_opts, key="station_status_filter")
    
    st.markdown("---")
    
    # ========== MAP SETTINGS ==========
    st.markdown("### 🗺️ Map Settings")
    map_style = st.selectbox("Map Style", ["Esri Satellite", "OpenStreetMap", "CartoDB Dark", "CartoDB Light"], index=0)
    show_heatmap = st.checkbox("Show Heatmap", True)
    show_clusters = st.checkbox("Show Clusters", True)
    show_stations = st.checkbox("Show Monitoring Stations", True)
    marker_size = st.slider("Marker Size", 5, 20, 12)
    st.markdown("---")
    st.caption(f"📊 Total Sources: {len(sources):,}")

# ========== APPLY FILTERS ==========

def apply_filters():
    f = sources.copy()
    if selected_state != "All States" and 'state' in f.columns:
        f = f[f['state'] == selected_state]
    if selected_district != "All Districts" and 'district' in f.columns:
        f = f[f['district'] == selected_district]
    if selected_type != "All Types" and 'source_type' in f.columns:
        f = f[f['source_type'] == selected_type]
    if 'build_year' in f.columns:
        f = f[(f['build_year'] >= year_range[0]) & (f['build_year'] <= year_range[1])]
    if 'capacity_percent' in f.columns:
        f = f[(f['capacity_percent'] >= capacity_range[0]) & (f['capacity_percent'] <= capacity_range[1])]
    if selected_risk != "All Risk Levels" and 'risk_level' in f.columns:
        f = f[f['risk_level'] == selected_risk]
    return f

def filter_stations():
    f = stations.copy()
    if selected_state != "All States" and 'state_name' in f.columns:
        f = f[f['state_name'] == selected_state]
    if selected_district != "All Districts" and 'district_name' in f.columns:
        f = f[f['district_name'] == selected_district]
    if 'ph_level' in f.columns:
        f = f[(f['ph_level'] >= selected_ph_min) & (f['ph_level'] <= selected_ph_max)]
    if 'dissolved_oxygen_mg_l' in f.columns:
        f = f[(f['dissolved_oxygen_mg_l'] >= selected_do_min) & (f['dissolved_oxygen_mg_l'] <= selected_do_max)]
    if selected_station_status != "All Status" and 'status' in f.columns:
        f = f[f['status'] == selected_station_status]
    return f

def filter_rainfall():
    f = rainfall.copy()
    if selected_rainfall_district != "All Districts" and 'district_name' in f.columns:
        f = f[f['district_name'] == selected_rainfall_district]
    if 'record_year' in f.columns:
        f = f[(f['record_year'] >= rain_year_range[0]) & (f['record_year'] <= rain_year_range[1])]
    if selected_rainfall_category != "All Categories" and 'rainfall_category' in f.columns:
        f = f[f['rainfall_category'] == selected_rainfall_category]
    return f

filtered_sources = apply_filters()
filtered_stations = filter_stations()
filtered_rainfall = filter_rainfall()

# ========== MAIN DASHBOARD ==========

st.title("💧 AQUASTAT National Water Command Center")
st.caption(f"**Live Intelligence** • Last Updated: {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}")

col1, col2, col3, col4, col5 = st.columns(5)
with col1: 
    st.markdown(f'<div class="metric-card"><h3 style="color:#8892b0;">Total Sources</h3><h1 style="color:#00e5ff;">{len(sources):,}</h1><p>{len(sources)-len(filtered_sources)} filtered</p></div>', unsafe_allow_html=True)
with col2: 
    avg_cap = filtered_sources['capacity_percent'].mean() if not filtered_sources.empty else 0
    st.markdown(f'<div class="metric-card"><h3 style="color:#8892b0;">Avg Capacity</h3><h1 style="color:#00e5ff;">{avg_cap:.1f}%</h1></div>', unsafe_allow_html=True)
with col3:
    critical = len(filtered_sources[filtered_sources['capacity_percent'] < 30]) if not filtered_sources.empty else 0
    st.markdown(f'<div class="metric-card"><h3 style="color:#8892b0;">Critical Sources</h3><h1 style="color:#ff4444;">{critical}</h1></div>', unsafe_allow_html=True)
with col4:
    on_map = len(filtered_sources[filtered_sources['latitude'].notna()]) if not filtered_sources.empty else 0
    st.markdown(f'<div class="metric-card"><h3 style="color:#8892b0;">On Map</h3><h1 style="color:#00e5ff;">{on_map}</h1></div>', unsafe_allow_html=True)
with col5:
    st.markdown(f'<div class="metric-card"><h3 style="color:#8892b0;">Active Alerts</h3><h1 style="color:#ffd700;">{len(alerts)}</h1></div>', unsafe_allow_html=True)

st.markdown("---")

# Main Tabs
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 DASHBOARD", "🗺️ MAP VIEW", "📈 ANALYTICS", "💧 WATER QUALITY", "⚠️ ALERTS", "📋 DATA TABLES"
])

# ===================== TAB 1: DASHBOARD =====================
with tab1:
    if filtered_sources.empty:
        st.warning("⚠️ No water sources match the current filters.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<p class="section-header">📊 Storage Capacity Table</p>', unsafe_allow_html=True)
            if 'capacity_percent' in filtered_sources.columns:
                capacity_table = filtered_sources[['source_name', 'source_type', 'capacity_percent', 'state', 'district', 'risk_level']].sort_values('capacity_percent')
                capacity_table['capacity_percent'] = capacity_table['capacity_percent'].round(1).astype(str) + '%'
                st.dataframe(capacity_table, use_container_width=True, hide_index=True)
        
        with col2:
            st.markdown('<p class="section-header">🏭 Source Types</p>', unsafe_allow_html=True)
            if 'source_type' in filtered_sources.columns:
                type_counts = filtered_sources['source_type'].value_counts().reset_index()
                type_counts.columns = ['Source Type', 'Count']
                fig = px.pie(type_counts, values='Count', names='Source Type', title="Water Sources by Type", template="plotly_dark", color_discrete_sequence=px.colors.sequential.Tealgrn)
                fig.update_traces(textposition='inside', textinfo='percent+label')
                fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<p class="section-header">📈 Groundwater Analysis</p>', unsafe_allow_html=True)
            if not groundwater.empty:
                filtered_gw = groundwater.copy()
                if selected_district != "All Districts" and 'district_name' in filtered_gw.columns:
                    filtered_gw = filtered_gw[filtered_gw['district_name'] == selected_district]
                if not filtered_gw.empty:
                    stress_counts = filtered_gw['stress_level'].value_counts().reset_index()
                    stress_counts.columns = ['Stress Level', 'Count']
                    fig = px.bar(stress_counts, x='Stress Level', y='Count', title="Groundwater Stress Distribution", template="plotly_dark", color='Stress Level', color_discrete_map={'Low':'#00ff9d','Moderate':'#ffd700','High':'#ff4444'}, text_auto=True)
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
            st.markdown('<p class="section-header">☔ Rainfall Analysis</p>', unsafe_allow_html=True)
            if not filtered_rainfall.empty:
                season_rain = filtered_rainfall.groupby('season')['rainfall_cm'].mean().reset_index()
                fig = px.bar(season_rain, x='season', y='rainfall_cm', title="Average Rainfall by Season", template="plotly_dark", color='rainfall_cm', color_continuous_scale='Blues', text_auto='.1f')
                fig.update_traces(textposition='outside')
                fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
                
                st.markdown("#### 📋 Rainfall Details")
                rain_table = filtered_rainfall[['district_name', 'rainfall_cm', 'record_year', 'season', 'rainfall_category']].sort_values('rainfall_cm', ascending=False)
                rain_table['rainfall_cm'] = rain_table['rainfall_cm'].round(1).astype(str) + ' cm'
                st.dataframe(rain_table, use_container_width=True, hide_index=True)
            else:
                st.info("No rainfall data available for selected filters")
        
        if 'risk_level' in filtered_sources.columns:
            st.markdown('<p class="section-header">⚠️ Risk Assessment</p>', unsafe_allow_html=True)
            risk_counts = filtered_sources['risk_level'].value_counts().reset_index()
            risk_counts.columns = ['Risk Level', 'Count']
            fig = px.bar(risk_counts, x='Risk Level', y='Count', color='Risk Level', color_discrete_map={'Good':'#00ff9d','Moderate':'#ffd700','Critical':'#ff4444'}, title="Infrastructure Risk Assessment", template="plotly_dark", text_auto=True)
            fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)

# ===================== TAB 2: MAP VIEW =====================
with tab2:
    st.markdown('<p class="section-header">🗺️ National Interactive Water Resources Map</p>', unsafe_allow_html=True)
    filter_info = []
    if selected_state != "All States": filter_info.append(f"State: {selected_state}")
    if selected_district != "All Districts": filter_info.append(f"District: {selected_district}")
    if selected_type != "All Types": filter_info.append(f"Type: {selected_type}")
    if filter_info: st.info(f"**Showing:** {', '.join(filter_info)} | **Total Sources:** {len(filtered_sources)}")
    
    style_map = {"Esri Satellite": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}", "OpenStreetMap": "OpenStreetMap", "CartoDB Dark": "CartoDB dark_matter", "CartoDB Light": "CartoDB positron"}
    center_lat, center_lon, zoom = 20.5937, 78.9629, 5
    if not filtered_sources.empty and 'latitude' in filtered_sources.columns:
        swc = filtered_sources[filtered_sources['latitude'].notna() & filtered_sources['longitude'].notna()]
        if not swc.empty:
            zoom = 9 if selected_district != "All Districts" else (7 if selected_state != "All States" else 5)
            center_lat, center_lon = swc['latitude'].mean(), swc['longitude'].mean()
    
    m = folium.Map(location=[center_lat, center_lon], zoom_start=zoom, tiles=style_map[map_style], attr='AQUASTAT')
    Fullscreen().add_to(m)
    mc = MarkerCluster().add_to(m) if (show_clusters and len(filtered_sources) > 10) else m
    heat_data, sources_on_map = [], 0
    
    if not filtered_sources.empty and 'latitude' in filtered_sources.columns:
        swc = filtered_sources[filtered_sources['latitude'].notna() & filtered_sources['longitude'].notna()]
        for _, source in swc.iterrows():
            cap = source.get('capacity_percent', 50)
            color = '#ff4444' if cap < 30 else ('#ffd700' if cap < 60 else '#00ff9d')
            risk_text = "CRITICAL" if cap < 30 else ("MODERATE" if cap < 60 else "GOOD")
            heat_data.append([source['latitude'], source['longitude']])
            sources_on_map += 1
            popup_html = f'<div style="font-family:Arial;min-width:250px;background:#0a192f;color:#e6f1ff;"><h4 style="color:{color};">{source.get("source_name","Unknown")}</h4><hr><table style="width:100%;">\\n<tr><td><b>Type:</b></td><td>{source.get("source_type","Unknown")}</td></tr>\\n<tr><td><b>Location:</b></td><td>{source.get("district","Unknown")}, {source.get("state","Unknown")}</td></tr>\\n<tr><td><b>Capacity:</b></td><td>{cap:.1f}%</td></tr>\\n<tr><td><b>Age:</b></td><td>{source.get("age",0):.0f} yrs</td></tr>\\n<tr><td><b>Risk:</b></td><td><span style="color:{color};">{risk_text}</span></td></tr>\\n</table></div>'
            marker = folium.CircleMarker(location=[source['latitude'], source['longitude']], radius=marker_size + (3 if cap < 30 else 0), color=color, fill=True, fillOpacity=0.7, popup=folium.Popup(popup_html, max_width=300), tooltip=f"{source.get('source_name')} - {cap:.0f}%")
            marker.add_to(mc)
        if show_heatmap and heat_data:
            HeatMap(heat_data, radius=15, blur=10, gradient={0.2:'blue',0.4:'cyan',0.6:'lime',0.8:'yellow',1:'red'}).add_to(m)
    
    if show_stations and not filtered_stations.empty and 'latitude' in filtered_stations.columns:
        for _, station in filtered_stations[filtered_stations['latitude'].notna() & filtered_stations['longitude'].notna()].iterrows():
            status = station.get('status', 'Unknown')
            station_color = 'green' if status == 'Active' else ('orange' if status == 'Maintenance' else 'red')
            station_popup = f'<div style="background:#0a192f;color:#e6f1ff;padding:10px;"><b>{station.get("station_name")}</b><br>Location: {station.get("district_name")}<br>Status: <span style="color:{station_color};">{status}</span><br>pH: {station.get("ph_level","N/A")}<br>DO: {station.get("dissolved_oxygen_mg_l","N/A")} mg/L</div>'
            folium.Marker(location=[station['latitude'], station['longitude']], icon=folium.Icon(color=station_color, icon='info-sign'), popup=folium.Popup(station_popup, max_width=300), tooltip=f"Station: {station.get('station_name')}").add_to(m)
    
    st_folium(m, width=1300, height=600)
    col1, col2, col3 = st.columns(3)
    with col1: st.metric("Sources on Map", sources_on_map)
    with col2: st.metric("Total Filtered Sources", len(filtered_sources))
    with col3: st.metric("Coordinate Coverage", f"{(sources_on_map/len(filtered_sources)*100) if len(filtered_sources)>0 else 0:.1f}%")
    st.markdown("---")
    cols = st.columns(5)
    with cols[0]: st.markdown('<span class="status-dot status-good"></span> Good (≥60%)', unsafe_allow_html=True)
    with cols[1]: st.markdown('<span class="status-dot status-warning"></span> Moderate (30-60%)', unsafe_allow_html=True)
    with cols[2]: st.markdown('<span class="status-dot status-critical"></span> Critical (<30%)', unsafe_allow_html=True)
    with cols[3]: st.markdown('<span class="status-dot" style="background:#4287f5;"></span> Monitoring Station', unsafe_allow_html=True)
    with cols[4]: st.markdown('🔥 Heatmap Area', unsafe_allow_html=True)

# ===================== TAB 3: ANALYTICS =====================
with tab3:
    st.markdown('<p class="section-header">📈 Advanced Analytics</p>', unsafe_allow_html=True)
    atab1, atab2, atab3 = st.tabs(["📊 Trends", "📉 Comparisons", "📐 Statistics"])
    
    with atab1:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Rainfall Trend")
            if not filtered_rainfall.empty:
                rain_trend = filtered_rainfall.groupby('record_year')['rainfall_cm'].mean().reset_index()
                if not rain_trend.empty:
                    fig = px.line(rain_trend, x='record_year', y='rainfall_cm', title="Average Rainfall Over Years", template="plotly_dark", markers=True)
                    fig.update_traces(line_color='#00e5ff', line_width=3)
                    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.subheader("Groundwater Trend")
            if not groundwater.empty and 'assessment_year' in groundwater.columns:
                filtered_gw = groundwater.copy()
                if selected_district != "All Districts" and 'district_name' in filtered_gw.columns:
                    filtered_gw = filtered_gw[filtered_gw['district_name'] == selected_district]
                if not filtered_gw.empty:
                    gw_trend = filtered_gw.groupby('assessment_year')['avg_depth_meters'].mean().reset_index()
                    if not gw_trend.empty:
                        fig = px.line(gw_trend, x='assessment_year', y='avg_depth_meters', title="Average Groundwater Depth", template="plotly_dark", markers=True)
                        fig.update_traces(line_color='#ffd700', line_width=3)
                        fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                        st.plotly_chart(fig, use_container_width=True)
    
    with atab2:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Capacity by State")
            if not filtered_sources.empty and 'state' in filtered_sources.columns:
                state_cap = filtered_sources.groupby('state')['capacity_percent'].mean().sort_values(ascending=False).head(10)
                if len(state_cap) > 0:
                    fig = px.bar(x=state_cap.values, y=state_cap.index, orientation='h', title="Average Capacity by State", template="plotly_dark", color=state_cap.values, color_continuous_scale='Tealgrn', labels={'x':'Avg Capacity (%)','y':'State'})
                    fig.update_traces(texttemplate='%{x:.1f}%', textposition='outside')
                    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.subheader("Extraction vs Recharge")
            if not groundwater.empty and 'recharge_rate_mcm' in groundwater.columns:
                filtered_gw = groundwater.copy()
                if selected_district != "All Districts" and 'district_name' in filtered_gw.columns:
                    filtered_gw = filtered_gw[filtered_gw['district_name'] == selected_district]
                if not filtered_gw.empty:
                    fig = px.scatter(filtered_gw, x='recharge_rate_mcm', y='extraction_pct', size='avg_depth_meters' if 'avg_depth_meters' in filtered_gw.columns else None, color='district_name' if 'district_name' in filtered_gw.columns else None, title="Groundwater Extraction vs Recharge", template="plotly_dark")
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
                            fig = px.imshow(corr_data.corr(), text_auto=True, aspect="auto", title="Correlation Matrix", template="plotly_dark", color_continuous_scale='RdBu_r')
                            fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                            st.plotly_chart(fig, use_container_width=True)

# ===================== TAB 4: WATER QUALITY =====================
with tab4:
    st.markdown('<p class="section-header">💧 Water Quality Monitoring</p>', unsafe_allow_html=True)
    
    if not filtered_stations.empty:
        st.info(f"**Showing:** {len(filtered_stations)} stations | pH: {selected_ph_min:.1f}-{selected_ph_max:.1f} | DO: {selected_do_min:.1f}-{selected_do_max:.1f} mg/L | Status: {selected_station_status}")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            avg_ph = filtered_stations['ph_level'].mean() if 'ph_level' in filtered_stations.columns else 0
            st.metric("Average pH", f"{avg_ph:.2f}", delta=None)
        with col2:
            avg_do = filtered_stations['dissolved_oxygen_mg_l'].mean() if 'dissolved_oxygen_mg_l' in filtered_stations.columns else 0
            st.metric("Dissolved Oxygen", f"{avg_do:.1f} mg/L", delta=None)
        with col3:
            avg_turbidity = filtered_stations['turbidity_ntu'].mean() if 'turbidity_ntu' in filtered_stations.columns else 0
            st.metric("Turbidity", f"{avg_turbidity:.1f} NTU", delta=None)
        with col4:
            active_count = len(filtered_stations[filtered_stations['status'] == 'Active']) if 'status' in filtered_stations.columns else 0
            st.metric("Active Stations", active_count, delta=None)
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        with col1:
            if 'ph_level' in filtered_stations.columns:
                fig = px.histogram(filtered_stations, x='ph_level', nbins=20, title="pH Distribution", template="plotly_dark", color_discrete_sequence=['#00e5ff'])
                fig.add_vline(x=7, line_dash="dash", line_color="white")
                fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
        with col2:
            if 'dissolved_oxygen_mg_l' in filtered_stations.columns:
                fig = px.box(filtered_stations, y='dissolved_oxygen_mg_l', title="Dissolved Oxygen Distribution", template="plotly_dark", color_discrete_sequence=['#00ff9d'])
                fig.add_hline(y=4, line_dash="dash", line_color="red", annotation_text="Critical Level (4 mg/L)")
                fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("### 📋 Water Quality Readings")
        display_cols = ['station_name', 'district_name', 'ph_level', 'dissolved_oxygen_mg_l', 'turbidity_ntu', 'status']
        available_cols = [c for c in display_cols if c in filtered_stations.columns]
        if available_cols:
            display_df = filtered_stations[available_cols].copy()
            if 'ph_level' in display_df.columns:
                display_df['ph_level'] = display_df['ph_level'].round(2)
            if 'dissolved_oxygen_mg_l' in display_df.columns:
                display_df['dissolved_oxygen_mg_l'] = display_df['dissolved_oxygen_mg_l'].round(1).astype(str) + ' mg/L'
            if 'turbidity_ntu' in display_df.columns:
                display_df['turbidity_ntu'] = display_df['turbidity_ntu'].round(1).astype(str) + ' NTU'
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            st.caption(f"Showing {len(display_df)} stations matching filters")
    else:
        st.info("No water quality data available for selected filters.")

# ===================== TAB 5: ALERTS =====================
with tab5:
    st.markdown('<p class="section-header">🚨 Active Alerts and Warnings</p>', unsafe_allow_html=True)
    
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
        with col1: st.markdown(f'<div class="alert-critical">🔴 CRITICAL: {len(critical_alerts)}</div>', unsafe_allow_html=True)
        with col2: st.markdown(f'<div class="alert-warning">🟡 WARNING: {len(warning_alerts)}</div>', unsafe_allow_html=True)
        with col3: st.markdown(f'<div class="alert-good">🟢 STABLE: {len(stable_alerts)}</div>', unsafe_allow_html=True)
        
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
    st.markdown('<p class="section-header">📋 Data Explorer</p>', unsafe_allow_html=True)
    table_choice = st.selectbox("Select Table to View", ["Water Sources", "Monitoring Stations", "Groundwater Levels", "Rainfall History", "Water Usage", "Active Alerts", "Regional Statistics"])
    
    if table_choice == "Water Sources":
        display_df = filtered_sources if not filtered_sources.empty else sources
        if not display_df.empty:
            cols = [c for c in ['source_id', 'source_name', 'source_type', 'capacity_percent', 'max_capacity_mcm', 'build_year', 'age', 'state', 'district', 'origin_state', 'is_transboundary', 'risk_level', 'health_score'] if c in display_df.columns]
            st.dataframe(display_df[cols] if cols else display_df, use_container_width=True, hide_index=True)
            st.download_button("📥 Download CSV", display_df.to_csv(index=False).encode('utf-8'), f"water_sources_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
    
    elif table_choice == "Monitoring Stations":
        display_df = filtered_stations if not filtered_stations.empty else stations
        if not display_df.empty:
            cols = [c for c in ['station_id', 'station_name', 'state_name', 'district_name', 'latitude', 'longitude', 'ph_level', 'dissolved_oxygen_mg_l', 'turbidity_ntu', 'status'] if c in display_df.columns]
            st.dataframe(display_df[cols] if cols else display_df, use_container_width=True, hide_index=True)
            st.download_button("📥 Download CSV", display_df.to_csv(index=False).encode('utf-8'), f"stations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
    
    elif table_choice == "Groundwater Levels":
        display_df = groundwater.copy()
        if selected_district != "All Districts" and 'district_name' in display_df.columns:
            display_df = display_df[display_df['district_name'] == selected_district]
        if not display_df.empty:
            cols = [c for c in ['district_name', 'avg_depth_meters', 'extraction_pct', 'recharge_rate_mcm', 'assessment_year', 'stress_level'] if c in display_df.columns]
            st.dataframe(display_df[cols] if cols else display_df, use_container_width=True, hide_index=True)
            st.download_button("📥 Download CSV", display_df.to_csv(index=False).encode('utf-8'), f"groundwater_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
    
    elif table_choice == "Rainfall History":
        display_df = filtered_rainfall if not filtered_rainfall.empty else rainfall
        if not display_df.empty:
            cols = [c for c in ['district_name', 'rainfall_cm', 'record_year', 'season', 'rainfall_category'] if c in display_df.columns]
            st.dataframe(display_df[cols] if cols else display_df, use_container_width=True, hide_index=True)
            st.download_button("📥 Download CSV", display_df.to_csv(index=False).encode('utf-8'), f"rainfall_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
    
    elif table_choice == "Water Usage":
        display_df = usage.copy()
        if selected_state != "All States" and 'state' in display_df.columns:
            display_df = display_df[display_df['state'] == selected_state]
        if selected_district != "All Districts" and 'district' in display_df.columns:
            display_df = display_df[display_df['district'] == selected_district]
        if not display_df.empty:
            cols = [c for c in ['source_name', 'source_type', 'sector', 'sub_sector', 'consumer_name', 'consumption_mcm', 'record_year', 'season', 'state', 'district'] if c in display_df.columns]
            st.dataframe(display_df[cols] if cols else display_df, use_container_width=True, hide_index=True)
            st.download_button("📥 Download CSV", display_df.to_csv(index=False).encode('utf-8'), f"usage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
    
    elif table_choice == "Active Alerts":
        display_df = alerts.copy()
        if not display_df.empty:
            cols = [c for c in ['alert_id', 'source_name', 'capacity_percent', 'ph_level', 'alert_status', 'alert_time'] if c in display_df.columns]
            st.dataframe(display_df[cols] if cols else display_df, use_container_width=True, hide_index=True)
            st.download_button("📥 Download CSV", display_df.to_csv(index=False).encode('utf-8'), f"alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
    
    elif table_choice == "Regional Statistics":
        display_df = regional.copy()
        if not display_df.empty:
            cols = [c for c in ['region_name', 'population_count', 'annual_rainfall_avg_cm'] if c in display_df.columns]
            st.dataframe(display_df[cols] if cols else display_df, use_container_width=True, hide_index=True)
            st.download_button("📥 Download CSV", display_df.to_csv(index=False).encode('utf-8'), f"regional_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")

# ===================== SIDEBAR SUMMARY =====================
with st.sidebar.expander("📊 Current Filter Summary", expanded=False):
    st.markdown(f"""
    **Time:** {year_range[0]} - {year_range[1]}
    **Geography:** {selected_state} / {selected_district}
    **Source:** {selected_type} | Capacity: {capacity_range[0]:.0f}%-{capacity_range[1]:.0f}% | Risk: {selected_risk}
    **Rainfall:** {selected_rainfall_district} | Years: {rain_year_range[0]}-{rain_year_range[1]} | Category: {selected_rainfall_category}
    **Water Quality:** pH {selected_ph_min:.1f}-{selected_ph_max:.1f} | DO {selected_do_min:.1f}-{selected_do_max:.1f} mg/L | Status: {selected_station_status}
    **Results:** {len(filtered_sources)} sources, {len(filtered_stations)} stations
    """)

# ===================== EXPORT =====================
st.sidebar.markdown("---")
if st.sidebar.button("📦 Export All Filtered Data", use_container_width=True):
    export_data = {
        'water_sources': filtered_sources,
        'monitoring_stations': filtered_stations,
        'groundwater': groundwater[groundwater['district_name'].isin(filtered_sources['district'].unique())] if not filtered_sources.empty and not groundwater.empty else pd.DataFrame(),
        'rainfall': filtered_rainfall,
        'alerts': alerts if not alerts.empty else pd.DataFrame()
    }
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in export_data.items():
            if not df.empty:
                df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    st.sidebar.download_button("📥 Download Excel Report", output.getvalue(), f"aquastat_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ===================== FOOTER =====================
st.markdown("---")
col1, col2, col3 = st.columns(3)
with col1: st.markdown('<div style="text-align:center;"><p style="color:#00e5ff;">💧 AQUASTAT</p><p style="color:#8892b0;">National Water Command Center</p></div>', unsafe_allow_html=True)
with col2: st.markdown(f'<div style="text-align:center;"><p style="color:#8892b0;">Data Source: Ministry of Jal Shakti</p><p style="color:#8892b0;">Last Updated: {current_time.strftime("%Y-%m-%d %H:%M:%S IST")}</p></div>', unsafe_allow_html=True)
with col3: st.markdown('<div style="text-align:center;"><p style="color:#8892b0;">© 2025 All Rights Reserved</p><p style="color:#8892b0;">Version 3.0 | For Official Use</p></div>', unsafe_allow_html=True)

st.markdown('<div style="position:fixed;bottom:10px;right:10px;background:rgba(0,229,255,0.1);padding:5px 10px;border-radius:5px;font-size:0.8rem;">🔄 Auto-refresh every 5 minutes • Cloud Connected</div>', unsafe_allow_html=True)
if st.button("🔄 Refresh Data", key="refresh_button"):
    st.cache_data.clear()
    st.rerun()
