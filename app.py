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
# DATABASE CONNECTION WITH ERROR HANDLING
# ─────────────────────────────────────────────────────────────────────────────
try:
    NEON_URL = st.secrets["NEON_URL"]
except:
    NEON_URL = None

@st.cache_resource
def init_connection():
    if NEON_URL is None:
        st.warning("⚠️ Database URL not found in secrets. Using demo mode with sample data.")
        return None
    try:
        engine = create_engine(NEON_URL, pool_size=5, max_overflow=10, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).fetchone()
        return engine
    except Exception as e:
        st.warning(f"⚠️ Database connection failed: {str(e)[:100]}. Using demo mode.")
        return None

engine = init_connection()

def execute_sql_query(query, params=None):
    """Execute SQL query with proper error handling and rollback"""
    if engine is None:
        # Return sample data for demo mode
        return get_sample_data_for_query(query), None
    
    try:
        with engine.connect() as conn:
            # Rollback any pending transactions
            conn.rollback()
            if params:
                result = pd.read_sql(query, conn, params=params)
            else:
                result = pd.read_sql(query, conn)
        return result, None
    except exc.OperationalError as e:
        # Handle connection issues
        return None, f"Connection error: {str(e)[:100]}"
    except Exception as e:
        return None, f"Query error: {str(e)[:100]}"

def get_sample_data_for_query(query):
    """Return sample data when database is not available"""
    if "rainfall" in query.lower():
        return pd.DataFrame({
            'district_name': ['Sample District 1', 'Sample District 2'],
            'rainfall_cm': [120.5, 85.3],
            'record_year': [2024, 2024],
            'season': ['Monsoon', 'Monsoon'],
            'rainfall_category': ['High', 'Moderate']
        })
    elif "groundwater" in query.lower():
        return pd.DataFrame({
            'district_name': ['Sample District 1', 'Sample District 2'],
            'avg_depth_meters': [25.6, 18.2],
            'extraction_pct': [65.4, 42.1],
            'stress_level': ['Moderate', 'Low'],
            'assessment_year': [2024, 2024]
        })
    elif "water" in query.lower() and "quality" in query.lower():
        return pd.DataFrame({
            'station_name': ['Station A', 'Station B'],
            'ph_level': [7.2, 7.5],
            'dissolved_oxygen_mg_l': [6.8, 7.1],
            'status': ['Active', 'Active'],
            'state_name': ['Sample State', 'Sample State']
        })
    else:
        return pd.DataFrame({'message': ['Demo mode - No data available']})

# ─────────────────────────────────────────────────────────────────────────────
# EXTENSIVE SQL QUERIES FOR EACH FILTER TYPE
# ─────────────────────────────────────────────────────────────────────────────

# ========== RAINFALL SQL QUERIES ==========
RAINFALL_QUERIES = {
    "basic_filter": """
        SELECT * FROM rainfall_history 
        WHERE 1=1
        {district_filter}
        {rainfall_range}
        {year_filter}
        {season_filter}
        {category_filter}
        ORDER BY record_year DESC, rainfall_cm DESC
    """,
    
    "yearly_trend": """
        SELECT 
            record_year,
            COUNT(*) as total_records,
            ROUND(AVG(rainfall_cm), 2) as avg_rainfall,
            ROUND(MIN(rainfall_cm), 2) as min_rainfall,
            ROUND(MAX(rainfall_cm), 2) as max_rainfall,
            ROUND(STDDEV(rainfall_cm), 2) as std_deviation,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rainfall_cm), 2) as median_rainfall
        FROM rainfall_history
        WHERE rainfall_cm IS NOT NULL
        {district_filter}
        GROUP BY record_year
        ORDER BY record_year DESC
    """,
    
    "seasonal_analysis": """
        SELECT 
            season,
            COUNT(*) as total_records,
            ROUND(AVG(rainfall_cm), 2) as avg_rainfall,
            ROUND(MIN(rainfall_cm), 2) as min_rainfall,
            ROUND(MAX(rainfall_cm), 2) as max_rainfall,
            ROUND(STDDEV(rainfall_cm), 2) as std_deviation
        FROM rainfall_history
        WHERE season IS NOT NULL AND rainfall_cm IS NOT NULL
        {district_filter}
        {year_filter}
        GROUP BY season
        ORDER BY avg_rainfall DESC
    """,
    
    "district_comparison": """
        SELECT 
            district_name,
            COUNT(*) as total_records,
            ROUND(AVG(rainfall_cm), 2) as avg_rainfall,
            ROUND(SUM(rainfall_cm), 2) as total_rainfall,
            ROUND(MIN(rainfall_cm), 2) as min_rainfall,
            ROUND(MAX(rainfall_cm), 2) as max_rainfall,
            CASE 
                WHEN AVG(rainfall_cm) > 200 THEN 'Extreme Rainfall'
                WHEN AVG(rainfall_cm) > 100 THEN 'High Rainfall'
                WHEN AVG(rainfall_cm) > 50 THEN 'Moderate Rainfall'
                ELSE 'Low Rainfall'
            END as rainfall_category
        FROM rainfall_history
        WHERE rainfall_cm IS NOT NULL
        GROUP BY district_name
        HAVING COUNT(*) > 3
        ORDER BY avg_rainfall DESC
        LIMIT 20
    """,
    
    "extreme_events": """
        SELECT 
            district_name,
            record_year,
            season,
            rainfall_cm,
            CASE 
                WHEN rainfall_cm > 300 THEN 'EXTREME_DANGER'
                WHEN rainfall_cm > 200 THEN 'SEVERE'
                WHEN rainfall_cm > 150 THEN 'HEAVY'
                ELSE 'MODERATE'
            END as severity_level,
            RANK() OVER (PARTITION BY district_name ORDER BY rainfall_cm DESC) as rank_within_district
        FROM rainfall_history
        WHERE rainfall_cm > 150
        {district_filter}
        {year_filter}
        ORDER BY rainfall_cm DESC
        LIMIT 50
    """,
    
    "anomaly_detection": """
        WITH stats AS (
            SELECT 
                district_name,
                AVG(rainfall_cm) as mean_rainfall,
                STDDEV(rainfall_cm) as std_rainfall
            FROM rainfall_history
            GROUP BY district_name
        )
        SELECT 
            rh.district_name,
            rh.record_year,
            rh.season,
            rh.rainfall_cm,
            s.mean_rainfall,
            s.std_rainfall,
            CASE 
                WHEN rh.rainfall_cm > s.mean_rainfall + 2 * s.std_rainfall THEN 'Extreme_High_Anomaly'
                WHEN rh.rainfall_cm < s.mean_rainfall - 2 * s.std_rainfall THEN 'Extreme_Low_Anomaly'
                WHEN rh.rainfall_cm > s.mean_rainfall + s.std_rainfall THEN 'High_Anomaly'
                WHEN rh.rainfall_cm < s.mean_rainfall - s.std_rainfall THEN 'Low_Anomaly'
                ELSE 'Normal'
            END as anomaly_type
        FROM rainfall_history rh
        JOIN stats s ON rh.district_name = s.district_name
        WHERE rh.rainfall_cm IS NOT NULL
        ORDER BY ABS(rh.rainfall_cm - s.mean_rainfall) DESC
        LIMIT 30
    """,
    
    "cumulative_analysis": """
        SELECT 
            district_name,
            record_year,
            season,
            rainfall_cm,
            SUM(rainfall_cm) OVER (PARTITION BY district_name, record_year ORDER BY season) as cumulative_rainfall,
            AVG(rainfall_cm) OVER (PARTITION BY district_name ORDER BY record_year) as moving_avg_rainfall,
            LAG(rainfall_cm, 1) OVER (PARTITION BY district_name ORDER BY record_year) as previous_year_rainfall,
            (rainfall_cm - LAG(rainfall_cm, 1) OVER (PARTITION BY district_name ORDER BY record_year)) as yoy_change
        FROM rainfall_history
        WHERE rainfall_cm IS NOT NULL
        {district_filter}
        ORDER BY district_name, record_year DESC
        LIMIT 100
    """
}

# ========== GROUNDWATER SQL QUERIES ==========
GROUNDWATER_QUERIES = {
    "basic_filter": """
        SELECT * FROM groundwater_levels 
        WHERE 1=1
        {district_filter}
        {depth_range}
        {stress_filter}
        {year_filter}
        ORDER BY assessment_year DESC, avg_depth_meters DESC
    """,
    
    "depletion_rate": """
        SELECT 
            district_name,
            MIN(assessment_year) as first_year,
            MAX(assessment_year) as last_year,
            ROUND(AVG(avg_depth_meters), 2) as avg_depth,
            ROUND(STDDEV(avg_depth_meters), 2) as depth_variability,
            ROUND((MAX(avg_depth_meters) - MIN(avg_depth_meters)) / 
                  NULLIF(EXTRACT(YEAR FROM AGE(MAX(assessment_year)::DATE, MIN(assessment_year)::DATE)), 0), 2) as annual_depletion_rate,
            CASE 
                WHEN (MAX(avg_depth_meters) - MIN(avg_depth_meters)) > 20 THEN 'CRITICAL_DEPLETION'
                WHEN (MAX(avg_depth_meters) - MIN(avg_depth_meters)) > 10 THEN 'MODERATE_DEPLETION'
                ELSE 'STABLE'
            END as depletion_status
        FROM groundwater_levels
        WHERE avg_depth_meters IS NOT NULL
        GROUP BY district_name
        HAVING COUNT(*) > 2
        ORDER BY annual_depletion_rate DESC
    """,
    
    "extraction_analysis": """
        SELECT 
            district_name,
            assessment_year,
            extraction_pct,
            recharge_rate_mcm,
            extraction_pct - recharge_rate_mcm as deficit_surplus,
            CASE 
                WHEN extraction_pct > recharge_rate_mcm * 1.5 THEN 'OVER_EXTRACTION_CRITICAL'
                WHEN extraction_pct > recharge_rate_mcm THEN 'OVER_EXTRACTION_MODERATE'
                WHEN extraction_pct < recharge_rate_mcm * 0.5 THEN 'UNDER_UTILIZATION'
                ELSE 'BALANCED'
            END as extraction_status
        FROM groundwater_levels
        WHERE extraction_pct IS NOT NULL AND recharge_rate_mcm IS NOT NULL
        {district_filter}
        {year_filter}
        ORDER BY deficit_surplus DESC
    """,
    
    "trend_forecast": """
        WITH depth_trend AS (
            SELECT 
                district_name,
                assessment_year,
                avg_depth_meters,
                AVG(avg_depth_meters) OVER (PARTITION BY district_name ORDER BY assessment_year 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg_3yr
            FROM groundwater_levels
            WHERE avg_depth_meters IS NOT NULL
        )
        SELECT 
            district_name,
            assessment_year,
            avg_depth_meters,
            moving_avg_3yr,
            avg_depth_meters - moving_avg_3yr as deviation_from_trend,
            CASE 
                WHEN avg_depth_meters > moving_avg_3yr * 1.1 THEN 'DEEPENING_FAST'
                WHEN avg_depth_meters > moving_avg_3yr * 1.05 THEN 'DEEPENING_SLOW'
                WHEN avg_depth_meters < moving_avg_3yr * 0.95 THEN 'RECOVERING'
                ELSE 'STABLE'
            END as trend_direction
        FROM depth_trend
        WHERE assessment_year >= (SELECT MAX(assessment_year) - 5 FROM groundwater_levels)
        ORDER BY district_name, assessment_year DESC
    """,
    
    "risk_zones": """
        SELECT 
            district_name,
            ROUND(AVG(avg_depth_meters), 2) as avg_depth,
            ROUND(AVG(extraction_pct), 2) as avg_extraction,
            ROUND(AVG(recharge_rate_mcm), 2) as avg_recharge,
            CASE 
                WHEN AVG(avg_depth_meters) > 40 AND AVG(extraction_pct) > 70 THEN 'CRITICAL_RISK'
                WHEN AVG(avg_depth_meters) > 30 AND AVG(extraction_pct) > 50 THEN 'HIGH_RISK'
                WHEN AVG(avg_depth_meters) > 20 AND AVG(extraction_pct) > 30 THEN 'MODERATE_RISK'
                ELSE 'LOW_RISK'
            END as risk_category,
            RANK() OVER (ORDER BY AVG(avg_depth_meters) DESC, AVG(extraction_pct) DESC) as risk_rank
        FROM groundwater_levels
        WHERE avg_depth_meters IS NOT NULL AND extraction_pct IS NOT NULL
        GROUP BY district_name
        ORDER BY risk_rank
        LIMIT 20
    """
}

# ========== WATER QUALITY SQL QUERIES ==========
WATER_QUALITY_QUERIES = {
    "basic_filter": """
        SELECT * FROM water_monitoring_stations 
        WHERE 1=1
        {state_filter}
        {district_filter}
        {ph_range}
        {status_filter}
        ORDER BY station_name
    """,
    
    "ph_analysis": """
        SELECT 
            state_name,
            district_name,
            COUNT(*) as total_stations,
            ROUND(AVG(ph_level), 2) as avg_ph,
            ROUND(MIN(ph_level), 2) as min_ph,
            ROUND(MAX(ph_level), 2) as max_ph,
            ROUND(STDDEV(ph_level), 2) as ph_variability,
            CASE 
                WHEN AVG(ph_level) BETWEEN 6.5 AND 8.5 THEN 'IDEAL'
                WHEN AVG(ph_level) BETWEEN 6.0 AND 9.0 THEN 'ACCEPTABLE'
                ELSE 'CRITICAL'
            END as ph_status
        FROM water_monitoring_stations
        WHERE ph_level IS NOT NULL
        GROUP BY state_name, district_name
        ORDER BY ph_status, avg_ph
    """,
    
    "do_analysis": """
        SELECT 
            state_name,
            district_name,
            ROUND(AVG(dissolved_oxygen_mg_l), 2) as avg_do,
            ROUND(MIN(dissolved_oxygen_mg_l), 2) as min_do,
            ROUND(MAX(dissolved_oxygen_mg_l), 2) as max_do,
            CASE 
                WHEN AVG(dissolved_oxygen_mg_l) > 7 THEN 'EXCELLENT'
                WHEN AVG(dissolved_oxygen_mg_l) > 5 THEN 'GOOD'
                WHEN AVG(dissolved_oxygen_mg_l) > 3 THEN 'FAIR'
                ELSE 'POOR'
            END as water_quality_class,
            CASE 
                WHEN MIN(dissolved_oxygen_mg_l) < 3 THEN 'FISH_KILL_RISK'
                WHEN MIN(dissolved_oxygen_mg_l) < 5 THEN 'STRESS_RISK'
                ELSE 'SAFE'
            END as ecological_risk
        FROM water_monitoring_stations
        WHERE dissolved_oxygen_mg_l IS NOT NULL
        GROUP BY state_name, district_name
        ORDER BY avg_do DESC
    """,
    
    "turbidity_analysis": """
        SELECT 
            state_name,
            district_name,
            station_name,
            turbidity_ntu,
            CASE 
                WHEN turbidity_ntu < 5 THEN 'CLEAR'
                WHEN turbidity_ntu < 10 THEN 'SLIGHTLY_TURBID'
                WHEN turbidity_ntu < 20 THEN 'TURBID'
                ELSE 'HIGHLY_TURBID'
            END as turbidity_level,
            CASE 
                WHEN turbidity_ntu > 20 THEN 'TREATMENT_REQUIRED'
                WHEN turbidity_ntu > 10 THEN 'MONITORING_REQUIRED'
                ELSE 'ACCEPTABLE'
            END as treatment_need
        FROM water_monitoring_stations
        WHERE turbidity_ntu IS NOT NULL
        {district_filter}
        ORDER BY turbidity_ntu DESC
        LIMIT 30
    """,
    
    "comprehensive_quality_index": """
        SELECT 
            station_name,
            state_name,
            district_name,
            ph_level,
            dissolved_oxygen_mg_l,
            turbidity_ntu,
            (CASE 
                WHEN ph_level BETWEEN 6.5 AND 8.5 THEN 100
                WHEN ph_level BETWEEN 6.0 AND 9.0 THEN 70
                ELSE 40
            END * 0.3 +
            CASE 
                WHEN dissolved_oxygen_mg_l > 7 THEN 100
                WHEN dissolved_oxygen_mg_l > 5 THEN 70
                WHEN dissolved_oxygen_mg_l > 3 THEN 40
                ELSE 10
            END * 0.4 +
            CASE 
                WHEN turbidity_ntu < 5 THEN 100
                WHEN turbidity_ntu < 10 THEN 70
                WHEN turbidity_ntu < 20 THEN 40
                ELSE 10
            END * 0.3) as water_quality_index,
            CASE 
                WHEN (CASE WHEN ph_level BETWEEN 6.5 AND 8.5 THEN 100 ELSE 0 END +
                     CASE WHEN dissolved_oxygen_mg_l > 5 THEN 100 ELSE 0 END +
                     CASE WHEN turbidity_ntu < 10 THEN 100 ELSE 0 END) >= 250 THEN 'EXCELLENT'
                WHEN (CASE WHEN ph_level BETWEEN 6.0 AND 9.0 THEN 100 ELSE 0 END +
                     CASE WHEN dissolved_oxygen_mg_l > 3 THEN 100 ELSE 0 END +
                     CASE WHEN turbidity_ntu < 20 THEN 100 ELSE 0 END) >= 200 THEN 'GOOD'
                ELSE 'POOR'
            END as overall_rating
        FROM water_monitoring_stations
        WHERE ph_level IS NOT NULL AND dissolved_oxygen_mg_l IS NOT NULL AND turbidity_ntu IS NOT NULL
        ORDER BY water_quality_index DESC
    """
}

# ========== SOURCE CAPACITY SQL QUERIES ==========
SOURCE_CAPACITY_QUERIES = {
    "basic_filter": """
        SELECT * FROM water_sources 
        WHERE 1=1
        {state_filter}
        {district_filter}
        {capacity_range}
        {risk_filter}
        ORDER BY capacity_percent DESC
    """,
    
    "capacity_analysis": """
        SELECT 
            source_type,
            state,
            COUNT(*) as total_sources,
            ROUND(AVG(capacity_percent), 2) as avg_capacity,
            ROUND(MIN(capacity_percent), 2) as min_capacity,
            ROUND(MAX(capacity_percent), 2) as max_capacity,
            ROUND(STDDEV(capacity_percent), 2) as capacity_variance,
            SUM(CASE WHEN capacity_percent < 30 THEN 1 ELSE 0 END) as critical_sources,
            SUM(CASE WHEN capacity_percent BETWEEN 30 AND 60 THEN 1 ELSE 0 END) as moderate_sources,
            SUM(CASE WHEN capacity_percent > 60 THEN 1 ELSE 0 END) as good_sources
        FROM water_sources
        WHERE capacity_percent IS NOT NULL
        GROUP BY source_type, state
        ORDER BY avg_capacity DESC
    """,
    
    "stress_assessment": """
        SELECT 
            source_name,
            source_type,
            state,
            district,
            capacity_percent,
            CASE 
                WHEN capacity_percent < 20 THEN 'EMERGENCY'
                WHEN capacity_percent < 30 THEN 'CRITICAL'
                WHEN capacity_percent < 50 THEN 'STRESSED'
                WHEN capacity_percent < 70 THEN 'MODERATE'
                ELSE 'NORMAL'
            END as stress_level,
            CASE 
                WHEN capacity_percent < 30 THEN 'IMMEDIATE_ACTION_REQUIRED'
                WHEN capacity_percent < 50 THEN 'ACTION_PLANNING_REQUIRED'
                WHEN capacity_percent < 70 THEN 'MONITORING_REQUIRED'
                ELSE 'ROUTINE_OPERATION'
            END as management_action,
            RANK() OVER (ORDER BY capacity_percent) as priority_rank
        FROM water_sources
        WHERE capacity_percent IS NOT NULL
        {state_filter}
        {district_filter}
        ORDER BY capacity_percent
        LIMIT 30
    """,
    
    "trend_analysis": """
        WITH capacity_history AS (
            SELECT 
                source_name,
                source_type,
                state,
                capacity_percent,
                LAG(capacity_percent, 1) OVER (PARTITION BY source_name ORDER BY assessment_date) as prev_capacity,
                LAG(capacity_percent, 2) OVER (PARTITION BY source_name ORDER BY assessment_date) as prev2_capacity
            FROM water_sources
            WHERE capacity_percent IS NOT NULL AND assessment_date IS NOT NULL
        )
        SELECT 
            source_name,
            source_type,
            state,
            capacity_percent,
            prev_capacity,
            ROUND(capacity_percent - prev_capacity, 2) as change_from_prev,
            ROUND((capacity_percent - prev_capacity) / NULLIF(prev_capacity, 0) * 100, 2) as percent_change,
            CASE 
                WHEN capacity_percent - prev_capacity > 10 THEN 'SIGNIFICANT_IMPROVEMENT'
                WHEN capacity_percent - prev_capacity > 5 THEN 'MODERATE_IMPROVEMENT'
                WHEN capacity_percent - prev_capacity < -10 THEN 'SIGNIFICANT_DECLINE'
                WHEN capacity_percent - prev_capacity < -5 THEN 'MODERATE_DECLINE'
                ELSE 'STABLE'
            END as trend_status
        FROM capacity_history
        WHERE prev_capacity IS NOT NULL
        ORDER BY ABS(percent_change) DESC
        LIMIT 40
    """,
    
    "resource_allocation_priority": """
        SELECT 
            state,
            district,
            COUNT(*) as total_sources,
            ROUND(AVG(capacity_percent), 2) as avg_capacity,
            SUM(CASE WHEN capacity_percent < 30 THEN 1 ELSE 0 END) as critical_count,
            SUM(CASE WHEN capacity_percent < 30 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as critical_ratio,
            ROW_NUMBER() OVER (ORDER BY SUM(CASE WHEN capacity_percent < 30 THEN 1 ELSE 0 END) DESC) as intervention_priority,
            CASE 
                WHEN SUM(CASE WHEN capacity_percent < 30 THEN 1 ELSE 0 END) > 5 THEN 'HIGH_PRIORITY_INTERVENTION'
                WHEN SUM(CASE WHEN capacity_percent < 30 THEN 1 ELSE 0 END) > 2 THEN 'MEDIUM_PRIORITY_INTERVENTION'
                ELSE 'ROUTINE_MONITORING'
            END as intervention_level
        FROM water_sources
        WHERE capacity_percent IS NOT NULL
        GROUP BY state, district
        HAVING COUNT(*) > 2
        ORDER BY critical_ratio DESC
        LIMIT 25
    """
}

# ========== EXTENDED WATER USAGE SQL QUERIES ==========
WATER_USAGE_QUERIES = {
    "sector_consumption": """
        SELECT 
            sector,
            record_year,
            SUM(consumption_mcm) as total_consumption,
            AVG(consumption_mcm) as avg_consumption,
            COUNT(*) as usage_records,
            LAG(SUM(consumption_mcm), 1) OVER (PARTITION BY sector ORDER BY record_year) as prev_year_consumption,
            ROUND(((SUM(consumption_mcm) - LAG(SUM(consumption_mcm), 1) OVER (PARTITION BY sector ORDER BY record_year)) / 
                   NULLIF(LAG(SUM(consumption_mcm), 1) OVER (PARTITION BY sector ORDER BY record_year), 0) * 100), 2) as yoy_growth_pct
        FROM water_usage_history
        WHERE consumption_mcm IS NOT NULL AND sector IS NOT NULL
        GROUP BY sector, record_year
        ORDER BY sector, record_year DESC
    """,
    
    "efficiency_metrics": """
        SELECT 
            sector,
            source_type,
            ROUND(AVG(consumption_mcm), 2) as avg_consumption,
            ROUND(SUM(consumption_mcm), 2) as total_consumption,
            ROUND(AVG(consumption_mcm) / NULLIF(AVG(source_capacity), 1), 2) as utilization_efficiency,
            CASE 
                WHEN AVG(consumption_mcm) / NULLIF(AVG(source_capacity), 1) > 0.8 THEN 'OVER_UTILIZED'
                WHEN AVG(consumption_mcm) / NULLIF(AVG(source_capacity), 1) > 0.6 THEN 'EFFICIENT'
                WHEN AVG(consumption_mcm) / NULLIF(AVG(source_capacity), 1) > 0.4 THEN 'MODERATE'
                ELSE 'UNDER_UTILIZED'
            END as efficiency_rating
        FROM water_usage_history wu
        LEFT JOIN water_sources ws ON wu.source_id = ws.source_id
        WHERE consumption_mcm IS NOT NULL
        GROUP BY sector, source_type
        ORDER BY utilization_efficiency DESC
    """,
    
    "peak_demand_analysis": """
        WITH ranked_usage AS (
            SELECT 
                sector,
                season,
                record_year,
                consumption_mcm,
                ROW_NUMBER() OVER (PARTITION BY sector, record_year ORDER BY consumption_mcm DESC) as rank_in_year
            FROM water_usage_history
            WHERE consumption_mcm IS NOT NULL
        )
        SELECT 
            sector,
            season,
            record_year,
            consumption_mcm as peak_demand,
            AVG(consumption_mcm) OVER (PARTITION BY sector, record_year) as avg_demand,
            consumption_mcm - AVG(consumption_mcm) OVER (PARTITION BY sector, record_year) as demand_surplus,
            ROUND((consumption_mcm / NULLIF(AVG(consumption_mcm) OVER (PARTITION BY sector, record_year), 0) * 100), 2) as peak_to_avg_ratio
        FROM ranked_usage
        WHERE rank_in_year = 1
        ORDER BY peak_demand DESC
        LIMIT 30
    """
}

# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_all_data():
    if engine is None:
        # Return sample data for demo mode
        return create_sample_data()
    
    try:
        with engine.connect() as conn:
            conn.rollback()  # Rollback any pending transactions
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
        st.warning(f"Error loading data: {str(e)[:100]}. Using demo data.")
        return create_sample_data()

def create_sample_data():
    """Create sample data for demo mode"""
    sources = pd.DataFrame({
        'source_id': range(1, 11),
        'source_name': [f'Source {i}' for i in range(1, 11)],
        'source_type': ['River', 'Reservoir', 'Lake', 'Well', 'Canal'] * 2,
        'state': ['State A', 'State B', 'State A', 'State B', 'State A'] * 2,
        'district': [f'District {i}' for i in range(1, 11)],
        'capacity_percent': np.random.randint(20, 95, 10),
        'latitude': np.random.uniform(8, 37, 10),
        'longitude': np.random.uniform(68, 97, 10),
        'assessment_date': pd.date_range('2024-01-01', periods=10)
    })
    
    stations = pd.DataFrame({
        'station_id': range(1, 11),
        'station_name': [f'Station {i}' for i in range(1, 11)],
        'state_name': ['State A', 'State B'] * 5,
        'district_name': [f'District {i}' for i in range(1, 11)],
        'ph_level': np.random.uniform(6.5, 8.5, 10),
        'dissolved_oxygen_mg_l': np.random.uniform(4, 8, 10),
        'turbidity_ntu': np.random.uniform(2, 15, 10),
        'status': np.random.choice(['Active', 'Maintenance', 'Inactive'], 10)
    })
    
    groundwater = pd.DataFrame({
        'record_id': range(1, 21),
        'district_name': [f'District {i}' for i in range(1, 11)] * 2,
        'avg_depth_meters': np.random.uniform(10, 50, 20),
        'extraction_pct': np.random.uniform(30, 80, 20),
        'recharge_rate_mcm': np.random.uniform(100, 500, 20),
        'assessment_year': [2023, 2024] * 10,
        'stress_level': np.random.choice(['Low', 'Moderate', 'High'], 20)
    })
    
    rainfall = pd.DataFrame({
        'record_id': range(1, 31),
        'district_name': [f'District {i}' for i in range(1, 11)] * 3,
        'rainfall_cm': np.random.uniform(20, 300, 30),
        'record_year': [2022, 2023, 2024] * 10,
        'season': np.random.choice(['Winter', 'Summer', 'Monsoon', 'Post-Monsoon'], 30),
        'rainfall_category': np.random.choice(['Low', 'Moderate', 'High', 'Extreme'], 30)
    })
    
    alerts = pd.DataFrame({
        'alert_id': range(1, 6),
        'source_name': [f'Source {i}' for i in range(1, 6)],
        'alert_status': np.random.choice(['CRITICAL', 'WARNING', 'INFO'], 5),
        'alert_message': ['Low capacity', 'High extraction', 'Poor quality', 'Drought risk', 'Flood risk'][:5],
        'timestamp': pd.date_range('2024-01-01', periods=5)
    })
    
    return sources, stations, groundwater, rainfall, alerts, pd.DataFrame(), pd.DataFrame(), stations

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
def build_query_with_filters(base_query, filters):
    """Build SQL query with applied filters"""
    query = base_query
    params = []
    
    for key, value in filters.items():
        if value is not None:
            query = query.replace(f"{{{key}}}", value)
        else:
            query = query.replace(f"{{{key}}}", "")
    
    # Clean up any remaining placeholders
    query = query.replace("WHERE  AND", "WHERE")
    query = query.replace("WHERE  WHERE", "WHERE")
    query = query.replace("AND  AND", "AND")
    query = query.replace("  ", " ")
    
    return query, params

# Initialize session state for filters
if "filter_type" not in st.session_state:
    st.session_state.filter_type = "🌧️ Rainfall Filter"
if "selected_sql_query" not in st.session_state:
    st.session_state.selected_sql_query = "basic_filter"

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR FILTERS
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🎮 AQUASTAT")
    st.caption("Advanced Water Management System")
    st.markdown("---")
    
    if st.button("🔄 Reset All Filters", use_container_width=True):
        st.cache_data.clear()
        for key in list(st.session_state.keys()):
            if key not in ["filter_type", "selected_sql_query"]:
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
    options=["🌧️ Rainfall Filter", "🌊 Groundwater Filter", "💧 Water Quality (pH) Filter", "🏭 Source Capacity Filter", "📊 Water Usage Filter"],
    key="filter_type"
)

st.markdown("---")

# Store results in session state
if "filter_results" not in st.session_state:
    st.session_state.filter_results = None
if "current_query" not in st.session_state:
    st.session_state.current_query = ""

# Dynamic Filter Controls based on selection
if filter_type == "🌧️ Rainfall Filter":
    st.markdown("#### 🌧️ Rainfall Data Filter")
    
    # SQL Query Type Selection
    query_type = st.selectbox(
        "📊 Select Analysis Type",
        options=list(RAINFALL_QUERIES.keys()),
        key="rain_query_type",
        help="Choose different SQL analysis queries"
    )
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        districts = rainfall['district_name'].unique().tolist() if 'district_name' in rainfall.columns else []
        district_filter = st.selectbox("District", ["All Districts"] + list(districts), key="rain_district")
        min_rainfall = st.number_input("Min Rainfall (cm)", 0.0, 1000.0, 0.0, key="min_rainfall")
    
    with col2:
        max_rainfall = st.number_input("Max Rainfall (cm)", 0.0, 1000.0, 500.0, key="max_rainfall")
        years = rainfall['record_year'].unique().tolist() if 'record_year' in rainfall.columns else []
        year_filter = st.selectbox("Year", ["All"] + sorted(years), key="rain_year")
    
    with col3:
        season_filter = st.selectbox("Season", ["All", "Winter", "Summer", "Monsoon", "Post-Monsoon"], key="season")
        category_filter = st.selectbox("Rainfall Category", ["All", "Low", "Moderate", "High", "Extreme"], key="rain_category")
    
    # Build filter conditions
    filters = {
        "district_filter": f"AND district_name = '{district_filter}'" if district_filter != "All Districts" else "",
        "rainfall_range": f"AND rainfall_cm BETWEEN {min_rainfall} AND {max_rainfall}" if min_rainfall > 0 or max_rainfall < 1000 else "",
        "year_filter": f"AND record_year = {year_filter}" if year_filter != "All" else "",
        "season_filter": f"AND season = '{season_filter}'" if season_filter != "All" else "",
        "category_filter": f"AND rainfall_category = '{category_filter}'" if category_filter != "All" else ""
    }
    
    if st.button("🔍 Apply Rainfall Filter", use_container_width=True, type="primary"):
        base_query = RAINFALL_QUERIES.get(query_type, RAINFALL_QUERIES["basic_filter"])
        query, params = build_query_with_filters(base_query, filters)
        st.session_state.current_query = query
        results, error = execute_sql_query(query, params)
        if error:
            st.error(f"Error: {error}")
            st.session_state.filter_results = None
        else:
            st.session_state.filter_results = results
            st.success(f"✅ Found {len(results)} records")

elif filter_type == "🌊 Groundwater Filter":
    st.markdown("#### 🌊 Groundwater Data Filter")
    
    query_type = st.selectbox(
        "📊 Select Analysis Type",
        options=list(GROUNDWATER_QUERIES.keys()),
        key="gw_query_type"
    )
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        districts = groundwater['district_name'].unique().tolist() if 'district_name' in groundwater.columns else []
        gw_district = st.selectbox("District", ["All Districts"] + list(districts), key="gw_district")
        min_depth = st.number_input("Min Depth (m)", 0.0, 100.0, 0.0, key="min_depth")
    
    with col2:
        max_depth = st.number_input("Max Depth (m)", 0.0, 100.0, 50.0, key="max_depth")
        stress_filter = st.selectbox("Stress Level", ["All", "Low", "Moderate", "High"], key="stress_level")
    
    with col3:
        years = groundwater['assessment_year'].unique().tolist() if 'assessment_year' in groundwater.columns else []
        gw_year = st.selectbox("Assessment Year", ["All"] + sorted(years), key="gw_year")
    
    filters = {
        "district_filter": f"AND district_name = '{gw_district}'" if gw_district != "All Districts" else "",
        "depth_range": f"AND avg_depth_meters BETWEEN {min_depth} AND {max_depth}" if min_depth > 0 or max_depth < 100 else "",
        "stress_filter": f"AND stress_level = '{stress_filter}'" if stress_filter != "All" else "",
        "year_filter": f"AND assessment_year = {gw_year}" if gw_year != "All" else ""
    }
    
    if st.button("🔍 Apply Groundwater Filter", use_container_width=True, type="primary"):
        base_query = GROUNDWATER_QUERIES.get(query_type, GROUNDWATER_QUERIES["basic_filter"])
        query, params = build_query_with_filters(base_query, filters)
        st.session_state.current_query = query
        results, error = execute_sql_query(query, params)
        if error:
            st.error(f"Error: {error}")
            st.session_state.filter_results = None
        else:
            st.session_state.filter_results = results
            st.success(f"✅ Found {len(results)} records")

elif filter_type == "💧 Water Quality (pH) Filter":
    st.markdown("#### 💧 Water Quality & pH Filter")
    
    query_type = st.selectbox(
        "📊 Select Analysis Type",
        options=list(WATER_QUALITY_QUERIES.keys()),
        key="wq_query_type"
    )
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        states = water_quality['state_name'].unique().tolist() if 'state_name' in water_quality.columns else []
        wq_state = st.selectbox("State", ["All States"] + list(states), key="wq_state")
        min_ph = st.number_input("Min pH Level", 0.0, 14.0, 0.0, key="min_ph")
    
    with col2:
        districts = water_quality['district_name'].unique().tolist() if 'district_name' in water_quality.columns else []
        wq_district = st.selectbox("District", ["All Districts"] + list(districts), key="wq_district")
        max_ph = st.number_input("Max pH Level", 0.0, 14.0, 14.0, key="max_ph")
    
    with col3:
        status_filter = st.selectbox("Station Status", ["All", "Active", "Maintenance", "Inactive"], key="wq_status")
        st.markdown("**pH Guide:** 6.5-8.5 (Ideal) | 6-9 (Acceptable)")
    
    filters = {
        "state_filter": f"AND state_name = '{wq_state}'" if wq_state != "All States" else "",
        "district_filter": f"AND district_name = '{wq_district}'" if wq_district != "All Districts" else "",
        "ph_range": f"AND ph_level BETWEEN {min_ph} AND {max_ph}" if min_ph > 0 or max_ph < 14 else "",
        "status_filter": f"AND status = '{status_filter}'" if status_filter != "All" else ""
    }
    
    if st.button("🔍 Apply Water Quality Filter", use_container_width=True, type="primary"):
        base_query = WATER_QUALITY_QUERIES.get(query_type, WATER_QUALITY_QUERIES["basic_filter"])
        query, params = build_query_with_filters(base_query, filters)
        st.session_state.current_query = query
        results, error = execute_sql_query(query, params)
        if error:
            st.error(f"Error: {error}")
            st.session_state.filter_results = None
        else:
            st.session_state.filter_results = results
            st.success(f"✅ Found {len(results)} records")

elif filter_type == "🏭 Source Capacity Filter":
    st.markdown("#### 🏭 Water Source Capacity Filter")
    
    query_type = st.selectbox(
        "📊 Select Analysis Type",
        options=list(SOURCE_CAPACITY_QUERIES.keys()),
        key="cap_query_type"
    )
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        states = sources['state'].unique().tolist() if not sources.empty and 'state' in sources.columns else []
        cap_state = st.selectbox("State", ["All States"] + list(states), key="cap_state")
        min_capacity = st.number_input("Min Capacity (%)", 0, 100, 0, key="min_capacity")
    
    with col2:
        districts = sources['district'].unique().tolist() if not sources.empty and 'district' in sources.columns else []
        cap_district = st.selectbox("District", ["All Districts"] + list(districts), key="cap_district")
        max_capacity = st.number_input("Max Capacity (%)", 0, 100, 100, key="max_capacity")
    
    with col3:
        risk_filter = st.selectbox("Risk Level", ["All Risk Levels", "Critical", "Moderate", "Good"], key="risk_level_filter")
        st.markdown("**Risk Guide:** <30% Critical | 30-60% Moderate | >60% Good")
    
    filters = {
        "state_filter": f"AND state = '{cap_state}'" if cap_state != "All States" else "",
        "district_filter": f"AND district = '{cap_district}'" if cap_district != "All Districts" else "",
        "capacity_range": f"AND capacity_percent BETWEEN {min_capacity} AND {max_capacity}" if min_capacity > 0 or max_capacity < 100 else "",
        "risk_filter": f"AND risk_level = '{risk_filter}'" if risk_filter != "All Risk Levels" else ""
    }
    
    if st.button("🔍 Apply Source Capacity Filter", use_container_width=True, type="primary"):
        base_query = SOURCE_CAPACITY_QUERIES.get(query_type, SOURCE_CAPACITY_QUERIES["basic_filter"])
        query, params = build_query_with_filters(base_query, filters)
        st.session_state.current_query = query
        results, error = execute_sql_query(query, params)
        if error:
            st.error(f"Error: {error}")
            st.session_state.filter_results = None
        else:
            st.session_state.filter_results = results
            st.success(f"✅ Found {len(results)} records")

elif filter_type == "📊 Water Usage Filter":
    st.markdown("#### 📊 Water Usage Data Filter")
    
    query_type = st.selectbox(
        "📊 Select Analysis Type",
        options=list(WATER_USAGE_QUERIES.keys()),
        key="usage_query_type"
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        sectors = ["All", "Agriculture", "Industrial", "Domestic", "Power Generation"]
        usage_sector = st.selectbox("Sector", sectors, key="usage_sector")
        min_consumption = st.number_input("Min Consumption (MCM)", 0.0, 10000.0, 0.0, key="min_consumption")
    
    with col2:
        usage_year = st.selectbox("Year", ["All"] + list(range(2020, 2026)), key="usage_year")
        max_consumption = st.number_input("Max Consumption (MCM)", 0.0, 10000.0, 5000.0, key="max_consumption")
    
    filters = {
        "sector_filter": f"AND sector = '{usage_sector}'" if usage_sector != "All" else "",
        "year_filter": f"AND record_year = {usage_year}" if usage_year != "All" else "",
        "consumption_range": f"AND consumption_mcm BETWEEN {min_consumption} AND {max_consumption}" if min_consumption > 0 or max_consumption < 10000 else ""
    }
    
    if st.button("🔍 Apply Water Usage Filter", use_container_width=True, type="primary"):
        base_query = WATER_USAGE_QUERIES.get(query_type, WATER_USAGE_QUERIES["sector_consumption"])
        query, params = build_query_with_filters(base_query, filters)
        st.session_state.current_query = query
        results, error = execute_sql_query(query, params)
        if error:
            st.error(f"Error: {error}")
            st.session_state.filter_results = None
        else:
            st.session_state.filter_results = results
            st.success(f"✅ Found {len(results)} records")

st.markdown('</div>', unsafe_allow_html=True)

# Display SQL Query and Results
if st.session_state.current_query:
    st.markdown("---")
    st.markdown("### 📝 Executed SQL Query")
    with st.expander("View SQL Query", expanded=False):
        st.code(st.session_state.current_query, language="sql")
    
    if st.session_state.filter_results is not None and not st.session_state.filter_results.empty:
        st.markdown("### 📊 Filter Results")
        st.dataframe(st.session_state.filter_results, use_container_width=True)
        st.caption(f"📈 Total records found: {len(st.session_state.filter_results)}")
        
        # Download button
        csv = st.session_state.filter_results.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download Results as CSV", csv, f"{filter_type.replace(' ', '_')}_results.csv", "text/csv")

# ─────────────────────────────────────────────────────────────────────────────
# APPLY FILTERS FOR MAP AND DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────
def apply_source_filters():
    df = sources.copy()
    if df.empty:
        return df
    
    if 'selected_state' in st.session_state and st.session_state.selected_state != "All States" and "state" in df.columns:
        df = df[df["state"] == st.session_state.selected_state]
    if 'selected_district' in st.session_state and st.session_state.selected_district != "All Districts" and "district" in df.columns:
        df = df[df["district"] == st.session_state.selected_district]
    if 'selected_type' in st.session_state and st.session_state.selected_type != "All Types" and "source_type" in df.columns:
        df = df[df["source_type"] == st.session_state.selected_type]
    if 'selected_risk' in st.session_state and st.session_state.selected_risk != "All Risk Levels" and "risk_level" in df.columns:
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

# ─────────────────────────────────────────────────────────────────────────────
# HEADER + KPIs
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(f"<h1 style='color:#00e5ff'>💧 AQUASTAT</h1>", unsafe_allow_html=True)
st.markdown(f"<p>National Water Command Center • Live Intelligence • {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}</p>", unsafe_allow_html=True)

total_cap = filtered_sources["capacity_percent"].mean() if not filtered_sources.empty and "capacity_percent" in filtered_sources.columns else 0
critical_n = len(filtered_sources[filtered_sources["capacity_percent"] < 30]) if not filtered_sources.empty and "capacity_percent" in filtered_sources.columns else 0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Sources", len(sources) if not sources.empty else 0)
k2.metric("Avg Capacity", f"{total_cap:.1f}%" if total_cap else "0%")
k3.metric("Critical Sources", critical_n)
k4.metric("Active Alerts", len(alerts) if not alerts.empty else 0)
k5.metric("GW Records", len(groundwater) if not groundwater.empty else 0)

# ─────────────────────────────────────────────────────────────────────────────
# MAIN TABS
# ─────────────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📊 Dashboard", "🗺️ Map View", "📈 Analytics", "💧 Water Quality", "⚠️ Alerts", "🗄️ SQL Queries"])

# TAB 1: DASHBOARD
with tab1:
    if filtered_sources.empty:
        st.warning("No data available for the selected filters")
    else:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Water Sources by Type")
            if 'source_type' in filtered_sources.columns:
                type_counts = filtered_sources['source_type'].value_counts()
                if not type_counts.empty:
                    fig = px.pie(values=type_counts.values, names=type_counts.index, title="Source Distribution")
                    st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Risk Level Distribution")
            if 'risk_level' in filtered_sources.columns:
                risk_counts = filtered_sources['risk_level'].value_counts()
                if not risk_counts.empty:
                    fig = px.bar(x=risk_counts.index, y=risk_counts.values, title="Risk Assessment", color=risk_counts.index)
                    st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Water Sources Data")
        st.dataframe(filtered_sources, use_container_width=True)

# TAB 2: MAP VIEW
with tab2:
    st.subheader("Interactive Water Resources Map")
    
    # Check if latitude and longitude columns exist
    if 'latitude' in filtered_sources.columns and 'longitude' in filtered_sources.columns:
        map_sources = filtered_sources[filtered_sources["latitude"].notna() & filtered_sources["longitude"].notna()].copy()
        
        if not map_sources.empty:
            center_lat = map_sources["latitude"].mean()
            center_lon = map_sources["longitude"].mean()
            m = folium.Map(location=[center_lat, center_lon], zoom_start=6)
            
            cluster = MarkerCluster().add_to(m) if show_clusters else m
            for _, row in map_sources.iterrows():
                capacity = row.get("capacity_percent", 100)
                if pd.isna(capacity):
                    capacity = 100
                color = "red" if capacity < 30 else "orange" if capacity < 60 else "green"
                popup_text = f"{row.get('source_name', 'Unknown')}<br>Capacity: {capacity:.1f}%"
                folium.CircleMarker(
                    [row["latitude"], row["longitude"]], 
                    radius=marker_size, 
                    color=color, 
                    fill=True,
                    popup=popup_text
                ).add_to(cluster)
            
            st_folium(m, width=1200, height=600)
        else:
            st.warning("No coordinates available for selected filters")
    else:
        st.warning("Latitude/Longitude columns not available in the data")

# TAB 3: ANALYTICS
with tab3:
    st.subheader("Analytics Dashboard")
    col1, col2 = st.columns(2)
    with col1:
        if not rainfall.empty and 'record_year' in rainfall.columns and 'rainfall_cm' in rainfall.columns:
            rain_trend = rainfall.groupby('record_year')['rainfall_cm'].mean()
            if not rain_trend.empty:
                fig = px.line(x=rain_trend.index, y=rain_trend.values, title="Rainfall Trend")
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No rainfall data available")
    with col2:
        if not groundwater.empty and 'assessment_year' in groundwater.columns and 'avg_depth_meters' in groundwater.columns:
            gw_trend = groundwater.groupby('assessment_year')['avg_depth_meters'].mean()
            if not gw_trend.empty:
                fig = px.line(x=gw_trend.index, y=gw_trend.values, title="Groundwater Depth Trend")
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No groundwater data available")

# TAB 4: WATER QUALITY
with tab4:
    st.subheader("Water Quality Monitoring")
    if not water_quality.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Avg pH", f"{water_quality['ph_level'].mean():.2f}" if 'ph_level' in water_quality else "N/A")
        col2.metric("Avg Dissolved Oxygen", f"{water_quality['dissolved_oxygen_mg_l'].mean():.1f} mg/L" if 'dissolved_oxygen_mg_l' in water_quality else "N/A")
        col3.metric("Active Stations", len(water_quality[water_quality['status'] == 'Active']) if 'status' in water_quality else 0)
        st.dataframe(water_quality.head(100), use_container_width=True)
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
    st.subheader("SQL Query Workspace - Advanced Analytics")
    
    # Quick SQL Templates
    st.markdown("### 📋 Quick SQL Templates")
    template_queries = {
        "Top 10 Water Sources by Capacity": "SELECT source_name, source_type, state, capacity_percent FROM water_sources ORDER BY capacity_percent DESC LIMIT 10",
        "Critical Sources (<30% capacity)": "SELECT source_name, state, district, capacity_percent FROM water_sources WHERE capacity_percent < 30 ORDER BY capacity_percent",
        "Average Rainfall by District": "SELECT district_name, ROUND(AVG(rainfall_cm), 2) as avg_rainfall FROM rainfall_history GROUP BY district_name ORDER BY avg_rainfall DESC",
        "Groundwater Extraction Analysis": "SELECT district_name, ROUND(AVG(extraction_pct), 2) as avg_extraction FROM groundwater_levels GROUP BY district_name ORDER BY avg_extraction DESC",
        "Water Quality Summary": "SELECT state_name, ROUND(AVG(ph_level), 2) as avg_ph, ROUND(AVG(dissolved_oxygen_mg_l), 2) as avg_do FROM water_monitoring_stations GROUP BY state_name",
        "Join Query - Sources with Usage": """
            SELECT ws.source_name, ws.source_type, wu.sector, wu.consumption_mcm, wu.record_year
            FROM water_sources ws
            LEFT JOIN water_usage_history wu ON ws.source_id = wu.source_id
            WHERE wu.consumption_mcm IS NOT NULL
            ORDER BY wu.consumption_mcm DESC
            LIMIT 20
        """
    }
    
    selected_template = st.selectbox("Load SQL Template", ["-- Select a template --"] + list(template_queries.keys()))
    if selected_template != "-- Select a template --":
        st.session_state.custom_query = template_queries[selected_template]
    
    st.markdown("### ✍️ Custom SQL Query")
    custom_query = st.text_area("Enter SQL Query:", height=200, value=st.session_state.get("custom_query", "SELECT * FROM water_sources LIMIT 50"), key="custom_query")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("▶️ Execute Query", use_container_width=True, type="primary"):
            if custom_query.strip():
                results, error = execute_sql_query(custom_query)
                if error:
                    st.error(f"Error: {error}")
                else:
                    st.session_state.custom_results = results
                    st.success(f"✅ Found {len(results)} records")
                    st.code(custom_query, language="sql")
                    st.dataframe(results, use_container_width=True)
                    
                    if not results.empty:
                        csv = results.to_csv(index=False).encode('utf-8')
                        st.download_button("📥 Download Results", csv, "query_results.csv", "text/csv")
            else:
                st.warning("Please enter a query")
    
    with col2:
        if st.button("📊 Show Table Info", use_container_width=True):
            try:
                # Get table information
                tables = ["water_sources", "water_monitoring_stations", "groundwater_levels", "rainfall_history", "active_alerts", "water_usage_history"]
                info_text = "### Available Tables:\n"
                for tbl in tables:
                    info_text += f"- `{tbl}`\n"
                st.markdown(info_text)
                st.info("Use these table names in your SQL queries. Check column names using: SELECT * FROM table_name LIMIT 1")
            except:
                st.warning("Could not fetch table information")

# ─────────────────────────────────────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(f"<p style='text-align:center'>AQUASTAT v4.0 • Advanced SQL Analytics • Updated: {current_time.strftime('%Y-%m-%d %H:%M IST')}</p>", unsafe_allow_html=True)

if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()
