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
}
.kpi-value { color:#00e5ff; font-size:2rem; font-weight:700; }
.badge-critical { background: linear-gradient(135deg,#c0392b,#e74c3c); color:#fff; padding:6px 18px; border-radius:30px; display:inline-block; }
.badge-warning { background: linear-gradient(135deg,#e67e22,#f1c40f); color:#1a1a1a; padding:6px 18px; border-radius:30px; display:inline-block; }
.badge-good { background: linear-gradient(135deg,#27ae60,#2ecc71); color:#fff; padding:6px 18px; border-radius:30px; display:inline-block; }
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
        })
    elif "groundwater" in query.lower():
        return pd.DataFrame({
            'district_name': ['North District', 'South District', 'East District', 'West District', 'Central District'],
            'avg_depth_meters': [35.6, 28.2, 42.1, 22.5, 31.8],
            'extraction_pct': [72.4, 58.3, 81.2, 45.6, 63.5],
            'assessment_year': [2024, 2024, 2024, 2024, 2024],
        })
    elif "water_monitoring" in query.lower():
        return pd.DataFrame({
            'station_name': ['Station A', 'Station B', 'Station C', 'Station D', 'Station E'],
            'ph_level': [7.2, 7.5, 6.8, 7.8, 7.1],
            'dissolved_oxygen_mg_l': [6.8, 7.2, 5.9, 7.5, 6.5],
            'status': ['Active', 'Active', 'Maintenance', 'Active', 'Active'],
        })
    else:
        return pd.DataFrame({'message': ['Demo data - Connect to database for live data']})

# ─────────────────────────────────────────────────────────────────────────────
# EXTENSIVE SQL QUERIES - 50+ QUERIES
# ─────────────────────────────────────────────────────────────────────────────

# ========== 15 RAINFALL SQL QUERIES ==========
RAINFALL_QUERIES = {
    "1. Basic Rainfall Filter": """
        SELECT * FROM rainfall_history 
        WHERE 1=1
        {district_filter}
        {rainfall_range}
        {year_filter}
        {season_filter}
        ORDER BY record_year DESC, rainfall_cm DESC
    """,
    
    "2. Yearly Rainfall Trend": """
        SELECT 
            record_year,
            COUNT(*) as total_records,
            ROUND(AVG(rainfall_cm), 2) as avg_rainfall,
            ROUND(MIN(rainfall_cm), 2) as min_rainfall,
            ROUND(MAX(rainfall_cm), 2) as max_rainfall,
            ROUND(STDDEV(rainfall_cm), 2) as std_deviation
        FROM rainfall_history
        WHERE rainfall_cm IS NOT NULL
        GROUP BY record_year
        ORDER BY record_year DESC
    """,
    
    "3. Seasonal Rainfall Analysis": """
        SELECT 
            season,
            COUNT(*) as total_records,
            ROUND(AVG(rainfall_cm), 2) as avg_rainfall,
            ROUND(MIN(rainfall_cm), 2) as min_rainfall,
            ROUND(MAX(rainfall_cm), 2) as max_rainfall,
            ROUND(AVG(rainfall_cm) - LAG(AVG(rainfall_cm)) OVER (ORDER BY season), 2) as change_from_prev_season
        FROM rainfall_history
        WHERE season IS NOT NULL
        GROUP BY season
        ORDER BY avg_rainfall DESC
    """,
    
    "4. District-wise Rainfall Comparison": """
        SELECT 
            district_name,
            COUNT(*) as records,
            ROUND(AVG(rainfall_cm), 2) as avg_rainfall,
            ROUND(SUM(rainfall_cm), 2) as total_rainfall,
            RANK() OVER (ORDER BY AVG(rainfall_cm) DESC) as rainfall_rank
        FROM rainfall_history
        GROUP BY district_name
        HAVING COUNT(*) > 3
        ORDER BY avg_rainfall DESC
    """,
    
    "5. Extreme Rainfall Events": """
        SELECT 
            district_name,
            record_year,
            season,
            rainfall_cm,
            CASE 
                WHEN rainfall_cm > 300 THEN 'EXTREME_DANGER'
                WHEN rainfall_cm > 200 THEN 'SEVERE'
                WHEN rainfall_cm > 150 THEN 'HEAVY'
                WHEN rainfall_cm > 100 THEN 'MODERATE'
                ELSE 'NORMAL'
            END as severity_level,
            RANK() OVER (PARTITION BY district_name ORDER BY rainfall_cm DESC) as rank_in_district
        FROM rainfall_history
        WHERE rainfall_cm > 100
        ORDER BY rainfall_cm DESC
        LIMIT 50
    """,
    
    "6. Rainfall Anomaly Detection": """
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
            ROUND(s.mean_rainfall, 2) as avg_rainfall,
            CASE 
                WHEN rh.rainfall_cm > s.mean_rainfall + 2 * s.std_rainfall THEN 'Extreme_High_Anomaly'
                WHEN rh.rainfall_cm < s.mean_rainfall - 2 * s.std_rainfall THEN 'Extreme_Low_Anomaly'
                WHEN rh.rainfall_cm > s.mean_rainfall + s.std_rainfall THEN 'High_Anomaly'
                WHEN rh.rainfall_cm < s.mean_rainfall - s.std_rainfall THEN 'Low_Anomaly'
                ELSE 'Normal'
            END as anomaly_type
        FROM rainfall_history rh
        JOIN stats s ON rh.district_name = s.district_name
        ORDER BY ABS(rh.rainfall_cm - s.mean_rainfall) DESC
        LIMIT 30
    """,
    
    "7. Cumulative Rainfall Analysis": """
        SELECT 
            district_name,
            record_year,
            season,
            rainfall_cm,
            SUM(rainfall_cm) OVER (PARTITION BY district_name, record_year ORDER BY season) as cumulative_rainfall,
            AVG(rainfall_cm) OVER (PARTITION BY district_name ORDER BY record_year) as moving_avg_3yr,
            LAG(rainfall_cm, 1) OVER (PARTITION BY district_name ORDER BY record_year) as previous_year_rainfall,
            ROUND((rainfall_cm - LAG(rainfall_cm, 1) OVER (PARTITION BY district_name ORDER BY record_year)) * 100.0 / 
                  NULLIF(LAG(rainfall_cm, 1) OVER (PARTITION BY district_name ORDER BY record_year), 0), 2) as yoy_change_pct
        FROM rainfall_history
        WHERE rainfall_cm IS NOT NULL
        ORDER BY district_name, record_year DESC
    """,
    
    "8. Monsoon Performance Analysis": """
        SELECT 
            district_name,
            record_year,
            SUM(CASE WHEN season = 'Monsoon' THEN rainfall_cm ELSE 0 END) as monsoon_rainfall,
            SUM(CASE WHEN season NOT IN ('Monsoon') THEN rainfall_cm ELSE 0 END) as non_monsoon_rainfall,
            ROUND(SUM(CASE WHEN season = 'Monsoon' THEN rainfall_cm ELSE 0 END) * 100.0 / 
                  NULLIF(SUM(rainfall_cm), 0), 2) as monsoon_contribution_pct
        FROM rainfall_history
        GROUP BY district_name, record_year
        ORDER BY monsoon_rainfall DESC
    """,
    
    "9. Rainfall Frequency Distribution": """
        SELECT 
            CASE 
                WHEN rainfall_cm BETWEEN 0 AND 50 THEN '0-50 cm (Low)'
                WHEN rainfall_cm BETWEEN 51 AND 150 THEN '51-150 cm (Moderate)'
                WHEN rainfall_cm BETWEEN 151 AND 300 THEN '151-300 cm (High)'
                ELSE '300+ cm (Extreme)'
            END as rainfall_range,
            COUNT(*) as frequency,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
        FROM rainfall_history
        WHERE rainfall_cm IS NOT NULL
        GROUP BY 
            CASE 
                WHEN rainfall_cm BETWEEN 0 AND 50 THEN '0-50 cm (Low)'
                WHEN rainfall_cm BETWEEN 51 AND 150 THEN '51-150 cm (Moderate)'
                WHEN rainfall_cm BETWEEN 151 AND 300 THEN '151-300 cm (High)'
                ELSE '300+ cm (Extreme)'
            END
        ORDER BY MIN(rainfall_cm)
    """,
    
    "10. Year-over-Year Rainfall Change": """
        SELECT 
            district_name,
            record_year,
            rainfall_cm,
            LAG(rainfall_cm, 1) OVER (PARTITION BY district_name ORDER BY record_year) as prev_year_rainfall,
            rainfall_cm - LAG(rainfall_cm, 1) OVER (PARTITION BY district_name ORDER BY record_year) as absolute_change,
            ROUND((rainfall_cm - LAG(rainfall_cm, 1) OVER (PARTITION BY district_name ORDER BY record_year)) * 100.0 / 
                  NULLIF(LAG(rainfall_cm, 1) OVER (PARTITION BY district_name ORDER BY record_year), 0), 2) as percent_change
        FROM (
            SELECT district_name, record_year, AVG(rainfall_cm) as rainfall_cm
            FROM rainfall_history
            GROUP BY district_name, record_year
        ) yearly_avg
        ORDER BY district_name, record_year DESC
    """,
    
    "11. Drought Risk Assessment": """
        SELECT 
            district_name,
            record_year,
            AVG(rainfall_cm) as avg_rainfall,
            CASE 
                WHEN AVG(rainfall_cm) < 50 THEN 'EXTREME_DROUGHT_RISK'
                WHEN AVG(rainfall_cm) < 100 THEN 'SEVERE_DROUGHT_RISK'
                WHEN AVG(rainfall_cm) < 150 THEN 'MODERATE_DROUGHT_RISK'
                ELSE 'NO_DROUGHT_RISK'
            END as drought_risk_level,
            RANK() OVER (ORDER BY AVG(rainfall_cm)) as drought_rank
        FROM rainfall_history
        GROUP BY district_name, record_year
        HAVING AVG(rainfall_cm) < 150
        ORDER BY avg_rainfall
        LIMIT 20
    """,
    
    "12. Peak Rainfall Months Analysis": """
        SELECT 
            district_name,
            EXTRACT(MONTH FROM record_date) as month,
            AVG(rainfall_cm) as avg_rainfall,
            MAX(rainfall_cm) as peak_rainfall,
            RANK() OVER (PARTITION BY district_name ORDER BY AVG(rainfall_cm) DESC) as peak_month_rank
        FROM rainfall_history
        WHERE record_date IS NOT NULL
        GROUP BY district_name, EXTRACT(MONTH FROM record_date)
        ORDER BY district_name, avg_rainfall DESC
    """,
    
    "13. Rainfall Trend Prediction": """
        WITH ranked_rainfall AS (
            SELECT 
                district_name,
                record_year,
                AVG(rainfall_cm) as avg_rainfall,
                ROW_NUMBER() OVER (PARTITION BY district_name ORDER BY record_year) as rn,
                COUNT(*) OVER (PARTITION BY district_name) as total_years
            FROM rainfall_history
            GROUP BY district_name, record_year
        )
        SELECT 
            district_name,
            record_year,
            avg_rainfall,
            CASE 
                WHEN avg_rainfall > AVG(avg_rainfall) OVER (PARTITION BY district_name) THEN 'ABOVE_AVERAGE'
                ELSE 'BELOW_AVERAGE'
            END as trend_direction,
            ROUND((avg_rainfall - LAG(avg_rainfall, 1) OVER (PARTITION BY district_name ORDER BY record_year)) * 100.0 /
                  NULLIF(LAG(avg_rainfall, 1) OVER (PARTITION BY district_name ORDER BY record_year), 0), 2) as yoy_trend_pct
        FROM ranked_rainfall
        WHERE total_years >= 3
        ORDER BY district_name, record_year DESC
    """,
    
    "14. Rainfall Variability Index": """
        SELECT 
            district_name,
            COUNT(*) as num_records,
            ROUND(AVG(rainfall_cm), 2) as mean_rainfall,
            ROUND(STDDEV(rainfall_cm), 2) as std_deviation,
            ROUND(STDDEV(rainfall_cm) * 100.0 / NULLIF(AVG(rainfall_cm), 0), 2) as coefficient_of_variation,
            CASE 
                WHEN STDDEV(rainfall_cm) * 100.0 / AVG(rainfall_cm) > 50 THEN 'HIGHLY_VARIABLE'
                WHEN STDDEV(rainfall_cm) * 100.0 / AVG(rainfall_cm) > 30 THEN 'MODERATELY_VARIABLE'
                ELSE 'STABLE'
            END as variability_category
        FROM rainfall_history
        WHERE rainfall_cm IS NOT NULL
        GROUP BY district_name
        HAVING COUNT(*) > 3
        ORDER BY coefficient_of_variation DESC
    """,
    
    "15. Rainfall Efficiency Score": """
        SELECT 
            district_name,
            record_year,
            AVG(rainfall_cm) as actual_rainfall,
            (AVG(rainfall_cm) - MIN(rainfall_cm) OVER (PARTITION BY district_name)) * 100.0 /
            NULLIF(MAX(rainfall_cm) OVER (PARTITION BY district_name) - MIN(rainfall_cm) OVER (PARTITION BY district_name), 0) as efficiency_score,
            CASE 
                WHEN (AVG(rainfall_cm) - MIN(rainfall_cm) OVER (PARTITION BY district_name)) * 100.0 /
                     NULLIF(MAX(rainfall_cm) OVER (PARTITION BY district_name) - MIN(rainfall_cm) OVER (PARTITION BY district_name), 0) > 75 THEN 'EXCELLENT'
                WHEN (AVG(rainfall_cm) - MIN(rainfall_cm) OVER (PARTITION BY district_name)) * 100.0 /
                     NULLIF(MAX(rainfall_cm) OVER (PARTITION BY district_name) - MIN(rainfall_cm) OVER (PARTITION BY district_name), 0) > 50 THEN 'GOOD'
                ELSE 'NEEDS_IMPROVEMENT'
            END as efficiency_rating
        FROM rainfall_history
        GROUP BY district_name, record_year
        ORDER BY efficiency_score DESC
    """
}

# ========== 12 GROUNDWATER SQL QUERIES ==========
GROUNDWATER_QUERIES = {
    "1. Basic Groundwater Filter": """
        SELECT * FROM groundwater_levels 
        WHERE 1=1
        {district_filter}
        {depth_range}
        {year_filter}
        ORDER BY assessment_year DESC, avg_depth_meters DESC
    """,
    
    "2. Groundwater Depletion Rate": """
        SELECT 
            district_name,
            MIN(assessment_year) as first_year,
            MAX(assessment_year) as last_year,
            ROUND(AVG(avg_depth_meters), 2) as avg_depth,
            ROUND(STDDEV(avg_depth_meters), 2) as depth_variability,
            ROUND((MAX(avg_depth_meters) - MIN(avg_depth_meters)) / 
                  NULLIF(MAX(assessment_year) - MIN(assessment_year), 0), 2) as annual_depletion_rate,
            CASE 
                WHEN (MAX(avg_depth_meters) - MIN(avg_depth_meters)) / (MAX(assessment_year) - MIN(assessment_year)) > 2 THEN 'CRITICAL_DEPLETION'
                WHEN (MAX(avg_depth_meters) - MIN(avg_depth_meters)) / (MAX(assessment_year) - MIN(assessment_year)) > 1 THEN 'MODERATE_DEPLETION'
                ELSE 'STABLE'
            END as depletion_status
        FROM groundwater_levels
        WHERE avg_depth_meters IS NOT NULL
        GROUP BY district_name
        HAVING COUNT(*) > 2
        ORDER BY annual_depletion_rate DESC
    """,
    
    "3. Extraction vs Recharge Analysis": """
        SELECT 
            district_name,
            assessment_year,
            ROUND(extraction_pct, 2) as extraction_pct,
            ROUND(recharge_rate_mcm, 2) as recharge_rate,
            ROUND(extraction_pct - recharge_rate_mcm, 2) as deficit_surplus,
            CASE 
                WHEN extraction_pct > recharge_rate_mcm * 1.5 THEN 'OVER_EXTRACTION_CRITICAL'
                WHEN extraction_pct > recharge_rate_mcm THEN 'OVER_EXTRACTION_MODERATE'
                WHEN extraction_pct < recharge_rate_mcm * 0.5 THEN 'UNDER_UTILIZATION'
                ELSE 'BALANCED'
            END as extraction_status,
            RANK() OVER (ORDER BY (extraction_pct - recharge_rate_mcm) DESC) as deficit_rank
        FROM groundwater_levels
        WHERE extraction_pct IS NOT NULL AND recharge_rate_mcm IS NOT NULL
        ORDER BY deficit_surplus DESC
    """,
    
    "4. Groundwater Trend Analysis": """
        WITH depth_trend AS (
            SELECT 
                district_name,
                assessment_year,
                avg_depth_meters,
                AVG(avg_depth_meters) OVER (PARTITION BY district_name ORDER BY assessment_year 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg_3yr,
                LAG(avg_depth_meters, 1) OVER (PARTITION BY district_name ORDER BY assessment_year) as prev_year_depth
            FROM groundwater_levels
            WHERE avg_depth_meters IS NOT NULL
        )
        SELECT 
            district_name,
            assessment_year,
            avg_depth_meters,
            ROUND(moving_avg_3yr, 2) as moving_avg_3yr,
            ROUND(avg_depth_meters - moving_avg_3yr, 2) as deviation_from_trend,
            ROUND((avg_depth_meters - prev_year_depth) * 100.0 / NULLIF(prev_year_depth, 0), 2) as yoy_change_pct,
            CASE 
                WHEN avg_depth_meters > moving_avg_3yr * 1.1 THEN 'DEEPENING_FAST'
                WHEN avg_depth_meters > moving_avg_3yr * 1.05 THEN 'DEEPENING_SLOW'
                WHEN avg_depth_meters < moving_avg_3yr * 0.95 THEN 'RECOVERING'
                ELSE 'STABLE'
            END as trend_direction
        FROM depth_trend
        ORDER BY district_name, assessment_year DESC
    """,
    
    "5. Groundwater Risk Zones": """
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
    """,
    
    "6. Sustainable Yield Analysis": """
        SELECT 
            district_name,
            assessment_year,
            recharge_rate_mcm,
            extraction_pct,
            ROUND(recharge_rate_mcm * (1 - extraction_pct/100.0), 2) as sustainable_yield,
            CASE 
                WHEN extraction_pct > 80 THEN 'UNSUSTAINABLE'
                WHEN extraction_pct > 60 THEN 'STRESSED'
                WHEN extraction_pct > 40 THEN 'MODERATE'
                ELSE 'SUSTAINABLE'
            END as sustainability_status
        FROM groundwater_levels
        WHERE recharge_rate_mcm IS NOT NULL AND extraction_pct IS NOT NULL
        ORDER BY sustainable_yield DESC
    """,
    
    "7. Depth Classification Analysis": """
        SELECT 
            district_name,
            COUNT(*) as total_wells,
            SUM(CASE WHEN avg_depth_meters < 20 THEN 1 ELSE 0 END) as shallow_wells,
            SUM(CASE WHEN avg_depth_meters BETWEEN 20 AND 40 THEN 1 ELSE 0 END) as medium_wells,
            SUM(CASE WHEN avg_depth_meters > 40 THEN 1 ELSE 0 END) as deep_wells,
            ROUND(SUM(CASE WHEN avg_depth_meters > 40 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as deep_well_percentage
        FROM groundwater_levels
        GROUP BY district_name
        ORDER BY deep_well_percentage DESC
    """,
    
    "8. Groundwater Quality Index": """
        SELECT 
            district_name,
            assessment_year,
            avg_depth_meters,
            extraction_pct,
            (CASE 
                WHEN avg_depth_meters < 20 THEN 100
                WHEN avg_depth_meters < 35 THEN 70
                WHEN avg_depth_meters < 50 THEN 40
                ELSE 10
            END * 0.6 + 
            CASE 
                WHEN extraction_pct < 30 THEN 100
                WHEN extraction_pct < 50 THEN 70
                WHEN extraction_pct < 70 THEN 40
                ELSE 10
            END * 0.4) as groundwater_health_score,
            CASE 
                WHEN (CASE WHEN avg_depth_meters < 20 THEN 100 ELSE 0 END + CASE WHEN extraction_pct < 30 THEN 100 ELSE 0 END) >= 150 THEN 'EXCELLENT'
                WHEN (CASE WHEN avg_depth_meters < 35 THEN 100 ELSE 0 END + CASE WHEN extraction_pct < 50 THEN 100 ELSE 0 END) >= 150 THEN 'GOOD'
                ELSE 'POOR'
            END as overall_health_status
        FROM groundwater_levels
        ORDER BY groundwater_health_score DESC
    """,
    
    "9. Year-over-Year Depth Change": """
        SELECT 
            current.district_name,
            current.assessment_year as current_year,
            current.avg_depth_meters as current_depth,
            prev.avg_depth_meters as previous_depth,
            ROUND(current.avg_depth_meters - prev.avg_depth_meters, 2) as depth_change,
            ROUND((current.avg_depth_meters - prev.avg_depth_meters) * 100.0 / NULLIF(prev.avg_depth_meters, 0), 2) as percent_change
        FROM groundwater_levels current
        LEFT JOIN groundwater_levels prev ON current.district_name = prev.district_name 
            AND prev.assessment_year = current.assessment_year - 1
        WHERE current.avg_depth_meters IS NOT NULL
        ORDER BY percent_change DESC
    """,
    
    "10. Critical Zone Identification": """
        SELECT 
            district_name,
            assessment_year,
            avg_depth_meters,
            extraction_pct,
            CASE 
                WHEN avg_depth_meters > 35 AND extraction_pct > 65 THEN 'CRITICAL_ZONE'
                WHEN avg_depth_meters > 25 AND extraction_pct > 50 THEN 'ALERT_ZONE'
                WHEN avg_depth_meters > 15 AND extraction_pct > 35 THEN 'MONITORING_ZONE'
                ELSE 'SAFE_ZONE'
            END as critical_zone,
            'IMMEDIATE_ACTION_REQUIRED' as recommended_action
        FROM groundwater_levels
        WHERE (avg_depth_meters > 35 AND extraction_pct > 65)
           OR (avg_depth_meters > 25 AND extraction_pct > 50)
        ORDER BY avg_depth_meters DESC, extraction_pct DESC
    """,
    
    "11. Recharge Potential Assessment": """
        SELECT 
            district_name,
            assessment_year,
            recharge_rate_mcm,
            extraction_pct,
            ROUND(recharge_rate_mcm * 0.3, 2) as artificial_recharge_potential,
            CASE 
                WHEN extraction_pct > recharge_rate_mcm THEN 'RECHARGE_REQUIRED'
                WHEN extraction_pct > recharge_rate_mcm * 0.8 THEN 'RECHARGE_RECOMMENDED'
                ELSE 'ADEQUATE_RECHARGE'
            END as recharge_necessity
        FROM groundwater_levels
        WHERE recharge_rate_mcm IS NOT NULL
        ORDER BY (extraction_pct - recharge_rate_mcm) DESC
    """,
    
    "12. Groundwater Stress Index": """
        SELECT 
            district_name,
            assessment_year,
            ROUND(avg_depth_meters, 2) as depth,
            ROUND(extraction_pct, 2) as extraction,
            ROUND((avg_depth_meters / 50.0 * 100 + extraction_pct) / 2, 2) as stress_index,
            CASE 
                WHEN (avg_depth_meters / 50.0 * 100 + extraction_pct) / 2 > 80 THEN 'EXTREME_STRESS'
                WHEN (avg_depth_meters / 50.0 * 100 + extraction_pct) / 2 > 60 THEN 'HIGH_STRESS'
                WHEN (avg_depth_meters / 50.0 * 100 + extraction_pct) / 2 > 40 THEN 'MODERATE_STRESS'
                ELSE 'LOW_STRESS'
            END as stress_level
        FROM groundwater_levels
        ORDER BY stress_index DESC
    """
}

# ========== 12 WATER QUALITY SQL QUERIES ==========
WATER_QUALITY_QUERIES = {
    "1. Basic Water Quality Filter": """
        SELECT * FROM water_monitoring_stations 
        WHERE 1=1
        {state_filter}
        {district_filter}
        {ph_range}
        {status_filter}
        ORDER BY station_name
    """,
    
    "2. pH Level Analysis": """
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
    
    "3. Dissolved Oxygen Analysis": """
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
    
    "4. Turbidity Analysis": """
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
        ORDER BY turbidity_ntu DESC
        LIMIT 30
    """,
    
    "5. Comprehensive Water Quality Index": """
        SELECT 
            station_name,
            state_name,
            district_name,
            ph_level,
            dissolved_oxygen_mg_l,
            turbidity_ntu,
            ROUND(
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
                END * 0.3), 2) as water_quality_index,
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
    """,
    
    "6. Polluted Station Identification": """
        SELECT 
            station_name,
            state_name,
            district_name,
            ph_level,
            dissolved_oxygen_mg_l,
            turbidity_ntu,
            CASE 
                WHEN ph_level < 6.0 OR ph_level > 9.0 THEN 'pH_Violation'
                WHEN dissolved_oxygen_mg_l < 4 THEN 'DO_Violation'
                WHEN turbidity_ntu > 10 THEN 'Turbidity_Violation'
                ELSE 'Compliant'
            END as violation_type,
            CASE 
                WHEN ph_level < 6.0 OR ph_level > 9.0 OR dissolved_oxygen_mg_l < 4 OR turbidity_ntu > 10 THEN 'POLLUTED'
                ELSE 'CLEAN'
            END as pollution_status
        FROM water_monitoring_stations
        WHERE ph_level < 6.0 OR ph_level > 9.0 OR dissolved_oxygen_mg_l < 4 OR turbidity_ntu > 10
        ORDER BY dissolved_oxygen_mg_l, ph_level
    """,
    
    "7. Seasonal Water Quality Variation": """
        SELECT 
            state_name,
            EXTRACT(MONTH FROM monitoring_date) as month,
            ROUND(AVG(ph_level), 2) as avg_ph,
            ROUND(AVG(dissolved_oxygen_mg_l), 2) as avg_do,
            COUNT(*) as sample_count
        FROM water_monitoring_stations
        WHERE monitoring_date IS NOT NULL
        GROUP BY state_name, EXTRACT(MONTH FROM monitoring_date)
        ORDER BY state_name, month
    """,
    
    "8. Station Performance Ranking": """
        SELECT 
            station_name,
            state_name,
            district_name,
            ROUND(AVG(ph_level), 2) as avg_ph,
            ROUND(AVG(dissolved_oxygen_mg_l), 2) as avg_do,
            RANK() OVER (ORDER BY AVG(dissolved_oxygen_mg_l) DESC) as do_rank,
            RANK() OVER (ORDER BY AVG(ph_level)) as ph_rank,
            RANK() OVER (ORDER BY AVG(dissolved_oxygen_mg_l) DESC) + RANK() OVER (ORDER BY AVG(ph_level)) as combined_rank
        FROM water_monitoring_stations
        GROUP BY station_name, state_name, district_name
        ORDER BY combined_rank
        LIMIT 20
    """,
    
    "9. Water Quality Trend Analysis": """
        SELECT 
            state_name,
            district_name,
            monitoring_date,
            ph_level,
            dissolved_oxygen_mg_l,
            AVG(ph_level) OVER (PARTITION BY district_name ORDER BY monitoring_date 
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ph_moving_avg,
            AVG(dissolved_oxygen_mg_l) OVER (PARTITION BY district_name ORDER BY monitoring_date 
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as do_moving_avg
        FROM water_monitoring_stations
        WHERE monitoring_date IS NOT NULL
        ORDER BY district_name, monitoring_date DESC
    """,
    
    "10. Critical Parameter Alert": """
        SELECT 
            station_name,
            state_name,
            district_name,
            ph_level,
            dissolved_oxygen_mg_l,
            turbidity_ntu,
            CASE 
                WHEN ph_level < 6.5 THEN 'ACIDIC_CONDITION'
                WHEN ph_level > 8.5 THEN 'ALKALINE_CONDITION'
                WHEN dissolved_oxygen_mg_l < 4 THEN 'LOW_OXYGEN'
                WHEN turbidity_ntu > 15 THEN 'HIGH_TURBIDITY'
                ELSE 'NORMAL'
            END as alert_type,
            'URGENT_ATTENTION_REQUIRED' as priority
        FROM water_monitoring_stations
        WHERE ph_level < 6.5 OR ph_level > 8.5 OR dissolved_oxygen_mg_l < 4 OR turbidity_ntu > 15
        ORDER BY dissolved_oxygen_mg_l, ph_level
    """,
    
    "11. Correlation Between Parameters": """
        SELECT 
            CORR(ph_level, dissolved_oxygen_mg_l) as ph_do_correlation,
            CORR(ph_level, turbidity_ntu) as ph_turbidity_correlation,
            CORR(dissolved_oxygen_mg_l, turbidity_ntu) as do_turbidity_correlation,
            COUNT(*) as sample_size
        FROM water_monitoring_stations
        WHERE ph_level IS NOT NULL AND dissolved_oxygen_mg_l IS NOT NULL AND turbidity_ntu IS NOT NULL
    """,
    
    "12. Water Quality Compliance Report": """
        SELECT 
            state_name,
            COUNT(*) as total_stations,
            SUM(CASE WHEN ph_level BETWEEN 6.5 AND 8.5 THEN 1 ELSE 0 END) as ph_compliant,
            SUM(CASE WHEN dissolved_oxygen_mg_l > 5 THEN 1 ELSE 0 END) as do_compliant,
            SUM(CASE WHEN turbidity_ntu < 10 THEN 1 ELSE 0 END) as turbidity_compliant,
            ROUND(SUM(CASE WHEN ph_level BETWEEN 6.5 AND 8.5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as ph_compliance_pct,
            ROUND(SUM(CASE WHEN dissolved_oxygen_mg_l > 5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as do_compliance_pct
        FROM water_monitoring_stations
        GROUP BY state_name
        ORDER BY ph_compliance_pct DESC
    """
}

# ========== 11 SOURCE CAPACITY SQL QUERIES ==========
SOURCE_QUERIES = {
    "1. Basic Source Filter": """
        SELECT * FROM water_sources 
        WHERE 1=1
        {state_filter}
        {district_filter}
        {capacity_range}
        ORDER BY capacity_percent DESC
    """,
    
    "2. Capacity Analysis by Type": """
        SELECT 
            source_type,
            state,
            COUNT(*) as total_sources,
            ROUND(AVG(capacity_percent), 2) as avg_capacity,
            ROUND(MIN(capacity_percent), 2) as min_capacity,
            ROUND(MAX(capacity_percent), 2) as max_capacity,
            SUM(CASE WHEN capacity_percent < 30 THEN 1 ELSE 0 END) as critical_sources,
            SUM(CASE WHEN capacity_percent BETWEEN 30 AND 60 THEN 1 ELSE 0 END) as moderate_sources,
            SUM(CASE WHEN capacity_percent > 60 THEN 1 ELSE 0 END) as good_sources
        FROM water_sources
        WHERE capacity_percent IS NOT NULL
        GROUP BY source_type, state
        ORDER BY avg_capacity DESC
    """,
    
    "3. Stress Assessment": """
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
        ORDER BY capacity_percent
    """,
    
    "4. Capacity Trend Analysis": """
        SELECT 
            source_name,
            source_type,
            state,
            capacity_percent,
            LAG(capacity_percent, 1) OVER (PARTITION BY source_name ORDER BY assessment_date) as prev_capacity,
            ROUND(capacity_percent - LAG(capacity_percent, 1) OVER (PARTITION BY source_name ORDER BY assessment_date), 2) as change_from_prev,
            ROUND((capacity_percent - LAG(capacity_percent, 1) OVER (PARTITION BY source_name ORDER BY assessment_date)) * 100.0 / 
                  NULLIF(LAG(capacity_percent, 1) OVER (PARTITION BY source_name ORDER BY assessment_date), 0), 2) as percent_change,
            CASE 
                WHEN capacity_percent - LAG(capacity_percent, 1) OVER (PARTITION BY source_name ORDER BY assessment_date) > 10 THEN 'SIGNIFICANT_IMPROVEMENT'
                WHEN capacity_percent - LAG(capacity_percent, 1) OVER (PARTITION BY source_name ORDER BY assessment_date) > 5 THEN 'MODERATE_IMPROVEMENT'
                WHEN capacity_percent - LAG(capacity_percent, 1) OVER (PARTITION BY source_name ORDER BY assessment_date) < -10 THEN 'SIGNIFICANT_DECLINE'
                WHEN capacity_percent - LAG(capacity_percent, 1) OVER (PARTITION BY source_name ORDER BY assessment_date) < -5 THEN 'MODERATE_DECLINE'
                ELSE 'STABLE'
            END as trend_status
        FROM water_sources
        WHERE capacity_percent IS NOT NULL AND assessment_date IS NOT NULL
        ORDER BY ABS(percent_change) DESC
    """,
    
    "5. Resource Allocation Priority": """
        SELECT 
            state,
            district,
            COUNT(*) as total_sources,
            ROUND(AVG(capacity_percent), 2) as avg_capacity,
            SUM(CASE WHEN capacity_percent < 30 THEN 1 ELSE 0 END) as critical_count,
            ROUND(SUM(CASE WHEN capacity_percent < 30 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as critical_ratio,
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
    """,
    
    "6. Efficiency Score": """
        SELECT 
            source_name,
            source_type,
            state,
            capacity_percent,
            (capacity_percent / 100.0 * 
             (CASE 
                 WHEN source_type = 'Reservoir' THEN 0.9
                 WHEN source_type = 'River' THEN 0.85
                 WHEN source_type = 'Lake' THEN 0.8
                 ELSE 0.75
             END) * 100) as efficiency_score,
            CASE 
                WHEN (capacity_percent / 100.0 * 
                     (CASE WHEN source_type = 'Reservoir' THEN 0.9 ELSE 0.8 END) * 100) > 70 THEN 'HIGH_EFFICIENCY'
                WHEN (capacity_percent / 100.0 * 
                     (CASE WHEN source_type = 'Reservoir' THEN 0.9 ELSE 0.8 END) * 100) > 50 THEN 'MODERATE_EFFICIENCY'
                ELSE 'LOW_EFFICIENCY'
            END as efficiency_rating
        FROM water_sources
        ORDER BY efficiency_score DESC
    """,
    
    "7. Geographic Distribution": """
        SELECT 
            state,
            source_type,
            COUNT(*) as count,
            ROUND(AVG(capacity_percent), 2) as avg_capacity,
            SUM(CASE WHEN capacity_percent < 30 THEN 1 ELSE 0 END) as critical_sources
        FROM water_sources
        GROUP BY state, source_type
        ORDER BY state, avg_capacity DESC
    """,
    
    "8. Capacity Deficit Analysis": """
        SELECT 
            source_name,
            state,
            district,
            capacity_percent,
            (100 - capacity_percent) as deficit_percent,
            CASE 
                WHEN (100 - capacity_percent) > 70 THEN 'CRITICAL_DEFICIT'
                WHEN (100 - capacity_percent) > 40 THEN 'MODERATE_DEFICIT'
                ELSE 'MINOR_DEFICIT'
            END as deficit_severity,
            RANK() OVER (ORDER BY (100 - capacity_percent) DESC) as deficit_rank
        FROM water_sources
        WHERE capacity_percent < 70
        ORDER BY deficit_percent DESC
    """,
    
    "9. Seasonal Capacity Variation": """
        SELECT 
            source_name,
            source_type,
            EXTRACT(MONTH FROM assessment_date) as month,
            AVG(capacity_percent) as avg_capacity,
            MAX(capacity_percent) - MIN(capacity_percent) as capacity_range
        FROM water_sources
        WHERE assessment_date IS NOT NULL
        GROUP BY source_name, source_type, EXTRACT(MONTH FROM assessment_date)
        ORDER BY source_name, month
    """,
    
    "10. Underperforming Sources": """
        SELECT 
            source_name,
            source_type,
            state,
            district,
            capacity_percent,
            AVG(capacity_percent) OVER (PARTITION BY source_type) as type_avg_capacity,
            capacity_percent - AVG(capacity_percent) OVER (PARTITION BY source_type) as deviation_from_type_avg,
            CASE 
                WHEN capacity_percent < AVG(capacity_percent) OVER (PARTITION BY source_type) - 20 THEN 'SEVERELY_UNDERPERFORMING'
                WHEN capacity_percent < AVG(capacity_percent) OVER (PARTITION BY source_type) - 10 THEN 'UNDERPERFORMING'
                ELSE 'PERFORMING'
            END as performance_status
        FROM water_sources
        WHERE capacity_percent IS NOT NULL
        ORDER BY deviation_from_type_avg
        LIMIT 20
    """,
    
    "11. Recovery Potential": """
        SELECT 
            source_name,
            state,
            district,
            capacity_percent,
            (100 - capacity_percent) * 0.8 as recovery_potential,
            CASE 
                WHEN capacity_percent < 30 THEN 'URGENT_RECOVERY_NEEDED'
                WHEN capacity_percent < 50 THEN 'RECOVERY_PLANNING_REQUIRED'
                ELSE 'MAINTENANCE_REQUIRED'
            END as recovery_priority,
            RANK() OVER (ORDER BY (100 - capacity_percent) DESC) as recovery_rank
        FROM water_sources
        WHERE capacity_percent < 60
        ORDER BY recovery_potential DESC
    """
}

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
            water_quality = stations.copy()
            
            return sources, stations, groundwater, rainfall, alerts, water_quality
    except Exception as e:
        st.warning(f"Using demo data. Error: {str(e)[:100]}")
        return create_sample_data()

def create_sample_data():
    """Create sample data for demo mode"""
    np.random.seed(42)
    
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
    
    stations = pd.DataFrame({
        'station_id': range(1, 16),
        'station_name': [f'Station_{i}' for i in range(1, 16)],
        'state_name': np.random.choice(['State A', 'State B', 'State C', 'State D'], 15),
        'district_name': [f'District_{i}' for i in range(1, 16)],
        'ph_level': np.random.uniform(6.2, 8.8, 15),
        'dissolved_oxygen_mg_l': np.random.uniform(3.5, 8.5, 15),
        'turbidity_ntu': np.random.uniform(1, 25, 15),
        'status': np.random.choice(['Active', 'Active', 'Active', 'Maintenance', 'Inactive'], 15, p=[0.7, 0.1, 0.1, 0.05, 0.05]),
        'monitoring_date': pd.date_range('2024-01-01', periods=15)
    })
    
    groundwater = pd.DataFrame({
        'record_id': range(1, 41),
        'district_name': [f'District_{i}' for i in range(1, 9)] * 5,
        'avg_depth_meters': np.random.uniform(15, 55, 40),
        'extraction_pct': np.random.uniform(35, 85, 40),
        'recharge_rate_mcm': np.random.uniform(150, 450, 40),
        'assessment_year': np.random.choice([2020, 2021, 2022, 2023, 2024], 40),
        'stress_level': np.random.choice(['Low', 'Moderate', 'High', 'Critical'], 40)
    })
    
    rainfall = pd.DataFrame({
        'record_id': range(1, 61),
        'district_name': [f'District_{i}' for i in range(1, 9)] * 7 + [f'District_{i}' for i in range(1, 5)],
        'rainfall_cm': np.random.uniform(20, 350, 60),
        'record_year': np.random.choice([2020, 2021, 2022, 2023, 2024], 60),
        'season': np.random.choice(['Winter', 'Summer', 'Monsoon', 'Post-Monsoon'], 60),
        'record_date': pd.date_range('2020-01-01', periods=60)
    })
    
    alerts = pd.DataFrame({
        'alert_id': range(1, 8),
        'source_name': [f'Source_{i}' for i in range(1, 8)],
        'alert_status': np.random.choice(['CRITICAL', 'WARNING', 'INFO'], 7, p=[0.3, 0.4, 0.3]),
        'alert_message': ['Low capacity', 'High extraction', 'Poor quality', 'Drought risk', 'Flood watch', 'Maintenance due', 'Over usage'],
        'timestamp': pd.date_range('2024-01-01', periods=7)
    })
    
    return sources, stations, groundwater, rainfall, alerts, stations

with st.spinner("🚀 Loading AQUASTAT Data..."):
    sources, stations, groundwater, rainfall, alerts, water_quality = load_all_data()

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
# SIDEBAR FILTERS
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🎮 AQUASTAT")
    st.caption("Complete Water Management System with 50+ SQL Queries")
    st.markdown("---")
    
    if st.button("🔄 Reset All Filters", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    st.markdown("---")
    
    # SQL Query Selector in Sidebar
    st.markdown("### 📊 Select SQL Query Category")
    query_category = st.selectbox(
        "Choose Analysis Type",
        options=["🌧️ Rainfall Analysis (15 Queries)", "🌊 Groundwater Analysis (12 Queries)", "💧 Water Quality Analysis (12 Queries)", "🏭 Source Capacity Analysis (11 Queries)"],
        key="query_category"
    )
    
    st.markdown("---")
    
    # Dynamic filters based on selection
    if "Rainfall" in query_category:
        st.markdown("### 🌧️ Rainfall Filters")
        if not rainfall.empty and "district_name" in rainfall.columns:
            rain_district = st.selectbox("District", ["All Districts"] + sorted(rainfall["district_name"].dropna().unique().tolist()))
        else:
            rain_district = "All Districts"
        
        if not rainfall.empty and "record_year" in rainfall.columns:
            rain_year = st.selectbox("Year", ["All Years"] + sorted(rainfall["record_year"].dropna().unique().tolist()))
        else:
            rain_year = "All Years"
        
        rain_range = st.slider("Rainfall Range (cm)", 0, 500, (0, 500))
        
    elif "Groundwater" in query_category:
        st.markdown("### 🌊 Groundwater Filters")
        if not groundwater.empty and "district_name" in groundwater.columns:
            gw_district = st.selectbox("District", ["All Districts"] + sorted(groundwater["district_name"].dropna().unique().tolist()))
        else:
            gw_district = "All Districts"
        
        depth_range = st.slider("Depth Range (m)", 0, 100, (0, 100))
        
    elif "Water Quality" in query_category:
        st.markdown("### 💧 Water Quality Filters")
        if not water_quality.empty and "state_name" in water_quality.columns:
            wq_state = st.selectbox("State", ["All States"] + sorted(water_quality["state_name"].dropna().unique().tolist()))
        else:
            wq_state = "All States"
        
        ph_range = st.slider("pH Range", 0.0, 14.0, (6.0, 8.5), 0.1)
        
    else:  # Source Capacity
        st.markdown("### 🏭 Source Capacity Filters")
        if not sources.empty and "state" in sources.columns:
            source_state = st.selectbox("State", ["All States"] + sorted(sources["state"].dropna().unique().tolist()))
        else:
            source_state = "All States"
        
        if not sources.empty and "source_type" in sources.columns:
            source_type = st.selectbox("Source Type", ["All Types"] + sorted(sources["source_type"].dropna().unique().tolist()))
        else:
            source_type = "All Types"
        
        capacity_range = st.slider("Capacity Range (%)", 0, 100, (0, 100))
    
    st.markdown("---")
    st.markdown("### 🗺️ Map Settings")
    show_clusters = st.checkbox("Show Marker Clusters", True)
    marker_size = st.slider("Marker Size", 5, 20, 10)

# ─────────────────────────────────────────────────────────────────────────────
# SQL QUERY EXECUTION FUNCTION
# ─────────────────────────────────────────────────────────────────────────────
def build_and_execute_query():
    query_category = st.session_state.get("query_category", "🌧️ Rainfall Analysis (15 Queries)")
    
    # Select specific query based on category
    if "Rainfall" in query_category:
        query_options = list(RAINFALL_QUERIES.keys())
        selected_query = st.session_state.get("selected_rain_query", query_options[0])
        
        # Build filters
        district_filter = f"AND district_name = '{rain_district}'" if 'rain_district' in locals() and rain_district != "All Districts" else ""
        year_filter = f"AND record_year = {rain_year}" if 'rain_year' in locals() and rain_year != "All Years" else ""
        rainfall_filter = f"AND rainfall_cm BETWEEN {rain_range[0]} AND {rain_range[1]}" if 'rain_range' in locals() else ""
        
        query = RAINFALL_QUERIES[selected_query].format(
            district_filter=district_filter,
            year_filter=year_filter,
            rainfall_range=rainfall_filter,
            season_filter=""
        )
        
    elif "Groundwater" in query_category:
        query_options = list(GROUNDWATER_QUERIES.keys())
        selected_query = st.session_state.get("selected_gw_query", query_options[0])
        
        district_filter = f"AND district_name = '{gw_district}'" if 'gw_district' in locals() and gw_district != "All Districts" else ""
        depth_filter = f"AND avg_depth_meters BETWEEN {depth_range[0]} AND {depth_range[1]}" if 'depth_range' in locals() else ""
        
        query = GROUNDWATER_QUERIES[selected_query].format(
            district_filter=district_filter,
            depth_range=depth_filter,
            year_filter=""
        )
        
    elif "Water Quality" in query_category:
        query_options = list(WATER_QUALITY_QUERIES.keys())
        selected_query = st.session_state.get("selected_wq_query", query_options[0])
        
        state_filter = f"AND state_name = '{wq_state}'" if 'wq_state' in locals() and wq_state != "All States" else ""
        ph_filter = f"AND ph_level BETWEEN {ph_range[0]} AND {ph_range[1]}" if 'ph_range' in locals() else ""
        
        query = WATER_QUALITY_QUERIES[selected_query].format(
            state_filter=state_filter,
            district_filter="",
            ph_range=ph_filter,
            status_filter=""
        )
        
    else:  # Source Capacity
        query_options = list(SOURCE_QUERIES.keys())
        selected_query = st.session_state.get("selected_source_query", query_options[0])
        
        state_filter = f"AND state = '{source_state}'" if 'source_state' in locals() and source_state != "All States" else ""
        type_filter = f"AND source_type = '{source_type}'" if 'source_type' in locals() and source_type != "All Types" else ""
        capacity_filter = f"AND capacity_percent BETWEEN {capacity_range[0]} AND {capacity_range[1]}" if 'capacity_range' in locals() else ""
        
        query = SOURCE_QUERIES[selected_query].format(
            state_filter=state_filter,
            district_filter=type_filter,
            capacity_range=capacity_filter
        )
    
    return query, selected_query, query_options

# ─────────────────────────────────────────────────────────────────────────────
# MAIN CONTENT AREA
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(f"<h1 style='color:#00e5ff'>💧 AQUASTAT</h1>", unsafe_allow_html=True)
st.markdown(f"<p>National Water Command Center • 50+ Advanced SQL Analytics Queries • {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}</p>", unsafe_allow_html=True)

# KPI Cards
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total Sources", len(sources))
col2.metric("Monitoring Stations", len(stations))
col3.metric("GW Records", len(groundwater))
col4.metric("Rainfall Records", len(rainfall))
col5.metric("Active Alerts", len(alerts[alerts['alert_status'] == 'CRITICAL']) if not alerts.empty else 0)

st.markdown("---")

# SQL Query Execution Section
st.markdown("### 🔍 SQL Query Executor")

# Get query options based on category
query_category = st.session_state.get("query_category", "🌧️ Rainfall Analysis (15 Queries)")

if "Rainfall" in query_category:
    query_options = list(RAINFALL_QUERIES.keys())
    selected_query = st.selectbox("Select Rainfall Analysis Query", query_options, key="selected_rain_query")
    
elif "Groundwater" in query_category:
    query_options = list(GROUNDWATER_QUERIES.keys())
    selected_query = st.selectbox("Select Groundwater Analysis Query", query_options, key="selected_gw_query")
    
elif "Water Quality" in query_category:
    query_options = list(WATER_QUALITY_QUERIES.keys())
    selected_query = st.selectbox("Select Water Quality Analysis Query", query_options, key="selected_wq_query")
    
else:
    query_options = list(SOURCE_QUERIES.keys())
    selected_query = st.selectbox("Select Source Capacity Analysis Query", query_options, key="selected_source_query")

# Execute button
if st.button("▶️ Execute SQL Query", type="primary", use_container_width=True):
    query, _, _ = build_and_execute_query()
    
    with st.spinner("Executing SQL Query..."):
        results, error = execute_sql_query(query)
        
        if error:
            st.error(f"Query Error: {error}")
        else:
            st.success(f"✅ Query executed successfully! Found {len(results)} records")
            
            # Display SQL Query
            with st.expander("📝 View SQL Query", expanded=False):
                st.code(query, language="sql")
            
            # Display Results
            st.markdown("### 📊 Query Results")
            if not results.empty:
                st.dataframe(results, use_container_width=True)
                
                # Download button
                csv = results.to_csv(index=False).encode('utf-8')
                st.download_button("📥 Download Results as CSV", csv, f"query_results.csv", "text/csv")
                
                # Basic statistics
                st.markdown("### 📈 Result Statistics")
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Rows", len(results))
                col2.metric("Total Columns", len(results.columns))
                col3.metric("Memory Usage", f"{results.memory_usage(deep=True).sum() / 1024:.1f} KB")
                
                # Show column info
                st.markdown("### 📋 Column Information")
                col_info = pd.DataFrame({
                    'Column Name': results.columns,
                    'Data Type': results.dtypes.values,
                    'Non-Null Count': results.count().values,
                    'Null Count': results.isnull().sum().values,
                    'Unique Values': [results[col].nunique() for col in results.columns]
                })
                st.dataframe(col_info, use_container_width=True)
            else:
                st.warning("No results returned from the query")

st.markdown("---")
st.markdown(f"<p style='text-align:center'>AQUASTAT v5.0 • 50+ Advanced SQL Analytics Queries • Updated: {current_time.strftime('%Y-%m-%d %H:%M IST')}</p>", unsafe_allow_html=True)
