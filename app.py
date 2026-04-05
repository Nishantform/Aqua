import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
from io import BytesIO
from collections import defaultdict

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
@import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@400;600;700&family=Exo+2:wght@300;400;600&display=swap');

html, body, [data-testid="stAppViewContainer"] {
    background: linear-gradient(160deg,#040d18 0%,#071525 60%,#0a1e30 100%);
    color: #cfe4f7;
    font-family: 'Exo 2', sans-serif;
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
    text-align: center;
}
.kpi-label { color: #7fb8d8; font-size:0.78rem; letter-spacing:0.12em; text-transform:uppercase; margin-bottom:4px; }
.kpi-value { color:#00e5ff; font-size:2rem; font-weight:700; font-family:'Rajdhani',sans-serif; }
.kpi-sub { color:#4a7fa0; font-size:0.72rem; margin-top:2px; }
.badge-critical {
    background: linear-gradient(135deg,#c0392b,#e74c3c);
    color:#fff; padding:4px 14px; border-radius:30px;
    display:inline-block; font-size:0.75rem; font-weight:600;
}
.badge-warning {
    background: linear-gradient(135deg,#e67e22,#f1c40f);
    color:#1a1a1a; padding:4px 14px; border-radius:30px;
    display:inline-block; font-size:0.75rem; font-weight:600;
}
.badge-good {
    background: linear-gradient(135deg,#27ae60,#2ecc71);
    color:#fff; padding:4px 14px; border-radius:30px;
    display:inline-block; font-size:0.75rem; font-weight:600;
}
.section-header {
    font-family:'Rajdhani',sans-serif;
    font-size:1.6rem; font-weight:700;
    color:#00e5ff; letter-spacing:0.05em;
    border-bottom:1px solid rgba(0,200,255,0.2);
    padding-bottom:8px; margin-bottom:16px;
}
.query-info-box {
    background: rgba(0,100,160,0.15);
    border-left: 3px solid #00b4d8;
    border-radius: 4px;
    padding: 10px 14px;
    font-size: 0.83rem;
    color: #9ecfec;
    margin-bottom: 12px;
}
.demo-badge {
    background: linear-gradient(135deg,#6a3de8,#9b59b6);
    color:#fff; padding:4px 14px; border-radius:30px;
    font-size:0.75rem; font-weight:600;
    display:inline-block; margin-bottom:8px;
}
div[data-testid="stDataFrameResizable"] {
    border: 1px solid rgba(0,200,255,0.15) !important;
    border-radius: 8px !important;
}
.stSelectbox > div > div {
    background: rgba(10,30,48,0.8) !important;
    border: 1px solid rgba(0,200,255,0.2) !important;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE CONNECTION
# ─────────────────────────────────────────────────────────────────────────────
try:
    from sqlalchemy import create_engine, text
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False

NEON_URL = None
try:
    NEON_URL = st.secrets["NEON_URL"]
except Exception:
    pass

@st.cache_resource
def init_connection():
    if not HAS_SQLALCHEMY or NEON_URL is None:
        return None
    try:
        engine = create_engine(NEON_URL, pool_size=5, max_overflow=10, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).fetchone()
        return engine
    except Exception:
        return None

engine = init_connection()
DEMO_MODE = engine is None

# ─────────────────────────────────────────────────────────────────────────────
# SAFE DICT for query formatting (missing keys → empty string)
# ─────────────────────────────────────────────────────────────────────────────
class SafeDict(defaultdict):
    def __missing__(self, key):
        return ""

# ─────────────────────────────────────────────────────────────────────────────
# SAMPLE DATA GENERATORS
# ─────────────────────────────────────────────────────────────────────────────
DISTRICTS = [
    "North District", "South District", "East District", "West District",
    "Central District", "Coastal District", "Highland District", "Riverside District",
    "Valley District", "Plains District"
]
STATES = [
    "Maharashtra", "Gujarat", "Rajasthan", "Karnataka",
    "Tamil Nadu", "Madhya Pradesh", "Uttar Pradesh", "Andhra Pradesh"
]
SEASONS = ["Monsoon", "Pre-Monsoon", "Post-Monsoon", "Winter"]
YEARS = list(range(2018, 2025))
STATUS_LIST = ["Active", "Maintenance", "Inactive"]

np.random.seed(42)

def _rainfall_base():
    rows = []
    for d in DISTRICTS:
        for y in YEARS:
            for s in SEASONS:
                rows.append({
                    "district_name": d,
                    "record_year": y,
                    "season": s,
                    "rainfall_cm": round(np.random.uniform(20, 350), 1),
                    "state_name": np.random.choice(STATES),
                })
    return pd.DataFrame(rows)

def _groundwater_base():
    rows = []
    for d in DISTRICTS:
        for y in YEARS:
            depth = round(np.random.uniform(10, 55), 1)
            rows.append({
                "district_name": d,
                "assessment_year": y,
                "avg_depth_meters": depth,
                "extraction_pct": round(np.random.uniform(20, 90), 1),
                "recharge_rate_mcm": round(np.random.uniform(15, 80), 1),
                "state_name": np.random.choice(STATES),
            })
    return pd.DataFrame(rows)

def _water_quality_base():
    station_names = [f"WMS-{d[:3].upper()}-{str(i).zfill(3)}" for d in DISTRICTS for i in range(1, 6)]
    rows = []
    for sn in station_names:
        dist = sn.split("-")[1]
        rows.append({
            "station_name": sn,
            "district_name": np.random.choice(DISTRICTS),
            "state_name": np.random.choice(STATES),
            "ph_level": round(np.random.uniform(5.8, 9.2), 2),
            "dissolved_oxygen_mg_l": round(np.random.uniform(2.5, 9.5), 2),
            "turbidity_ntu": round(np.random.uniform(1, 35), 1),
            "status": np.random.choice(STATUS_LIST, p=[0.75, 0.15, 0.10]),
            "conductivity_us_cm": round(np.random.uniform(100, 1200), 1),
            "temperature_c": round(np.random.uniform(18, 32), 1),
        })
    return pd.DataFrame(rows)

@st.cache_data(ttl=300)
def get_demo_rainfall():   return _rainfall_base()
@st.cache_data(ttl=300)
def get_demo_groundwater(): return _groundwater_base()
@st.cache_data(ttl=300)
def get_demo_water_quality(): return _water_quality_base()

# ─────────────────────────────────────────────────────────────────────────────
# QUERY EXECUTION
# ─────────────────────────────────────────────────────────────────────────────
def execute_query(query: str) -> tuple[pd.DataFrame | None, str | None]:
    if DEMO_MODE:
        return None, "demo"
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df, None
    except Exception as e:
        return None, str(e)

def run_demo_query(query_key: str, category: str, df_base: pd.DataFrame) -> pd.DataFrame:
    """Simulate query results from in-memory dataframe."""
    df = df_base.copy()
    k = query_key.lower()

    if category == "rainfall":
        if "yearly" in k or "trend" in k:
            return df.groupby("record_year").agg(
                total_records=("rainfall_cm", "count"),
                avg_rainfall=("rainfall_cm", lambda x: round(x.mean(), 2)),
                min_rainfall=("rainfall_cm", lambda x: round(x.min(), 2)),
                max_rainfall=("rainfall_cm", lambda x: round(x.max(), 2)),
                std_deviation=("rainfall_cm", lambda x: round(x.std(), 2)),
            ).reset_index().sort_values("record_year", ascending=False)

        elif "seasonal" in k:
            return df.groupby("season").agg(
                total_records=("rainfall_cm", "count"),
                avg_rainfall=("rainfall_cm", lambda x: round(x.mean(), 2)),
                min_rainfall=("rainfall_cm", lambda x: round(x.min(), 2)),
                max_rainfall=("rainfall_cm", lambda x: round(x.max(), 2)),
            ).reset_index().sort_values("avg_rainfall", ascending=False)

        elif "district" in k and "comparison" in k:
            return df.groupby("district_name").agg(
                records=("rainfall_cm", "count"),
                avg_rainfall=("rainfall_cm", lambda x: round(x.mean(), 2)),
                total_rainfall=("rainfall_cm", lambda x: round(x.sum(), 2)),
            ).reset_index().sort_values("avg_rainfall", ascending=False)

        elif "extreme" in k:
            sub = df[df["rainfall_cm"] > 200].copy()
            sub["severity_level"] = pd.cut(
                sub["rainfall_cm"],
                bins=[200, 250, 300, 1000],
                labels=["HEAVY", "SEVERE", "EXTREME_DANGER"]
            ).astype(str)
            return sub.sort_values("rainfall_cm", ascending=False).head(30)

        elif "anomaly" in k:
            stats = df.groupby("district_name")["rainfall_cm"].agg(["mean", "std"]).reset_index()
            stats.columns = ["district_name", "mean_rainfall", "std_rainfall"]
            merged = df.merge(stats, on="district_name")
            merged["anomaly_type"] = np.where(
                merged["rainfall_cm"] > merged["mean_rainfall"] + 2 * merged["std_rainfall"], "Extreme_High_Anomaly",
                np.where(merged["rainfall_cm"] < merged["mean_rainfall"] - 2 * merged["std_rainfall"], "Extreme_Low_Anomaly",
                np.where(merged["rainfall_cm"] > merged["mean_rainfall"] + merged["std_rainfall"], "High_Anomaly",
                np.where(merged["rainfall_cm"] < merged["mean_rainfall"] - merged["std_rainfall"], "Low_Anomaly", "Normal"))))
            return merged[["district_name", "record_year", "season", "rainfall_cm", "mean_rainfall", "anomaly_type"]].head(30)

        elif "cumulative" in k:
            df = df.sort_values(["district_name", "record_year", "season"])
            df["cumulative_rainfall"] = df.groupby(["district_name", "record_year"])["rainfall_cm"].cumsum()
            df["moving_avg_3yr"] = df.groupby("district_name")["rainfall_cm"].transform(
                lambda x: x.rolling(3, min_periods=1).mean().round(2))
            df["yoy_change_pct"] = df.groupby("district_name")["rainfall_cm"].transform(
                lambda x: x.pct_change() * 100).round(2)
            return df.head(40)

        elif "monsoon" in k:
            monsoon = df[df["season"] == "Monsoon"].groupby(["district_name", "record_year"])["rainfall_cm"].sum().reset_index()
            monsoon.columns = ["district_name", "record_year", "monsoon_rainfall"]
            non_monsoon = df[df["season"] != "Monsoon"].groupby(["district_name", "record_year"])["rainfall_cm"].sum().reset_index()
            non_monsoon.columns = ["district_name", "record_year", "non_monsoon_rainfall"]
            result = monsoon.merge(non_monsoon, on=["district_name", "record_year"])
            result["monsoon_contribution_pct"] = (result["monsoon_rainfall"] /
                (result["monsoon_rainfall"] + result["non_monsoon_rainfall"]) * 100).round(2)
            return result.sort_values("monsoon_rainfall", ascending=False)

        elif "frequency" in k or "distribution" in k:
            bins = [0, 50, 150, 300, 99999]
            labels = ["0-50 cm (Low)", "51-150 cm (Moderate)", "151-300 cm (High)", "300+ cm (Extreme)"]
            df["rainfall_range"] = pd.cut(df["rainfall_cm"], bins=bins, labels=labels)
            freq = df["rainfall_range"].value_counts().reset_index()
            freq.columns = ["rainfall_range", "frequency"]
            freq["percentage"] = (freq["frequency"] / freq["frequency"].sum() * 100).round(2)
            return freq

        elif "year-over-year" in k or "yoy" in k:
            yearly = df.groupby(["district_name", "record_year"])["rainfall_cm"].mean().reset_index()
            yearly = yearly.sort_values(["district_name", "record_year"])
            yearly["prev_year_rainfall"] = yearly.groupby("district_name")["rainfall_cm"].shift(1)
            yearly["absolute_change"] = (yearly["rainfall_cm"] - yearly["prev_year_rainfall"]).round(2)
            yearly["percent_change"] = ((yearly["absolute_change"] / yearly["prev_year_rainfall"]) * 100).round(2)
            return yearly.sort_values(["district_name", "record_year"], ascending=[True, False])

        elif "drought" in k:
            yearly = df.groupby(["district_name", "record_year"])["rainfall_cm"].mean().reset_index()
            yearly.columns = ["district_name", "record_year", "avg_rainfall"]
            yearly = yearly[yearly["avg_rainfall"] < 150]
            yearly["drought_risk_level"] = pd.cut(
                yearly["avg_rainfall"],
                bins=[0, 50, 100, 150],
                labels=["EXTREME_DROUGHT_RISK", "SEVERE_DROUGHT_RISK", "MODERATE_DROUGHT_RISK"]
            ).astype(str)
            return yearly.sort_values("avg_rainfall").head(20)

        elif "variability" in k:
            result = df.groupby("district_name")["rainfall_cm"].agg(
                num_records="count",
                mean_rainfall=lambda x: round(x.mean(), 2),
                std_deviation=lambda x: round(x.std(), 2),
            ).reset_index()
            result["coefficient_of_variation"] = (result["std_deviation"] / result["mean_rainfall"] * 100).round(2)
            result["variability_category"] = np.where(
                result["coefficient_of_variation"] > 50, "HIGHLY_VARIABLE",
                np.where(result["coefficient_of_variation"] > 30, "MODERATELY_VARIABLE", "STABLE"))
            return result.sort_values("coefficient_of_variation", ascending=False)

        elif "efficiency" in k:
            yearly = df.groupby(["district_name", "record_year"])["rainfall_cm"].mean().reset_index()
            yearly.columns = ["district_name", "record_year", "actual_rainfall"]
            mn = yearly.groupby("district_name")["actual_rainfall"].min().rename("min_r")
            mx = yearly.groupby("district_name")["actual_rainfall"].max().rename("max_r")
            yearly = yearly.join(mn, on="district_name").join(mx, on="district_name")
            yearly["efficiency_score"] = ((yearly["actual_rainfall"] - yearly["min_r"]) /
                                          (yearly["max_r"] - yearly["min_r"]) * 100).round(2)
            yearly["efficiency_rating"] = np.where(
                yearly["efficiency_score"] > 75, "EXCELLENT",
                np.where(yearly["efficiency_score"] > 50, "GOOD", "NEEDS_IMPROVEMENT"))
            return yearly[["district_name", "record_year", "actual_rainfall", "efficiency_score", "efficiency_rating"]].sort_values("efficiency_score", ascending=False)

        elif "prediction" in k or "trend_pred" in k:
            yearly = df.groupby(["district_name", "record_year"])["rainfall_cm"].mean().reset_index()
            yearly.columns = ["district_name", "record_year", "avg_rainfall"]
            mean_map = yearly.groupby("district_name")["avg_rainfall"].mean()
            yearly["district_mean"] = yearly["district_name"].map(mean_map)
            yearly["trend_direction"] = np.where(yearly["avg_rainfall"] > yearly["district_mean"], "ABOVE_AVERAGE", "BELOW_AVERAGE")
            yearly["yoy_trend_pct"] = yearly.groupby("district_name")["avg_rainfall"].pct_change().mul(100).round(2)
            return yearly.sort_values(["district_name", "record_year"], ascending=[True, False])

        # Default: return filtered base data
        return df.sort_values("record_year", ascending=False).head(50)

    elif category == "groundwater":
        if "depletion" in k:
            result = df.groupby("district_name").apply(lambda g: pd.Series({
                "first_year": g["assessment_year"].min(),
                "last_year": g["assessment_year"].max(),
                "avg_depth": round(g["avg_depth_meters"].mean(), 2),
                "depth_variability": round(g["avg_depth_meters"].std(), 2),
                "annual_depletion_rate": round(
                    (g["avg_depth_meters"].max() - g["avg_depth_meters"].min()) /
                    max(g["assessment_year"].max() - g["assessment_year"].min(), 1), 2),
            })).reset_index()
            result["depletion_status"] = np.where(
                result["annual_depletion_rate"] > 2, "CRITICAL_DEPLETION",
                np.where(result["annual_depletion_rate"] > 1, "MODERATE_DEPLETION", "STABLE"))
            return result.sort_values("annual_depletion_rate", ascending=False)

        elif "extraction" in k and "recharge" in k:
            df["deficit_surplus"] = (df["extraction_pct"] - df["recharge_rate_mcm"]).round(2)
            df["extraction_status"] = np.where(
                df["extraction_pct"] > df["recharge_rate_mcm"] * 1.5, "OVER_EXTRACTION_CRITICAL",
                np.where(df["extraction_pct"] > df["recharge_rate_mcm"], "OVER_EXTRACTION_MODERATE",
                np.where(df["extraction_pct"] < df["recharge_rate_mcm"] * 0.5, "UNDER_UTILIZATION", "BALANCED")))
            return df[["district_name", "assessment_year", "extraction_pct", "recharge_rate_mcm",
                       "deficit_surplus", "extraction_status"]].sort_values("deficit_surplus", ascending=False)

        elif "trend" in k:
            df = df.sort_values(["district_name", "assessment_year"])
            df["moving_avg_3yr"] = df.groupby("district_name")["avg_depth_meters"].transform(
                lambda x: x.rolling(3, min_periods=1).mean().round(2))
            df["yoy_change_pct"] = df.groupby("district_name")["avg_depth_meters"].transform(
                lambda x: x.pct_change() * 100).round(2)
            df["trend_direction"] = np.where(
                df["avg_depth_meters"] > df["moving_avg_3yr"] * 1.1, "DEEPENING_FAST",
                np.where(df["avg_depth_meters"] > df["moving_avg_3yr"] * 1.05, "DEEPENING_SLOW",
                np.where(df["avg_depth_meters"] < df["moving_avg_3yr"] * 0.95, "RECOVERING", "STABLE")))
            return df.sort_values(["district_name", "assessment_year"], ascending=[True, False]).head(40)

        elif "risk zone" in k:
            result = df.groupby("district_name").agg(
                avg_depth=("avg_depth_meters", lambda x: round(x.mean(), 2)),
                avg_extraction=("extraction_pct", lambda x: round(x.mean(), 2)),
                avg_recharge=("recharge_rate_mcm", lambda x: round(x.mean(), 2)),
            ).reset_index()
            result["risk_category"] = np.where(
                (result["avg_depth"] > 40) & (result["avg_extraction"] > 70), "CRITICAL_RISK",
                np.where((result["avg_depth"] > 30) & (result["avg_extraction"] > 50), "HIGH_RISK",
                np.where((result["avg_depth"] > 20) & (result["avg_extraction"] > 30), "MODERATE_RISK", "LOW_RISK")))
            return result.sort_values("avg_depth", ascending=False)

        elif "sustainable" in k:
            df["sustainable_yield"] = (df["recharge_rate_mcm"] * (1 - df["extraction_pct"] / 100)).round(2)
            df["sustainability_status"] = np.where(
                df["extraction_pct"] > 80, "UNSUSTAINABLE",
                np.where(df["extraction_pct"] > 60, "STRESSED",
                np.where(df["extraction_pct"] > 40, "MODERATE", "SUSTAINABLE")))
            return df[["district_name", "assessment_year", "recharge_rate_mcm",
                       "extraction_pct", "sustainable_yield", "sustainability_status"]].sort_values("sustainable_yield", ascending=False)

        elif "depth class" in k:
            result = df.groupby("district_name").apply(lambda g: pd.Series({
                "total_wells": len(g),
                "shallow_wells": (g["avg_depth_meters"] < 20).sum(),
                "medium_wells": ((g["avg_depth_meters"] >= 20) & (g["avg_depth_meters"] <= 40)).sum(),
                "deep_wells": (g["avg_depth_meters"] > 40).sum(),
            })).reset_index()
            result["deep_well_percentage"] = (result["deep_wells"] / result["total_wells"] * 100).round(2)
            return result.sort_values("deep_well_percentage", ascending=False)

        elif "quality index" in k or "health" in k:
            df["depth_score"] = np.where(df["avg_depth_meters"] < 20, 100,
                                np.where(df["avg_depth_meters"] < 35, 70,
                                np.where(df["avg_depth_meters"] < 50, 40, 10)))
            df["extraction_score"] = np.where(df["extraction_pct"] < 30, 100,
                                     np.where(df["extraction_pct"] < 50, 70,
                                     np.where(df["extraction_pct"] < 70, 40, 10)))
            df["groundwater_health_score"] = (df["depth_score"] * 0.6 + df["extraction_score"] * 0.4).round(2)
            df["overall_health_status"] = np.where(
                df["groundwater_health_score"] >= 80, "EXCELLENT",
                np.where(df["groundwater_health_score"] >= 55, "GOOD", "POOR"))
            return df[["district_name", "assessment_year", "avg_depth_meters", "extraction_pct",
                       "groundwater_health_score", "overall_health_status"]].sort_values("groundwater_health_score", ascending=False)

        elif "year-over-year" in k or "yoy" in k:
            df = df.sort_values(["district_name", "assessment_year"])
            df["previous_depth"] = df.groupby("district_name")["avg_depth_meters"].shift(1)
            df["depth_change"] = (df["avg_depth_meters"] - df["previous_depth"]).round(2)
            df["percent_change"] = (df["depth_change"] / df["previous_depth"] * 100).round(2)
            return df[["district_name", "assessment_year", "avg_depth_meters", "previous_depth",
                       "depth_change", "percent_change"]].dropna().sort_values("percent_change", ascending=False)

        elif "critical zone" in k:
            critical = df[(df["avg_depth_meters"] > 25) | (df["extraction_pct"] > 50)].copy()
            critical["critical_zone"] = np.where(
                (critical["avg_depth_meters"] > 35) & (critical["extraction_pct"] > 65), "CRITICAL_ZONE",
                np.where((critical["avg_depth_meters"] > 25) & (critical["extraction_pct"] > 50), "ALERT_ZONE", "MONITORING_ZONE"))
            return critical[["district_name", "assessment_year", "avg_depth_meters",
                             "extraction_pct", "critical_zone"]].sort_values("avg_depth_meters", ascending=False)

        elif "recharge potential" in k:
            df["artificial_recharge_potential"] = (df["recharge_rate_mcm"] * 0.3).round(2)
            df["recharge_necessity"] = np.where(
                df["extraction_pct"] > df["recharge_rate_mcm"], "RECHARGE_REQUIRED",
                np.where(df["extraction_pct"] > df["recharge_rate_mcm"] * 0.8, "RECHARGE_RECOMMENDED", "ADEQUATE_RECHARGE"))
            return df[["district_name", "assessment_year", "recharge_rate_mcm",
                       "extraction_pct", "artificial_recharge_potential", "recharge_necessity"]]

        elif "stress" in k:
            df["stress_index"] = ((df["avg_depth_meters"] / 50 * 100 + df["extraction_pct"]) / 2).round(2)
            df["stress_level"] = np.where(
                df["stress_index"] > 80, "EXTREME_STRESS",
                np.where(df["stress_index"] > 60, "HIGH_STRESS",
                np.where(df["stress_index"] > 40, "MODERATE_STRESS", "LOW_STRESS")))
            return df[["district_name", "assessment_year", "avg_depth_meters",
                       "extraction_pct", "stress_index", "stress_level"]].sort_values("stress_index", ascending=False)

        return df.sort_values("assessment_year", ascending=False).head(50)

    elif category == "water_quality":
        if "ph level" in k:
            return df.groupby(["state_name", "district_name"]).apply(lambda g: pd.Series({
                "total_stations": len(g),
                "avg_ph": round(g["ph_level"].mean(), 2),
                "min_ph": round(g["ph_level"].min(), 2),
                "max_ph": round(g["ph_level"].max(), 2),
                "ph_variability": round(g["ph_level"].std(), 2),
                "ph_status": "IDEAL" if 6.5 <= g["ph_level"].mean() <= 8.5 else
                             "ACCEPTABLE" if 6.0 <= g["ph_level"].mean() <= 9.0 else "CRITICAL",
            })).reset_index().sort_values("avg_ph")

        elif "dissolved oxygen" in k:
            return df.groupby(["state_name", "district_name"]).apply(lambda g: pd.Series({
                "avg_do": round(g["dissolved_oxygen_mg_l"].mean(), 2),
                "min_do": round(g["dissolved_oxygen_mg_l"].min(), 2),
                "max_do": round(g["dissolved_oxygen_mg_l"].max(), 2),
                "water_quality_class": "EXCELLENT" if g["dissolved_oxygen_mg_l"].mean() > 7 else
                                       "GOOD" if g["dissolved_oxygen_mg_l"].mean() > 5 else
                                       "FAIR" if g["dissolved_oxygen_mg_l"].mean() > 3 else "POOR",
                "ecological_risk": "FISH_KILL_RISK" if g["dissolved_oxygen_mg_l"].min() < 3 else
                                   "STRESS_RISK" if g["dissolved_oxygen_mg_l"].min() < 5 else "SAFE",
            })).reset_index().sort_values("avg_do", ascending=False)

        elif "turbidity" in k:
            df["turbidity_level"] = pd.cut(
                df["turbidity_ntu"],
                bins=[0, 5, 10, 20, 1000],
                labels=["CLEAR", "SLIGHTLY_TURBID", "TURBID", "HIGHLY_TURBID"]
            ).astype(str)
            df["treatment_need"] = np.where(
                df["turbidity_ntu"] > 20, "TREATMENT_REQUIRED",
                np.where(df["turbidity_ntu"] > 10, "MONITORING_REQUIRED", "ACCEPTABLE"))
            return df[["station_name", "district_name", "state_name", "turbidity_ntu",
                       "turbidity_level", "treatment_need"]].sort_values("turbidity_ntu", ascending=False).head(30)

        elif "quality index" in k or "comprehensive" in k:
            df["ph_score"] = np.where(df["ph_level"].between(6.5, 8.5), 100,
                            np.where(df["ph_level"].between(6.0, 9.0), 70, 40))
            df["do_score"] = np.where(df["dissolved_oxygen_mg_l"] > 7, 100,
                            np.where(df["dissolved_oxygen_mg_l"] > 5, 70,
                            np.where(df["dissolved_oxygen_mg_l"] > 3, 40, 10)))
            df["turb_score"] = np.where(df["turbidity_ntu"] < 5, 100,
                              np.where(df["turbidity_ntu"] < 10, 70,
                              np.where(df["turbidity_ntu"] < 20, 40, 10)))
            df["water_quality_index"] = (df["ph_score"] * 0.3 + df["do_score"] * 0.4 + df["turb_score"] * 0.3).round(2)
            df["overall_rating"] = np.where(df["water_quality_index"] >= 80, "EXCELLENT",
                                   np.where(df["water_quality_index"] >= 55, "GOOD", "POOR"))
            return df[["station_name", "state_name", "district_name", "ph_level",
                       "dissolved_oxygen_mg_l", "turbidity_ntu", "water_quality_index",
                       "overall_rating"]].sort_values("water_quality_index", ascending=False)

        elif "pollut" in k:
            polluted = df[(df["ph_level"] < 6.0) | (df["ph_level"] > 9.0) |
                         (df["dissolved_oxygen_mg_l"] < 4) | (df["turbidity_ntu"] > 25)].copy()
            polluted["primary_concern"] = np.where(
                (polluted["ph_level"] < 6.0) | (polluted["ph_level"] > 9.0), "pH_VIOLATION",
                np.where(polluted["dissolved_oxygen_mg_l"] < 4, "DO_CRITICAL", "TURBIDITY_HIGH"))
            return polluted[["station_name", "state_name", "district_name", "ph_level",
                             "dissolved_oxygen_mg_l", "turbidity_ntu", "primary_concern"]].sort_values("dissolved_oxygen_mg_l")

        elif "status summary" in k:
            return df.groupby("status").apply(lambda g: pd.Series({
                "total_stations": len(g),
                "avg_ph": round(g["ph_level"].mean(), 2),
                "avg_do": round(g["dissolved_oxygen_mg_l"].mean(), 2),
                "avg_turbidity": round(g["turbidity_ntu"].mean(), 2),
            })).reset_index().sort_values("total_stations", ascending=False)

        elif "state-wise" in k or "statewise" in k:
            return df.groupby("state_name").apply(lambda g: pd.Series({
                "total_stations": len(g),
                "avg_ph": round(g["ph_level"].mean(), 2),
                "avg_do": round(g["dissolved_oxygen_mg_l"].mean(), 2),
                "avg_turbidity": round(g["turbidity_ntu"].mean(), 2),
                "ph_compliance_pct": round(g["ph_level"].between(6.5, 8.5).mean() * 100, 2),
            })).reset_index().sort_values("avg_do", ascending=False)

        elif "contamination" in k:
            df["primary_concern"] = np.where(
                (df["ph_level"] < 6.0) | (df["ph_level"] > 9.0), "pH_VIOLATION",
                np.where(df["dissolved_oxygen_mg_l"] < 3, "DO_CRITICAL",
                np.where(df["turbidity_ntu"] > 25, "TURBIDITY_HIGH", "WITHIN_LIMITS")))
            df["contamination_risk"] = np.where(
                ((df["ph_level"] < 6.0) | (df["ph_level"] > 9.0)) & (df["dissolved_oxygen_mg_l"] < 3), "HIGH_RISK",
                np.where((~df["ph_level"].between(6.5, 8.5)) | (df["dissolved_oxygen_mg_l"] < 5) | (df["turbidity_ntu"] > 20), "MODERATE_RISK", "LOW_RISK"))
            return df[["station_name", "state_name", "district_name", "ph_level",
                       "dissolved_oxygen_mg_l", "turbidity_ntu", "primary_concern",
                       "contamination_risk"]].sort_values("contamination_risk")

        elif "critical" in k and "alert" in k:
            critical = df[(df["dissolved_oxygen_mg_l"] < 5) |
                         (~df["ph_level"].between(6.0, 9.0)) |
                         (df["turbidity_ntu"] > 20)].copy()
            critical["alert_level"] = np.where(
                (critical["dissolved_oxygen_mg_l"] < 3) | (~critical["ph_level"].between(5.5, 10)) | (critical["turbidity_ntu"] > 30),
                "IMMEDIATE_ACTION",
                np.where((critical["dissolved_oxygen_mg_l"] < 5) | (~critical["ph_level"].between(6.0, 9.0)) | (critical["turbidity_ntu"] > 20),
                         "URGENT_MONITORING", "ROUTINE_CHECK"))
            return critical[["station_name", "state_name", "district_name", "ph_level",
                             "dissolved_oxygen_mg_l", "turbidity_ntu", "status", "alert_level"]].head(25)

        elif "correlation" in k:
            return df.groupby("state_name").apply(lambda g: pd.Series({
                "ph_do_correlation": round(g["ph_level"].corr(g["dissolved_oxygen_mg_l"]), 3),
                "ph_turbidity_correlation": round(g["ph_level"].corr(g["turbidity_ntu"]), 3),
                "do_turbidity_correlation": round(g["dissolved_oxygen_mg_l"].corr(g["turbidity_ntu"]), 3),
                "sample_size": len(g),
                "mean_ph": round(g["ph_level"].mean(), 2),
                "mean_do": round(g["dissolved_oxygen_mg_l"].mean(), 2),
                "mean_turbidity": round(g["turbidity_ntu"].mean(), 2),
            })).reset_index().sort_values("ph_do_correlation", ascending=False)

        return df.head(50)

    return df.head(50)

# ─────────────────────────────────────────────────────────────────────────────
# SQL QUERY DICTIONARIES
# ─────────────────────────────────────────────────────────────────────────────

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
            ROUND(MAX(rainfall_cm), 2) as max_rainfall
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
            district_name, record_year, season, rainfall_cm,
            CASE
                WHEN rainfall_cm > 300 THEN 'EXTREME_DANGER'
                WHEN rainfall_cm > 200 THEN 'SEVERE'
                WHEN rainfall_cm > 150 THEN 'HEAVY'
                ELSE 'MODERATE'
            END as severity_level,
            RANK() OVER (PARTITION BY district_name ORDER BY rainfall_cm DESC) as rank_in_district
        FROM rainfall_history
        WHERE rainfall_cm > 100
        ORDER BY rainfall_cm DESC
        LIMIT 50
    """,
    "6. Rainfall Anomaly Detection": """
        WITH stats AS (
            SELECT district_name, AVG(rainfall_cm) as mean_rainfall, STDDEV(rainfall_cm) as std_rainfall
            FROM rainfall_history GROUP BY district_name
        )
        SELECT
            rh.district_name, rh.record_year, rh.season, rh.rainfall_cm,
            ROUND(s.mean_rainfall, 2) as avg_rainfall,
            CASE
                WHEN rh.rainfall_cm > s.mean_rainfall + 2*s.std_rainfall THEN 'Extreme_High_Anomaly'
                WHEN rh.rainfall_cm < s.mean_rainfall - 2*s.std_rainfall THEN 'Extreme_Low_Anomaly'
                WHEN rh.rainfall_cm > s.mean_rainfall + s.std_rainfall    THEN 'High_Anomaly'
                WHEN rh.rainfall_cm < s.mean_rainfall - s.std_rainfall    THEN 'Low_Anomaly'
                ELSE 'Normal'
            END as anomaly_type
        FROM rainfall_history rh
        JOIN stats s ON rh.district_name = s.district_name
        ORDER BY ABS(rh.rainfall_cm - s.mean_rainfall) DESC
        LIMIT 30
    """,
    "7. Cumulative Rainfall Analysis": """
        SELECT
            district_name, record_year, season, rainfall_cm,
            SUM(rainfall_cm) OVER (PARTITION BY district_name, record_year ORDER BY season) as cumulative_rainfall,
            AVG(rainfall_cm) OVER (PARTITION BY district_name ORDER BY record_year) as moving_avg_3yr,
            LAG(rainfall_cm, 1) OVER (PARTITION BY district_name ORDER BY record_year) as previous_year_rainfall,
            ROUND((rainfall_cm - LAG(rainfall_cm,1) OVER (PARTITION BY district_name ORDER BY record_year))*100.0 /
                  NULLIF(LAG(rainfall_cm,1) OVER (PARTITION BY district_name ORDER BY record_year), 0), 2) as yoy_change_pct
        FROM rainfall_history
        WHERE rainfall_cm IS NOT NULL
        ORDER BY district_name, record_year DESC
    """,
    "8. Monsoon Performance Analysis": """
        SELECT
            district_name, record_year,
            SUM(CASE WHEN season='Monsoon' THEN rainfall_cm ELSE 0 END) as monsoon_rainfall,
            SUM(CASE WHEN season!='Monsoon' THEN rainfall_cm ELSE 0 END) as non_monsoon_rainfall,
            ROUND(SUM(CASE WHEN season='Monsoon' THEN rainfall_cm ELSE 0 END)*100.0/NULLIF(SUM(rainfall_cm),0),2) as monsoon_contribution_pct
        FROM rainfall_history
        GROUP BY district_name, record_year
        ORDER BY monsoon_rainfall DESC
    """,
    "9. Rainfall Frequency Distribution": """
        SELECT
            CASE
                WHEN rainfall_cm BETWEEN 0   AND 50  THEN '0-50 cm (Low)'
                WHEN rainfall_cm BETWEEN 51  AND 150 THEN '51-150 cm (Moderate)'
                WHEN rainfall_cm BETWEEN 151 AND 300 THEN '151-300 cm (High)'
                ELSE '300+ cm (Extreme)'
            END as rainfall_range,
            COUNT(*) as frequency,
            ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(), 2) as percentage
        FROM rainfall_history
        WHERE rainfall_cm IS NOT NULL
        GROUP BY 1
        ORDER BY MIN(rainfall_cm)
    """,
    "10. Year-over-Year Rainfall Change": """
        SELECT
            district_name, record_year, rainfall_cm,
            LAG(rainfall_cm,1) OVER (PARTITION BY district_name ORDER BY record_year) as prev_year_rainfall,
            rainfall_cm - LAG(rainfall_cm,1) OVER (PARTITION BY district_name ORDER BY record_year) as absolute_change,
            ROUND((rainfall_cm - LAG(rainfall_cm,1) OVER (PARTITION BY district_name ORDER BY record_year))*100.0/
                  NULLIF(LAG(rainfall_cm,1) OVER (PARTITION BY district_name ORDER BY record_year),0),2) as percent_change
        FROM (
            SELECT district_name, record_year, AVG(rainfall_cm) as rainfall_cm
            FROM rainfall_history GROUP BY district_name, record_year
        ) yearly_avg
        ORDER BY district_name, record_year DESC
    """,
    "11. Drought Risk Assessment": """
        SELECT
            district_name, record_year,
            AVG(rainfall_cm) as avg_rainfall,
            CASE
                WHEN AVG(rainfall_cm) < 50  THEN 'EXTREME_DROUGHT_RISK'
                WHEN AVG(rainfall_cm) < 100 THEN 'SEVERE_DROUGHT_RISK'
                WHEN AVG(rainfall_cm) < 150 THEN 'MODERATE_DROUGHT_RISK'
                ELSE 'NO_DROUGHT_RISK'
            END as drought_risk_level,
            RANK() OVER (ORDER BY AVG(rainfall_cm)) as drought_rank
        FROM rainfall_history
        GROUP BY district_name, record_year
        HAVING AVG(rainfall_cm) < 150
        ORDER BY avg_rainfall LIMIT 20
    """,
    "12. Rainfall Frequency by Month": """
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
                district_name, record_year,
                AVG(rainfall_cm) as avg_rainfall,
                COUNT(*) OVER (PARTITION BY district_name) as total_years
            FROM rainfall_history
            GROUP BY district_name, record_year
        )
        SELECT
            district_name, record_year, avg_rainfall,
            CASE
                WHEN avg_rainfall > AVG(avg_rainfall) OVER (PARTITION BY district_name) THEN 'ABOVE_AVERAGE'
                ELSE 'BELOW_AVERAGE'
            END as trend_direction,
            ROUND((avg_rainfall - LAG(avg_rainfall,1) OVER (PARTITION BY district_name ORDER BY record_year))*100.0/
                  NULLIF(LAG(avg_rainfall,1) OVER (PARTITION BY district_name ORDER BY record_year),0),2) as yoy_trend_pct
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
            ROUND(STDDEV(rainfall_cm)*100.0/NULLIF(AVG(rainfall_cm),0), 2) as coefficient_of_variation,
            CASE
                WHEN STDDEV(rainfall_cm)*100.0/AVG(rainfall_cm) > 50 THEN 'HIGHLY_VARIABLE'
                WHEN STDDEV(rainfall_cm)*100.0/AVG(rainfall_cm) > 30 THEN 'MODERATELY_VARIABLE'
                ELSE 'STABLE'
            END as variability_category
        FROM rainfall_history
        WHERE rainfall_cm IS NOT NULL
        GROUP BY district_name HAVING COUNT(*) > 3
        ORDER BY coefficient_of_variation DESC
    """,
    "15. Rainfall Efficiency Score": """
        SELECT
            district_name, record_year,
            AVG(rainfall_cm) as actual_rainfall,
            (AVG(rainfall_cm)-MIN(rainfall_cm) OVER (PARTITION BY district_name))*100.0/
            NULLIF(MAX(rainfall_cm) OVER (PARTITION BY district_name)-MIN(rainfall_cm) OVER (PARTITION BY district_name),0) as efficiency_score,
            CASE
                WHEN (AVG(rainfall_cm)-MIN(rainfall_cm) OVER (PARTITION BY district_name))*100.0/
                     NULLIF(MAX(rainfall_cm) OVER (PARTITION BY district_name)-MIN(rainfall_cm) OVER (PARTITION BY district_name),0) > 75 THEN 'EXCELLENT'
                WHEN (AVG(rainfall_cm)-MIN(rainfall_cm) OVER (PARTITION BY district_name))*100.0/
                     NULLIF(MAX(rainfall_cm) OVER (PARTITION BY district_name)-MIN(rainfall_cm) OVER (PARTITION BY district_name),0) > 50 THEN 'GOOD'
                ELSE 'NEEDS_IMPROVEMENT'
            END as efficiency_rating
        FROM rainfall_history
        GROUP BY district_name, record_year
        ORDER BY efficiency_score DESC
    """,
}

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
            MIN(assessment_year) as first_year, MAX(assessment_year) as last_year,
            ROUND(AVG(avg_depth_meters), 2) as avg_depth,
            ROUND(STDDEV(avg_depth_meters), 2) as depth_variability,
            ROUND((MAX(avg_depth_meters)-MIN(avg_depth_meters))/
                  NULLIF(MAX(assessment_year)-MIN(assessment_year),0), 2) as annual_depletion_rate,
            CASE
                WHEN (MAX(avg_depth_meters)-MIN(avg_depth_meters))/(MAX(assessment_year)-MIN(assessment_year)) > 2 THEN 'CRITICAL_DEPLETION'
                WHEN (MAX(avg_depth_meters)-MIN(avg_depth_meters))/(MAX(assessment_year)-MIN(assessment_year)) > 1 THEN 'MODERATE_DEPLETION'
                ELSE 'STABLE'
            END as depletion_status
        FROM groundwater_levels
        WHERE avg_depth_meters IS NOT NULL
        GROUP BY district_name HAVING COUNT(*) > 2
        ORDER BY annual_depletion_rate DESC
    """,
    "3. Extraction vs Recharge Analysis": """
        SELECT
            district_name, assessment_year,
            ROUND(extraction_pct, 2) as extraction_pct,
            ROUND(recharge_rate_mcm, 2) as recharge_rate,
            ROUND(extraction_pct-recharge_rate_mcm, 2) as deficit_surplus,
            CASE
                WHEN extraction_pct > recharge_rate_mcm*1.5 THEN 'OVER_EXTRACTION_CRITICAL'
                WHEN extraction_pct > recharge_rate_mcm     THEN 'OVER_EXTRACTION_MODERATE'
                WHEN extraction_pct < recharge_rate_mcm*0.5 THEN 'UNDER_UTILIZATION'
                ELSE 'BALANCED'
            END as extraction_status,
            RANK() OVER (ORDER BY (extraction_pct-recharge_rate_mcm) DESC) as deficit_rank
        FROM groundwater_levels
        WHERE extraction_pct IS NOT NULL AND recharge_rate_mcm IS NOT NULL
        ORDER BY deficit_surplus DESC
    """,
    "4. Groundwater Trend Analysis": """
        WITH depth_trend AS (
            SELECT
                district_name, assessment_year, avg_depth_meters,
                AVG(avg_depth_meters) OVER (PARTITION BY district_name ORDER BY assessment_year
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg_3yr,
                LAG(avg_depth_meters,1) OVER (PARTITION BY district_name ORDER BY assessment_year) as prev_year_depth
            FROM groundwater_levels WHERE avg_depth_meters IS NOT NULL
        )
        SELECT
            district_name, assessment_year, avg_depth_meters,
            ROUND(moving_avg_3yr, 2) as moving_avg_3yr,
            ROUND(avg_depth_meters-moving_avg_3yr, 2) as deviation_from_trend,
            ROUND((avg_depth_meters-prev_year_depth)*100.0/NULLIF(prev_year_depth,0), 2) as yoy_change_pct,
            CASE
                WHEN avg_depth_meters > moving_avg_3yr*1.1  THEN 'DEEPENING_FAST'
                WHEN avg_depth_meters > moving_avg_3yr*1.05 THEN 'DEEPENING_SLOW'
                WHEN avg_depth_meters < moving_avg_3yr*0.95 THEN 'RECOVERING'
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
                WHEN AVG(avg_depth_meters)>40 AND AVG(extraction_pct)>70 THEN 'CRITICAL_RISK'
                WHEN AVG(avg_depth_meters)>30 AND AVG(extraction_pct)>50 THEN 'HIGH_RISK'
                WHEN AVG(avg_depth_meters)>20 AND AVG(extraction_pct)>30 THEN 'MODERATE_RISK'
                ELSE 'LOW_RISK'
            END as risk_category,
            RANK() OVER (ORDER BY AVG(avg_depth_meters) DESC, AVG(extraction_pct) DESC) as risk_rank
        FROM groundwater_levels
        WHERE avg_depth_meters IS NOT NULL AND extraction_pct IS NOT NULL
        GROUP BY district_name ORDER BY risk_rank
    """,
    "6. Sustainable Yield Analysis": """
        SELECT
            district_name, assessment_year, recharge_rate_mcm, extraction_pct,
            ROUND(recharge_rate_mcm*(1-extraction_pct/100.0), 2) as sustainable_yield,
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
            ROUND(SUM(CASE WHEN avg_depth_meters>40 THEN 1 ELSE 0 END)*100.0/COUNT(*), 2) as deep_well_percentage
        FROM groundwater_levels
        GROUP BY district_name ORDER BY deep_well_percentage DESC
    """,
    "8. Groundwater Health Index": """
        SELECT
            district_name, assessment_year, avg_depth_meters, extraction_pct,
            (CASE
                WHEN avg_depth_meters < 20 THEN 100
                WHEN avg_depth_meters < 35 THEN 70
                WHEN avg_depth_meters < 50 THEN 40 ELSE 10
            END * 0.6 +
            CASE
                WHEN extraction_pct < 30 THEN 100
                WHEN extraction_pct < 50 THEN 70
                WHEN extraction_pct < 70 THEN 40 ELSE 10
            END * 0.4) as groundwater_health_score,
            CASE
                WHEN (CASE WHEN avg_depth_meters<20 THEN 100 ELSE 0 END +
                      CASE WHEN extraction_pct<30 THEN 100 ELSE 0 END) >= 150 THEN 'EXCELLENT'
                WHEN (CASE WHEN avg_depth_meters<35 THEN 100 ELSE 0 END +
                      CASE WHEN extraction_pct<50 THEN 100 ELSE 0 END) >= 150 THEN 'GOOD'
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
            ROUND(current.avg_depth_meters-prev.avg_depth_meters, 2) as depth_change,
            ROUND((current.avg_depth_meters-prev.avg_depth_meters)*100.0/NULLIF(prev.avg_depth_meters,0), 2) as percent_change
        FROM groundwater_levels current
        LEFT JOIN groundwater_levels prev ON current.district_name=prev.district_name
            AND prev.assessment_year=current.assessment_year-1
        WHERE current.avg_depth_meters IS NOT NULL
        ORDER BY percent_change DESC
    """,
    "10. Critical Zone Identification": """
        SELECT
            district_name, assessment_year, avg_depth_meters, extraction_pct,
            CASE
                WHEN avg_depth_meters>35 AND extraction_pct>65 THEN 'CRITICAL_ZONE'
                WHEN avg_depth_meters>25 AND extraction_pct>50 THEN 'ALERT_ZONE'
                WHEN avg_depth_meters>15 AND extraction_pct>35 THEN 'MONITORING_ZONE'
                ELSE 'SAFE_ZONE'
            END as critical_zone
        FROM groundwater_levels
        WHERE (avg_depth_meters>35 AND extraction_pct>65)
           OR (avg_depth_meters>25 AND extraction_pct>50)
        ORDER BY avg_depth_meters DESC, extraction_pct DESC
    """,
    "11. Recharge Potential Assessment": """
        SELECT
            district_name, assessment_year, recharge_rate_mcm, extraction_pct,
            ROUND(recharge_rate_mcm*0.3, 2) as artificial_recharge_potential,
            CASE
                WHEN extraction_pct > recharge_rate_mcm   THEN 'RECHARGE_REQUIRED'
                WHEN extraction_pct > recharge_rate_mcm*0.8 THEN 'RECHARGE_RECOMMENDED'
                ELSE 'ADEQUATE_RECHARGE'
            END as recharge_necessity
        FROM groundwater_levels
        WHERE recharge_rate_mcm IS NOT NULL
        ORDER BY (extraction_pct-recharge_rate_mcm) DESC
    """,
    "12. Groundwater Stress Index": """
        SELECT
            district_name, assessment_year,
            ROUND(avg_depth_meters, 2) as depth,
            ROUND(extraction_pct, 2) as extraction,
            ROUND((avg_depth_meters/50.0*100 + extraction_pct)/2, 2) as stress_index,
            CASE
                WHEN (avg_depth_meters/50.0*100+extraction_pct)/2 > 80 THEN 'EXTREME_STRESS'
                WHEN (avg_depth_meters/50.0*100+extraction_pct)/2 > 60 THEN 'HIGH_STRESS'
                WHEN (avg_depth_meters/50.0*100+extraction_pct)/2 > 40 THEN 'MODERATE_STRESS'
                ELSE 'LOW_STRESS'
            END as stress_level
        FROM groundwater_levels
        ORDER BY stress_index DESC
    """,
}

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
            state_name, district_name,
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
            state_name, district_name,
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
            state_name, district_name, station_name, turbidity_ntu,
            CASE
                WHEN turbidity_ntu < 5  THEN 'CLEAR'
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
        ORDER BY turbidity_ntu DESC LIMIT 30
    """,
    "5. Comprehensive Water Quality Index": """
        SELECT
            station_name, state_name, district_name,
            ph_level, dissolved_oxygen_mg_l, turbidity_ntu,
            ROUND(
                (CASE WHEN ph_level BETWEEN 6.5 AND 8.5 THEN 100 WHEN ph_level BETWEEN 6.0 AND 9.0 THEN 70 ELSE 40 END*0.3
                +CASE WHEN dissolved_oxygen_mg_l>7 THEN 100 WHEN dissolved_oxygen_mg_l>5 THEN 70 WHEN dissolved_oxygen_mg_l>3 THEN 40 ELSE 10 END*0.4
                +CASE WHEN turbidity_ntu<5 THEN 100 WHEN turbidity_ntu<10 THEN 70 WHEN turbidity_ntu<20 THEN 40 ELSE 10 END*0.3), 2
            ) as water_quality_index,
            CASE
                WHEN (CASE WHEN ph_level BETWEEN 6.5 AND 8.5 THEN 100 ELSE 0 END
                     +CASE WHEN dissolved_oxygen_mg_l>5 THEN 100 ELSE 0 END
                     +CASE WHEN turbidity_ntu<10 THEN 100 ELSE 0 END) >= 250 THEN 'EXCELLENT'
                WHEN (CASE WHEN ph_level BETWEEN 6.0 AND 9.0 THEN 100 ELSE 0 END
                     +CASE WHEN dissolved_oxygen_mg_l>3 THEN 100 ELSE 0 END
                     +CASE WHEN turbidity_ntu<20 THEN 100 ELSE 0 END) >= 200 THEN 'GOOD'
                ELSE 'POOR'
            END as overall_rating
        FROM water_monitoring_stations
        WHERE ph_level IS NOT NULL AND dissolved_oxygen_mg_l IS NOT NULL AND turbidity_ntu IS NOT NULL
        ORDER BY water_quality_index DESC
    """,
    "6. Polluted Station Identification": """
        SELECT
            station_name, state_name, district_name,
            ph_level, dissolved_oxygen_mg_l, turbidity_ntu,
            CASE
                WHEN ph_level < 6.0 OR ph_level > 9.0 THEN 'pH_VIOLATION'
                WHEN dissolved_oxygen_mg_l < 4            THEN 'DO_CRITICAL'
                WHEN turbidity_ntu > 25                   THEN 'TURBIDITY_HIGH'
                ELSE 'WITHIN_LIMITS'
            END as primary_concern
        FROM water_monitoring_stations
        WHERE ph_level < 6.0 OR ph_level > 9.0 OR dissolved_oxygen_mg_l < 4 OR turbidity_ntu > 25
        ORDER BY dissolved_oxygen_mg_l
    """,
    "7. Station Status Summary": """
        SELECT
            status,
            COUNT(*) as total_stations,
            ROUND(AVG(ph_level), 2) as avg_ph,
            ROUND(AVG(dissolved_oxygen_mg_l), 2) as avg_do,
            ROUND(AVG(turbidity_ntu), 2) as avg_turbidity
        FROM water_monitoring_stations
        WHERE status IS NOT NULL
        GROUP BY status
        ORDER BY total_stations DESC
    """,
    "8. State-wise Water Quality Summary": """
        SELECT
            state_name,
            COUNT(*) as total_stations,
            ROUND(AVG(ph_level), 2) as avg_ph,
            ROUND(AVG(dissolved_oxygen_mg_l), 2) as avg_do,
            ROUND(AVG(turbidity_ntu), 2) as avg_turbidity,
            ROUND(AVG(CASE WHEN ph_level BETWEEN 6.5 AND 8.5 THEN 1.0 ELSE 0.0 END)*100, 2) as ph_compliance_pct
        FROM water_monitoring_stations
        WHERE state_name IS NOT NULL
        GROUP BY state_name
        ORDER BY avg_do DESC
    """,
    "9. Contamination Risk Assessment": """
        SELECT
            station_name, state_name, district_name,
            ph_level, dissolved_oxygen_mg_l, turbidity_ntu,
            CASE
                WHEN ph_level < 6.0 OR ph_level > 9.0   THEN 'pH_VIOLATION'
                WHEN dissolved_oxygen_mg_l < 3            THEN 'DO_CRITICAL'
                WHEN turbidity_ntu > 25                   THEN 'TURBIDITY_HIGH'
                ELSE 'WITHIN_LIMITS'
            END as primary_concern,
            CASE
                WHEN (ph_level<6.0 OR ph_level>9.0) AND dissolved_oxygen_mg_l<3 THEN 'HIGH_RISK'
                WHEN (NOT ph_level BETWEEN 6.5 AND 8.5) OR dissolved_oxygen_mg_l<5 OR turbidity_ntu>20 THEN 'MODERATE_RISK'
                ELSE 'LOW_RISK'
            END as contamination_risk
        FROM water_monitoring_stations
        ORDER BY contamination_risk, turbidity_ntu DESC
    """,
    "10. Critical Stations Alert": """
        SELECT
            station_name, state_name, district_name,
            ph_level, dissolved_oxygen_mg_l, turbidity_ntu, status,
            CASE
                WHEN dissolved_oxygen_mg_l<3 OR ph_level<5.5 OR ph_level>10 OR turbidity_ntu>30 THEN 'IMMEDIATE_ACTION'
                WHEN dissolved_oxygen_mg_l<5 OR NOT ph_level BETWEEN 6.0 AND 9.0 OR turbidity_ntu>20 THEN 'URGENT_MONITORING'
                ELSE 'ROUTINE_CHECK'
            END as alert_level
        FROM water_monitoring_stations
        WHERE dissolved_oxygen_mg_l<5 OR NOT ph_level BETWEEN 6.0 AND 9.0 OR turbidity_ntu>20
        ORDER BY dissolved_oxygen_mg_l, ph_level LIMIT 25
    """,
    "11. Multi-Parameter Correlation": """
        SELECT
            state_name,
            ROUND(CORR(ph_level, dissolved_oxygen_mg_l)::numeric, 3) as ph_do_correlation,
            ROUND(CORR(ph_level, turbidity_ntu)::numeric, 3) as ph_turbidity_correlation,
            ROUND(CORR(dissolved_oxygen_mg_l, turbidity_ntu)::numeric, 3) as do_turbidity_correlation,
            COUNT(*) as sample_size,
            ROUND(AVG(ph_level)::numeric, 2) as mean_ph,
            ROUND(AVG(dissolved_oxygen_mg_l)::numeric, 2) as mean_do,
            ROUND(AVG(turbidity_ntu)::numeric, 2) as mean_turbidity
        FROM water_monitoring_stations
        WHERE ph_level IS NOT NULL AND dissolved_oxygen_mg_l IS NOT NULL AND turbidity_ntu IS NOT NULL
        GROUP BY state_name HAVING COUNT(*)>5
        ORDER BY ph_do_correlation DESC
    """,
    "12. Ecological Risk Index": """
        SELECT
            station_name, state_name, district_name,
            dissolved_oxygen_mg_l, ph_level, turbidity_ntu,
            ROUND((
                CASE WHEN dissolved_oxygen_mg_l<3 THEN 100 WHEN dissolved_oxygen_mg_l<5 THEN 60 ELSE 10 END*0.5 +
                CASE WHEN NOT ph_level BETWEEN 6.5 AND 8.5 THEN 80 ELSE 10 END*0.3 +
                CASE WHEN turbidity_ntu>20 THEN 80 WHEN turbidity_ntu>10 THEN 40 ELSE 10 END*0.2
            )::numeric, 2) as ecological_risk_index,
            CASE
                WHEN dissolved_oxygen_mg_l<3 OR (NOT ph_level BETWEEN 5.5 AND 9.5) THEN 'EXTREME_RISK'
                WHEN dissolved_oxygen_mg_l<5 OR (NOT ph_level BETWEEN 6.0 AND 9.0) OR turbidity_ntu>20 THEN 'HIGH_RISK'
                WHEN dissolved_oxygen_mg_l<6 OR (NOT ph_level BETWEEN 6.5 AND 8.5) OR turbidity_ntu>10 THEN 'MODERATE_RISK'
                ELSE 'LOW_RISK'
            END as risk_category
        FROM water_monitoring_stations
        ORDER BY ecological_risk_index DESC LIMIT 30
    """,
}

# ─────────────────────────────────────────────────────────────────────────────
# FILTER BUILDERS
# ─────────────────────────────────────────────────────────────────────────────
def safe_format(query: str, **kwargs) -> str:
    """Format query with placeholders; missing keys → empty string."""
    return query.format_map(SafeDict(kwargs))

def build_rainfall_filters(district, min_rain, max_rain, year, season):
    d = f"AND district_name = '{district}'" if district != "All" else ""
    r = f"AND rainfall_cm BETWEEN {min_rain} AND {max_rain}"
    y = f"AND record_year = {year}" if year != 0 else ""
    s = f"AND season = '{season}'" if season != "All" else ""
    return d, r, y, s

def build_groundwater_filters(district, min_depth, max_depth, year):
    d = f"AND district_name = '{district}'" if district != "All" else ""
    r = f"AND avg_depth_meters BETWEEN {min_depth} AND {max_depth}"
    y = f"AND assessment_year = {year}" if year != 0 else ""
    return d, r, y

def build_wq_filters(state, district, min_ph, max_ph, status):
    st_f = f"AND state_name = '{state}'" if state != "All" else ""
    d_f  = f"AND district_name = '{district}'" if district != "All" else ""
    ph_f = f"AND ph_level BETWEEN {min_ph} AND {max_ph}"
    s_f  = f"AND status = '{status}'" if status != "All" else ""
    return st_f, d_f, ph_f, s_f

# ─────────────────────────────────────────────────────────────────────────────
# SMART VISUALIZATION
# ─────────────────────────────────────────────────────────────────────────────
COLOR_SEQ = px.colors.sequential.Plasma
RISK_COLOR = {"CRITICAL_RISK":"#e74c3c","HIGH_RISK":"#e67e22","MODERATE_RISK":"#f1c40f",
              "LOW_RISK":"#2ecc71","EXTREME_STRESS":"#c0392b","HIGH_STRESS":"#e67e22",
              "MODERATE_STRESS":"#f39c12","LOW_STRESS":"#27ae60",
              "CRITICAL_DEPLETION":"#e74c3c","MODERATE_DEPLETION":"#e67e22","STABLE":"#2ecc71",
              "IMMEDIATE_ACTION":"#e74c3c","URGENT_MONITORING":"#e67e22","ROUTINE_CHECK":"#2ecc71",
              "EXCELLENT":"#2ecc71","GOOD":"#3498db","POOR":"#e74c3c",
              "UNSUSTAINABLE":"#c0392b","STRESSED":"#e67e22","MODERATE":"#f1c40f","SUSTAINABLE":"#27ae60"}

CHART_BG   = "rgba(0,0,0,0)"
CHART_FONT = dict(family="Exo 2, sans-serif", color="#cfe4f7", size=12)
CHART_LAYOUT = dict(
    paper_bgcolor=CHART_BG,
    plot_bgcolor="rgba(255,255,255,0.02)",
    font=CHART_FONT,
    margin=dict(l=40, r=20, t=40, b=40),
    legend=dict(bgcolor="rgba(0,0,0,0.3)", bordercolor="#1a3550", borderwidth=1),
    xaxis=dict(gridcolor="#1a3550", zerolinecolor="#1a3550"),
    yaxis=dict(gridcolor="#1a3550", zerolinecolor="#1a3550"),
)

def make_fig(fig):
    fig.update_layout(**CHART_LAYOUT)
    return fig

def auto_chart(df: pd.DataFrame, query_name: str):
    """Generate the most appropriate chart for a given result DataFrame."""
    if df is None or df.empty:
        return None
    cols = df.columns.tolist()
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    cat_cols = df.select_dtypes(exclude=[np.number]).columns.tolist()

    q = query_name.lower()

    # ── Rainfall Charts ────────────────────────────────────────────────
    if "yearly" in q and "avg_rainfall" in cols:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df["record_year"], y=df["avg_rainfall"],
            mode="lines+markers", name="Avg Rainfall",
            line=dict(color="#00b4d8", width=2.5), marker=dict(size=7)))
        if "max_rainfall" in cols:
            fig.add_trace(go.Scatter(x=df["record_year"], y=df["max_rainfall"],
                mode="lines", name="Max", line=dict(color="#e74c3c", dash="dash", width=1.5)))
        fig.update_layout(title="Yearly Rainfall Trend", xaxis_title="Year", yaxis_title="Rainfall (cm)", **CHART_LAYOUT)
        return fig

    if "seasonal" in q and "avg_rainfall" in cols and "season" in cols:
        fig = px.bar(df, x="season", y="avg_rainfall", color="avg_rainfall",
                     color_continuous_scale="Blues", title="Seasonal Rainfall Comparison")
        return make_fig(fig)

    if "district" in q and "comparison" in q and "avg_rainfall" in cols:
        fig = px.bar(df.head(15), x="district_name", y="avg_rainfall", color="avg_rainfall",
                     color_continuous_scale="Plasma", title="District-wise Avg Rainfall")
        return make_fig(fig)

    if "extreme" in q and "rainfall_cm" in cols:
        color_map = {"EXTREME_DANGER":"#e74c3c","SEVERE":"#e67e22","HEAVY":"#f1c40f","MODERATE":"#3498db"}
        fig = px.scatter(df, x="record_year", y="rainfall_cm",
                         color="severity_level", color_discrete_map=color_map,
                         size="rainfall_cm", hover_data=["district_name","season"],
                         title="Extreme Rainfall Events")
        return make_fig(fig)

    if "anomaly" in q and "anomaly_type" in cols:
        fig = px.scatter(df, x="record_year", y="rainfall_cm", color="anomaly_type",
                         hover_data=["district_name","season"], title="Rainfall Anomaly Detection")
        return make_fig(fig)

    if "monsoon" in q and "monsoon_rainfall" in cols:
        fig = px.bar(df.groupby("district_name")[["monsoon_rainfall","non_monsoon_rainfall"]].mean().reset_index(),
                     x="district_name", y=["monsoon_rainfall","non_monsoon_rainfall"],
                     barmode="stack", title="Monsoon vs Non-Monsoon Rainfall",
                     color_discrete_sequence=["#00b4d8","#e67e22"])
        return make_fig(fig)

    if "frequency" in q and "frequency" in cols:
        fig = px.pie(df, values="frequency", names="rainfall_range",
                     color_discrete_sequence=px.colors.sequential.Plasma,
                     title="Rainfall Frequency Distribution", hole=0.4)
        return make_fig(fig)

    if "drought" in q and "drought_risk_level" in cols:
        fig = px.bar(df, x="district_name", y="avg_rainfall", color="drought_risk_level",
                     color_discrete_map={"EXTREME_DROUGHT_RISK":"#c0392b","SEVERE_DROUGHT_RISK":"#e67e22","MODERATE_DROUGHT_RISK":"#f1c40f"},
                     title="Drought Risk by District")
        return make_fig(fig)

    if "variability" in q and "coefficient_of_variation" in cols:
        fig = px.bar(df, x="district_name", y="coefficient_of_variation", color="variability_category",
                     title="Rainfall Variability Index by District")
        return make_fig(fig)

    if ("yoy" in q or "year-over-year" in q) and "percent_change" in cols:
        fig = px.bar(df.dropna(subset=["percent_change"]).head(30),
                     x="district_name", y="percent_change", color="percent_change",
                     color_continuous_scale="RdYlGn", title="Year-over-Year Rainfall Change (%)")
        return make_fig(fig)

    # ── Groundwater Charts ─────────────────────────────────────────────
    if "depletion" in q and "annual_depletion_rate" in cols:
        fig = px.bar(df, x="district_name", y="annual_depletion_rate", color="depletion_status",
                     color_discrete_map={"CRITICAL_DEPLETION":"#e74c3c","MODERATE_DEPLETION":"#e67e22","STABLE":"#2ecc71"},
                     title="Annual Groundwater Depletion Rate by District")
        return make_fig(fig)

    if ("extraction" in q and "recharge" in q) or "deficit_surplus" in cols:
        fig = px.scatter(df, x="recharge_rate" if "recharge_rate" in cols else "recharge_rate_mcm",
                         y="extraction_pct", color="extraction_status",
                         size=np.abs(df.get("deficit_surplus", pd.Series([10]*len(df))).fillna(10)).clip(1).values,
                         hover_data=["district_name","assessment_year"] if "assessment_year" in cols else ["district_name"],
                         title="Extraction vs Recharge Analysis")
        return make_fig(fig)

    if "trend" in q and "trend_direction" in cols and "avg_depth_meters" in cols:
        fig = px.line(df.head(50), x="assessment_year", y="avg_depth_meters",
                      color="district_name", title="Groundwater Depth Trend Over Years")
        return make_fig(fig)

    if "risk" in q and "risk_category" in cols:
        fig = px.scatter(df, x="avg_extraction", y="avg_depth",
                         color="risk_category",
                         color_discrete_map={"CRITICAL_RISK":"#e74c3c","HIGH_RISK":"#e67e22","MODERATE_RISK":"#f1c40f","LOW_RISK":"#2ecc71"},
                         text="district_name", size_max=20,
                         title="Groundwater Risk Zone Map")
        fig.update_traces(textposition="top center")
        return make_fig(fig)

    if "sustainable" in q and "sustainable_yield" in cols:
        fig = px.bar(df.head(15), x="district_name", y="sustainable_yield", color="sustainability_status",
                     color_discrete_map={"UNSUSTAINABLE":"#c0392b","STRESSED":"#e67e22","MODERATE":"#f1c40f","SUSTAINABLE":"#27ae60"},
                     title="Sustainable Yield by District")
        return make_fig(fig)

    if "stress" in q and "stress_index" in cols:
        fig = px.bar(df.head(20), x="district_name", y="stress_index", color="stress_level",
                     color_discrete_map={"EXTREME_STRESS":"#c0392b","HIGH_STRESS":"#e67e22","MODERATE_STRESS":"#f1c40f","LOW_STRESS":"#27ae60"},
                     title="Groundwater Stress Index")
        return make_fig(fig)

    if "health" in q and "groundwater_health_score" in cols:
        fig = px.bar(df.head(20), x="district_name", y="groundwater_health_score", color="overall_health_status",
                     color_discrete_map={"EXCELLENT":"#2ecc71","GOOD":"#3498db","POOR":"#e74c3c"},
                     title="Groundwater Health Score")
        return make_fig(fig)

    # ── Water Quality Charts ───────────────────────────────────────────
    if "ph" in q and "avg_ph" in cols:
        fig = px.bar(df, x="district_name" if "district_name" in cols else "state_name",
                     y="avg_ph", color="ph_status",
                     color_discrete_map={"IDEAL":"#2ecc71","ACCEPTABLE":"#f1c40f","CRITICAL":"#e74c3c"},
                     title="pH Level by District/State")
        fig.add_hline(y=6.5, line_dash="dash", line_color="#2ecc71", annotation_text="Min Safe (6.5)")
        fig.add_hline(y=8.5, line_dash="dash", line_color="#2ecc71", annotation_text="Max Safe (8.5)")
        return make_fig(fig)

    if "dissolved oxygen" in q and "avg_do" in cols:
        fig = px.bar(df, x="district_name" if "district_name" in cols else "state_name",
                     y="avg_do", color="water_quality_class",
                     color_discrete_map={"EXCELLENT":"#2ecc71","GOOD":"#3498db","FAIR":"#f1c40f","POOR":"#e74c3c"},
                     title="Dissolved Oxygen Levels")
        fig.add_hline(y=5, line_dash="dot", line_color="#f1c40f", annotation_text="Min Acceptable (5 mg/L)")
        return make_fig(fig)

    if "quality index" in q and "water_quality_index" in cols:
        fig = px.bar(df.head(20), x="station_name", y="water_quality_index", color="overall_rating",
                     color_discrete_map={"EXCELLENT":"#2ecc71","GOOD":"#3498db","POOR":"#e74c3c"},
                     title="Water Quality Index by Station")
        return make_fig(fig)

    if "turbidity" in q and "turbidity_ntu" in cols:
        fig = px.bar(df.head(20), x="station_name", y="turbidity_ntu", color="turbidity_level",
                     color_discrete_map={"CLEAR":"#2ecc71","SLIGHTLY_TURBID":"#3498db","TURBID":"#f1c40f","HIGHLY_TURBID":"#e74c3c"},
                     title="Turbidity Levels by Station")
        return make_fig(fig)

    if "alert" in q and "alert_level" in cols:
        fig = px.scatter(df, x="ph_level", y="dissolved_oxygen_mg_l",
                         color="alert_level",
                         color_discrete_map={"IMMEDIATE_ACTION":"#e74c3c","URGENT_MONITORING":"#e67e22","ROUTINE_CHECK":"#2ecc71"},
                         size="turbidity_ntu", hover_data=["station_name","state_name"],
                         title="Critical Stations Alert — pH vs DO")
        return make_fig(fig)

    if "contamination" in q and "contamination_risk" in cols:
        risk_counts = df["contamination_risk"].value_counts().reset_index()
        risk_counts.columns = ["risk", "count"]
        fig = px.pie(risk_counts, values="count", names="risk",
                     color="risk",
                     color_discrete_map={"HIGH_RISK":"#e74c3c","MODERATE_RISK":"#e67e22","LOW_RISK":"#2ecc71"},
                     title="Contamination Risk Distribution", hole=0.45)
        return make_fig(fig)

    if "state-wise" in q and "avg_do" in cols:
        fig = make_subplots(rows=1, cols=3, subplot_titles=("Avg pH","Avg DO","Avg Turbidity"))
        fig.add_trace(go.Bar(x=df["state_name"], y=df["avg_ph"], name="pH", marker_color="#3498db"), row=1, col=1)
        fig.add_trace(go.Bar(x=df["state_name"], y=df["avg_do"], name="DO", marker_color="#2ecc71"), row=1, col=2)
        fig.add_trace(go.Bar(x=df["state_name"], y=df["avg_turbidity"], name="Turbidity", marker_color="#e67e22"), row=1, col=3)
        fig.update_layout(title="State-wise Water Quality Metrics", **CHART_LAYOUT)
        return fig

    if "correlation" in q and "ph_do_correlation" in cols:
        fig = go.Figure(data=go.Heatmap(
            z=df[["ph_do_correlation","ph_turbidity_correlation","do_turbidity_correlation"]].values,
            x=["pH-DO","pH-Turbidity","DO-Turbidity"],
            y=df["state_name"].tolist(),
            colorscale="RdYlGn", zmid=0,
        ))
        fig.update_layout(title="Parameter Correlation Heatmap", **CHART_LAYOUT)
        return fig

    # ── Fallback: generic numeric chart ───────────────────────────────
    if len(num_cols) >= 1 and len(cat_cols) >= 1:
        fig = px.bar(df.head(20), x=cat_cols[0], y=num_cols[0],
                     color=num_cols[0], color_continuous_scale="Plasma",
                     title=f"{num_cols[0]} by {cat_cols[0]}")
        return make_fig(fig)

    return None

# ─────────────────────────────────────────────────────────────────────────────
# KPI CARD HELPER
# ─────────────────────────────────────────────────────────────────────────────
def kpi(label, value, sub=""):
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-sub">{sub}</div>
    </div>""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='text-align:center;padding:12px 0 8px'>
        <span style='font-size:2.2rem'>💧</span><br>
        <span style='font-family:Rajdhani,sans-serif;font-size:1.4rem;color:#00e5ff;font-weight:700;'>AQUASTAT</span><br>
        <span style='font-size:0.7rem;color:#4a7fa0;letter-spacing:0.12em'>NATIONAL WATER COMMAND CENTER</span>
    </div>""", unsafe_allow_html=True)

    if DEMO_MODE:
        st.markdown("<div class='demo-badge'>⚡ DEMO MODE — No DB connected</div>", unsafe_allow_html=True)
    else:
        st.markdown("<div class='badge-good'>🟢 LIVE DATABASE</div>", unsafe_allow_html=True)

    st.markdown("---")
    section = st.radio(
        "Navigate",
        ["🏠  Overview", "🌧  Rainfall", "💧  Groundwater", "🔬  Water Quality", "💻  SQL Explorer"],
        label_visibility="collapsed"
    )
    st.markdown("---")
    st.markdown("<span style='color:#4a7fa0;font-size:0.72rem'>© 2025 AQUASTAT · National Water Authority</span>",
                unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# OVERVIEW PAGE
# ─────────────────────────────────────────────────────────────────────────────
if section == "🏠  Overview":
    st.markdown("<div class='section-header'>🏠 National Water Overview</div>", unsafe_allow_html=True)

    df_r  = get_demo_rainfall()
    df_gw = get_demo_groundwater()
    df_wq = get_demo_water_quality()

    c1, c2, c3, c4 = st.columns(4)
    with c1: kpi("Avg National Rainfall", f"{df_r['rainfall_cm'].mean():.1f} cm", "Across all districts")
    with c2: kpi("Avg Groundwater Depth", f"{df_gw['avg_depth_meters'].mean():.1f} m", "National average")
    with c3: kpi("Avg Extraction Rate", f"{df_gw['extraction_pct'].mean():.1f}%", "Of recharge capacity")
    with c4: kpi("Active Monitoring Stations", str((df_wq["status"]=="Active").sum()), "Water quality stations")

    st.markdown("---")
    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("🌧 Rainfall by District")
        dist_rain = df_r.groupby("district_name")["rainfall_cm"].mean().reset_index().sort_values("rainfall_cm", ascending=False)
        fig = px.bar(dist_rain, x="district_name", y="rainfall_cm",
                     color="rainfall_cm", color_continuous_scale="Blues",
                     title="Average Rainfall by District (cm)")
        st.plotly_chart(make_fig(fig), use_container_width=True)

    with col_r:
        st.subheader("📉 Groundwater Extraction vs Recharge")
        gw_agg = df_gw.groupby("district_name")[["extraction_pct","recharge_rate_mcm"]].mean().reset_index()
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(x=gw_agg["district_name"], y=gw_agg["extraction_pct"],
                              name="Extraction %", marker_color="#e74c3c"))
        fig2.add_trace(go.Bar(x=gw_agg["district_name"], y=gw_agg["recharge_rate_mcm"],
                              name="Recharge MCM", marker_color="#2ecc71"))
        fig2.update_layout(barmode="group", title="Extraction vs Recharge", **CHART_LAYOUT)
        st.plotly_chart(fig2, use_container_width=True)

    col_l2, col_r2 = st.columns(2)
    with col_l2:
        st.subheader("🔬 Water Quality Index Distribution")
        df_wq2 = df_wq.copy()
        df_wq2["wqi"] = (
            np.where(df_wq2["ph_level"].between(6.5, 8.5), 100,
            np.where(df_wq2["ph_level"].between(6.0, 9.0), 70, 40)) * 0.3 +
            np.where(df_wq2["dissolved_oxygen_mg_l"] > 7, 100,
            np.where(df_wq2["dissolved_oxygen_mg_l"] > 5, 70,
            np.where(df_wq2["dissolved_oxygen_mg_l"] > 3, 40, 10))) * 0.4 +
            np.where(df_wq2["turbidity_ntu"] < 5, 100,
            np.where(df_wq2["turbidity_ntu"] < 10, 70,
            np.where(df_wq2["turbidity_ntu"] < 20, 40, 10))) * 0.3
        )
        fig3 = px.histogram(df_wq2, x="wqi", nbins=20, color_discrete_sequence=["#00b4d8"],
                            title="Water Quality Index Distribution")
        fig3.add_vline(x=70, line_dash="dash", line_color="#2ecc71", annotation_text="Good Threshold")
        st.plotly_chart(make_fig(fig3), use_container_width=True)

    with col_r2:
        st.subheader("📊 Seasonal Rainfall Pattern")
        seasonal = df_r.groupby("season")["rainfall_cm"].mean().reset_index()
        fig4 = px.bar(seasonal, x="season", y="rainfall_cm",
                      color="season", color_discrete_sequence=px.colors.qualitative.Bold,
                      title="Average Rainfall by Season")
        st.plotly_chart(make_fig(fig4), use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# RAINFALL PAGE
# ─────────────────────────────────────────────────────────────────────────────
elif section == "🌧  Rainfall":
    st.markdown("<div class='section-header'>🌧 Rainfall Analysis Module</div>", unsafe_allow_html=True)

    with st.expander("🔧 Filters (applies to Query 1)", expanded=False):
        fc1, fc2, fc3, fc4 = st.columns(4)
        with fc1:
            r_district = st.selectbox("District", ["All"] + DISTRICTS, key="r_dist")
        with fc2:
            r_rain = st.slider("Rainfall Range (cm)", 0, 500, (0, 500), key="r_rain")
        with fc3:
            r_year = st.selectbox("Year", [0] + YEARS, format_func=lambda x: "All" if x == 0 else str(x), key="r_year")
        with fc4:
            r_season = st.selectbox("Season", ["All"] + SEASONS, key="r_season")

    query_name = st.selectbox("📋 Select Analysis Query", list(RAINFALL_QUERIES.keys()), key="r_qname")

    st.markdown(f"<div class='query-info-box'>Running: <b>{query_name}</b>"
                + (" · <i>Demo mode — synthetic data</i>" if DEMO_MODE else "") + "</div>",
                unsafe_allow_html=True)

    if st.button("▶ Run Query", key="r_run", type="primary"):
        with st.spinner("Executing..."):
            d_f, r_f, y_f, s_f = build_rainfall_filters(r_district, r_rain[0], r_rain[1], r_year, r_season)
            raw_query = safe_format(
                RAINFALL_QUERIES[query_name],
                district_filter=d_f, rainfall_range=r_f, year_filter=y_f, season_filter=s_f
            )

            if DEMO_MODE:
                df_result = run_demo_query(query_name.lower(), "rainfall", get_demo_rainfall())
            else:
                df_result, err = execute_query(raw_query)
                if err:
                    st.error(f"Query Error: {err}")
                    df_result = None

            if df_result is not None and not df_result.empty:
                st.success(f"✅ {len(df_result)} rows returned")
                tab_data, tab_chart, tab_sql = st.tabs(["📊 Data", "📈 Chart", "📝 SQL"])

                with tab_data:
                    st.dataframe(df_result.style.background_gradient(
                        subset=df_result.select_dtypes(include=[np.number]).columns.tolist(),
                        cmap="Blues"), use_container_width=True)
                    buf = BytesIO()
                    df_result.to_csv(buf, index=False)
                    st.download_button("⬇ Download CSV", buf.getvalue(), f"rainfall_{query_name[:15]}.csv", "text/csv")

                with tab_chart:
                    fig = auto_chart(df_result, query_name)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No suitable chart for this result set.")

                with tab_sql:
                    st.code(raw_query.strip(), language="sql")
            elif df_result is not None:
                st.warning("Query returned 0 rows.")
    else:
        st.info("👆 Configure filters above then click **Run Query**.")

# ─────────────────────────────────────────────────────────────────────────────
# GROUNDWATER PAGE
# ─────────────────────────────────────────────────────────────────────────────
elif section == "💧  Groundwater":
    st.markdown("<div class='section-header'>💧 Groundwater Analysis Module</div>", unsafe_allow_html=True)

    with st.expander("🔧 Filters (applies to Query 1)", expanded=False):
        gc1, gc2, gc3 = st.columns(3)
        with gc1:
            g_district = st.selectbox("District", ["All"] + DISTRICTS, key="g_dist")
        with gc2:
            g_depth = st.slider("Depth Range (m)", 0, 100, (0, 100), key="g_depth")
        with gc3:
            g_year = st.selectbox("Year", [0] + YEARS, format_func=lambda x: "All" if x == 0 else str(x), key="g_year")

    query_name = st.selectbox("📋 Select Analysis Query", list(GROUNDWATER_QUERIES.keys()), key="g_qname")

    st.markdown(f"<div class='query-info-box'>Running: <b>{query_name}</b>"
                + (" · <i>Demo mode — synthetic data</i>" if DEMO_MODE else "") + "</div>",
                unsafe_allow_html=True)

    if st.button("▶ Run Query", key="g_run", type="primary"):
        with st.spinner("Executing..."):
            d_f, r_f, y_f = build_groundwater_filters(g_district, g_depth[0], g_depth[1], g_year)
            raw_query = safe_format(
                GROUNDWATER_QUERIES[query_name],
                district_filter=d_f, depth_range=r_f, year_filter=y_f
            )

            if DEMO_MODE:
                df_result = run_demo_query(query_name.lower(), "groundwater", get_demo_groundwater())
            else:
                df_result, err = execute_query(raw_query)
                if err:
                    st.error(f"Query Error: {err}")
                    df_result = None

            if df_result is not None and not df_result.empty:
                st.success(f"✅ {len(df_result)} rows returned")
                tab_data, tab_chart, tab_sql = st.tabs(["📊 Data", "📈 Chart", "📝 SQL"])

                with tab_data:
                    st.dataframe(df_result.style.background_gradient(
                        subset=df_result.select_dtypes(include=[np.number]).columns.tolist(),
                        cmap="Blues"), use_container_width=True)
                    buf = BytesIO()
                    df_result.to_csv(buf, index=False)
                    st.download_button("⬇ Download CSV", buf.getvalue(), f"groundwater_{query_name[:15]}.csv", "text/csv")

                with tab_chart:
                    fig = auto_chart(df_result, query_name)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No suitable chart for this result set.")

                with tab_sql:
                    st.code(raw_query.strip(), language="sql")
            elif df_result is not None:
                st.warning("Query returned 0 rows.")
    else:
        st.info("👆 Configure filters above then click **Run Query**.")

# ─────────────────────────────────────────────────────────────────────────────
# WATER QUALITY PAGE
# ─────────────────────────────────────────────────────────────────────────────
elif section == "🔬  Water Quality":
    st.markdown("<div class='section-header'>🔬 Water Quality Analysis Module</div>", unsafe_allow_html=True)

    with st.expander("🔧 Filters (applies to Query 1)", expanded=False):
        wc1, wc2, wc3, wc4 = st.columns(4)
        with wc1:
            w_state = st.selectbox("State", ["All"] + STATES, key="w_state")
        with wc2:
            w_district = st.selectbox("District", ["All"] + DISTRICTS, key="w_dist")
        with wc3:
            w_ph = st.slider("pH Range", 4.0, 12.0, (5.5, 9.5), step=0.1, key="w_ph")
        with wc4:
            w_status = st.selectbox("Status", ["All"] + STATUS_LIST, key="w_status")

    query_name = st.selectbox("📋 Select Analysis Query", list(WATER_QUALITY_QUERIES.keys()), key="w_qname")

    st.markdown(f"<div class='query-info-box'>Running: <b>{query_name}</b>"
                + (" · <i>Demo mode — synthetic data</i>" if DEMO_MODE else "") + "</div>",
                unsafe_allow_html=True)

    if st.button("▶ Run Query", key="w_run", type="primary"):
        with st.spinner("Executing..."):
            st_f, d_f, ph_f, s_f = build_wq_filters(w_state, w_district, w_ph[0], w_ph[1], w_status)
            raw_query = safe_format(
                WATER_QUALITY_QUERIES[query_name],
                state_filter=st_f, district_filter=d_f, ph_range=ph_f, status_filter=s_f
            )

            if DEMO_MODE:
                df_result = run_demo_query(query_name.lower(), "water_quality", get_demo_water_quality())
            else:
                df_result, err = execute_query(raw_query)
                if err:
                    st.error(f"Query Error: {err}")
                    df_result = None

            if df_result is not None and not df_result.empty:
                st.success(f"✅ {len(df_result)} rows returned")
                tab_data, tab_chart, tab_sql = st.tabs(["📊 Data", "📈 Chart", "📝 SQL"])

                with tab_data:
                    st.dataframe(df_result.style.background_gradient(
                        subset=df_result.select_dtypes(include=[np.number]).columns.tolist(),
                        cmap="Blues"), use_container_width=True)
                    buf = BytesIO()
                    df_result.to_csv(buf, index=False)
                    st.download_button("⬇ Download CSV", buf.getvalue(), f"wq_{query_name[:15]}.csv", "text/csv")

                with tab_chart:
                    fig = auto_chart(df_result, query_name)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No suitable chart for this result set.")

                with tab_sql:
                    st.code(raw_query.strip(), language="sql")
            elif df_result is not None:
                st.warning("Query returned 0 rows.")
    else:
        st.info("👆 Configure filters above then click **Run Query**.")

# ─────────────────────────────────────────────────────────────────────────────
# SQL EXPLORER
# ─────────────────────────────────────────────────────────────────────────────
elif section == "💻  SQL Explorer":
    st.markdown("<div class='section-header'>💻 Custom SQL Explorer</div>", unsafe_allow_html=True)

    if DEMO_MODE:
        st.warning("⚠️ SQL Explorer requires a live database connection. Add `NEON_URL` to your Streamlit secrets to enable live queries.")
        st.markdown("**Available Tables:**")
        st.code("rainfall_history\ngroundwater_levels\nwater_monitoring_stations", language="sql")

        st.markdown("**Sample Query:**")
        sample = st.selectbox("Load sample query", [
            "-- Select a sample --",
            "SELECT * FROM rainfall_history LIMIT 10;",
            "SELECT district_name, AVG(rainfall_cm) FROM rainfall_history GROUP BY district_name ORDER BY 2 DESC;",
            "SELECT * FROM groundwater_levels WHERE extraction_pct > 70 ORDER BY avg_depth_meters DESC LIMIT 20;",
            "SELECT state_name, COUNT(*) FROM water_monitoring_stations GROUP BY state_name;",
        ])
        default_query = "" if sample == "-- Select a sample --" else sample
    else:
        default_query = "SELECT * FROM rainfall_history LIMIT 10;"

    custom_sql = st.text_area("✍️ Enter SQL Query", value=default_query, height=180,
                              placeholder="SELECT * FROM rainfall_history LIMIT 10;")

    if st.button("▶ Execute Query", key="sql_run", type="primary") and not DEMO_MODE:
        with st.spinner("Running..."):
            df_result, err = execute_query(custom_sql)
            if err:
                st.error(f"❌ Error: {err}")
            elif df_result is not None:
                st.success(f"✅ {len(df_result)} rows · {len(df_result.columns)} columns")
                st.dataframe(df_result, use_container_width=True)
                buf = BytesIO()
                df_result.to_csv(buf, index=False)
                st.download_button("⬇ Download CSV", buf.getvalue(), "custom_query.csv", "text/csv")

    st.markdown("---")
    st.markdown("### 📚 Quick Reference — All Available Queries")
    tab_r, tab_g, tab_w = st.tabs(["🌧 Rainfall (15)", "💧 Groundwater (12)", "🔬 Water Quality (12)"])

    with tab_r:
        for k, v in RAINFALL_QUERIES.items():
            with st.expander(k):
                st.code(v.strip(), language="sql")

    with tab_g:
        for k, v in GROUNDWATER_QUERIES.items():
            with st.expander(k):
                st.code(v.strip(), language="sql")

    with tab_w:
        for k, v in WATER_QUALITY_QUERIES.items():
            with st.expander(k):
                st.code(v.strip(), language="sql")
