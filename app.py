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
.badge-warning  { background: linear-gradient(135deg,#e67e22,#f1c40f); color:#1a1a1a; padding:6px 18px; border-radius:30px; display:inline-block; }
.badge-good     { background: linear-gradient(135deg,#27ae60,#2ecc71); color:#fff; padding:6px 18px; border-radius:30px; display:inline-block; }
.filter-section {
    background: rgba(10,30,48,0.4);
    border: 1px solid rgba(0,200,255,0.15);
    border-radius: 10px;
    padding: 15px;
    margin-bottom: 15px;
}
.kpi-label { color:#7fb8d8; font-size:0.78rem; letter-spacing:0.1em; text-transform:uppercase; margin-bottom:4px; }
.kpi-sub   { color:#4a7fa0; font-size:0.72rem; margin-top:2px; }
.section-header {
    font-size:1.5rem; font-weight:700; color:#00e5ff;
    border-bottom:1px solid rgba(0,200,255,0.2);
    padding-bottom:8px; margin-bottom:16px;
}
.query-info-box {
    background: rgba(0,100,160,0.15);
    border-left: 3px solid #00b4d8;
    border-radius:4px; padding:10px 14px;
    font-size:0.83rem; color:#9ecfec; margin-bottom:12px;
}
.demo-badge {
    background: linear-gradient(135deg,#6a3de8,#9b59b6);
    color:#fff; padding:4px 14px; border-radius:30px;
    font-size:0.75rem; font-weight:600; display:inline-block; margin-bottom:8px;
}
div[data-testid="stDataFrameResizable"] {
    border:1px solid rgba(0,200,255,0.15) !important;
    border-radius:8px !important;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE CONNECTION
# ─────────────────────────────────────────────────────────────────────────────
try:
    NEON_URL = st.secrets["NEON_URL"]
except Exception:
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
    except Exception:
        return None

engine      = init_connection()
DEMO_MODE   = engine is None

# ─────────────────────────────────────────────────────────────────────────────
# SAFE DICT — missing filter placeholders → empty string (no KeyError)
# ─────────────────────────────────────────────────────────────────────────────
class SafeDict(defaultdict):
    def __missing__(self, key):
        return ""

# ─────────────────────────────────────────────────────────────────────────────
# DEMO DATA CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
DISTRICTS   = ["North District","South District","East District","West District",
                "Central District","Coastal District","Highland District",
                "Riverside District","Valley District","Plains District"]
STATES      = ["Maharashtra","Gujarat","Rajasthan","Karnataka",
                "Tamil Nadu","Madhya Pradesh","Uttar Pradesh","Andhra Pradesh"]
SEASONS     = ["Monsoon","Pre-Monsoon","Post-Monsoon","Winter"]
YEARS       = list(range(2018, 2025))
STATUS_LIST = ["Active","Maintenance","Inactive"]

np.random.seed(42)

# ─────────────────────────────────────────────────────────────────────────────
# CACHED DEMO DATAFRAMES
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def get_demo_rainfall():
    rows = []
    for d in DISTRICTS:
        for y in YEARS:
            for s in SEASONS:
                rows.append({"district_name":d,"record_year":y,"season":s,
                              "rainfall_cm":round(np.random.uniform(20,350),1),
                              "state_name":np.random.choice(STATES)})
    return pd.DataFrame(rows)

@st.cache_data(ttl=300)
def get_demo_groundwater():
    rows = []
    for d in DISTRICTS:
        for y in YEARS:
            rows.append({"district_name":d,"assessment_year":y,
                         "avg_depth_meters":round(np.random.uniform(10,55),1),
                         "extraction_pct":round(np.random.uniform(20,90),1),
                         "recharge_rate_mcm":round(np.random.uniform(15,80),1),
                         "state_name":np.random.choice(STATES)})
    return pd.DataFrame(rows)

@st.cache_data(ttl=300)
def get_demo_water_quality():
    rows = []
    for d in DISTRICTS:
        for i in range(1, 6):
            rows.append({"station_name":f"WMS-{d[:3].upper()}-{i:03d}",
                         "district_name":d,
                         "state_name":np.random.choice(STATES),
                         "ph_level":round(np.random.uniform(5.8,9.2),2),
                         "dissolved_oxygen_mg_l":round(np.random.uniform(2.5,9.5),2),
                         "turbidity_ntu":round(np.random.uniform(1,35),1),
                         "status":np.random.choice(STATUS_LIST,p=[0.75,0.15,0.10]),
                         "latitude":round(np.random.uniform(8,37),4),
                         "longitude":round(np.random.uniform(68,97),4)})
    return pd.DataFrame(rows)

# ─────────────────────────────────────────────────────────────────────────────
# QUERY EXECUTION
# ─────────────────────────────────────────────────────────────────────────────
def execute_query(query: str):
    try:
        with engine.connect() as conn:
            return pd.read_sql(query, conn), None
    except Exception as e:
        return None, str(e)

def safe_format(query: str, **kwargs) -> str:
    return query.format_map(SafeDict(kwargs))

# ─────────────────────────────────────────────────────────────────────────────
# DEMO QUERY SIMULATOR
# ─────────────────────────────────────────────────────────────────────────────
def run_demo_query(query_key: str, category: str, df_base: pd.DataFrame) -> pd.DataFrame:
    df = df_base.copy()
    k  = query_key.lower()

    # ── RAINFALL ──────────────────────────────────────────────────────────
    if category == "rainfall":
        if "yearly" in k:
            return df.groupby("record_year").agg(
                total_records=("rainfall_cm","count"),
                avg_rainfall=("rainfall_cm", lambda x: round(x.mean(),2)),
                min_rainfall=("rainfall_cm", lambda x: round(x.min(),2)),
                max_rainfall=("rainfall_cm", lambda x: round(x.max(),2)),
                std_deviation=("rainfall_cm", lambda x: round(x.std(),2)),
            ).reset_index().sort_values("record_year",ascending=False)

        if "seasonal" in k:
            return df.groupby("season").agg(
                total_records=("rainfall_cm","count"),
                avg_rainfall=("rainfall_cm", lambda x: round(x.mean(),2)),
                min_rainfall=("rainfall_cm", lambda x: round(x.min(),2)),
                max_rainfall=("rainfall_cm", lambda x: round(x.max(),2)),
            ).reset_index().sort_values("avg_rainfall",ascending=False)

        if "district" in k and "comparison" in k:
            return df.groupby("district_name").agg(
                records=("rainfall_cm","count"),
                avg_rainfall=("rainfall_cm", lambda x: round(x.mean(),2)),
                total_rainfall=("rainfall_cm", lambda x: round(x.sum(),2)),
            ).reset_index().sort_values("avg_rainfall",ascending=False)

        if "extreme" in k:
            sub = df[df["rainfall_cm"]>200].copy()
            sub["severity_level"] = pd.cut(sub["rainfall_cm"],
                bins=[200,250,300,9999], labels=["HEAVY","SEVERE","EXTREME_DANGER"]).astype(str)
            return sub.sort_values("rainfall_cm",ascending=False).head(30)

        if "anomaly" in k:
            stats = df.groupby("district_name")["rainfall_cm"].agg(["mean","std"]).reset_index()
            stats.columns = ["district_name","mean_rainfall","std_rainfall"]
            m = df.merge(stats, on="district_name")
            m["anomaly_type"] = np.where(m["rainfall_cm"]>m["mean_rainfall"]+2*m["std_rainfall"],"Extreme_High_Anomaly",
                                np.where(m["rainfall_cm"]<m["mean_rainfall"]-2*m["std_rainfall"],"Extreme_Low_Anomaly",
                                np.where(m["rainfall_cm"]>m["mean_rainfall"]+m["std_rainfall"],"High_Anomaly",
                                np.where(m["rainfall_cm"]<m["mean_rainfall"]-m["std_rainfall"],"Low_Anomaly","Normal"))))
            return m[["district_name","record_year","season","rainfall_cm","mean_rainfall","anomaly_type"]].head(30)

        if "cumulative" in k:
            df = df.sort_values(["district_name","record_year","season"])
            df["cumulative_rainfall"] = df.groupby(["district_name","record_year"])["rainfall_cm"].cumsum()
            df["moving_avg_3yr"]      = df.groupby("district_name")["rainfall_cm"].transform(
                                            lambda x: x.rolling(3,min_periods=1).mean().round(2))
            df["yoy_change_pct"]      = df.groupby("district_name")["rainfall_cm"].transform(
                                            lambda x: x.pct_change()*100).round(2)
            return df.head(40)

        if "monsoon" in k:
            mon   = df[df["season"]=="Monsoon"].groupby(["district_name","record_year"])["rainfall_cm"].sum().reset_index()
            mon.columns=["district_name","record_year","monsoon_rainfall"]
            non   = df[df["season"]!="Monsoon"].groupby(["district_name","record_year"])["rainfall_cm"].sum().reset_index()
            non.columns=["district_name","record_year","non_monsoon_rainfall"]
            r = mon.merge(non,on=["district_name","record_year"])
            r["monsoon_contribution_pct"] = (r["monsoon_rainfall"]/(r["monsoon_rainfall"]+r["non_monsoon_rainfall"])*100).round(2)
            return r.sort_values("monsoon_rainfall",ascending=False)

        if "frequency" in k or "distribution" in k:
            bins   = [0,50,150,300,99999]
            labels = ["0-50 cm (Low)","51-150 cm (Moderate)","151-300 cm (High)","300+ cm (Extreme)"]
            df["rainfall_range"] = pd.cut(df["rainfall_cm"],bins=bins,labels=labels)
            freq = df["rainfall_range"].value_counts().reset_index()
            freq.columns = ["rainfall_range","frequency"]
            freq["percentage"] = (freq["frequency"]/freq["frequency"].sum()*100).round(2)
            return freq

        if "year-over-year" in k or "yoy" in k:
            y = df.groupby(["district_name","record_year"])["rainfall_cm"].mean().reset_index().sort_values(["district_name","record_year"])
            y["prev_year_rainfall"] = y.groupby("district_name")["rainfall_cm"].shift(1)
            y["absolute_change"]    = (y["rainfall_cm"]-y["prev_year_rainfall"]).round(2)
            y["percent_change"]     = (y["absolute_change"]/y["prev_year_rainfall"]*100).round(2)
            return y.sort_values(["district_name","record_year"],ascending=[True,False])

        if "drought" in k:
            y = df.groupby(["district_name","record_year"])["rainfall_cm"].mean().reset_index()
            y.columns = ["district_name","record_year","avg_rainfall"]
            y = y[y["avg_rainfall"]<150]
            y["drought_risk_level"] = pd.cut(y["avg_rainfall"],bins=[0,50,100,150],
                labels=["EXTREME_DROUGHT_RISK","SEVERE_DROUGHT_RISK","MODERATE_DROUGHT_RISK"]).astype(str)
            return y.sort_values("avg_rainfall").head(20)

        if "variability" in k:
            r = df.groupby("district_name")["rainfall_cm"].agg(
                num_records="count",
                mean_rainfall=lambda x: round(x.mean(),2),
                std_deviation=lambda x: round(x.std(),2),
            ).reset_index()
            r["coefficient_of_variation"] = (r["std_deviation"]/r["mean_rainfall"]*100).round(2)
            r["variability_category"] = np.where(r["coefficient_of_variation"]>50,"HIGHLY_VARIABLE",
                                        np.where(r["coefficient_of_variation"]>30,"MODERATELY_VARIABLE","STABLE"))
            return r.sort_values("coefficient_of_variation",ascending=False)

        if "efficiency" in k:
            y = df.groupby(["district_name","record_year"])["rainfall_cm"].mean().reset_index()
            y.columns = ["district_name","record_year","actual_rainfall"]
            mn = y.groupby("district_name")["actual_rainfall"].min().rename("min_r")
            mx = y.groupby("district_name")["actual_rainfall"].max().rename("max_r")
            y  = y.join(mn,on="district_name").join(mx,on="district_name")
            y["efficiency_score"]  = ((y["actual_rainfall"]-y["min_r"])/(y["max_r"]-y["min_r"])*100).round(2)
            y["efficiency_rating"] = np.where(y["efficiency_score"]>75,"EXCELLENT",
                                     np.where(y["efficiency_score"]>50,"GOOD","NEEDS_IMPROVEMENT"))
            return y[["district_name","record_year","actual_rainfall","efficiency_score","efficiency_rating"]].sort_values("efficiency_score",ascending=False)

        if "prediction" in k or "trend" in k:
            y = df.groupby(["district_name","record_year"])["rainfall_cm"].mean().reset_index()
            y.columns = ["district_name","record_year","avg_rainfall"]
            mean_map = y.groupby("district_name")["avg_rainfall"].mean()
            y["district_mean"]  = y["district_name"].map(mean_map)
            y["trend_direction"]= np.where(y["avg_rainfall"]>y["district_mean"],"ABOVE_AVERAGE","BELOW_AVERAGE")
            y["yoy_trend_pct"]  = y.groupby("district_name")["avg_rainfall"].pct_change().mul(100).round(2)
            return y.sort_values(["district_name","record_year"],ascending=[True,False])

        return df.sort_values("record_year",ascending=False).head(50)

    # ── GROUNDWATER ───────────────────────────────────────────────────────
    elif category == "groundwater":
        if "depletion" in k:
            r = df.groupby("district_name").apply(lambda g: pd.Series({
                "first_year":g["assessment_year"].min(), "last_year":g["assessment_year"].max(),
                "avg_depth":round(g["avg_depth_meters"].mean(),2),
                "depth_variability":round(g["avg_depth_meters"].std(),2),
                "annual_depletion_rate":round(
                    (g["avg_depth_meters"].max()-g["avg_depth_meters"].min())/
                    max(g["assessment_year"].max()-g["assessment_year"].min(),1),2),
            })).reset_index()
            r["depletion_status"] = np.where(r["annual_depletion_rate"]>2,"CRITICAL_DEPLETION",
                                    np.where(r["annual_depletion_rate"]>1,"MODERATE_DEPLETION","STABLE"))
            return r.sort_values("annual_depletion_rate",ascending=False)

        if "extraction" in k and "recharge" in k:
            df["deficit_surplus"]    = (df["extraction_pct"]-df["recharge_rate_mcm"]).round(2)
            df["extraction_status"]  = np.where(df["extraction_pct"]>df["recharge_rate_mcm"]*1.5,"OVER_EXTRACTION_CRITICAL",
                                       np.where(df["extraction_pct"]>df["recharge_rate_mcm"],"OVER_EXTRACTION_MODERATE",
                                       np.where(df["extraction_pct"]<df["recharge_rate_mcm"]*0.5,"UNDER_UTILIZATION","BALANCED")))
            return df[["district_name","assessment_year","extraction_pct","recharge_rate_mcm",
                        "deficit_surplus","extraction_status"]].sort_values("deficit_surplus",ascending=False)

        if "trend" in k:
            df = df.sort_values(["district_name","assessment_year"])
            df["moving_avg_3yr"]  = df.groupby("district_name")["avg_depth_meters"].transform(
                                        lambda x: x.rolling(3,min_periods=1).mean().round(2))
            df["yoy_change_pct"]  = df.groupby("district_name")["avg_depth_meters"].transform(
                                        lambda x: x.pct_change()*100).round(2)
            df["trend_direction"] = np.where(df["avg_depth_meters"]>df["moving_avg_3yr"]*1.1,"DEEPENING_FAST",
                                    np.where(df["avg_depth_meters"]>df["moving_avg_3yr"]*1.05,"DEEPENING_SLOW",
                                    np.where(df["avg_depth_meters"]<df["moving_avg_3yr"]*0.95,"RECOVERING","STABLE")))
            return df.sort_values(["district_name","assessment_year"],ascending=[True,False]).head(40)

        if "risk" in k:
            r = df.groupby("district_name").agg(
                avg_depth=("avg_depth_meters",lambda x: round(x.mean(),2)),
                avg_extraction=("extraction_pct",lambda x: round(x.mean(),2)),
                avg_recharge=("recharge_rate_mcm",lambda x: round(x.mean(),2)),
            ).reset_index()
            r["risk_category"] = np.where((r["avg_depth"]>40)&(r["avg_extraction"]>70),"CRITICAL_RISK",
                                 np.where((r["avg_depth"]>30)&(r["avg_extraction"]>50),"HIGH_RISK",
                                 np.where((r["avg_depth"]>20)&(r["avg_extraction"]>30),"MODERATE_RISK","LOW_RISK")))
            return r.sort_values("avg_depth",ascending=False)

        if "sustainable" in k:
            df["sustainable_yield"]    = (df["recharge_rate_mcm"]*(1-df["extraction_pct"]/100)).round(2)
            df["sustainability_status"]= np.where(df["extraction_pct"]>80,"UNSUSTAINABLE",
                                         np.where(df["extraction_pct"]>60,"STRESSED",
                                         np.where(df["extraction_pct"]>40,"MODERATE","SUSTAINABLE")))
            return df[["district_name","assessment_year","recharge_rate_mcm",
                        "extraction_pct","sustainable_yield","sustainability_status"]].sort_values("sustainable_yield",ascending=False)

        if "depth class" in k:
            r = df.groupby("district_name").apply(lambda g: pd.Series({
                "total_wells":len(g),
                "shallow_wells":(g["avg_depth_meters"]<20).sum(),
                "medium_wells":((g["avg_depth_meters"]>=20)&(g["avg_depth_meters"]<=40)).sum(),
                "deep_wells":(g["avg_depth_meters"]>40).sum(),
            })).reset_index()
            r["deep_well_percentage"] = (r["deep_wells"]/r["total_wells"]*100).round(2)
            return r.sort_values("deep_well_percentage",ascending=False)

        if "health" in k or "quality index" in k:
            df["depth_score"]      = np.where(df["avg_depth_meters"]<20,100,np.where(df["avg_depth_meters"]<35,70,np.where(df["avg_depth_meters"]<50,40,10)))
            df["extraction_score"] = np.where(df["extraction_pct"]<30,100,np.where(df["extraction_pct"]<50,70,np.where(df["extraction_pct"]<70,40,10)))
            df["groundwater_health_score"] = (df["depth_score"]*0.6+df["extraction_score"]*0.4).round(2)
            df["overall_health_status"]    = np.where(df["groundwater_health_score"]>=80,"EXCELLENT",
                                             np.where(df["groundwater_health_score"]>=55,"GOOD","POOR"))
            return df[["district_name","assessment_year","avg_depth_meters","extraction_pct",
                        "groundwater_health_score","overall_health_status"]].sort_values("groundwater_health_score",ascending=False)

        if "year-over-year" in k or "yoy" in k:
            df = df.sort_values(["district_name","assessment_year"])
            df["previous_depth"] = df.groupby("district_name")["avg_depth_meters"].shift(1)
            df["depth_change"]   = (df["avg_depth_meters"]-df["previous_depth"]).round(2)
            df["percent_change"] = (df["depth_change"]/df["previous_depth"]*100).round(2)
            return df[["district_name","assessment_year","avg_depth_meters",
                        "previous_depth","depth_change","percent_change"]].dropna().sort_values("percent_change",ascending=False)

        if "critical zone" in k:
            c = df[(df["avg_depth_meters"]>25)|(df["extraction_pct"]>50)].copy()
            c["critical_zone"] = np.where((c["avg_depth_meters"]>35)&(c["extraction_pct"]>65),"CRITICAL_ZONE",
                                 np.where((c["avg_depth_meters"]>25)&(c["extraction_pct"]>50),"ALERT_ZONE","MONITORING_ZONE"))
            return c[["district_name","assessment_year","avg_depth_meters","extraction_pct","critical_zone"]].sort_values("avg_depth_meters",ascending=False)

        if "recharge potential" in k:
            df["artificial_recharge_potential"] = (df["recharge_rate_mcm"]*0.3).round(2)
            df["recharge_necessity"]            = np.where(df["extraction_pct"]>df["recharge_rate_mcm"],"RECHARGE_REQUIRED",
                                                  np.where(df["extraction_pct"]>df["recharge_rate_mcm"]*0.8,"RECHARGE_RECOMMENDED","ADEQUATE_RECHARGE"))
            return df[["district_name","assessment_year","recharge_rate_mcm",
                        "extraction_pct","artificial_recharge_potential","recharge_necessity"]]

        if "stress" in k:
            df["stress_index"] = ((df["avg_depth_meters"]/50*100+df["extraction_pct"])/2).round(2)
            df["stress_level"] = np.where(df["stress_index"]>80,"EXTREME_STRESS",
                                 np.where(df["stress_index"]>60,"HIGH_STRESS",
                                 np.where(df["stress_index"]>40,"MODERATE_STRESS","LOW_STRESS")))
            return df[["district_name","assessment_year","avg_depth_meters",
                        "extraction_pct","stress_index","stress_level"]].sort_values("stress_index",ascending=False)

        return df.sort_values("assessment_year",ascending=False).head(50)

    # ── WATER QUALITY ─────────────────────────────────────────────────────
    elif category == "water_quality":
        if "ph level" in k:
            return df.groupby(["state_name","district_name"]).apply(lambda g: pd.Series({
                "total_stations":len(g),
                "avg_ph":round(g["ph_level"].mean(),2),
                "min_ph":round(g["ph_level"].min(),2),
                "max_ph":round(g["ph_level"].max(),2),
                "ph_variability":round(g["ph_level"].std(),2),
                "ph_status":"IDEAL" if 6.5<=g["ph_level"].mean()<=8.5 else
                            "ACCEPTABLE" if 6.0<=g["ph_level"].mean()<=9.0 else "CRITICAL",
            })).reset_index().sort_values("avg_ph")

        if "dissolved oxygen" in k:
            return df.groupby(["state_name","district_name"]).apply(lambda g: pd.Series({
                "avg_do":round(g["dissolved_oxygen_mg_l"].mean(),2),
                "min_do":round(g["dissolved_oxygen_mg_l"].min(),2),
                "max_do":round(g["dissolved_oxygen_mg_l"].max(),2),
                "water_quality_class":"EXCELLENT" if g["dissolved_oxygen_mg_l"].mean()>7 else
                                      "GOOD" if g["dissolved_oxygen_mg_l"].mean()>5 else
                                      "FAIR" if g["dissolved_oxygen_mg_l"].mean()>3 else "POOR",
                "ecological_risk":"FISH_KILL_RISK" if g["dissolved_oxygen_mg_l"].min()<3 else
                                  "STRESS_RISK" if g["dissolved_oxygen_mg_l"].min()<5 else "SAFE",
            })).reset_index().sort_values("avg_do",ascending=False)

        if "turbidity" in k:
            df["turbidity_level"] = pd.cut(df["turbidity_ntu"],bins=[0,5,10,20,1000],
                labels=["CLEAR","SLIGHTLY_TURBID","TURBID","HIGHLY_TURBID"]).astype(str)
            df["treatment_need"]  = np.where(df["turbidity_ntu"]>20,"TREATMENT_REQUIRED",
                                    np.where(df["turbidity_ntu"]>10,"MONITORING_REQUIRED","ACCEPTABLE"))
            return df[["station_name","district_name","state_name","turbidity_ntu",
                        "turbidity_level","treatment_need"]].sort_values("turbidity_ntu",ascending=False).head(30)

        if "quality index" in k or "comprehensive" in k:
            df["ph_score"]   = np.where(df["ph_level"].between(6.5,8.5),100,np.where(df["ph_level"].between(6.0,9.0),70,40))
            df["do_score"]   = np.where(df["dissolved_oxygen_mg_l"]>7,100,np.where(df["dissolved_oxygen_mg_l"]>5,70,np.where(df["dissolved_oxygen_mg_l"]>3,40,10)))
            df["turb_score"] = np.where(df["turbidity_ntu"]<5,100,np.where(df["turbidity_ntu"]<10,70,np.where(df["turbidity_ntu"]<20,40,10)))
            df["water_quality_index"] = (df["ph_score"]*0.3+df["do_score"]*0.4+df["turb_score"]*0.3).round(2)
            df["overall_rating"] = np.where(df["water_quality_index"]>=80,"EXCELLENT",
                                   np.where(df["water_quality_index"]>=55,"GOOD","POOR"))
            return df[["station_name","state_name","district_name","ph_level",
                        "dissolved_oxygen_mg_l","turbidity_ntu","water_quality_index","overall_rating"]].sort_values("water_quality_index",ascending=False)

        if "pollut" in k:
            p = df[(df["ph_level"]<6.0)|(df["ph_level"]>9.0)|(df["dissolved_oxygen_mg_l"]<4)|(df["turbidity_ntu"]>25)].copy()
            p["primary_concern"] = np.where((p["ph_level"]<6.0)|(p["ph_level"]>9.0),"pH_VIOLATION",
                                   np.where(p["dissolved_oxygen_mg_l"]<4,"DO_CRITICAL","TURBIDITY_HIGH"))
            return p[["station_name","state_name","district_name","ph_level",
                       "dissolved_oxygen_mg_l","turbidity_ntu","primary_concern"]].sort_values("dissolved_oxygen_mg_l")

        if "status summary" in k:
            return df.groupby("status").apply(lambda g: pd.Series({
                "total_stations":len(g),
                "avg_ph":round(g["ph_level"].mean(),2),
                "avg_do":round(g["dissolved_oxygen_mg_l"].mean(),2),
                "avg_turbidity":round(g["turbidity_ntu"].mean(),2),
            })).reset_index().sort_values("total_stations",ascending=False)

        if "state-wise" in k or "statewise" in k:
            return df.groupby("state_name").apply(lambda g: pd.Series({
                "total_stations":len(g),
                "avg_ph":round(g["ph_level"].mean(),2),
                "avg_do":round(g["dissolved_oxygen_mg_l"].mean(),2),
                "avg_turbidity":round(g["turbidity_ntu"].mean(),2),
                "ph_compliance_pct":round(g["ph_level"].between(6.5,8.5).mean()*100,2),
            })).reset_index().sort_values("avg_do",ascending=False)

        if "contamination" in k:
            df["primary_concern"] = np.where((df["ph_level"]<6.0)|(df["ph_level"]>9.0),"pH_VIOLATION",
                                    np.where(df["dissolved_oxygen_mg_l"]<3,"DO_CRITICAL",
                                    np.where(df["turbidity_ntu"]>25,"TURBIDITY_HIGH","WITHIN_LIMITS")))
            df["contamination_risk"] = np.where(
                ((df["ph_level"]<6.0)|(df["ph_level"]>9.0))&(df["dissolved_oxygen_mg_l"]<3),"HIGH_RISK",
                np.where((~df["ph_level"].between(6.5,8.5))|(df["dissolved_oxygen_mg_l"]<5)|(df["turbidity_ntu"]>20),"MODERATE_RISK","LOW_RISK"))
            return df[["station_name","state_name","district_name","ph_level",
                        "dissolved_oxygen_mg_l","turbidity_ntu","primary_concern","contamination_risk"]].sort_values("contamination_risk")

        if "critical" in k and "alert" in k:
            c = df[(df["dissolved_oxygen_mg_l"]<5)|(~df["ph_level"].between(6.0,9.0))|(df["turbidity_ntu"]>20)].copy()
            c["alert_level"] = np.where(
                (c["dissolved_oxygen_mg_l"]<3)|(~c["ph_level"].between(5.5,10))|(c["turbidity_ntu"]>30),"IMMEDIATE_ACTION",
                np.where((c["dissolved_oxygen_mg_l"]<5)|(~c["ph_level"].between(6.0,9.0))|(c["turbidity_ntu"]>20),"URGENT_MONITORING","ROUTINE_CHECK"))
            return c[["station_name","state_name","district_name","ph_level",
                       "dissolved_oxygen_mg_l","turbidity_ntu","status","alert_level"]].head(25)

        if "correlation" in k:
            return df.groupby("state_name").apply(lambda g: pd.Series({
                "ph_do_correlation":round(g["ph_level"].corr(g["dissolved_oxygen_mg_l"]),3),
                "ph_turbidity_correlation":round(g["ph_level"].corr(g["turbidity_ntu"]),3),
                "do_turbidity_correlation":round(g["dissolved_oxygen_mg_l"].corr(g["turbidity_ntu"]),3),
                "sample_size":len(g),
                "mean_ph":round(g["ph_level"].mean(),2),
                "mean_do":round(g["dissolved_oxygen_mg_l"].mean(),2),
            })).reset_index().sort_values("ph_do_correlation",ascending=False)

        if "ecological" in k:
            df["eco_risk_index"] = (
                np.where(df["dissolved_oxygen_mg_l"]<3,100,np.where(df["dissolved_oxygen_mg_l"]<5,60,10))*0.5 +
                np.where(~df["ph_level"].between(6.5,8.5),80,10)*0.3 +
                np.where(df["turbidity_ntu"]>20,80,np.where(df["turbidity_ntu"]>10,40,10))*0.2
            ).round(2)
            df["risk_category"] = np.where(df["eco_risk_index"]>70,"EXTREME_RISK",
                                  np.where(df["eco_risk_index"]>45,"HIGH_RISK",
                                  np.where(df["eco_risk_index"]>25,"MODERATE_RISK","LOW_RISK")))
            return df[["station_name","state_name","district_name","dissolved_oxygen_mg_l",
                        "ph_level","turbidity_ntu","eco_risk_index","risk_category"]].sort_values("eco_risk_index",ascending=False).head(30)

        return df.head(50)

    return df.head(50)

# ─────────────────────────────────────────────────────────────────────────────
# ═══════════════════════ SQL QUERY DICTIONARIES ══════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

# ── 15 RAINFALL QUERIES ───────────────────────────────────────────────────────
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
            ROUND(AVG(rainfall_cm), 2)    as avg_rainfall,
            ROUND(MIN(rainfall_cm), 2)    as min_rainfall,
            ROUND(MAX(rainfall_cm), 2)    as max_rainfall,
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
            SELECT district_name,
                   AVG(rainfall_cm)    as mean_rainfall,
                   STDDEV(rainfall_cm) as std_rainfall
            FROM rainfall_history
            GROUP BY district_name
        )
        SELECT
            rh.district_name, rh.record_year, rh.season, rh.rainfall_cm,
            ROUND(s.mean_rainfall, 2) as avg_rainfall,
            CASE
                WHEN rh.rainfall_cm > s.mean_rainfall + 2*s.std_rainfall THEN 'Extreme_High_Anomaly'
                WHEN rh.rainfall_cm < s.mean_rainfall - 2*s.std_rainfall THEN 'Extreme_Low_Anomaly'
                WHEN rh.rainfall_cm > s.mean_rainfall + s.std_rainfall   THEN 'High_Anomaly'
                WHEN rh.rainfall_cm < s.mean_rainfall - s.std_rainfall   THEN 'Low_Anomaly'
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
            AVG(rainfall_cm) OVER (PARTITION BY district_name ORDER BY record_year)         as moving_avg_3yr,
            LAG(rainfall_cm,1) OVER (PARTITION BY district_name ORDER BY record_year)       as previous_year_rainfall,
            ROUND((rainfall_cm - LAG(rainfall_cm,1) OVER (PARTITION BY district_name ORDER BY record_year))*100.0 /
                  NULLIF(LAG(rainfall_cm,1) OVER (PARTITION BY district_name ORDER BY record_year),0), 2) as yoy_change_pct
        FROM rainfall_history
        WHERE rainfall_cm IS NOT NULL
        ORDER BY district_name, record_year DESC
    """,
    "8. Monsoon Performance Analysis": """
        SELECT
            district_name, record_year,
            SUM(CASE WHEN season = 'Monsoon' THEN rainfall_cm ELSE 0 END) as monsoon_rainfall,
            SUM(CASE WHEN season != 'Monsoon' THEN rainfall_cm ELSE 0 END) as non_monsoon_rainfall,
            ROUND(SUM(CASE WHEN season = 'Monsoon' THEN rainfall_cm ELSE 0 END)*100.0 /
                  NULLIF(SUM(rainfall_cm),0), 2) as monsoon_contribution_pct
        FROM rainfall_history
        GROUP BY district_name, record_year
        ORDER BY monsoon_rainfall DESC
    """,
    "9. Rainfall Frequency Distribution": """
        SELECT
            CASE
                WHEN rainfall_cm BETWEEN   0 AND  50 THEN '0-50 cm (Low)'
                WHEN rainfall_cm BETWEEN  51 AND 150 THEN '51-150 cm (Moderate)'
                WHEN rainfall_cm BETWEEN 151 AND 300 THEN '151-300 cm (High)'
                ELSE '300+ cm (Extreme)'
            END as rainfall_range,
            COUNT(*) as frequency,
            ROUND(COUNT(*)*100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
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
            ROUND((rainfall_cm - LAG(rainfall_cm,1) OVER (PARTITION BY district_name ORDER BY record_year))*100.0 /
                  NULLIF(LAG(rainfall_cm,1) OVER (PARTITION BY district_name ORDER BY record_year),0), 2) as percent_change
        FROM (
            SELECT district_name, record_year, AVG(rainfall_cm) as rainfall_cm
            FROM rainfall_history
            GROUP BY district_name, record_year
        ) yearly_avg
        ORDER BY district_name, record_year DESC
    """,
    "11. Drought Risk Assessment": """
        SELECT
            district_name, record_year,
            AVG(rainfall_cm) as avg_rainfall,
            CASE
                WHEN AVG(rainfall_cm) <  50 THEN 'EXTREME_DROUGHT_RISK'
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
    "12. Peak Rainfall Month Analysis": """
        SELECT
            district_name,
            EXTRACT(MONTH FROM record_date) as month,
            AVG(rainfall_cm)  as avg_rainfall,
            MAX(rainfall_cm)  as peak_rainfall,
            RANK() OVER (PARTITION BY district_name ORDER BY AVG(rainfall_cm) DESC) as peak_month_rank
        FROM rainfall_history
        WHERE record_date IS NOT NULL
        GROUP BY district_name, EXTRACT(MONTH FROM record_date)
        ORDER BY district_name, avg_rainfall DESC
    """,
    "13. Rainfall Trend Prediction": """
        WITH ranked AS (
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
            ROUND((avg_rainfall - LAG(avg_rainfall,1) OVER (PARTITION BY district_name ORDER BY record_year))*100.0 /
                  NULLIF(LAG(avg_rainfall,1) OVER (PARTITION BY district_name ORDER BY record_year),0), 2) as yoy_trend_pct
        FROM ranked
        WHERE total_years >= 3
        ORDER BY district_name, record_year DESC
    """,
    "14. Rainfall Variability Index": """
        SELECT
            district_name,
            COUNT(*) as num_records,
            ROUND(AVG(rainfall_cm),    2) as mean_rainfall,
            ROUND(STDDEV(rainfall_cm), 2) as std_deviation,
            ROUND(STDDEV(rainfall_cm)*100.0 / NULLIF(AVG(rainfall_cm),0), 2) as coefficient_of_variation,
            CASE
                WHEN STDDEV(rainfall_cm)*100.0/AVG(rainfall_cm) > 50 THEN 'HIGHLY_VARIABLE'
                WHEN STDDEV(rainfall_cm)*100.0/AVG(rainfall_cm) > 30 THEN 'MODERATELY_VARIABLE'
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
            district_name, record_year,
            AVG(rainfall_cm) as actual_rainfall,
            (AVG(rainfall_cm) - MIN(rainfall_cm) OVER (PARTITION BY district_name))*100.0 /
            NULLIF(MAX(rainfall_cm) OVER (PARTITION BY district_name) -
                   MIN(rainfall_cm) OVER (PARTITION BY district_name), 0) as efficiency_score,
            CASE
                WHEN (AVG(rainfall_cm)-MIN(rainfall_cm) OVER (PARTITION BY district_name))*100.0 /
                     NULLIF(MAX(rainfall_cm) OVER (PARTITION BY district_name)-MIN(rainfall_cm) OVER (PARTITION BY district_name),0) > 75 THEN 'EXCELLENT'
                WHEN (AVG(rainfall_cm)-MIN(rainfall_cm) OVER (PARTITION BY district_name))*100.0 /
                     NULLIF(MAX(rainfall_cm) OVER (PARTITION BY district_name)-MIN(rainfall_cm) OVER (PARTITION BY district_name),0) > 50 THEN 'GOOD'
                ELSE 'NEEDS_IMPROVEMENT'
            END as efficiency_rating
        FROM rainfall_history
        GROUP BY district_name, record_year
        ORDER BY efficiency_score DESC
    """,
}

# ── 12 GROUNDWATER QUERIES ────────────────────────────────────────────────────
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
            ROUND(AVG(avg_depth_meters),    2) as avg_depth,
            ROUND(STDDEV(avg_depth_meters), 2) as depth_variability,
            ROUND((MAX(avg_depth_meters)-MIN(avg_depth_meters)) /
                  NULLIF(MAX(assessment_year)-MIN(assessment_year),0), 2) as annual_depletion_rate,
            CASE
                WHEN (MAX(avg_depth_meters)-MIN(avg_depth_meters))/(MAX(assessment_year)-MIN(assessment_year)) > 2 THEN 'CRITICAL_DEPLETION'
                WHEN (MAX(avg_depth_meters)-MIN(avg_depth_meters))/(MAX(assessment_year)-MIN(assessment_year)) > 1 THEN 'MODERATE_DEPLETION'
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
            district_name, assessment_year,
            ROUND(extraction_pct,    2) as extraction_pct,
            ROUND(recharge_rate_mcm, 2) as recharge_rate,
            ROUND(extraction_pct - recharge_rate_mcm, 2) as deficit_surplus,
            CASE
                WHEN extraction_pct > recharge_rate_mcm*1.5  THEN 'OVER_EXTRACTION_CRITICAL'
                WHEN extraction_pct > recharge_rate_mcm       THEN 'OVER_EXTRACTION_MODERATE'
                WHEN extraction_pct < recharge_rate_mcm*0.5  THEN 'UNDER_UTILIZATION'
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
                district_name, assessment_year, avg_depth_meters,
                AVG(avg_depth_meters) OVER (PARTITION BY district_name ORDER BY assessment_year
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg_3yr,
                LAG(avg_depth_meters,1) OVER (PARTITION BY district_name ORDER BY assessment_year) as prev_year_depth
            FROM groundwater_levels
            WHERE avg_depth_meters IS NOT NULL
        )
        SELECT
            district_name, assessment_year, avg_depth_meters,
            ROUND(moving_avg_3yr, 2) as moving_avg_3yr,
            ROUND(avg_depth_meters - moving_avg_3yr, 2) as deviation_from_trend,
            ROUND((avg_depth_meters - prev_year_depth)*100.0 / NULLIF(prev_year_depth,0), 2) as yoy_change_pct,
            CASE
                WHEN avg_depth_meters > moving_avg_3yr*1.10 THEN 'DEEPENING_FAST'
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
            ROUND(AVG(avg_depth_meters),  2) as avg_depth,
            ROUND(AVG(extraction_pct),    2) as avg_extraction,
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
        GROUP BY district_name
        ORDER BY risk_rank
    """,
    "6. Sustainable Yield Analysis": """
        SELECT
            district_name, assessment_year, recharge_rate_mcm, extraction_pct,
            ROUND(recharge_rate_mcm*(1 - extraction_pct/100.0), 2) as sustainable_yield,
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
            SUM(CASE WHEN avg_depth_meters <  20 THEN 1 ELSE 0 END) as shallow_wells,
            SUM(CASE WHEN avg_depth_meters BETWEEN 20 AND 40 THEN 1 ELSE 0 END) as medium_wells,
            SUM(CASE WHEN avg_depth_meters >  40 THEN 1 ELSE 0 END) as deep_wells,
            ROUND(SUM(CASE WHEN avg_depth_meters>40 THEN 1 ELSE 0 END)*100.0/COUNT(*), 2) as deep_well_percentage
        FROM groundwater_levels
        GROUP BY district_name
        ORDER BY deep_well_percentage DESC
    """,
    "8. Groundwater Health Index": """
        SELECT
            district_name, assessment_year, avg_depth_meters, extraction_pct,
            (CASE WHEN avg_depth_meters<20 THEN 100 WHEN avg_depth_meters<35 THEN 70
                  WHEN avg_depth_meters<50 THEN 40  ELSE 10 END * 0.6 +
             CASE WHEN extraction_pct<30   THEN 100 WHEN extraction_pct<50   THEN 70
                  WHEN extraction_pct<70   THEN 40  ELSE 10 END * 0.4) as groundwater_health_score,
            CASE
                WHEN (CASE WHEN avg_depth_meters<20 THEN 100 ELSE 0 END +
                      CASE WHEN extraction_pct<30   THEN 100 ELSE 0 END) >= 150 THEN 'EXCELLENT'
                WHEN (CASE WHEN avg_depth_meters<35 THEN 100 ELSE 0 END +
                      CASE WHEN extraction_pct<50   THEN 100 ELSE 0 END) >= 150 THEN 'GOOD'
                ELSE 'POOR'
            END as overall_health_status
        FROM groundwater_levels
        ORDER BY groundwater_health_score DESC
    """,
    "9. Year-over-Year Depth Change": """
        SELECT
            c.district_name,
            c.assessment_year                                                           as current_year,
            c.avg_depth_meters                                                          as current_depth,
            p.avg_depth_meters                                                          as previous_depth,
            ROUND(c.avg_depth_meters - p.avg_depth_meters, 2)                         as depth_change,
            ROUND((c.avg_depth_meters - p.avg_depth_meters)*100.0 /
                  NULLIF(p.avg_depth_meters,0), 2)                                     as percent_change
        FROM groundwater_levels c
        LEFT JOIN groundwater_levels p
               ON c.district_name    = p.district_name
              AND p.assessment_year  = c.assessment_year - 1
        WHERE c.avg_depth_meters IS NOT NULL
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
                WHEN extraction_pct > recharge_rate_mcm      THEN 'RECHARGE_REQUIRED'
                WHEN extraction_pct > recharge_rate_mcm*0.8  THEN 'RECHARGE_RECOMMENDED'
                ELSE 'ADEQUATE_RECHARGE'
            END as recharge_necessity
        FROM groundwater_levels
        WHERE recharge_rate_mcm IS NOT NULL
        ORDER BY (extraction_pct - recharge_rate_mcm) DESC
    """,
    "12. Groundwater Stress Index": """
        SELECT
            district_name, assessment_year,
            ROUND(avg_depth_meters, 2) as depth,
            ROUND(extraction_pct,   2) as extraction,
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

# ── 12 WATER QUALITY QUERIES ──────────────────────────────────────────────────
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
            ROUND(AVG(ph_level),    2) as avg_ph,
            ROUND(MIN(ph_level),    2) as min_ph,
            ROUND(MAX(ph_level),    2) as max_ph,
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
                WHEN turbidity_ntu <  5 THEN 'CLEAR'
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
            station_name, state_name, district_name,
            ph_level, dissolved_oxygen_mg_l, turbidity_ntu,
            ROUND(
                (CASE WHEN ph_level BETWEEN 6.5 AND 8.5 THEN 100
                      WHEN ph_level BETWEEN 6.0 AND 9.0 THEN 70 ELSE 40 END * 0.3 +
                 CASE WHEN dissolved_oxygen_mg_l > 7 THEN 100
                      WHEN dissolved_oxygen_mg_l > 5 THEN 70
                      WHEN dissolved_oxygen_mg_l > 3 THEN 40 ELSE 10 END * 0.4 +
                 CASE WHEN turbidity_ntu < 5  THEN 100
                      WHEN turbidity_ntu < 10 THEN 70
                      WHEN turbidity_ntu < 20 THEN 40 ELSE 10 END * 0.3), 2) as water_quality_index,
            CASE
                WHEN (CASE WHEN ph_level BETWEEN 6.5 AND 8.5 THEN 100 ELSE 0 END +
                      CASE WHEN dissolved_oxygen_mg_l > 5   THEN 100 ELSE 0 END +
                      CASE WHEN turbidity_ntu < 10          THEN 100 ELSE 0 END) >= 250 THEN 'EXCELLENT'
                WHEN (CASE WHEN ph_level BETWEEN 6.0 AND 9.0 THEN 100 ELSE 0 END +
                      CASE WHEN dissolved_oxygen_mg_l > 3   THEN 100 ELSE 0 END +
                      CASE WHEN turbidity_ntu < 20          THEN 100 ELSE 0 END) >= 200 THEN 'GOOD'
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
                WHEN dissolved_oxygen_mg_l < 4         THEN 'DO_CRITICAL'
                WHEN turbidity_ntu > 25                THEN 'TURBIDITY_HIGH'
                ELSE 'WITHIN_LIMITS'
            END as primary_concern
        FROM water_monitoring_stations
        WHERE ph_level < 6.0 OR ph_level > 9.0
           OR dissolved_oxygen_mg_l < 4
           OR turbidity_ntu > 25
        ORDER BY dissolved_oxygen_mg_l
    """,
    "7. Station Status Summary": """
        SELECT
            status,
            COUNT(*) as total_stations,
            ROUND(AVG(ph_level),             2) as avg_ph,
            ROUND(AVG(dissolved_oxygen_mg_l),2) as avg_do,
            ROUND(AVG(turbidity_ntu),        2) as avg_turbidity
        FROM water_monitoring_stations
        WHERE status IS NOT NULL
        GROUP BY status
        ORDER BY total_stations DESC
    """,
    "8. State-wise Water Quality Summary": """
        SELECT
            state_name,
            COUNT(*) as total_stations,
            ROUND(AVG(ph_level),             2) as avg_ph,
            ROUND(AVG(dissolved_oxygen_mg_l),2) as avg_do,
            ROUND(AVG(turbidity_ntu),        2) as avg_turbidity,
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
                WHEN ph_level < 6.0 OR ph_level > 9.0 THEN 'pH_VIOLATION'
                WHEN dissolved_oxygen_mg_l < 3         THEN 'DO_CRITICAL'
                WHEN turbidity_ntu > 25                THEN 'TURBIDITY_HIGH'
                ELSE 'WITHIN_LIMITS'
            END as primary_concern,
            CASE
                WHEN (ph_level<6.0 OR ph_level>9.0) AND dissolved_oxygen_mg_l<3 THEN 'HIGH_RISK'
                WHEN NOT ph_level BETWEEN 6.5 AND 8.5 OR dissolved_oxygen_mg_l<5 OR turbidity_ntu>20 THEN 'MODERATE_RISK'
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
        WHERE dissolved_oxygen_mg_l<5
           OR NOT ph_level BETWEEN 6.0 AND 9.0
           OR turbidity_ntu>20
        ORDER BY dissolved_oxygen_mg_l, ph_level
        LIMIT 25
    """,
    "11. Multi-Parameter Correlation": """
        SELECT
            state_name,
            ROUND(CORR(ph_level, dissolved_oxygen_mg_l)::numeric, 3) as ph_do_correlation,
            ROUND(CORR(ph_level, turbidity_ntu)::numeric,          3) as ph_turbidity_correlation,
            ROUND(CORR(dissolved_oxygen_mg_l, turbidity_ntu)::numeric, 3) as do_turbidity_correlation,
            COUNT(*) as sample_size,
            ROUND(AVG(ph_level)::numeric,             2) as mean_ph,
            ROUND(AVG(dissolved_oxygen_mg_l)::numeric,2) as mean_do,
            ROUND(AVG(turbidity_ntu)::numeric,        2) as mean_turbidity
        FROM water_monitoring_stations
        WHERE ph_level IS NOT NULL AND dissolved_oxygen_mg_l IS NOT NULL AND turbidity_ntu IS NOT NULL
        GROUP BY state_name
        HAVING COUNT(*) > 5
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
                WHEN dissolved_oxygen_mg_l<3 OR NOT ph_level BETWEEN 5.5 AND 9.5 THEN 'EXTREME_RISK'
                WHEN dissolved_oxygen_mg_l<5 OR NOT ph_level BETWEEN 6.0 AND 9.0 OR turbidity_ntu>20 THEN 'HIGH_RISK'
                WHEN dissolved_oxygen_mg_l<6 OR NOT ph_level BETWEEN 6.5 AND 8.5 OR turbidity_ntu>10 THEN 'MODERATE_RISK'
                ELSE 'LOW_RISK'
            END as risk_category
        FROM water_monitoring_stations
        ORDER BY ecological_risk_index DESC
        LIMIT 30
    """,
}

# ─────────────────────────────────────────────────────────────────────────────
# FILTER BUILDERS
# ─────────────────────────────────────────────────────────────────────────────
def build_rainfall_filters(district, min_r, max_r, year, season):
    d = f"AND district_name = '{district}'" if district != "All" else ""
    r = f"AND rainfall_cm BETWEEN {min_r} AND {max_r}"
    y = f"AND record_year = {year}"         if year    != 0     else ""
    s = f"AND season = '{season}'"          if season  != "All" else ""
    return d, r, y, s

def build_groundwater_filters(district, min_d, max_d, year):
    d = f"AND district_name = '{district}'"       if district != "All" else ""
    r = f"AND avg_depth_meters BETWEEN {min_d} AND {max_d}"
    y = f"AND assessment_year = {year}"           if year     != 0     else ""
    return d, r, y

def build_wq_filters(state, district, min_ph, max_ph, status):
    sf = f"AND state_name    = '{state}'"    if state    != "All" else ""
    df = f"AND district_name = '{district}'" if district != "All" else ""
    ph = f"AND ph_level BETWEEN {min_ph} AND {max_ph}"
    st = f"AND status = '{status}'"          if status   != "All" else ""
    return sf, df, ph, st

# ─────────────────────────────────────────────────────────────────────────────
# AUTO CHART ENGINE
# ─────────────────────────────────────────────────────────────────────────────
_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(255,255,255,0.02)",
    font=dict(family="sans-serif", color="#cfe4f7", size=12),
    margin=dict(l=40,r=20,t=40,b=40),
    legend=dict(bgcolor="rgba(0,0,0,0.3)", bordercolor="#1a3550", borderwidth=1),
    xaxis=dict(gridcolor="#1a3550", zerolinecolor="#1a3550"),
    yaxis=dict(gridcolor="#1a3550", zerolinecolor="#1a3550"),
)

def _fig(f): f.update_layout(**_LAYOUT); return f

def auto_chart(df: pd.DataFrame, qname: str):
    if df is None or df.empty: return None
    q    = qname.lower()
    cols = df.columns.tolist()
    num  = df.select_dtypes(include=[np.number]).columns.tolist()
    cat  = df.select_dtypes(exclude=[np.number]).columns.tolist()

    # ── Rainfall
    if "yearly" in q and "avg_rainfall" in cols:
        f = go.Figure()
        f.add_trace(go.Scatter(x=df["record_year"],y=df["avg_rainfall"],mode="lines+markers",
            name="Avg",line=dict(color="#00b4d8",width=2.5),marker=dict(size=7)))
        if "max_rainfall" in cols:
            f.add_trace(go.Scatter(x=df["record_year"],y=df["max_rainfall"],mode="lines",
                name="Max",line=dict(color="#e74c3c",dash="dash",width=1.5)))
        f.update_layout(title="Yearly Rainfall Trend",xaxis_title="Year",yaxis_title="cm",**_LAYOUT); return f

    if "seasonal" in q and "season" in cols and "avg_rainfall" in cols:
        return _fig(px.bar(df,x="season",y="avg_rainfall",color="avg_rainfall",
                           color_continuous_scale="Blues",title="Seasonal Rainfall Comparison"))

    if "district" in q and "comparison" in q and "avg_rainfall" in cols:
        return _fig(px.bar(df.head(12),x="district_name",y="avg_rainfall",color="avg_rainfall",
                           color_continuous_scale="Plasma",title="District-wise Avg Rainfall"))

    if "extreme" in q and "rainfall_cm" in cols and "severity_level" in cols:
        return _fig(px.scatter(df,x="record_year",y="rainfall_cm",color="severity_level",
                               size="rainfall_cm",hover_data=["district_name","season"],
                               title="Extreme Rainfall Events"))

    if "anomaly" in q and "anomaly_type" in cols:
        return _fig(px.scatter(df,x="record_year",y="rainfall_cm",color="anomaly_type",
                               hover_data=["district_name","season"],title="Rainfall Anomaly Detection"))

    if "monsoon" in q and "monsoon_rainfall" in cols:
        g = df.groupby("district_name")[["monsoon_rainfall","non_monsoon_rainfall"]].mean().reset_index()
        f = go.Figure()
        f.add_trace(go.Bar(x=g["district_name"],y=g["monsoon_rainfall"],name="Monsoon",marker_color="#00b4d8"))
        f.add_trace(go.Bar(x=g["district_name"],y=g["non_monsoon_rainfall"],name="Non-Monsoon",marker_color="#e67e22"))
        f.update_layout(barmode="stack",title="Monsoon vs Non-Monsoon",**_LAYOUT); return f

    if "frequency" in q and "frequency" in cols:
        return _fig(px.pie(df,values="frequency",names="rainfall_range",hole=0.4,
                           color_discrete_sequence=px.colors.sequential.Plasma,title="Rainfall Frequency Distribution"))

    if "drought" in q and "drought_risk_level" in cols:
        return _fig(px.bar(df,x="district_name",y="avg_rainfall",color="drought_risk_level",
                           color_discrete_map={"EXTREME_DROUGHT_RISK":"#c0392b","SEVERE_DROUGHT_RISK":"#e67e22","MODERATE_DROUGHT_RISK":"#f1c40f"},
                           title="Drought Risk by District"))

    if "variability" in q and "coefficient_of_variation" in cols:
        return _fig(px.bar(df,x="district_name",y="coefficient_of_variation",color="variability_category",
                           title="Rainfall Variability Index"))

    if ("yoy" in q or "year-over-year" in q) and "percent_change" in cols:
        return _fig(px.bar(df.dropna(subset=["percent_change"]).head(30),
                           x="district_name",y="percent_change",color="percent_change",
                           color_continuous_scale="RdYlGn",title="Year-over-Year Rainfall Change (%)"))

    if "efficiency" in q and "efficiency_score" in cols:
        return _fig(px.bar(df.head(20),x="district_name",y="efficiency_score",color="efficiency_rating",
                           color_discrete_map={"EXCELLENT":"#2ecc71","GOOD":"#3498db","NEEDS_IMPROVEMENT":"#e74c3c"},
                           title="Rainfall Efficiency Score"))

    # ── Groundwater
    if "depletion" in q and "annual_depletion_rate" in cols:
        return _fig(px.bar(df,x="district_name",y="annual_depletion_rate",color="depletion_status",
                           color_discrete_map={"CRITICAL_DEPLETION":"#e74c3c","MODERATE_DEPLETION":"#e67e22","STABLE":"#2ecc71"},
                           title="Annual Groundwater Depletion Rate"))

    if ("extraction" in q and "recharge" in q) or "deficit_surplus" in cols:
        return _fig(px.scatter(df,x="recharge_rate" if "recharge_rate" in cols else "recharge_rate_mcm",
                               y="extraction_pct",color="extraction_status",
                               hover_data=[c for c in ["district_name","assessment_year"] if c in cols],
                               title="Extraction vs Recharge Analysis"))

    if "trend" in q and "trend_direction" in cols and "avg_depth_meters" in cols:
        return _fig(px.line(df.head(60),x="assessment_year",y="avg_depth_meters",
                            color="district_name",title="Groundwater Depth Trend"))

    if "risk" in q and "risk_category" in cols and "avg_depth" in cols:
        f = px.scatter(df,x="avg_extraction",y="avg_depth",color="risk_category",
                       color_discrete_map={"CRITICAL_RISK":"#e74c3c","HIGH_RISK":"#e67e22",
                                           "MODERATE_RISK":"#f1c40f","LOW_RISK":"#2ecc71"},
                       text="district_name",title="Groundwater Risk Zone Scatter")
        f.update_traces(textposition="top center")
        return _fig(f)

    if "sustainable" in q and "sustainable_yield" in cols:
        return _fig(px.bar(df.head(15),x="district_name",y="sustainable_yield",color="sustainability_status",
                           color_discrete_map={"UNSUSTAINABLE":"#c0392b","STRESSED":"#e67e22",
                                               "MODERATE":"#f1c40f","SUSTAINABLE":"#27ae60"},
                           title="Sustainable Yield by District"))

    if "stress" in q and "stress_index" in cols:
        return _fig(px.bar(df.head(20),x="district_name",y="stress_index",color="stress_level",
                           color_discrete_map={"EXTREME_STRESS":"#c0392b","HIGH_STRESS":"#e67e22",
                                               "MODERATE_STRESS":"#f1c40f","LOW_STRESS":"#27ae60"},
                           title="Groundwater Stress Index"))

    if "health" in q and "groundwater_health_score" in cols:
        return _fig(px.bar(df.head(20),x="district_name",y="groundwater_health_score",
                           color="overall_health_status",
                           color_discrete_map={"EXCELLENT":"#2ecc71","GOOD":"#3498db","POOR":"#e74c3c"},
                           title="Groundwater Health Score"))

    # ── Water Quality
    if "ph" in q and "avg_ph" in cols:
        x_col = "district_name" if "district_name" in cols else "state_name"
        f = _fig(px.bar(df,x=x_col,y="avg_ph",color="ph_status",
                        color_discrete_map={"IDEAL":"#2ecc71","ACCEPTABLE":"#f1c40f","CRITICAL":"#e74c3c"},
                        title="pH Level Analysis"))
        f.add_hline(y=6.5,line_dash="dash",line_color="#2ecc71",annotation_text="Min Safe")
        f.add_hline(y=8.5,line_dash="dash",line_color="#2ecc71",annotation_text="Max Safe")
        return f

    if "dissolved oxygen" in q and "avg_do" in cols:
        x_col = "district_name" if "district_name" in cols else "state_name"
        f = _fig(px.bar(df,x=x_col,y="avg_do",color="water_quality_class",
                        color_discrete_map={"EXCELLENT":"#2ecc71","GOOD":"#3498db","FAIR":"#f1c40f","POOR":"#e74c3c"},
                        title="Dissolved Oxygen Analysis"))
        f.add_hline(y=5,line_dash="dot",line_color="#f1c40f",annotation_text="Min Acceptable (5 mg/L)")
        return f

    if "turbidity" in q and "turbidity_ntu" in cols and "turbidity_level" in cols:
        return _fig(px.bar(df.head(20),x="station_name",y="turbidity_ntu",color="turbidity_level",
                           color_discrete_map={"CLEAR":"#2ecc71","SLIGHTLY_TURBID":"#3498db",
                                               "TURBID":"#f1c40f","HIGHLY_TURBID":"#e74c3c"},
                           title="Turbidity by Station"))

    if ("quality index" in q or "comprehensive" in q) and "water_quality_index" in cols:
        return _fig(px.bar(df.head(20),x="station_name",y="water_quality_index",color="overall_rating",
                           color_discrete_map={"EXCELLENT":"#2ecc71","GOOD":"#3498db","POOR":"#e74c3c"},
                           title="Water Quality Index by Station"))

    if "alert" in q and "alert_level" in cols:
        return _fig(px.scatter(df,x="ph_level",y="dissolved_oxygen_mg_l",color="alert_level",
                               color_discrete_map={"IMMEDIATE_ACTION":"#e74c3c","URGENT_MONITORING":"#e67e22","ROUTINE_CHECK":"#2ecc71"},
                               size="turbidity_ntu",hover_data=["station_name","state_name"],
                               title="Critical Stations — pH vs DO"))

    if "contamination" in q and "contamination_risk" in cols:
        rc = df["contamination_risk"].value_counts().reset_index()
        rc.columns=["risk","count"]
        return _fig(px.pie(rc,values="count",names="risk",hole=0.45,
                           color="risk",color_discrete_map={"HIGH_RISK":"#e74c3c","MODERATE_RISK":"#e67e22","LOW_RISK":"#2ecc71"},
                           title="Contamination Risk Distribution"))

    if "state-wise" in q and "avg_do" in cols:
        f = make_subplots(rows=1,cols=3,subplot_titles=("Avg pH","Avg DO","Avg Turbidity"))
        f.add_trace(go.Bar(x=df["state_name"],y=df["avg_ph"],      name="pH",       marker_color="#3498db"),row=1,col=1)
        f.add_trace(go.Bar(x=df["state_name"],y=df["avg_do"],      name="DO",        marker_color="#2ecc71"),row=1,col=2)
        f.add_trace(go.Bar(x=df["state_name"],y=df["avg_turbidity"],name="Turbidity",marker_color="#e67e22"),row=1,col=3)
        f.update_layout(title="State-wise Water Quality Metrics",**_LAYOUT); return f

    if "correlation" in q and "ph_do_correlation" in cols:
        f = go.Figure(go.Heatmap(
            z=df[["ph_do_correlation","ph_turbidity_correlation","do_turbidity_correlation"]].values,
            x=["pH-DO","pH-Turbidity","DO-Turbidity"], y=df["state_name"].tolist(),
            colorscale="RdYlGn",zmid=0))
        f.update_layout(title="Parameter Correlation Heatmap",**_LAYOUT); return f

    # Fallback
    if num and cat:
        return _fig(px.bar(df.head(20),x=cat[0],y=num[0],color=num[0],
                           color_continuous_scale="Plasma",title=f"{num[0]} by {cat[0]}"))
    return None

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def kpi(label, value, sub=""):
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-sub">{sub}</div>
    </div>""", unsafe_allow_html=True)

def run_section(category, queries_dict, base_df_fn, filters_ui_fn, filters_build_fn, filter_keys):
    """Generic section renderer shared by Rainfall / Groundwater / Water Quality."""
    with st.expander("🔧 Filters  (apply to Query 1 only)", expanded=False):
        ui_vals = filters_ui_fn()

    qname = st.selectbox("📋 Select Query", list(queries_dict.keys()), key=f"{category}_q")

    st.markdown(
        f"<div class='query-info-box'>Selected: <b>{qname}</b>"
        + (" &nbsp;·&nbsp; <i>Demo mode — synthetic data shown</i>" if DEMO_MODE else "") + "</div>",
        unsafe_allow_html=True)

    if st.button("▶  Run Query", key=f"{category}_run", type="primary"):
        with st.spinner("Executing…"):
            filter_strings = filters_build_fn(*ui_vals)
            fmt_kwargs     = dict(zip(filter_keys, filter_strings))
            raw_sql        = safe_format(queries_dict[qname], **fmt_kwargs)

            if DEMO_MODE:
                df_res = run_demo_query(qname.lower(), category, base_df_fn())
            else:
                df_res, err = execute_query(raw_sql)
                if err:
                    st.error(f"❌ Query error: {err}")
                    return

            if df_res is not None and not df_res.empty:
                st.success(f"✅ {len(df_res)} rows · {len(df_res.columns)} columns")
                t_data, t_chart, t_sql = st.tabs(["📊 Data", "📈 Chart", "📝 SQL"])

                with t_data:
                    num_cols = df_res.select_dtypes(include=[np.number]).columns.tolist()
                    st.dataframe(
                        df_res.style.background_gradient(subset=num_cols, cmap="Blues") if num_cols else df_res,
                        use_container_width=True)
                    buf = BytesIO(); df_res.to_csv(buf, index=False)
                    st.download_button("⬇ Download CSV", buf.getvalue(),
                                       f"{category}_{qname[:12].strip()}.csv", "text/csv")

                with t_chart:
                    fig = auto_chart(df_res, qname)
                    if fig: st.plotly_chart(fig, use_container_width=True)
                    else:   st.info("No chart template for this result — check the Data tab.")

                with t_sql:
                    st.code(raw_sql.strip(), language="sql")

            elif df_res is not None:
                st.warning("Query returned 0 rows. Adjust your filters.")
    else:
        st.info("Configure filters above, then click **▶ Run Query**.")

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR  (original style preserved)
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='text-align:center;padding:10px 0 6px'>
        <span style='font-size:2rem'>💧</span><br>
        <b style='color:#00e5ff;font-size:1.3rem;'>AQUASTAT</b><br>
        <span style='font-size:0.68rem;color:#4a7fa0;letter-spacing:0.1em'>
            NATIONAL WATER COMMAND CENTER</span>
    </div>""", unsafe_allow_html=True)

    if DEMO_MODE:
        st.markdown("<div class='demo-badge'>⚡ DEMO MODE — No DB connected</div>",
                    unsafe_allow_html=True)
    else:
        st.markdown("<div class='badge-good'>🟢 LIVE DATABASE</div>", unsafe_allow_html=True)

    st.markdown("---")
    section = st.radio(
        "Navigate",
        ["🏠  Overview", "🌧  Rainfall", "💧  Groundwater",
         "🔬  Water Quality", "🗺  Map View", "💻  SQL Explorer"],
        label_visibility="collapsed",
    )
    ist_time = datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%d %b %Y  %H:%M IST")
    st.markdown("---")
    st.caption(f"🕐 {ist_time}")
    st.caption("© 2025 AQUASTAT · National Water Authority")

# ─────────────────────────────────────────────────────────────────────────────
# ══════════════════════ PAGE ROUTING ═════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

# ── OVERVIEW ─────────────────────────────────────────────────────────────────
if section == "🏠  Overview":
    st.markdown("<div class='section-header'>🏠 National Water Overview</div>", unsafe_allow_html=True)

    df_r  = get_demo_rainfall()
    df_gw = get_demo_groundwater()
    df_wq = get_demo_water_quality()

    c1,c2,c3,c4 = st.columns(4)
    with c1: kpi("Avg National Rainfall",    f"{df_r['rainfall_cm'].mean():.1f} cm",       "All districts · all seasons")
    with c2: kpi("Avg Groundwater Depth",    f"{df_gw['avg_depth_meters'].mean():.1f} m",  "National average depth")
    with c3: kpi("Avg Extraction Rate",      f"{df_gw['extraction_pct'].mean():.1f}%",     "Of recharge capacity")
    with c4: kpi("Active Monitoring Stns",   str((df_wq["status"]=="Active").sum()),        "Water quality stations")

    st.markdown("---")
    l, r = st.columns(2)

    with l:
        st.subheader("🌧 District-wise Average Rainfall")
        dist_rain = df_r.groupby("district_name")["rainfall_cm"].mean().reset_index().sort_values("rainfall_cm",ascending=False)
        st.plotly_chart(_fig(px.bar(dist_rain,x="district_name",y="rainfall_cm",color="rainfall_cm",
            color_continuous_scale="Blues",title="Average Rainfall by District (cm)")), use_container_width=True)

    with r:
        st.subheader("💧 Extraction vs Recharge")
        gw = df_gw.groupby("district_name")[["extraction_pct","recharge_rate_mcm"]].mean().reset_index()
        f2 = go.Figure()
        f2.add_trace(go.Bar(x=gw["district_name"],y=gw["extraction_pct"],     name="Extraction %", marker_color="#e74c3c"))
        f2.add_trace(go.Bar(x=gw["district_name"],y=gw["recharge_rate_mcm"],  name="Recharge MCM", marker_color="#2ecc71"))
        f2.update_layout(barmode="group",title="Extraction vs Recharge",**_LAYOUT)
        st.plotly_chart(f2, use_container_width=True)

    l2,r2 = st.columns(2)
    with l2:
        st.subheader("🔬 Water Quality Index Distribution")
        wq2 = df_wq.copy()
        wq2["wqi"] = (np.where(wq2["ph_level"].between(6.5,8.5),100,np.where(wq2["ph_level"].between(6.0,9.0),70,40))*0.3 +
                      np.where(wq2["dissolved_oxygen_mg_l"]>7,100,np.where(wq2["dissolved_oxygen_mg_l"]>5,70,np.where(wq2["dissolved_oxygen_mg_l"]>3,40,10)))*0.4 +
                      np.where(wq2["turbidity_ntu"]<5,100,np.where(wq2["turbidity_ntu"]<10,70,np.where(wq2["turbidity_ntu"]<20,40,10)))*0.3)
        f3 = px.histogram(wq2,x="wqi",nbins=20,color_discrete_sequence=["#00b4d8"],title="WQI Distribution")
        f3.add_vline(x=70,line_dash="dash",line_color="#2ecc71",annotation_text="Good Threshold")
        st.plotly_chart(_fig(f3), use_container_width=True)

    with r2:
        st.subheader("📊 Seasonal Rainfall Pattern")
        seasonal = df_r.groupby("season")["rainfall_cm"].mean().reset_index()
        st.plotly_chart(_fig(px.bar(seasonal,x="season",y="rainfall_cm",color="season",
            color_discrete_sequence=px.colors.qualitative.Bold,title="Avg Rainfall by Season")),
            use_container_width=True)

# ── RAINFALL ──────────────────────────────────────────────────────────────────
elif section == "🌧  Rainfall":
    st.markdown("<div class='section-header'>🌧 Rainfall Analysis  —  15 Queries</div>", unsafe_allow_html=True)

    def _rainfall_filters_ui():
        c1,c2,c3,c4 = st.columns(4)
        with c1: district = st.selectbox("District", ["All"]+DISTRICTS, key="rf_dist")
        with c2: rain     = st.slider("Rainfall (cm)", 0, 500, (0,500), key="rf_rain")
        with c3: year     = st.selectbox("Year", [0]+YEARS, format_func=lambda x:"All" if x==0 else str(x), key="rf_yr")
        with c4: season   = st.selectbox("Season", ["All"]+SEASONS, key="rf_sea")
        return district, rain[0], rain[1], year, season

    run_section(
        category      = "rainfall",
        queries_dict  = RAINFALL_QUERIES,
        base_df_fn    = get_demo_rainfall,
        filters_ui_fn = _rainfall_filters_ui,
        filters_build_fn = build_rainfall_filters,
        filter_keys   = ["district_filter","rainfall_range","year_filter","season_filter"],
    )

# ── GROUNDWATER ───────────────────────────────────────────────────────────────
elif section == "💧  Groundwater":
    st.markdown("<div class='section-header'>💧 Groundwater Analysis  —  12 Queries</div>", unsafe_allow_html=True)

    def _gw_filters_ui():
        c1,c2,c3 = st.columns(3)
        with c1: district = st.selectbox("District", ["All"]+DISTRICTS, key="gw_dist")
        with c2: depth    = st.slider("Depth (m)", 0, 100, (0,100), key="gw_depth")
        with c3: year     = st.selectbox("Year", [0]+YEARS, format_func=lambda x:"All" if x==0 else str(x), key="gw_yr")
        return district, depth[0], depth[1], year

    run_section(
        category      = "groundwater",
        queries_dict  = GROUNDWATER_QUERIES,
        base_df_fn    = get_demo_groundwater,
        filters_ui_fn = _gw_filters_ui,
        filters_build_fn = build_groundwater_filters,
        filter_keys   = ["district_filter","depth_range","year_filter"],
    )

# ── WATER QUALITY ─────────────────────────────────────────────────────────────
elif section == "🔬  Water Quality":
    st.markdown("<div class='section-header'>🔬 Water Quality Analysis  —  12 Queries</div>", unsafe_allow_html=True)

    def _wq_filters_ui():
        c1,c2,c3,c4 = st.columns(4)
        with c1: state    = st.selectbox("State",    ["All"]+STATES,       key="wq_state")
        with c2: district = st.selectbox("District", ["All"]+DISTRICTS,    key="wq_dist")
        with c3: ph       = st.slider("pH Range", 4.0, 12.0, (5.5,9.5), step=0.1, key="wq_ph")
        with c4: status   = st.selectbox("Status",   ["All"]+STATUS_LIST,  key="wq_stat")
        return state, district, ph[0], ph[1], status

    run_section(
        category      = "water_quality",
        queries_dict  = WATER_QUALITY_QUERIES,
        base_df_fn    = get_demo_water_quality,
        filters_ui_fn = _wq_filters_ui,
        filters_build_fn = build_wq_filters,
        filter_keys   = ["state_filter","district_filter","ph_range","status_filter"],
    )

# ── MAP VIEW ──────────────────────────────────────────────────────────────────
elif section == "🗺  Map View":
    st.markdown("<div class='section-header'>🗺 Monitoring Station Map</div>", unsafe_allow_html=True)

    df_wq = get_demo_water_quality()

    # colour each station by WQI band
    df_wq["wqi"] = (
        np.where(df_wq["ph_level"].between(6.5,8.5),100,np.where(df_wq["ph_level"].between(6.0,9.0),70,40))*0.3 +
        np.where(df_wq["dissolved_oxygen_mg_l"]>7,100,np.where(df_wq["dissolved_oxygen_mg_l"]>5,70,np.where(df_wq["dissolved_oxygen_mg_l"]>3,40,10)))*0.4 +
        np.where(df_wq["turbidity_ntu"]<5,100,np.where(df_wq["turbidity_ntu"]<10,70,np.where(df_wq["turbidity_ntu"]<20,40,10)))*0.3
    )
    df_wq["color"] = np.where(df_wq["wqi"]>=80,"green",np.where(df_wq["wqi"]>=55,"orange","red"))

    m = folium.Map(location=[20.5937, 78.9629], zoom_start=5,
                   tiles="CartoDB dark_matter")
    Fullscreen().add_to(m)
    cluster = MarkerCluster(name="Stations").add_to(m)

    for _, row in df_wq.iterrows():
        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=7, color=row["color"], fill=True, fill_opacity=0.8,
            popup=folium.Popup(
                f"<b>{row['station_name']}</b><br>"
                f"District: {row['district_name']}<br>"
                f"pH: {row['ph_level']} · DO: {row['dissolved_oxygen_mg_l']} mg/L<br>"
                f"Turbidity: {row['turbidity_ntu']} NTU<br>"
                f"WQI: {row['wqi']:.1f} · Status: {row['status']}", max_width=220),
        ).add_to(cluster)

    # heatmap layer
    heat_data = [[r["latitude"], r["longitude"], r["wqi"]] for _, r in df_wq.iterrows()]
    HeatMap(heat_data, name="WQI Heatmap", radius=18, blur=12).add_to(m)
    folium.LayerControl().add_to(m)

    col_m, col_leg = st.columns([3,1])
    with col_m:
        st_folium(m, width=None, height=520, returned_objects=[])
    with col_leg:
        st.markdown("### Legend")
        st.markdown("<span style='color:#2ecc71'>●</span> WQI ≥ 80 — Excellent", unsafe_allow_html=True)
        st.markdown("<span style='color:#e67e22'>●</span> WQI 55–79 — Good", unsafe_allow_html=True)
        st.markdown("<span style='color:#e74c3c'>●</span> WQI < 55 — Poor", unsafe_allow_html=True)
        st.markdown("---")
        st.metric("Total Stations", len(df_wq))
        st.metric("Active", int((df_wq["status"]=="Active").sum()))
        st.metric("Avg WQI", f"{df_wq['wqi'].mean():.1f}")

# ── SQL EXPLORER ──────────────────────────────────────────────────────────────
elif section == "💻  SQL Explorer":
    st.markdown("<div class='section-header'>💻 SQL Explorer  —  39 Queries Reference</div>", unsafe_allow_html=True)

    if DEMO_MODE:
        st.warning("⚠️ Live execution requires a database. Add `NEON_URL` to `.streamlit/secrets.toml`.")
        st.markdown("**Available tables:** `rainfall_history` · `groundwater_levels` · `water_monitoring_stations`")
    else:
        custom = st.text_area("✍️ Custom SQL", height=160, value="SELECT * FROM rainfall_history LIMIT 10;")
        if st.button("▶ Execute", type="primary"):
            df_r, err = execute_query(custom)
            if err: st.error(err)
            elif df_r is not None:
                st.success(f"{len(df_r)} rows · {len(df_r.columns)} columns")
                st.dataframe(df_r, use_container_width=True)
                buf = BytesIO(); df_r.to_csv(buf,index=False)
                st.download_button("⬇ CSV", buf.getvalue(), "custom.csv","text/csv")

    st.markdown("---")
    st.markdown("### 📚 All 39 Queries at a Glance")
    t1, t2, t3 = st.tabs(["🌧 Rainfall  (15)", "💧 Groundwater  (12)", "🔬 Water Quality  (12)"])
    with t1:
        for k,v in RAINFALL_QUERIES.items():
            with st.expander(k): st.code(v.strip(), language="sql")
    with t2:
        for k,v in GROUNDWATER_QUERIES.items():
            with st.expander(k): st.code(v.strip(), language="sql")
    with t3:
        for k,v in WATER_QUALITY_QUERIES.items():
            with st.expander(k): st.code(v.strip(), language="sql")
