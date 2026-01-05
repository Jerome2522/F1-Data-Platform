import streamlit as st
import pandas as pd
import os

# -----------------------------
# DATA PATH (Docker-first)
# -----------------------------
if os.path.exists("/data/processed"):
    DATA_PATH = "/data"
else:
    DATA_PATH = "data"

PROCESSED_DIR = os.path.join(DATA_PATH, "processed")
RAW_DIR = os.path.join(DATA_PATH, "raw")

st.set_page_config(page_title="F1 Analytics Platform", layout="wide")
st.sidebar.success(f"📂 Using data from: {os.path.abspath(DATA_PATH)}")

# -----------------------------
# SAFETY CHECK
# -----------------------------
if not os.path.exists(PROCESSED_DIR):
    st.error("❌ Processed data not found. Run the pipeline first.")
    st.stop()

# -----------------------------
# LOADERS (YOU WERE MISSING THESE)
# -----------------------------
@st.cache_data
def load_processed_parquet(folder_name):
    path = os.path.join(PROCESSED_DIR, folder_name)
    if os.path.exists(path):
        return pd.read_parquet(path)
    return None


@st.cache_data
def load_raw_csv(file_name):
    path = os.path.join(RAW_DIR, file_name)
    if os.path.exists(path):
        return pd.read_csv(path)
    return None


# -----------------------------
# UI
# -----------------------------
st.title("🏎️ Formula 1 Data Engineering Platform")
st.markdown("### Near-Real-Time Analytics Dashboard")

tab1, tab2, tab3 = st.tabs(["🏆 Standings", "📈 Progression", "🧠 Advanced Metrics"])

# =============================
# TAB 1 — STANDINGS
# =============================
with tab1:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Driver Standings (2023)")
        df_drivers = load_processed_parquet("driver_standings.parquet")
        if df_drivers is not None:
            st.dataframe(df_drivers, use_container_width=True)
        else:
            st.warning("Driver standings missing")

    with col2:
        st.subheader("Constructor Standings")
        df_constructors = load_processed_parquet("constructor_standings.parquet")
        if df_constructors is not None:
            st.dataframe(df_constructors, use_container_width=True)
        else:
            st.warning("Constructor standings missing")

    st.divider()
    st.subheader("Latest Race Results")

    df_results = load_raw_csv("results.csv")
    df_races = load_raw_csv("races.csv")

    if df_results is not None and df_races is not None:
        merged = pd.merge(
            df_results,
            df_races,
            left_on="race_id",
            right_on="round",
            how="left"
        )

        last_round = merged["round"].max()
        latest = merged[merged["round"] == last_round]

        st.write(f"Round {last_round}: {latest['name'].iloc[0]}")
        st.dataframe(
            latest[["position_order", "number", "points", "status", "laps"]]
            .sort_values("position_order"),
            use_container_width=True
        )

# =============================
# TAB 2 — PROGRESSION
# =============================
with tab2:
    st.subheader("Driver Points Progression")

    df_prog = load_processed_parquet("driver_points_progression.parquet")
    if df_prog is not None:
        top = (
            df_prog.groupby("driver_id")["cumulative_points"]
            .max()
            .nlargest(10)
            .index
        )

        df_plot = df_prog[df_prog["driver_id"].isin(top)].copy()
        df_plot["Driver"] = df_plot["forename"] + " " + df_plot["surname"]

        st.line_chart(df_plot, x="round", y="cumulative_points", color="Driver")
    else:
        st.info("Progression data missing")

# =============================
# TAB 3 — ADVANCED METRICS
# =============================
with tab3:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Driver Consistency Index")
        df_consistency = load_processed_parquet("driver_consistency.parquet")
        if df_consistency is not None:
            st.dataframe(df_consistency.head(10), use_container_width=True)
        else:
            st.info("Consistency data missing")

    with col2:
        st.subheader("Constructor Reliability")
        df_reliability = load_processed_parquet("constructor_reliability.parquet")
        if df_reliability is not None:
            st.dataframe(df_reliability, use_container_width=True)
        else:
            st.info("Reliability data missing")

    st.subheader("Points Efficiency")
    df_eff = load_processed_parquet("points_efficiency.parquet")
    if df_eff is not None:
        st.bar_chart(df_eff.head(10), x="surname", y="points_efficiency")
