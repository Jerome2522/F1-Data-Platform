import streamlit as st
import pandas as pd
import os

# Path Resolution for Docker vs Local vs Cloud
if os.path.exists("/data/processed"):
    DATA_PATH = "/data"
elif os.path.exists("data/processed"):
    DATA_PATH = "data"
elif os.path.exists("../data/processed"):
    DATA_PATH = "../data"
elif os.path.exists(os.path.join(os.path.dirname(__file__), "../data/processed")):
    # Fallback using script location
    DATA_PATH = os.path.join(os.path.dirname(__file__), "../data")
else:
    DATA_PATH = "data" # Default fallback

PROCESSED_DIR = os.path.join(DATA_PATH, "processed")
RAW_DIR = os.path.join(DATA_PATH, "raw")

st.set_page_config(page_title="F1 Analytics Platform", layout="wide")

st.title("🏎️ Formula 1 Data Engineering Platform")
st.markdown("### Near-Real-Time Analytics Dashboard")

# Helper to load data
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

# Check if data exists
if not os.path.exists(PROCESSED_DIR):
    st.error(f"Data directory not found at {PROCESSED_DIR}. Please run the pipeline first or ensure data is committed!")
    st.stop()

# Layout
tab1, tab2, tab3 = st.tabs(["🏆 Standings", "📈 Progression", "🧠 Advanced Metrics"])

with tab1:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Driver Standings (Season 2023)")
        df_drivers = load_processed_parquet("driver_standings.parquet")

        if df_drivers is not None:
            st.dataframe(df_drivers.style.highlight_max(axis=0, subset=['total_points']), use_container_width=True)
        else:
            st.warning("Driver standings data not available.")

    with col2:
        st.subheader("Constructor Standings")
        df_constructors = load_processed_parquet("constructor_standings.parquet")
        if df_constructors is not None:
            st.dataframe(df_constructors, use_container_width=True)
        else:
            st.warning("Constructor standings data not available.")
            
    st.divider()
    st.subheader("Latest Race Results")
    # Quick raw view or processed view
    # Let's show the raw results sorted by latest race
    df_results = load_raw_csv("results.csv")
    df_races = load_raw_csv("races.csv")
    if df_results is not None and df_races is not None:
        # Join to get race name
        df_merged = pd.merge(df_results, df_races, left_on='race_id', right_on='round', suffixes=('_res', '_race'))
        last_round = df_merged['round'].max()
        latest_race = df_merged[df_merged['round'] == last_round]
        st.write(f"Results for Round {last_round} ({latest_race['name'].iloc[0] if not latest_race.empty else 'Unknown'})")
        # Show key columns
        st.dataframe(latest_race[['position_order', 'number', 'points', 'status', 'laps']].sort_values('position_order'), use_container_width=True)

with tab2:
    st.subheader("Driver Points Progression")
    df_progression = load_processed_parquet("driver_points_progression.parquet")
    if df_progression is not None:
        # Pivot for line chart: index=race_name/round, columns=driver, values=cumulative_points
        # Filter top 10 drivers for clarity
        top_drivers = df_progression.groupby('driver_id')['cumulative_points'].max().nlargest(10).index
        df_filtered = df_progression[df_progression['driver_id'].isin(top_drivers)]
        
        # Create a cleaner label
        df_filtered['Driver'] = df_filtered['forename'] + " " + df_filtered['surname']
        
        st.line_chart(df_filtered, x='round', y='cumulative_points', color='Driver')
    else:
        st.info("Progression data not available.")

with tab3:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Driver Consistency Index")
        st.caption("Lower variance = More consistent finishes")
        df_consistency = load_processed_parquet("driver_consistency.parquet")
        if df_consistency is not None:
            st.dataframe(df_consistency.head(10), use_container_width=True)
        else:
            st.info("Consistency data missing.")

    with col2:
        st.subheader("Constructor Reliability Score")
        st.caption("% of races finished without DNF")
        df_reliability = load_processed_parquet("constructor_reliability.parquet")
        if df_reliability is not None:
            st.dataframe(df_reliability, use_container_width=True)
        else:
            st.info("Reliability data missing.")

    st.subheader("Points Efficiency")
    st.caption("Average points per race entered")
    df_efficiency = load_processed_parquet("points_efficiency.parquet")
    if df_efficiency is not None:
        st.bar_chart(df_efficiency.head(10), x='surname', y='points_efficiency')
