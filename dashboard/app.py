import streamlit as st
import pandas as pd
import os

# Path Resolution for Docker vs Local vs Cloud
# Path Resolution for Docker vs Local vs Cloud
# We simply look for the data folder relative to the script execution
# Path Resolution for Docker vs Local vs Cloud
# Recursive search to find 'processed' directory anywhere
DATA_PATH = None

def find_data_recursive(start_path):
    for root, dirs, files in os.walk(start_path):
        if "processed" in dirs:
            return root
    return None

# Check current dir and parents
current_path = os.getcwd()
# Search down
DATA_PATH = find_data_recursive(current_path)

# If not found, search up one level and down
if DATA_PATH is None:
    parent = os.path.dirname(current_path)
    DATA_PATH = find_data_recursive(parent)

# Check fallback locations explicitly
if DATA_PATH is None:
    possible_paths = [
        "/data", 
        "data",
        "../data",
        os.path.join(os.path.dirname(__file__), "../data")
    ]
    for p in possible_paths:
        if os.path.exists(os.path.join(p, "processed")):
            DATA_PATH = p
            break

if DATA_PATH is None:
    # DEBUG INFO FOR USER
    st.error(f"❌ Data directory not found!")
    
    st.markdown("### Debugging Info")
    st.code(f"Current Working Directory: {os.getcwd()}")
    st.code(f"Script Location: {os.path.dirname(__file__)}")
    
    # List files in current directory to help debug
    st.markdown("### Files in Current Dir:")
    try:
        st.code(str(os.listdir(os.getcwd())))
    except:
        st.write("Could not list current dir")

    # List files in parent directory
    st.markdown("### Files in Parent Dir:")
    try:
        st.code(str(os.listdir(os.path.dirname(os.getcwd()))))
    except:
        st.write("Could not list parent dir")

    st.stop()

PROCESSED_DIR = os.path.join(DATA_PATH, "processed")
RAW_DIR = os.path.join(DATA_PATH, "raw")

st.sidebar.success(f"📂 Loaded data from: `{os.path.abspath(DATA_PATH)}`")

# Check if data exists
if not os.path.exists(PROCESSED_DIR):
    st.error(f"Processed data not found at {PROCESSED_DIR}. Please run the pipeline first!")
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
