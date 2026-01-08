import streamlit as st
import pandas as pd
import altair as alt
import os

# ------------------------------------------------------
# 1. PAGE CONFIG & STYLING
# ------------------------------------------------------
st.set_page_config(
    page_title="F1 Telemetry & Analytics",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Dark Mode Engineering Aesthetic
st.markdown("""
<style>
    /* Metric Cards */
    div[data-testid="stMetric"] {
        background-color: #1E1E1E;
        padding: 15px;
        border-radius: 8px;
        border-left: 5px solid #FF1801;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    div[data-testid="stMetricLabel"] {
        color: #B0B0B0;
        font-size: 0.9rem;
    }
    div[data-testid="stMetricValue"] {
        color: #FFFFFF;
        font-size: 1.8rem;
        font-family: 'Consolas', 'Courier New', monospace;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 40px;
        white-space: pre-wrap;
        background-color: #0E1117;
        border-radius: 5px;
        color: #8C8C8C;
        font-weight: 600;
        padding-left: 20px;
        padding-right: 20px;
    }
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background-color: #FF1801;
        color: white;
    }
    
    /* Header */
    h1, h2, h3 {
        font-family: 'Helvetica Neue', sans-serif;
        font-weight: 800;
        letter-spacing: -0.5px;
    }
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------------
# 2. DATA PATH RESOLUTION
# ------------------------------------------------------
DATA_PATH = None
def find_data_recursive(start_path):
    for root, dirs, files in os.walk(start_path):
        if "raw" in dirs: # Look for raw data now as we rely on it
            return root
    return None

current_path = os.getcwd()
DATA_PATH = find_data_recursive(current_path)
if DATA_PATH is None:
    parent = os.path.dirname(current_path)
    DATA_PATH = find_data_recursive(parent)

# Explicit fallback
if DATA_PATH is None:
    possible_paths = ["/data", "data", "../data", os.path.join(os.path.dirname(__file__), "../data")]
    for p in possible_paths:
        if os.path.exists(os.path.join(p, "raw")):
            DATA_PATH = p
            break

RAW_DIR = os.path.join(DATA_PATH, "raw") if DATA_PATH else None

# ------------------------------------------------------
# 3. DATA LOADING & PREP
# ------------------------------------------------------
@st.cache_data
def load_master_data():
    if not RAW_DIR or not os.path.exists(RAW_DIR):
        return None
    
    try:
        results = pd.read_csv(os.path.join(RAW_DIR, "results.csv"))
        races = pd.read_csv(os.path.join(RAW_DIR, "races.csv"))
        drivers = pd.read_csv(os.path.join(RAW_DIR, "drivers.csv"))
        constructors = pd.read_csv(os.path.join(RAW_DIR, "constructors.csv"))
        
        # Merge Chain with Explicit Renaming
        
        # 1. Races -> race_name, date, year, round
        # Rename race columns BEFORE merge to avoid issues
        races = races.rename(columns={'name': 'name_race', 'date': 'race_date', 'year': 'year', 'round': 'round'})
        master = results.merge(races[['race_id', 'name_race', 'race_date', 'year', 'round']], on="race_id", how="inner")
        
        # 2. Drivers -> forename, surname
        drivers = drivers.rename(columns={'forename': 'drv_forename', 'surname': 'drv_surname', 'number': 'drv_number'})
        master = master.merge(drivers[['driver_id', 'drv_forename', 'drv_surname']], on="driver_id", how="left")
        
        # 3. Constructors -> name
        constructors = constructors.rename(columns={'name': 'team_name', 'nationality': 'team_nat'})
        master = master.merge(constructors[['constructor_id', 'team_name']], on="constructor_id", how="left")
        
        # 4. Clean up columns
        master['points'] = pd.to_numeric(master['points'], errors='coerce').fillna(0)
        master['position_order'] = pd.to_numeric(master['position_order'], errors='coerce')
        master['grid'] = pd.to_numeric(master['grid'], errors='coerce')
        
        # Full Name & Team
        master['Driver'] = (master['drv_forename'] + " " + master['drv_surname']).fillna("Unknown Driver")
        master['Team'] = master['team_name'].fillna("Unknown Team")
        master['date'] = pd.to_datetime(master['race_date'])
        
        return master
        
    except Exception as e:
        st.error(f"Error loading data: {e}. Check raw files.")
        return None

df = load_master_data()

if df is None:
    st.error("❌ No Data Found. Please run the Airflow Ingestion Pipeline.")
    st.image("https://media.giphy.com/media/d2Z4rTi11c9LRita/giphy.gif", width=300)
    st.stop()

# ------------------------------------------------------
# 4. SIDEBAR CONTROLS
# ------------------------------------------------------
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/3/33/F1.svg", width=120)
st.sidebar.title("Telemetry Control")

analysis_mode = st.sidebar.radio(
    "Analysis Mode",
    ["🏆 Driver Analysis", "🔧 Team Analysis", "📅 Season Recap"],
    index=0
)

st.sidebar.markdown("---")

# Global Date Filter logic could go here, but per-mode is better

# ------------------------------------------------------
# 5. MAIN DASHBOARD LOGIC
# ------------------------------------------------------

# ======================================================
# MODE 1: DRIVER ANALYSIS
# ======================================================
if analysis_mode == "🏆 Driver Analysis":
    st.title("🏆 Driver Performance Analysis")
    
    # Select Driver from List (sorted by activity)
    active_drivers = df.groupby('Driver')['year'].max().sort_values(ascending=False).index.tolist()
    selected_driver = st.sidebar.selectbox("Select Driver", active_drivers, index=0) # Default to first
    
    # Filter Data
    ddf = df[df['Driver'] == selected_driver]
    
    # --- HEADER METRICS ---
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Career Entries", len(ddf))
    with m2:
        st.metric("Total Points", f"{ddf['points'].sum():.1f}")
    with m3:
        wins = len(ddf[ddf['position_order'] == 1])
        st.metric("Career Wins", wins)
    with m4:
        podiums = len(ddf[ddf['position_order'] <= 3])
        st.metric("Podiums", podiums)
        
    # --- YEARLY BREAKDOWN ---
    st.markdown("### 📅 Yearly Progression")
    
    yearly_stats = ddf.groupby('year').agg({
        'points': 'sum',
        'position_order': 'mean',
        'race_id': 'count'
    }).reset_index()
    yearly_stats.columns = ['Year', 'Points', 'Avg Finish', 'Entries']
    
    # Line Chart: Points per Year
    c_points = alt.Chart(yearly_stats).mark_line(point=True, color='#FF1801').encode(
        x='Year:O',
        y='Points:Q',
        tooltip=['Year', 'Points', 'Entries']
    ).properties(height=300, title="Points Scored per Season")
    
    st.altair_chart(c_points, use_container_width=True)
    
    # --- DEEP DIVE TABS ---
    t1, t2 = st.tabs(["📊 Performance Matrix", "🗺️ Track Dominance"])
    
    with t1:
        colA, colB = st.columns(2)
        with colA:
            st.markdown("**Grid vs Finish (The 'Overtake' Factor)**")
            # Scatter plot
            chart_ov = alt.Chart(ddf).mark_circle(size=60).encode(
                x=alt.X('grid', title='Grid Pos', scale=alt.Scale(domain=[0,22])),
                y=alt.Y('position_order', title='Finish Pos', scale=alt.Scale(domain=[0,22])),
                color=alt.Color('year:N', title='Season'),
                tooltip=['name_race', 'year', 'grid', 'position_order']
            ).interactive()
            
            line = alt.Chart(pd.DataFrame({'x': [0, 22], 'y': [0, 22]})).mark_rule(color='white', strokeDash=[3,3]).encode(x='x', y='y')
            st.altair_chart(chart_ov + line, use_container_width=True)
            st.caption("Points below the white line indicate places GAINED during the race.")
            
        with colB:
            st.markdown("**Finishing Position Distribution**")
            # Histogram
            chart_hist = alt.Chart(ddf).mark_bar(color='#30333F').encode(
                x=alt.X('position_order:Q', bin=alt.Bin(maxbins=20), title='Finish Position'),
                y='count()',
                tooltip='count()'
            )
            st.altair_chart(chart_hist, use_container_width=True)

    with t2:
        st.markdown("**Best Circuits**")
        circuit_stats = ddf.groupby('name_race').agg({'points':'sum', 'position_order':'mean'}).sort_values('points', ascending=False).head(10).reset_index()
        
        base = alt.Chart(circuit_stats).encode(
            x=alt.X('points:Q', title='Total Points'),
            y=alt.Y('name_race', sort='-x', title=None),
            tooltip=['name_race', 'points']
        )
        bar = base.mark_bar(color='#E10600')
        text = base.mark_text(align='left', dx=2, color='white').encode(text='points:Q')
        
        st.altair_chart(bar + text, use_container_width=True)

# ======================================================
# MODE 2: TEAM ANALYSIS
# ======================================================
elif analysis_mode == "🔧 Team Analysis":
    st.title("🔧 Constructor Reliability & Performance")
    
    teams = sorted(df['Team'].unique().tolist())
    selected_team = st.sidebar.selectbox("Select Constructor", teams)
    
    tdf = df[df['Team'] == selected_team]
    
    # Metrics
    m1, m2, m3, m4 = st.columns(4)
    with m1: st.metric("Entries", len(tdf))
    with m2: st.metric("Total Points", f"{tdf['points'].sum():.0f}")
    with m3: st.metric("Wins", len(tdf[tdf['position_order']==1]))
    with m4:
        dnf_count = len(tdf[~tdf['status'].str.contains('Finished|X Lap', regex=True, case=False)])
        dnf_rate = (dnf_count / len(tdf)) * 100
        st.metric("DNF Rate", f"{dnf_rate:.1f}%", help="Did Not Finish %")

    st.divider()
    
    # Driver Contribution
    st.subheader("Driver Point Contribution")
    # Aggregate points per driver per year
    contrib = tdf.groupby(['year', 'Driver'])['points'].sum().reset_index()
    
    chart_stack = alt.Chart(contrib).mark_bar().encode(
        x='year:O',
        y='points:Q',
        color=alt.Color('Driver:N', scale=alt.Scale(scheme='category20')),
        tooltip=['year', 'Driver', 'points']
    ).properties(title="Stacked Points by Driver")
    
    st.altair_chart(chart_stack, use_container_width=True)

# ======================================================
# MODE 3: SEASON RECAP
# ======================================================
elif analysis_mode == "📅 Season Recap":
    st.title("📅 Season Analysis")
    
    years = sorted(df['year'].unique().tolist(), reverse=True)
    selected_year = st.sidebar.selectbox("Select Season", years)
    
    sdf = df[df['year'] == selected_year]
    
    # Championship Table
    st.subheader(f"{selected_year} Championship Standings")
    standings = sdf.groupby('Driver').agg({'points':'sum', 'position_order': lambda x: (x==1).sum()}).reset_index()
    standings.columns = ['Driver', 'Points', 'Wins']
    standings = standings.sort_values('Points', ascending=False).reset_index(drop=True)
    standings.index += 1 # 1-based index
    
    st.dataframe(standings.style.background_gradient(subset=['Points'], cmap='Reds'), use_container_width=True)
    
    # Cumulative Chart
    st.subheader("Title Fight Trajectory")
    
    # Pivot for Cumulative Sum
    # We need race round logic
    # round is in sdf['round']
    
    # 1. Get points per round per driver
    pivoted = sdf.pivot_table(index='round', columns='Driver', values='points', aggfunc='sum').fillna(0)
    # 2. Cumsum
    cumulative = pivoted.cumsum()
    # 3. Melt back for Altair
    cumulative = cumulative.reset_index().melt(id_vars='round', var_name='Driver', value_name='Total Points')
    
    # Filter top 10 drivers end of season
    top_drivers = standings.head(5)['Driver'].tolist()
    filtered_cum = cumulative[cumulative['Driver'].isin(top_drivers)]
    
    chart_title = alt.Chart(filtered_cum).mark_line(point=True).encode(
        x=alt.X('round:O', title='Race Round'),
        y='Total Points:Q',
        color='Driver:N',
        tooltip=['Driver', 'Total Points']
    ).interactive()
    
    st.altair_chart(chart_title, use_container_width=True)

# Footer
st.sidebar.markdown("---")
st.sidebar.info("💡 **Pro Tip:** Double-click charts to reset zoom.")

