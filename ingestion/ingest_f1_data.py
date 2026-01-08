import requests
import pandas as pd
import os
import time

# Constants
# Ergast is deprecated/unstable. Using Jolpica mirror which is compatible.
BASE_URL = "http://api.jolpi.ca/ergast/f1"
DATA_DIR = "/data/raw"
SEASONS = range(2018, 2025) # 2018 to 2024 (inclusive)

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

def fetch_data(endpoint):
    """
    Fetches data from Jolpica API (Ergast compatible) with retry-safe logic.
    """
    url = f"https://api.jolpi.ca/ergast/f1/{endpoint}.json?limit=1000"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json"
    }
    
    # print(f"Fetching {url}...") 
    # print is annoying in logs 
    # Timeout added, strict SSL enabled (verify=True is default)
    try:
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def process_drivers(data, existing_df=None):
    if not data: return existing_df
    drivers = data['MRData']['DriverTable']['Drivers']
    df = pd.DataFrame(drivers)
    # Rename for clarity
    df = df.rename(columns={'driverId': 'driver_id', 'permanentNumber': 'number', 
                            'givenName': 'forename', 'familyName': 'surname'})
    
    if existing_df is not None:
        return pd.concat([existing_df, df]).drop_duplicates(subset=['driver_id'])
    return df

def process_constructors(data, existing_df=None):
    if not data: return existing_df
    constructors = data['MRData']['ConstructorTable']['Constructors']
    df = pd.DataFrame(constructors)
    df = df.rename(columns={'constructorId': 'constructor_id', 'name': 'name', 'nationality': 'nationality'})
    
    if existing_df is not None:
        return pd.concat([existing_df, df]).drop_duplicates(subset=['constructor_id'])
    return df

def process_races(data, existing_df=None):
    if not data: return existing_df
    races = data['MRData']['RaceTable']['Races']
    # Extract relevant fields
    race_list = []
    for r in races:
        race_info = {
            'race_id': int(r.get('round')) + (int(r.get('season')) * 100), # Synthetic unique ID (e.g. 202301)
            'year': int(r.get('season')),
            'round': int(r.get('round')),
            'circuit_id': r['Circuit']['circuitId'],
            'name': r['raceName'],
            'date': r['date']
        }
        race_list.append(race_info)
    
    df = pd.DataFrame(race_list)
    if existing_df is not None:
        return pd.concat([existing_df, df]).drop_duplicates(subset=['race_id'])
    return df

def process_results(season, existing_df=None):
    """
    Fetches results for all races in the season.
    """
    all_results = []
    
    # Correct endpoint for results
    data = fetch_data(f"{season}/results")
    if not data: return existing_df

    races = data['MRData']['RaceTable']['Races']
    for race in races:
        race_id = int(race['round']) + (int(race['season']) * 100) # Match race_id logic
        for result in race['Results']:
            row = {
                'result_id': f"{race['season']}_{race['round']}_{result['position']}", # Synthetic primary key
                'race_id': race_id, # Join key with races
                'driver_id': result['Driver']['driverId'],
                'constructor_id': result['Constructor']['constructorId'],
                'number': result.get('number'),
                'grid': result['grid'],
                'position': result['position'],
                'position_text': result['positionText'],
                'position_order': result['position'],
                'points': result['points'],
                'laps': result['laps'],
                'status': result['status']
            }
            all_results.append(row)
    
    df = pd.DataFrame(all_results)
    if existing_df is not None:
        return pd.concat([existing_df, df]).drop_duplicates(subset=['result_id'])
    return df

def file_exists(name):
    # For multi-season, we want to force refresh if we are expanding scope
    # But for now, let's keep it simple: if files exist, we assume they are good. 
    # Use "rm -rf data/raw" to force re-ingest.
    return os.path.exists(os.path.join(DATA_DIR, name))

def main():
    print(f"Starting F1 Data Ingestion for seasons: {list(SEASONS)}...")
    
    # We will aggregate data in memory then save
    all_drivers = None
    all_constructors = None
    all_races = None
    all_results = None

    for season in SEASONS:
        print(f"Processing Season {season}...")
        
        # 1. Drivers
        drivers_data = fetch_data(f"{season}/drivers")
        all_drivers = process_drivers(drivers_data, all_drivers)
        
        # 2. Constructors
        constructors_data = fetch_data(f"{season}/constructors")
        all_constructors = process_constructors(constructors_data, all_constructors)
        
        # 3. Races
        races_data = fetch_data(f"{season}/races")
        all_races = process_races(races_data, all_races)
        
        # 4. Results
        all_results = process_results(season, all_results)
        
        time.sleep(1) # Be polite between seasons

    # Save aggregated data
    if all_drivers is not None:
        all_drivers.to_csv(f"{DATA_DIR}/drivers.csv", index=False)
        print(f"Saved {len(all_drivers)} drivers.")

    if all_constructors is not None:
        all_constructors.to_csv(f"{DATA_DIR}/constructors.csv", index=False)
        print(f"Saved {len(all_constructors)} constructors.")

    if all_races is not None:
        all_races.to_csv(f"{DATA_DIR}/races.csv", index=False)
        print(f"Saved {len(all_races)} races.")

    if all_results is not None:
        all_results.to_csv(f"{DATA_DIR}/results.csv", index=False)
        print(f"Saved {len(all_results)} results.")
    
    print("Ingestion complete!")

if __name__ == "__main__":
    main()
