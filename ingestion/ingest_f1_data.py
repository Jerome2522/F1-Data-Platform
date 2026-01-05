import requests
import pandas as pd
import os
import time
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Constants
# Ergast is deprecated/unstable. Using Jolpica mirror which is compatible.
BASE_URL = "http://api.jolpi.ca/ergast/f1"
DATA_DIR = "/data/raw"
SEASON = "2023" # Focusing on 2023 for analysis (Ergast v1 style)

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

def fetch_data(endpoint, limit=1000):
    """
    Fetches data from Ergast API with pagination handling if needed,
    but we keep it simple by requesting a large limit for a single season.
    """
    url = f"{BASE_URL}/{endpoint}.json?limit={limit}"
    print(f"Fetching {url}...")
    
    # 403 Fix: APIs often block default python-requests User-Agent.
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json'
    }
    
    response = requests.get(url, headers=headers, verify=False)
    response.raise_for_status()
    time.sleep(2) # Be polite to the API to avoid being dropped
    return response.json()

def process_drivers(data):
    if not data: return
    drivers = data['MRData']['DriverTable']['Drivers']
    df = pd.DataFrame(drivers)
    # Rename for clarity
    df = df.rename(columns={'driverId': 'driver_id', 'permanentNumber': 'number', 
                            'givenName': 'forename', 'familyName': 'surname'})
    output_path = f"{DATA_DIR}/drivers.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved drivers to {output_path}")

def process_constructors(data):
    if not data: return
    constructors = data['MRData']['ConstructorTable']['Constructors']
    df = pd.DataFrame(constructors)
    df = df.rename(columns={'constructorId': 'constructor_id', 'name': 'name', 'nationality': 'nationality'})
    output_path = f"{DATA_DIR}/constructors.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved constructors to {output_path}")

def process_races(data):
    if not data: return
    races = data['MRData']['RaceTable']['Races']
    # Extract relevant fields
    race_list = []
    for r in races:
        race_info = {
            'race_id': r.get('round'), # Using round as ID for season specific
            'year': r.get('season'),
            'round': r.get('round'),
            'circuit_id': r['Circuit']['circuitId'],
            'name': r['raceName'],
            'date': r['date']
        }
        race_list.append(race_info)
    
    df = pd.DataFrame(race_list)
    output_path = f"{DATA_DIR}/races.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved races to {output_path}")

def process_results(season):
    """
    Fetches results for all races in the season.
    """
    all_results = []
    # Get number of rounds first or just loop until empty
    # We fetched races already, let's just fetch results for the season (endpoint: {season}/results)
    # Note: 2023 had 22 races. Limit 1000 is enough for total rows (22 * 20 = 440)
    
    data = fetch_data(f"{season}/results", limit=1000)
    if not data: return

    races = data['MRData']['RaceTable']['Races']
    for race in races:
        race_round = race['round']
        for result in race['Results']:
            row = {
                'result_id': f"{race['season']}_{race['round']}_{result['position']}", # Synthetic primary key
                'race_id': race_round, # Join key with races
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
    output_path = f"{DATA_DIR}/results.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved results to {output_path}")

def main():
    print("Starting F1 Data Ingestion...")
    
    # 1. Drivers (All drivers for the season)
    drivers_data = fetch_data(f"{SEASON}/drivers")
    process_drivers(drivers_data)
    
    # 2. Constructors
    constructors_data = fetch_data(f"{SEASON}/constructors")
    process_constructors(constructors_data)
    
    # 3. Races
    races_data = fetch_data(f"{SEASON}")
    process_races(races_data)
    
    # 4. Results
    process_results(SEASON)
    
    print("Ingestion complete!")

if __name__ == "__main__":
    main()
