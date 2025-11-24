import requests
import json
from datetime import datetime

QUESTDB_URL = "http://localhost:9000/exec"

def get_stats():
    for instrument in ['spot', 'swap']:
        query = f"SELECT min(time), max(time), count() FROM aggTrades WHERE instrument_type='{instrument}'"
        response = requests.get(QUESTDB_URL, params={"query": query})
        
        if response.status_code == 200:
            data = response.json()
            if data['dataset']:
                row = data['dataset'][0]
                print(f"[{instrument}] Min: {row[0]}, Max: {row[1]}, Count: {row[2]}")
            else:
                print(f"[{instrument}] No data")
        else:
            print(f"Error: {response.status_code} - {response.text}")

if __name__ == "__main__":
    get_stats()
