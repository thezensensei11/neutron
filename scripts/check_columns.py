import requests

QUESTDB_URL = "http://localhost:9000/exec"

def check_columns():
    query = "SHOW COLUMNS FROM aggTrades"
    response = requests.get(QUESTDB_URL, params={"query": query})
    
    if response.status_code == 200:
        data = response.json()
        if data['dataset']:
            print("Columns in aggTrades:")
            for row in data['dataset']:
                print(f"- {row[0]} ({row[1]})")
        else:
            print("No columns found")
    else:
        print(f"Error: {response.status_code} - {response.text}")

if __name__ == "__main__":
    check_columns()
