
import requests
import zipfile
import io
import pandas as pd

def debug_bookdepth():
    # Use a valid past date
    url = "https://data.binance.vision/data/futures/um/daily/bookDepth/BTCUSDT/BTCUSDT-bookDepth-2024-11-01.zip"
    print(f"Downloading {url}...")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            print(f"Files in zip: {z.namelist()}")
            csv_filename = z.namelist()[0]
            with z.open(csv_filename) as f:
                # Read first few lines as text
                print("\n--- First 5 lines of CSV ---")
                for _ in range(5):
                    print(f.readline().decode('utf-8').strip())
                
                # Reset and read with pandas using our headers
                f.seek(0)
                headers = ["symbol", "time", "first_update_id", "last_update_id", "side", "price", "qty"]
                df = pd.read_csv(f, header=None, names=headers)
                
                print("\n--- DataFrame Head ---")
                print(df.head())
                
                print("\n--- Time Column Info ---")
                print(df['time'].dtype)
                print(df['time'].iloc[0])
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_bookdepth()
