
import requests
import csv
from datetime import datetime
from pathlib import Path 
import time
import json  
import os


def extract_nyb():

    API_KEY = os.getenv("MTA_API_KEY")
    if not API_KEY:
        raise RuntimeError("MTA_API_KEY no definida") 
    
    URL = "http://bustime.mta.info/api/siri/vehicle-monitoring.json"


    LINE_REFS = [
    "MTA NYCT_B44",
    "MTA NYCT_B46",
    "MTA NYCT_Q27", 
    "MTA NYCT_M34+"
    ]

    BASE_DIR= Path(__file__).resolve().parents[1]
    RAW_DIR = BASE_DIR / "data" / "raw"
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    BASE_PARAMS = {
     "key": API_KEY,
     "OperatorRef": "MTA",
     "VehicleMonitorDetailLevel": "normal"
    }

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    for line in LINE_REFS: 
        params = BASE_PARAMS.copy()
        params["LineRef"] = line

        response = requests.get(URL, params=params, timeout=30)

        if response.status_code != 200: 
            print(f"Error {response.status_code} en {line}")
            continue

        data = response.json()

        output_file = RAW_DIR / f"nyc_bus_raw_{line.replace(' ', '_')}_{timestamp}.json"
        with open(output_file, "w", encoding="utf-8") as f: 
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"Guardado: {output_file}")

        time.sleep(0.5)
    
if __name__ == "__main__":
    extract_nyb()


