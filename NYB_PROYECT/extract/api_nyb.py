print("hello world")
print("extract")


import json 
import requests 
from datetime import datetime 
from pathlib import Path
import os 

API_KEY = os.getenv("MTA_API_KEY")

URL = "http://bustime.mta.info/api/siri/vehicle-monitoring.json"

LINE_REFS = [
    "MTA NYCT_B44",
    "MTA NYCT_B46",
    "MTA NYCT_Q27", 
    "MTA NYCT_M34+"
]

params = {
    "key": API_KEY,
    "OperatorRef": "MTA",
    "VehicleMonitorDetailLevel": "normal"
}

BASE_DIR = Path(__file__).resolve().parents[1]
raw_path = BASE_DIR / "data" / "raw"
raw_path.mkdir(parents=True, exist_ok=True)

response = requests.get(URL, params=params, timeout=30)

if response.status_code == 200: 
    data = response.json()

    traffic = data["flowSegmentData"]

    timestamp = datetime.utcnow().strftime("%Y-%m-&d_%H-%M-%S")
    file_path = raw_path / f"bus_position_{timestamp}.json"

    with open(file_path, "w", encoding="utf-8") as f: 
        json.dump(data, f, indent=2)

    print(f"archivo guardado en: {file_path}")

else: 
    print("error:", response.status_code, response.text)
