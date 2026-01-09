import pandas as pd
from pathlib import Path 
from datetime import datetime 
import json 

#Vamos a definir las rutas

def transform_nyb():

    BASE_DIR = Path(__file__).resolve().parents[1]
    RAW_DIR = BASE_DIR / "data" / "raw"
    PROCESSED_DIR = BASE_DIR / "data" / "processed"
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    #Aquí vamos a cargar el archivo json, el mas reciente que tengamos 

    raw_file = sorted(RAW_DIR.glob("nyc_bus_raw_*.json"))


    if not raw_file: 
        raise FileNotFoundError("No hay archivos fila para poder transformar")

    latest_file = raw_file[-1]

    print(f"Tranformando: {latest_file.name}")

    #ya aquí leemos el archivo json

    with open(latest_file, "r", encoding="utf-8") as f: 
        data = json.load(f)


    #aqui por ejemplo, ya accedemos a una lista de autobuses

    deliveries = (
        data["Siri"]
            ["ServiceDelivery"]
            ["VehicleMonitoringDelivery"]
    )

    if not deliveries: 
       raise ValueError("VehicleMonitorinDelivery vacío")

    vehicle_activity = deliveries[0].get("VehicleActivity", [])

    rows = []

    for vehicle in vehicle_activity:

        #en esta parte se extraen los niveles del json

        journey = vehicle.get("MonitoredVehicleJourney", {})
        location = journey.get("VehicleLocation", {})
        call = journey.get("MonitoredCall", {})
        distances = call.get("Extensions", {}).get("Distances", {})

        #aqui las filas

        row = {
           "line_name": journey.get("PublishedLineName"),
           "vehicle_id": journey.get("VehicleRef"),
           "direction": journey.get("DirectionRef"),
           "latitude": location.get("Latitude"),
           "longitude": location.get("Longitude"),
           "stop_name": call.get("StopPointName"),
           "stop_id": call.get("StopPointRef"),
           "distance_from_stop_m": distances.get("DistanceFromCall"),
           "stops_from_call": distances.get("StopsFromCall"),
           "aimed_arrival": call.get("AimedArrivalTime"),
           "expected_arrival": call.get("ExpectedArrivalTime"),
           "recorded_at": vehicle.get("RecordedAtTime"),
        }

    
        rows.append(row)
    
        print(f"filas extraídas: {len(rows)}")

    df = pd.DataFrame(rows)

    #convertimos a datetime para trabajar con pandas

    df["aimed_arrival"] = pd.to_datetime(df["aimed_arrival"], utc=True)
    df["expected_arrival"] = pd.to_datetime(df["expected_arrival"], utc=True)
    df["recorded_at"] = pd.to_datetime(df["recorded_at"], utc=True)

    df.dtypes

    #aqui separamos la fecha y hora, ya que la columnas tiene datos juntos

    df["date"] = df["expected_arrival"].dt.date
    df["hour"] = df["expected_arrival"].dt.hour
    df["minute"] = df["expected_arrival"].dt.minute

    #calculo del delay en segundos

    df["delay_seconds"] = (df["expected_arrival"] - df["aimed_arrival"]).dt.total_seconds()

    #aqui clasificamos el status del retraso (delay)

    DS = df["delay_seconds"]

    def d_s(DS):
        if DS > 0: return 'retraso'
        elif DS < 0: return 'adelantado'
        elif DS == 0: return 'puntual'
        else:
            return 'Desconocido'

    df["Delay_status"] = DS.apply(d_s)    

    #calculamos la velocidad promedio 

    df = df.sort_values(["vehicle_id", "recorded_at"])

    df["delta_distance_m"] = (
        df.groupby("vehicle_id")["distance_from_stop_m"].diff() * -1
    )

    df["delta_time_s"] = (
        df.groupby("vehicle_id")["recorded_at"].diff().dt.total_seconds()
    )

    df["speed_m_s"] = df["delta_distance_m"] / df["delta_time_s"]

    #vamos a definir una ventana temporal para poder 
    #analizar la distancia promedio en un cierto tiempo 
    #a partir del id de un vehiculo

    windowed = (
        df
        .set_index("recorded_at")
        .groupby("vehicle_id")
        .resample("min")
    )


    #aqui vamos agregar las metricas dentro de la ventada con la funcion aggs (agregation)

    features = windowed.agg(
        avg_distance_m=("distance_from_stop_m", "mean"),
        avg_delay_s=("delay_seconds", "mean"),
        samples=("distance_from_stop_m", "count")
    ).reset_index()

    #velocidad promedio 

    features["avg_speed_m_per_min"] = features["avg_distance_m"]

    #en este punto vamos a hacer tranformaciones espaciales

 

    #ahora vamos a analizar ventanas de 1 minuto. Ventanas como hicimos 
    #anteriormente en windowed, para ello tomamos de recorded_at el tiempo
    #para ello tambien separamos los minutos de dicha columna como sigue: 
    
    df["recorded_at"] = pd.to_datetime(df['recorded_at'], utc=True)

    #ahora vamos a redondear las columnas longitude y latitude a dos decimales

    df['grid_longitude'] = df['longitude'].round(2)

    df['grid_latitude'] = df['latitude'].round(2)

    #aqui indexamos el tiempo 

    df1 = df.set_index("recorded_at")

    #aqui contamos cuantos vehículos distintos hay en cada celda espacial 
    #por minuto 

    windowed1 = (
        df1
        .groupby(["grid_latitude", "grid_longitude"])
        .resample("1min")
    )

    features1 = windowed1.agg(
        vehicle_count=("vehicle_id", "nunique"),
        avg_delay_s=("delay_seconds", "mean"),
        avg_distance_m=("distance_from_stop_m", "mean"),
    ).reset_index()

    features1["vehicles_per_cell"] = features1["vehicle_count"]

    #aqui guardamos el csv

    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    output_file = PROCESSED_DIR / f"nyb_bus_processed_{timestamp}.csv"

    df.to_csv(output_file, index=False)
    features.to_csv(PROCESSED_DIR / f"nyb_bus_windowed_{timestamp}.csv", index=False)
    
    window_output_file = PROCESSED_DIR / f"nyb_bus_windowed2_{timestamp}.csv"

    df.to_csv(window_output_file, index=False)
    features1.to_csv(window_output_file, index=False)
    print(f"archivo guardado en: {output_file}")

    return str(output_file)