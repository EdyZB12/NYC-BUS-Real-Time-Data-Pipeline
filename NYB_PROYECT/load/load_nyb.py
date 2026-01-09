import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from pathlib import Path
from dotenv import load_dotenv


def load_nyb():
    load_dotenv()

    BASE_DIR = Path(__file__).resolve().parents[1]
    PROCESSED_DIR = BASE_DIR / "data" / "processed"

    DB_CONFIG = {
        "host": os.getenv("DB_HOST", "postgres"),  # 👈 Docker
        "port": int(os.getenv("DB_PORT", 5432)),
        "user": os.getenv("DB_USER", "airflow"),
        "password": os.getenv("DB_PASSWORD", "airflow"),
        "database": os.getenv("DB_NAME", "airflow"),
    }

    # -----------------------
    # 1️⃣ Crear tablas
    # -----------------------
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bus_positions(
        vehicle_id TEXT,
        recorded_at TIMESTAMP,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        line_name TEXT,
        stop_name TEXT,
        delay_seconds DOUBLE PRECISION
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bus_features_1min(
        window_start TIMESTAMP,
        vehicle_id TEXT,
        avg_speed_m_per_min DOUBLE PRECISION,
        avg_delay_s DOUBLE PRECISION,
        samples INTEGER
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bus_spatial_density_1min(
        window_start TIMESTAMP,
        grid_latitude DOUBLE PRECISION,
        grid_longitude DOUBLE PRECISION,
        bus_count INTEGER
    );
    """)

    conn.commit()
    cursor.close()
    conn.close()

    # -----------------------
    # 2️⃣ Motor SQLAlchemy
    # -----------------------
    engine = create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    # -----------------------
    # 3️⃣ Utilidad: último CSV
    # -----------------------
    def latest_csv(pattern: str) -> Path:
        files = sorted(PROCESSED_DIR.glob(pattern))
        if not files:
            raise FileNotFoundError(f"No se encontró CSV para {pattern}")
        return files[-1]

    # -----------------------
    # 4️⃣ Carga de datos
    # -----------------------

    # Tabla 1: posiciones
    df_positions = pd.read_csv(
        latest_csv("nyb_bus_processed_*.csv"),
        parse_dates=["recorded_at"]
    )

    df_positions = df_positions[
        ["vehicle_id", "recorded_at", "latitude", "longitude",
         "line_name", "stop_name", "delay_seconds"]
    ]

    df_positions.to_sql(
        "bus_positions",
        engine,
        if_exists="append",
        index=False
    )

    # Tabla 2: features temporales
    df_features = pd.read_csv(
        latest_csv("nyb_bus_windowed_*.csv"),
        parse_dates=["recorded_at"]
    )

    df_features.rename(columns={"recorded_at": "window_start"}, inplace=True)

    df_features = df_features[
        ["window_start", "vehicle_id",
         "avg_speed_m_per_min", "avg_delay_s", "samples"]
    ]

    df_features.to_sql(
        "bus_features_1min",
        engine,
        if_exists="append",
        index=False
    )

    # Tabla 3: densidad espacial
    df_density = pd.read_csv(
        latest_csv("nyb_bus_windowed2_*.csv"),
        parse_dates=["recorded_at"]
    )

    df_density.rename(columns={"recorded_at": "window_start"}, inplace=True)

    df_density = df_density[
        ["window_start", "grid_latitude", "grid_longitude", "vehicles_per_cell"]
    ]

    df_density.rename(columns={"vehicles_per_cell": "bus_count"}, inplace=True)

    df_density.to_sql(
        "bus_spatial_density_1min",
        engine,
        if_exists="append",
        index=False
    )

    print("Carga completada en las 3 tablas correctamente")

#vamos a juntar todo, es decir a orquestar 

##def main():
#    create_tables()
#
#    engine=create_engine(
##        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
#       f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
##   )
#
#    load_bus_positions(find_latest_csv("nyb_bus_processed_*.csv"), engine)
#    load_bus_features_1min(find_latest_csv("nyb_bus_windowed_*.csv"), engine)
#    load_bus_spatial_density_1min(find_latest_csv("nyb_bus_windowed2_*.csv"), engine)
#
#if __name__ == "__main__":
#    main()