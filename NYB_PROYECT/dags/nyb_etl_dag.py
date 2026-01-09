from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime 
import sys
from pathlib import Path 

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

from extract.extract_nyb import extract_nyb
from transform.transform_nyb import transform_nyb
from load.load_nyb import load_nyb

with DAG(
    dag_id="ytp_etl_dag",
    start_date=datetime(2024,1, 1),
    schedule="@hourly",
    catchup=False, 
    tags=["ytp", "etl"],
) as dags:
    
    extract = PythonOperator(
        task_id="extract_nyb",
        python_callable=extract_nyb,
    )

    transform = PythonOperator(
        task_id="transform_nyb",
        python_callable=transform_nyb,
    )

    load = PythonOperator(
        task_id="load_nyb",
        python_callable=load_nyb,
    )

    extract >> transform >> load 

    #falta la funcion que los conecta y que falta en
    #la parte de extract