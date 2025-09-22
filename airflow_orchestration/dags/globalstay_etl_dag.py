import sys
from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# Importa le funzioni che verranno eseguite dai task
import importlib.util
import os

# Carica i moduli dinamicamente per gestire nomi con numeri
scripts_path = DAGS_SCRIPTS_PATH

# Carica 01_ingestion.py
spec_ingestion = importlib.util.spec_from_file_location("ingestion", os.path.join(scripts_path, "01_ingestion.py"))
ingestion_module = importlib.util.module_from_spec(spec_ingestion)
spec_ingestion.loader.exec_module(ingestion_module)

# Carica 02_data_quality.py  
spec_quality = importlib.util.spec_from_file_location("data_quality", os.path.join(scripts_path, "02_data_quality.py"))
quality_module = importlib.util.module_from_spec(spec_quality)
spec_quality.loader.exec_module(quality_module)

# Carica 03_kpis.py
spec_kpis = importlib.util.spec_from_file_location("kpis", os.path.join(scripts_path, "03_kpis.py"))
kpis_module = importlib.util.module_from_spec(spec_kpis)
spec_kpis.loader.exec_module(kpis_module)

with DAG(
    dag_id='globalstay_full_etl_pipeline',
    start_date=datetime(2023, 1, 1), # È buona norma usare una start_date nel passato
    schedule=None,
    catchup=False,
    tags=['globalstay', 'project', 'azure']
) as dag:
    
    ingest_to_bronze_layer = PythonOperator(
        task_id='ingest_to_bronze_layer',
        python_callable=ingestion_module.run_bronze_ingestion_task
    )
    
    transform_to_silver_layer = PythonOperator(
        task_id='transform_to_silver_layer_spark',
        python_callable=quality_module.run_silver_transformation_task
    )
    
    generate_gold_layer = PythonOperator(
        task_id='aggregate_to_gold_layer_spark',
        python_callable=kpis_module.run_gold_kpi_generation_task_pandas
    )

    # definizione delle dipendenze
    ingest_to_bronze_layer >> transform_to_silver_layer >> generate_gold_layer