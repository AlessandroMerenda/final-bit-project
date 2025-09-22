"""DAG Airflow per la pipeline ETL GlobalStay.

Implementa l'architettura medallion (Bronze/Silver/Gold) per l'elaborazione
dei dati di prenotazioni alberghiere con data quality e calcolo KPI.
"""
import importlib.util
import logging
import os
import sys
from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# Configurazione logging
logger = logging.getLogger(__name__)

# Configurazione path dinamico per gli script
DAGS_SCRIPTS_PATH: str = os.path.join(os.path.dirname(__file__), '..', 'scripts')
DAGS_SCRIPTS_PATH = os.path.abspath(DAGS_SCRIPTS_PATH)

if DAGS_SCRIPTS_PATH not in sys.path:
    sys.path.append(DAGS_SCRIPTS_PATH)
    logger.info(f"Aggiunto path scripts: {DAGS_SCRIPTS_PATH}")

# Caricamento dinamico moduli per gestire nomi file con numeri
try:
    logger.info("Caricamento moduli ETL")
    
    # Carica 01_ingestion.py
    spec_ingestion = importlib.util.spec_from_file_location(
        "ingestion", 
        os.path.join(DAGS_SCRIPTS_PATH, "01_ingestion.py")
    )
    if spec_ingestion is None:
        raise ImportError("File 01_ingestion.py non trovato")
    ingestion_module = importlib.util.module_from_spec(spec_ingestion)
    spec_ingestion.loader.exec_module(ingestion_module)
    logger.info("Modulo ingestion caricato")

    # Carica 02_data_quality.py  
    spec_quality = importlib.util.spec_from_file_location(
        "data_quality", 
        os.path.join(DAGS_SCRIPTS_PATH, "02_data_quality.py")
    )
    if spec_quality is None:
        raise ImportError("File 02_data_quality.py non trovato")
    quality_module = importlib.util.module_from_spec(spec_quality)
    spec_quality.loader.exec_module(quality_module)
    logger.info("Modulo data_quality caricato")

    # Carica 03_kpis.py
    spec_kpis = importlib.util.spec_from_file_location(
        "kpis", 
        os.path.join(DAGS_SCRIPTS_PATH, "03_kpis.py")
    )
    if spec_kpis is None:
        raise ImportError("File 03_kpis.py non trovato")
    kpis_module = importlib.util.module_from_spec(spec_kpis)
    spec_kpis.loader.exec_module(kpis_module)
    logger.info("Modulo kpis caricato")
    
except Exception as e:
    logger.error(f"Errore nel caricamento dei moduli: {e}")
    logger.error(f"Path scripts: {DAGS_SCRIPTS_PATH}")
    raise

# Configurazione DAG
DEFAULT_ARGS = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='globalstay_full_etl_pipeline',
    default_args=DEFAULT_ARGS,
    description='Pipeline ETL GlobalStay con architettura medallion Bronze/Silver/Gold',
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Trigger manuale
    catchup=False,
    max_active_runs=1,
    tags=['globalstay', 'etl', 'azure', 'medallion']
) as dag:
    
    # Task Bronze Layer: Ingestion
    ingest_to_bronze_layer = PythonOperator(
        task_id='ingest_to_bronze_layer',
        python_callable=ingestion_module.run_bronze_ingestion_task,
        doc_md="""### Ingestion Bronze Layer
        Carica i dati grezzi da Azure Storage landing-zone 
        al container datalake/bronze/ aggiungendo timestamp di ingestion.
        """
    )
    
    # Task Silver Layer: Data Quality
    transform_to_silver_layer = PythonOperator(
        task_id='transform_to_silver_layer',
        python_callable=quality_module.run_silver_transformation_task,
        doc_md="""### Trasformazione Silver Layer
        Applica regole di data quality ai dati Bronze:
        - Pulizia duplicati
        - Correzione date invertite
        - Normalizzazione valute
        - Aggiunta flag anomalie
        """
    )
    
    # Task Gold Layer: KPI Generation
    generate_gold_layer = PythonOperator(
        task_id='generate_gold_layer_kpis',
        python_callable=kpis_module.run_gold_kpi_generation_task_pandas,
        doc_md="""### Generazione Gold Layer
        Calcola KPI di business dai dati Silver:
        - Daily Revenue
        - Cancellation Rate
        - Collection Rate
        - Overbooking Alerts
        - Customer Value
        """
    )

    # Definizione dipendenze pipeline
    ingest_to_bronze_layer >> transform_to_silver_layer >> generate_gold_layer