"""Modulo per l'ingestion dei dati dalla Landing Zone alla Bronze Layer.

Questo modulo gestisce il caricamento dei file CSV da Azure Storage
container 'landing-zone' al container 'datalake/bronze/' aggiungendo
metadati di ingestione.
"""
import io
import logging
from datetime import datetime
from typing import List

import pandas as pd
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

# Configurazione logging
logger = logging.getLogger(__name__)

def run_bronze_ingestion_task() -> None:
    """Esegue l'ingestion dei dati dalla Landing Zone alla Bronze Layer.
    
    Legge tutti i file CSV dal container 'landing-zone' su Azure Storage,
    aggiunge la colonna 'ingestion_date' e salva i dati nella Bronze Layer
    mantenendo la struttura originale senza applicare trasformazioni.
    
    Raises:
        Exception: Se si verificano errori durante l'ingestion
    """
    logger.info("Avvio task di ingestion da Azure Storage (Landing Zone -> Bronze)")
    
    # Configurazione connessioni e container
    LANDING_ZONE_CONTAINER = "landing-zone"
    BRONZE_CONTAINER = "datalake"
    
    try:
        hook = WasbHook(wasb_conn_id='azure_storage_connection')
        client = hook.get_conn()
        
        ingestion_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Data ingestion: {ingestion_date}")

        # Recupera lista file CSV dalla landing zone
        blob_list = client.get_container_client(LANDING_ZONE_CONTAINER).list_blobs()
        csv_files: List[str] = [blob.name for blob in blob_list if blob.name.endswith('.csv')]
        
        if not csv_files:
            logger.warning("Nessun file CSV trovato nella landing zone")
            return
            
        logger.info(f"Trovati {len(csv_files)} file CSV da processare: {csv_files}")

        # Processa ogni file CSV
        for blob_name in csv_files:
            table_name = blob_name.replace('.csv', '')
            logger.info(f"Elaborazione {blob_name}")
            
            try:
                # Download file CSV in memoria come stream
                blob_downloader = client.get_blob_client(LANDING_ZONE_CONTAINER, blob_name).download_blob()
                stream = io.BytesIO(blob_downloader.readall())
                df = pd.read_csv(stream)
                
                # Aggiunge metadati di ingestion
                df['ingestion_date'] = ingestion_date
                
                # Log delle metriche
                logger.info(f"Elaborazione {table_name}: {len(df)} record, {df.memory_usage(deep=True).sum() / 1e6:.2f} MB")
                
                # Converte DataFrame in buffer CSV
                output_buffer = df.to_csv(index=False, encoding='utf-8').encode('utf-8')
                
                # Upload alla Bronze Layer
                bronze_blob_name = f"bronze/{table_name}/{table_name}.csv"
                blob_client = client.get_blob_client(
                    container=BRONZE_CONTAINER,
                    blob=bronze_blob_name
                )
                blob_client.upload_blob(output_buffer, overwrite=True)
                
                logger.info(f"File {blob_name} caricato con successo in {bronze_blob_name}")
                
            except Exception as e:
                logger.error(f"Errore nell'elaborazione di {blob_name}: {e}")
                raise
        
        logger.info("Task di ingestion Bronze completato con successo")
        
    except Exception as e:
        logger.error(f"Errore durante l'ingestion: {e}")
        raise