"""Modulo per il calcolo dei KPI e la generazione della Gold Layer.

Calcula metriche di business aggregate dai dati puliti della Silver Layer
per supportare analisi e reporting.
"""
import io
import logging
from typing import Dict

import numpy as np
import pandas as pd
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

# Configurazione logging
logger = logging.getLogger(__name__)

# Costanti
TABLES_TO_READ = ['bookings', 'payments', 'customers', 'rooms']

# FUNZIONI PER IL CALCOLO DEI KPI

def calculate_daily_revenue(df_bookings: pd.DataFrame) -> pd.DataFrame:
    """Calcola il fatturato giornaliero dalle prenotazioni confermate.
    
    Args:
        df_bookings: DataFrame delle prenotazioni dalla Silver Layer
        
    Returns:
        DataFrame con date, fatturato lordo e conteggio prenotazioni
    """
    logger.info("Calcolo KPI: Daily Revenue")
    
    df_confirmed = df_bookings[df_bookings['status'] == 'confirmed'].copy()
    
    if df_confirmed.empty:
        logger.warning("Nessuna prenotazione confermata trovata")
        return pd.DataFrame(columns=['date', 'gross_revenue', 'bookings_count'])
    
    # Normalizza date per aggregazione giornaliera
    df_confirmed['date'] = pd.to_datetime(df_confirmed['checkin_date']).dt.date
    
    kpi = df_confirmed.groupby('date').agg(
        gross_revenue=('total_amount', 'sum'),
        bookings_count=('booking_id', 'count')
    ).reset_index().sort_values('date')
    
    logger.info(f"Daily Revenue: {len(kpi)} giorni, fatturato totale {kpi['gross_revenue'].sum():.2f}")
    return kpi

def calculate_cancellation_rate(df_bookings: pd.DataFrame) -> pd.DataFrame:
    """Calcola il tasso di cancellazione per canale di prenotazione.
    
    Args:
        df_bookings: DataFrame delle prenotazioni dalla Silver Layer
        
    Returns:
        DataFrame con source, totale prenotazioni, cancellazioni e percentuale
    """
    logger.info("Calcolo KPI: Cancellation Rate by Source")
    
    kpi = df_bookings.groupby('source').agg(
        total_bookings=('booking_id', 'count'),
        cancelled=('status', lambda s: (s == 'cancelled').sum())
    ).reset_index()
    
    # Calcola percentuale di cancellazione
    kpi['cancellation_rate_pct'] = np.where(
        kpi['total_bookings'] > 0,
        (kpi['cancelled'] / kpi['total_bookings']) * 100,
        0
    ).round(2)
    
    avg_cancellation = kpi['cancellation_rate_pct'].mean()
    logger.info(f"Cancellation Rate: {len(kpi)} sources, tasso medio {avg_cancellation:.2f}%")
    
    return kpi

def calculate_collection_rate(df_bookings: pd.DataFrame, df_payments: pd.DataFrame) -> pd.DataFrame:
    """Calcola il tasso di incasso per hotel.
    
    Args:
        df_bookings: DataFrame delle prenotazioni dalla Silver Layer
        df_payments: DataFrame dei pagamenti dalla Silver Layer
        
    Returns:
        DataFrame con hotel_id, valore prenotazioni, pagamenti e tasso incasso
    """
    logger.info("Calcolo KPI: Collection Rate by Hotel")
    
    # Aggrega valore totale prenotazioni per hotel
    bookings_value = df_bookings.groupby('hotel_id')['total_amount'].sum().reset_index(name='total_bookings_value')
    
    # Join pagamenti con hotel_id dalle prenotazioni
    payments_with_hotel = pd.merge(
        df_payments, 
        df_bookings[['booking_id', 'hotel_id']], 
        on='booking_id', 
        how='inner'
    )
    
    # Aggrega pagamenti per hotel
    payments_value = payments_with_hotel.groupby('hotel_id')['amount'].sum().reset_index(name='total_payments_value')
    
    # Calcola collection rate
    kpi = pd.merge(bookings_value, payments_value, on='hotel_id', how='left').fillna(0)
    kpi['collection_rate'] = np.where(
        kpi['total_bookings_value'] > 0,
        kpi['total_payments_value'] / kpi['total_bookings_value'],
        0
    )
    
    avg_collection = kpi['collection_rate'].mean()
    logger.info(f"Collection Rate: {len(kpi)} hotel, tasso medio {avg_collection:.2%}")
    
    return kpi

def calculate_overbooking_alerts(df_bookings: pd.DataFrame) -> pd.DataFrame:
    """Identifica prenotazioni sovrapposte per la stessa camera.
    
    Args:
        df_bookings: DataFrame delle prenotazioni dalla Silver Layer
        
    Returns:
        DataFrame con room_id, booking_id sovrapposti e date di overlap
    """
    logger.info("Calcolo KPI: Overbooking Alerts")
    
    if df_bookings.empty:
        return pd.DataFrame(columns=['room_id', 'booking_id_1', 'booking_id_2', 'overlap_start', 'overlap_end'])
    
    # Self-join per confrontare prenotazioni della stessa camera
    b_merged = pd.merge(df_bookings, df_bookings, on='room_id', suffixes=('_1', '_2'))
    
    # Filtra sovrapposizioni temporali reali
    overlap_filter = (
        (b_merged['booking_id_1'] < b_merged['booking_id_2']) &
        (pd.to_datetime(b_merged['checkin_date_1']) < pd.to_datetime(b_merged['checkout_date_2'])) &
        (pd.to_datetime(b_merged['checkout_date_1']) > pd.to_datetime(b_merged['checkin_date_2']))
    )
    
    df_overlaps = b_merged[overlap_filter].copy()
    
    if not df_overlaps.empty:
        # Calcola finestre di sovrapposizione
        df_overlaps['overlap_start'] = df_overlaps[['checkin_date_1', 'checkin_date_2']].max(axis=1)
        df_overlaps['overlap_end'] = df_overlaps[['checkout_date_1', 'checkout_date_2']].min(axis=1)
    
    kpi = df_overlaps[['room_id', 'booking_id_1', 'booking_id_2', 'overlap_start', 'overlap_end']] if not df_overlaps.empty else pd.DataFrame(columns=['room_id', 'booking_id_1', 'booking_id_2', 'overlap_start', 'overlap_end'])
    
    logger.info(f"Overbooking Alerts: {len(kpi)} sovrapposizioni rilevate")
    return kpi

def calculate_customer_value(df_bookings: pd.DataFrame, df_customers: pd.DataFrame) -> pd.DataFrame:
    """Calcola metriche di valore per cliente.
    
    Args:
        df_bookings: DataFrame delle prenotazioni dalla Silver Layer
        df_customers: DataFrame dei clienti dalla Silver Layer
        
    Returns:
        DataFrame con customer_id, conteggio prenotazioni, fatturato e ticket medio
    """
    logger.info("Calcolo KPI: Customer Value")
    
    # Join prenotazioni con dati clienti
    merged_df = pd.merge(df_bookings, df_customers, on='customer_id', how='inner')
    
    # Aggrega metriche per cliente
    kpi = merged_df.groupby(['customer_id', 'first_name', 'last_name', 'email']).agg(
        bookings_count=('booking_id', 'count'),
        revenue_sum=('total_amount', 'sum')
    ).reset_index()
    
    # Calcola ticket medio
    kpi['avg_ticket'] = np.where(
        kpi['bookings_count'] > 0,
        kpi['revenue_sum'] / kpi['bookings_count'],
        0
    )
    
    # Ordina per valore decrescente
    kpi = kpi.sort_values('revenue_sum', ascending=False)
    
    top_revenue = kpi['revenue_sum'].sum()
    logger.info(f"Customer Value: {len(kpi)} clienti, fatturato totale {top_revenue:.2f}")
    
    return kpi


# FUNZIONE PRINCIPALE PER AIRFLOW (VERSIONE PANDAS)

def run_gold_kpi_generation_task_pandas() -> None:
    """Genera i KPI della Gold Layer dai dati puliti della Silver Layer.
    
    Calcola 5 KPI principali:
    1. Daily Revenue - fatturato giornaliero
    2. Cancellation Rate - tasso cancellazioni per source
    3. Collection Rate - tasso incasso per hotel
    4. Overbooking Alerts - sovrapposizioni prenotazioni
    5. Customer Value - valore clienti
    
    Raises:
        Exception: Se si verificano errori durante la generazione
    """
    logger.info("Avvio task di generazione della GOLD LAYER")
    
    CONTAINER_NAME = "datalake"
    SILVER_BASE_PATH = "silver"
    GOLD_BASE_PATH = "gold"
    
    try:
        # Connessione Azure Storage
        hook = WasbHook(wasb_conn_id='azure_storage_connection')
        client = hook.get_conn()
        
        # Caricamento dati dalla Silver Layer
        dataframes_silver: Dict[str, pd.DataFrame] = {}
        
        for table in TABLES_TO_READ:
            logger.info(f"Lettura '{table}' dalla Silver Layer")
            
            blob_name = f"{SILVER_BASE_PATH}/{table}/{table}.csv"
            blob_downloader = client.get_blob_client(CONTAINER_NAME, blob_name).download_blob()
            stream = io.BytesIO(blob_downloader.readall())
            df = pd.read_csv(stream)
            dataframes_silver[table] = df
            
            logger.info(f"Caricati {len(df)} record per {table}")
        
        # Calcolo KPI
        logger.info("Inizio calcolo KPI")
        
        kpi_map = {
            "daily_revenue": calculate_daily_revenue(dataframes_silver['bookings']),
            "cancellation_rate_by_source": calculate_cancellation_rate(dataframes_silver['bookings']),
            "collection_rate_by_hotel": calculate_collection_rate(
                dataframes_silver['bookings'], 
                dataframes_silver['payments']
            ),
            "overbooking_alerts": calculate_overbooking_alerts(dataframes_silver['bookings']),
            "customer_value": calculate_customer_value(
                dataframes_silver['bookings'], 
                dataframes_silver['customers']
            ),
        }
        
        # Scrittura nella Gold Layer
        logger.info("Scrittura KPI nella Gold Layer")
        
        for name, df_kpi in kpi_map.items():
            if df_kpi.empty:
                logger.warning(f"KPI {name} è vuoto, skip scrittura")
                continue
                
            logger.info(f"Salvando KPI {name}: {len(df_kpi)} record")
            
            # Prepara buffer CSV
            output_buffer = io.BytesIO()
            output_buffer.write(df_kpi.to_csv(index=False, encoding='utf-8').encode('utf-8'))
            output_buffer.seek(0)
            
            # Upload su Azure
            gold_blob_name = f"{GOLD_BASE_PATH}/{name}/{name}.csv"
            blob_client = client.get_blob_client(container=CONTAINER_NAME, blob=gold_blob_name)
            blob_client.upload_blob(output_buffer.getvalue(), overwrite=True)
            
            logger.info(f"KPI {name} salvato con successo")
        
        logger.info("Generazione della GOLD LAYER completata con successo")
        
    except Exception as e:
        logger.error(f"Errore durante la generazione Gold Layer: {e}")
        raise