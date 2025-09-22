# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a GlobalStay data engineering project implementing a medallion architecture (Bronze/Silver/Gold) ETL pipeline for hotel booking data analysis. The project processes 5 CSV files with intentionally embedded data quality issues to demonstrate data cleaning and transformation capabilities.

## Architecture

**Data Pipeline Flow:**
- **Bronze Layer**: Raw CSV ingestion from Azure Storage landing-zone with `ingestion_date` column added
- **Silver Layer**: Data quality transformations and anomaly flagging 
- **Gold Layer**: Business KPI calculations and aggregated metrics

**Orchestration**: Apache Airflow DAG with 3 sequential tasks:
1. `ingest_to_bronze_layer` → `01_ingestion.py`
2. `transform_to_silver_layer_spark` → `02_data_quality.py` 
3. `aggregate_to_gold_layer_spark` → `03_kpis.py`

## Data Schema

Core entities: hotels, rooms, customers, bookings, payments with intentional anomalies:
- Invalid country codes ('XX')
- Duplicate records
- Negative amounts
- Date inversions (checkin > checkout)
- Invalid currencies ('XXX', 'ZZZ')
- Orphaned foreign keys

## Key Implementation Patterns

**Azure Integration:**
- Uses `WasbHook` with connection ID `azure_storage_connection`
- Container structure: `landing-zone` → `datalake/bronze/` → `datalake/silver/` → `datalake/gold/`
- In-memory CSV processing with `io.BytesIO` streams

**Data Quality Rules:**
- Silver layer adds `dq_*` columns for anomaly tracking
- Bronze preserves all original data including errors
- Specific cleaning functions per table with business rules

**Gold Layer KPIs:**
1. Daily revenue aggregation (confirmed bookings only)
2. Cancellation rates by booking source
3. Collection rates by hotel
4. Overbooking alerts (room overlap detection)
5. Customer lifetime value analysis

## Linee Guida per lo Sviluppo

**IMPORTANTE: Tutta la documentazione e i commenti devono essere scritti in italiano.**

### Principi Fondamentali
- DRY, KISS, YAGNI, SOLID
- Conformità PEP 8
- Codice production-ready fin dall'inizio
- Nessun emoji o caratteri non-ASCII nel codice

### PySpark & Big Data
- Usare DataFrames invece di RDD
- Broadcast per lookup piccoli: `broadcast(small_df)`
- Partizionamento intelligente: `repartition()` per shuffle pesanti, `coalesce()` per riduzione
- Cache strategica: `.cache()` o `.persist()` per DataFrames riutilizzati
- Operazioni su colonne invece di UDF quando possibile
- Schema esplicito: sempre definire `StructType`

### Integrazione MLflow
```python
# Tracciare sempre gli esperimenti
with mlflow.start_run(run_name="nome_descrittivo"):
    mlflow.log_params(params)
    mlflow.log_metrics(metrics)
    mlflow.log_artifact(artifact_path)
```
- Versionare dataset con `mlflow.data`
- Tag per run: environment, data_version, model_type
- Registrare modelli production nel Model Registry

### Pattern di Feature Engineering
- Transformer stateless come funzioni
- Approccio pipeline: `Pipeline([('scaler', StandardScaler()), ...])`
- Feature store per riusabilità
- Feature temporali: window functions per time series
- Encoding categorico: target encoding con CV
- Validazione feature: assert per colonne/tipi attesi

### Validazione Dati
- Contratti schema con Pandera o Great Expectations
- Controllare distribuzioni: `df.describe()`, conteggi nulli
- Regole business come assertions
- Metriche qualità dati loggate in MLflow
- Fallimento rapido su violazioni schema

### Gestione Configurazione
```python
# config.py
@dataclass
class Config:
    MODEL_PATH: str = os.getenv("MODEL_PATH", "models/")
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "1000"))
```
- Config specifiche per ambiente
- Nessun path/credenziali hardcoded
- Pydantic per validazione
- Modulo config centralizzato

### Logging & Monitoraggio
```python
logger = logging.getLogger(__name__)
# Log ai confini
logger.info(f"Elaborazione di {len(df)} record")
# Metriche da tracciare
logger.info(f"Uso memoria: {df.memory_usage().sum() / 1e6:.2f} MB")
```
- Logging strutturato (JSON)
- Log data lineage
- Metriche performance
- Tassi errore e qualità dati

### Pattern Gestione Errori
```python
@retry(max_attempts=3, backoff=exponential)
def read_data(path: str) -> DataFrame:
    try:
        return spark.read.parquet(path)
    except AnalysisException as e:
        logger.error(f"Fallita lettura {path}: {e}")
        raise DataIngestionError(f"Impossibile accedere a {path}") from e
```
- Tipi eccezioni specifici
- Retry con exponential backoff
- Circuit breaker per servizi esterni
- Degradazione elegante

### Gestione Risorse
- Impostare config Spark esplicitamente
- Limiti memoria: `spark.conf.set("spark.sql.adaptive.enabled", "true")`
- Policy auto-scaling
- Pulizia: `df.unpersist()`, `spark.catalog.clearCache()`
- Monitorare metriche cluster

### Struttura Notebook Jupyter
```markdown
# 1. Setup
# 2. Caricamento Dati
# 3. EDA
# 4. Feature Engineering
# 5. Modellazione
# 6. Valutazione
# 7. Export
```
- Sezioni chiare con header markdown
- Parametri in alto
- Nessun side effect tra celle
- Esportare script puliti: `nbconvert`

### Anti-pattern da Evitare
- ❌ Stato mutabile globale
- ❌ Pandas per big data
- ❌ Modelli non versionati
- ❌ Print debugging in production
- ❌ Cattura Exception generica
- ❌ Notebook come codice production

## Project Structure

```
airflow_orchestration/
├── dags/globalstay_etl_dag.py    # Main Airflow DAG
└── scripts/
    ├── 01_ingestion.py           # Bronze layer ingestion
    ├── 02_data_quality.py        # Silver layer transformations  
    └── 03_kpis.py               # Gold layer KPI calculations
data/                            # Source CSV files with schema
```

## Airflow Configuration

**DAG Details:**
- ID: `globalstay_full_etl_pipeline`
- Schedule: Manual trigger (`schedule=None`)
- Tags: `['globalstay', 'project', 'azure']`
- Script path: `/home/dataeng/airflow/dags/scripts`

**Required Connections:**
- `azure_storage_connection`: Azure Storage account for WASB operations

## Setup Ambiente di Sviluppo

### Creazione Ambiente Conda
```bash
# Creare nuovo ambiente
conda env create -f environment.yml

# Attivare ambiente
conda activate globalstay-etl

# Verificare installazione
python -c "import airflow, pandas, numpy; print('Setup completato')"
```

### Configurazione Airflow Locale
```bash
# Inizializzare database Airflow
export AIRFLOW_HOME=~/airflow
airflow db init

# Creare utente admin
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com

# Avviare webserver e scheduler
airflow webserver --port 8080 &
airflow scheduler &
```