# Code Generation Guidelines

## Core Principles
- DRY, KISS, YAGNI, SOLID
- PEP 8 compliance
- Production-ready from start
- No emojis or non-ASCII in code

## PySpark & Big Data
- Use DataFrames over RDDs
- Broadcast small lookups: `broadcast(small_df)`
- Partition wisely: `repartition()` for heavy shuffles, `coalesce()` for reduction
- Cache strategically: `.cache()` or `.persist()` for reused DataFrames
- Column operations over UDFs when possible
- Schema explicit: always define `StructType`

## MLflow Integration
```python
# Always track experiments
with mlflow.start_run(run_name="descriptive_name"):
    mlflow.log_params(params)
    mlflow.log_metrics(metrics)
    mlflow.log_artifact(artifact_path)
```
- Version datasets with `mlflow.data`
- Tag runs: environment, data_version, model_type
- Register production models in Model Registry

## Feature Engineering Patterns
- Stateless transformers as functions
- Pipeline approach: `Pipeline([('scaler', StandardScaler()), ...])`
- Feature stores for reusability
- Temporal features: window functions for time series
- Categorical encoding: target encoding with CV
- Feature validation: assert expected columns/types

## Data Validation
- Schema contracts with Pandera or Great Expectations
- Check distributions: `df.describe()`, null counts
- Business rules as assertions
- Data quality metrics logged to MLflow
- Fail fast on schema violations

## Configuration Management
```python
# config.py
@dataclass
class Config:
    MODEL_PATH: str = os.getenv("MODEL_PATH", "models/")
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "1000"))
```
- Environment-specific configs
- No hardcoded paths/credentials
- Pydantic for validation
- Centralized config module

## Logging & Monitoring
```python
logger = logging.getLogger(__name__)
# Log at boundaries
logger.info(f"Processing {len(df)} records")
# Metrics to track
logger.info(f"Memory usage: {df.memory_usage().sum() / 1e6:.2f} MB")
```
- Structured logging (JSON)
- Log data lineage
- Performance metrics
- Error rates and data quality

## Error Handling Patterns
```python
@retry(max_attempts=3, backoff=exponential)
def read_data(path: str) -> DataFrame:
    try:
        return spark.read.parquet(path)
    except AnalysisException as e:
        logger.error(f"Failed to read {path}: {e}")
        raise DataIngestionError(f"Cannot access {path}") from e
```
- Specific exception types
- Retry with exponential backoff
- Circuit breakers for external services
- Graceful degradation

## Resource Management
- Set Spark configs explicitly
- Memory limits: `spark.conf.set("spark.sql.adaptive.enabled", "true")`
- Auto-scaling policies
- Clean up: `df.unpersist()`, `spark.catalog.clearCache()`
- Monitor cluster metrics

## Jupyter Notebook Structure
```markdown
# 1. Setup
# 2. Data Loading
# 3. EDA
# 4. Feature Engineering
# 5. Modeling
# 6. Evaluation
# 7. Export
```
- Clear sections with markdown headers
- Parameters at top
- No side effects between cells
- Export clean scripts: `nbconvert`

## Anti-patterns to Avoid
- ❌ Global mutable state
- ❌ Pandas for big data
- ❌ Unversioned models
- ❌ Print debugging in production
- ❌ Catching generic Exception
- ❌ Notebooks as production code