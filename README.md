# Core Utils

A comprehensive Python utility library for building data pipelines with Airflow, dbt, Snowflake, and PostgreSQL. This toolkit helps generate configurations, templates, Airflow DAGs, database DDLs, and dbt models for data ingestion workflows.

## Features

- **Configuration Generation**: Automatically generate pipeline configurations from data files with schema inference
- **Airflow DAG Generation**: Create production-ready Airflow DAGs with custom operators
- **dbt Model Generation**: Generate dbt models for Mirror and Stage layers with transformations
- **Snowflake Pipeline Generation**: Generate Snowpipe, streams, and tasks for native Snowflake pipelines
- **Database DDLs**: Generate PostgreSQL and Snowflake table DDLs
- **Multi-Layer Architecture**: Support for Mirror (append-only historical) and Stage (SCD Type 2) layers
- **Database Support**: Works with both Snowflake and PostgreSQL
- **S3 Integration**: Optional S3 bucket support for file storage and retrieval
- **Schema Inference**: Automatic data type detection from CSV files
- **Data Quality**: Built-in schema validation, data checks, and hash-based change detection

## Architecture

### Data Layers

1. **Mirror Layer**: Append-only tables for historical data storage
   - Stores raw data with file metadata (FILE_DATE, FILENAME, FILE_ROW_NUMBER, etc.)
   - Tracks file dates, filenames, and row numbers
   - Includes hash-based change detection (UNIQUE_HASH_ID, ROW_HASH_ID)
   - Supports both Snowflake and PostgreSQL

2. **Stage Layer**: SCD Type 2 (Slowly Changing Dimension) implementation
   - Applies data transformations (select, filter, join, pivot, unpivot)
   - Tracks effective start/end dates (EFFECTIVE_START_DATE, EFFECTIVE_END_DATE)
   - Maintains active/inactive record flags (ACTIVE_FL)
   - Supports incremental updates with merge strategies

### Pipeline Types

1. **SNOWPIPE**: Native Snowflake pipeline
   - Snowpipe for continuous data ingestion
   - Streams for change data capture
   - Tasks for automated data processing
   - No Airflow required

2. **AIRFLOW**: Airflow-based pipeline
   - Custom Airflow operators for each pipeline stage
   - dbt integration for data transformation
   - Flexible task orchestration
   - Support for both Snowflake and PostgreSQL

## Installation

```bash
pip install core_utils
```

### Dependencies

- `custom-operators==1.0.0` - Custom Airflow operators for data pipeline tasks
- `ruamel.yaml` - YAML file handling with order preservation
- `pandas` - Data processing and schema inference
- Standard Python libraries: `json`, `os`, `pathlib`, `logging`, `csv`, `re`

## Core Components

### 1. ConfigTemplate (`generate_configs.py`)

Generates pipeline configurations from data files with automatic schema inference.

**Key Methods:**
- `generate_configs(configs_tmp_dir)`: Main method to generate all configuration files
- `get_mirror_schema(schema, db_type)`: Creates mirror layer schema with metadata columns
- `get_stage_schema(data_types, db_type)`: Creates stage layer schema with SCD columns
- `add_meta_cols(schema, layer, db_type)`: Adds layer-specific metadata columns

**Supported Pipeline Types:**
- `SNOWPIPE`: Snowflake native pipeline with Snowpipe, streams, and tasks
- `AIRFLOW`: Airflow-based pipeline with dbt integration

**Usage Example:**
```python
from core_utils.generate_configs import ConfigTemplate

config_template = ConfigTemplate(
    bucket="my-bucket",
    file_path="/path/to/data.csv",
    pipeline_type="AIRFLOW",
    dataset_name="my_dataset",
    db_type="SNOWFLAKE",
    start_date="2024,01,01",
    schedule_interval="0 23 * * 1-5",
    layer="Mirror -> Stage -> Standard",
    layer_0_db="MIRROR_DB",
    layer_1_db="STAGE_DB",
    encoding="utf-8"
)

configs_dir = config_template.generate_configs("/output/dir")
```

### 2. SnowflakePipeline (`generate_snowflake_pipeline.py`)

Generates complete Snowflake pipeline SQL including Snowpipe, streams, and tasks.

**Key Methods:**
- `get_stage_sql()`: Creates Snowflake stage SQL
- `get_snowpipe_sql(copy_statement)`: Creates Snowpipe SQL
- `get_stream_sql(stream_name, table_name)`: Creates stream SQL
- `get_task_sql(stream_name, task_name, table_name, table_schema, layer)`: Creates task SQL
- `get_all_sqls()`: Main method to generate all pipeline SQL

**Features:**
- Automatic Snowpipe creation with auto-ingest
- Stream-based change data capture
- Scheduled tasks for data processing
- File metadata tracking
- Validation procedures

**Usage Example:**
```python
from core_utils.generate_snowflake_pipeline import SnowflakePipeline

pipeline = SnowflakePipeline(
    bucket="my-bucket",
    dataset_path="/path/to/data",
    dataset_name="my_dataset",
    file_extension="csv",
    delimiter=",",
    mirror_schema=mirror_schema,
    file_schema=file_schema,
    stage_schema=stage_schema,
    schedule_interval="0 23 * * 1-5",
    layer="Mirror -> Stage -> Standard",
    layer_0_db="MIRROR_DB",
    layer_1_db="STAGE_DB"
)

pipeline_sql = pipeline.get_all_sqls()
```

### 3. DagGenerator (`dag_generator.py`)

Generates Airflow DAGs and database DDLs from configuration files.

**Key Methods:**
- `generate_dag(dataset_configs, dag_template)`: Creates Airflow DAG Python code
- `generate_ddls(database, schema, table_name, table_schema, layer, layer_name)`: Generates table DDLs
- `generate_dag_ddls()`: Main method to generate both DAG and DDL files

**Supported Tasks:**
- `acq_task`: File acquisition from local or S3
- `download_task`: File download to Airflow temp area
- `move_to_snowflake_task`: Move files to Snowflake internal stage
- `snowflake_schema_check_task`: Schema validation for Snowflake
- `copy_to_snowflake_task`: Copy data from stage to Snowflake tables
- `snowflake_mirror_task`: Load data to mirror layer
- `snowflake_stage_task`: Load data to stage layer with SCD Type 2
- `postgres_*_task`: Equivalent PostgreSQL tasks
- `*_tests_task`: Data quality tests for both layers

**Usage Example:**
```python
from core_utils.dag_generator import DagGenerator

dag_gen = DagGenerator(
    configs_dir="/path/to/configs",
    dataset_name="my_dataset"
)

dag_gen.generate_dag_ddls()
```

### 4. DBTMirrorModel (`dbt_models.py`)

Generates dbt models for Mirror and Stage layers with configurable materialization strategies.

**Key Methods:**
- `generate_mirror_model()`: Creates dbt model SQL for mirror layer
- `generate_stage_model()`: Creates dbt model SQL for stage layer with transformations
- `generate()`: Main method to generate all dbt artifacts (SQL, sources, tests)
- `get_tests_yml()`: Generates dbt test configurations
- `get_sources_yml()`: Generates dbt source configurations

**Configuration Parameters:**
- `materialization`: Materialization strategy (e.g., "incremental", "table")
- `scd_config`: SCD configuration for Type 2 implementation
- `db_type`: Database type ("SNOWFLAKE" or "POSTGRES")

**Features:**
- Automatic hash-based change detection (UNIQUE_HASH_ID, ROW_HASH_ID)
- Metadata columns tracking (CREATED_DTS, UPDATED_DTS, etc.)
- Support for data transformations (select, filter, join, pivot, unpivot)
- Database-specific SQL generation (PostgreSQL vs Snowflake)
- Configurable unique key and not null tests

**Usage Example:**
```python
from core_utils.dbt_models import DBTMirrorModel

dbt_model = DBTMirrorModel(
    configs=dataset_configs,
    layer="mirror",
    db_type="SNOWFLAKE",
    materialization="incremental",
    scd_config={
        "scd_columns": {"EFFECTIVE_START_DATE": "TIMESTAMP"},
        "excluded_columns": ["TEMP_COL"],
        "unique_key": ["ID"]
    }
)

dbt_model.generate()
```

### 5. ConfigReader (`config_reader.py`)

Reads and consolidates configuration files from the generated config structure.

**Key Methods:**
- `read_configs()`: Loads and merges all configuration JSON files

**Configuration Structure:**
```
dataset_name/
├── dataset_name.json          # Main dataset configuration
├── mirror/
│   ├── dataset_name_mirror_ver.json    # Version configuration
│   └── dataset_name_mirror_v1.json     # V1 mirror configuration
└── stage/
    ├── dataset_name_stage_ver.json     # Version configuration
    └── dataset_name_stage_v1.json      # V1 stage configuration
```

### 6. SnowflakeUtils (`snowflake_utils.py`)

Utility class for Snowflake-specific SQL generation.

**Key Methods:**
- `get_file_format_sql()`: Generates file format SQL
- `get_copy_into_table_sql()`: Generates COPY INTO SQL
- `get_mirror_stage_ddls()`: Generates table DDLs

**Usage Example:**
```python
from core_utils.snowflake_utils import SnowflakeUtils

util = SnowflakeUtils(
    stage_name="MY_DB.MY_SCHEMA.STG_MY_DATASET",
    table_name="MY_DB.MY_SCHEMA.T_ML_MY_DATASET"
)

file_format_sql = util.get_file_format_sql(
    file_format_name="MY_DB.MY_SCHEMA.FF_MY_DATASET",
    file_type="CSV",
    delimiter=",",
    skip_header=1
)
```

### 7. File Utilities (`file_utils.py`)

Utilities for file processing and schema inference.

**Key Functions:**
- `identify_delimiter(file_path)`: Detects CSV delimiter
- `infer_and_convert_data_types(csv_file_path)`: Infers data types from CSV
- `read_and_infer(file_path)`: Combined delimiter detection and schema inference
- `get_unique_keys(file_path, delimiter, header_line)`: Identifies unique key columns
- `get_file_name_pattern(file_name, file_date_format)`: Extracts filename pattern
- `write_to_json_file(data, file_path)`: Writes data to JSON file
- `write_to_file(data, file_path)`: Writes data to file

**Supported Data Type Mappings:**
- Pandas to Snowflake: int64→NUMBER, float64→FLOAT, bool→BOOLEAN, datetime64→TIMESTAMP, object→TEXT
- Pandas to PostgreSQL: int64→NUMERIC, float64→DOUBLE PRECISION, bool→BOOLEAN, datetime64→TIMESTAMP, object→TEXT

### 8. Meta Classes (`meta_classes.py`)

Dataclass definitions for structured configuration management.

**Classes:**
- `DatasetConfigs`: Main dataset configuration with task definitions
- `DatasetVersion`: Version control for schema changes
- `DatasetMirror`: Mirror layer table configuration
- `DatasetStage`: Stage layer table configuration with transformations

### 9. Constants (`constants.py`)

SQL templates and constant definitions for Snowflake pipelines.

**Templates:**
- `snowflake_stage_template`: Snowflake stage creation
- `snowflake_pipe_template`: Snowpipe creation
- Metadata column definitions for mirror and stage layers

## Pipeline Workflow

### 1. Configuration Generation
```
Data File → Delimiter Detection → Schema Inference → Config Generation → JSON Configs
```

### 2. Snowflake Pipeline Generation (SNOWPIPE)
```
JSON Configs → SnowflakePipeline → Stage + Snowpipe + Streams + Tasks → SQL Script
```

### 3. DAG and DDL Generation (AIRFLOW)
```
JSON Configs → DAG Generator → Airflow DAG + Database DDLs
```

### 4. dbt Model Generation
```
JSON Configs → DBT Model Generator → dbt SQL + Sources + Tests
```

### 5. Pipeline Execution
```
Airflow DAG → Custom Operators → Mirror Layer → Stage Layer
```

## Custom Operators Integration

The generated DAGs use the `custom-operators` library for data pipeline tasks:

**Snowflake Operators:**
- **AcquisitionOperator**: File presence checking from local or S3
- **DownloadOperator**: File download from S3/local to Airflow temp area
- **MoveFileToSnowflakeOperator**: Snowflake internal stage management
- **FileSnowflakeTableSchemaCheckOperator**: Schema validation between file and table
- **SnowflakeCopyOperator**: Data loading from stage to table
- **SnowflakeLoadToMirrorOperator**: Mirror layer population with hash-based change detection
- **SnowflakeLoadToStageOperator**: Stage layer SCD Type 2 loading
- **SnowflakeMirrorTestsOperator**: Data quality tests for mirror layer
- **SnowflakeStageTestsOperator**: Data quality tests for stage layer

**PostgreSQL Operators:**
- **FilePostgresTableSchemaCheckOperator**: Schema validation for PostgreSQL
- **CopyFileToPostgresOperator**: Direct file loading with COPY command
- **PostgresLoadToMirrorOperator**: Mirror layer population
- **PostgresLoadToStageOperator**: Stage layer SCD Type 2 loading
- **PostgresMirrorTestsOperator**: Data quality tests for mirror layer
- **PostgresStageTestsOperator**: Data quality tests for stage layer

## Database Support

### Snowflake
- Native Snowpipe support for continuous ingestion
- Internal and external stage management
- Stream-based change data capture
- Task automation with cron scheduling
- File format configuration
- COPY INTO for data loading
- MERGE statements for SCD Type 2

### PostgreSQL
- Direct file loading with COPY command
- Schema validation and data quality checks
- Incremental loading strategies
- SCD Type 2 implementation
- Support for various file formats

## Data Quality Features

- **Schema Validation**: Automatic schema checking between files and tables
- **Data Validation**: Row count and data integrity checks
- **Unique Key Testing**: Configurable unique key constraints with WARN severity
- **Not Null Testing**: Column-level not null constraints
- **Hash-based Change Detection**: MD5 hashing for row-level change tracking
- **File Metadata Tracking**: FILE_DATE, FILENAME, FILE_ROW_NUMBER, FILE_LAST_MODIFIED
- **Audit Columns**: CREATED_DTS, CREATED_BY, UPDATED_DTS, UPDATED_BY

## Configuration Examples

### Mirror Layer Configuration
```json
{
  "table_name": "T_ML_MY_DATASET",
  "table_schema": {
    "COLUMN1": "TEXT",
    "COLUMN2": "NUMBER",
    "COLUMN3": "TIMESTAMP"
  },
  "unique_keys": ["ID"],
  "file_format_params": {
    "delimiter": ",",
    "skip_header": 1,
    "compressed": true
  },
  "file_name_pattern": "data_{datetime_pattern}.csv",
  "datetime_pattern": "YYYY-MM-DD",
  "encoding": "utf-8"
}
```

### Stage Layer Configuration
```json
{
  "table_name": "T_STG_MY_DATASET",
  "table_schema": {
    "COLUMN1": "VARCHAR",
    "COLUMN2": "INTEGER",
    "COLUMN3": "TIMESTAMP"
  },
  "unique_keys": ["ID"],
  "transformations": [
    {
      "type": "select",
      "columns": ["COLUMN1", "COLUMN2", "COLUMN3"]
    },
    {
      "type": "filter",
      "condition": "COLUMN1 IS NOT NULL"
    }
  ]
}
```

### Dataset Configuration
```json
{
  "dataset_name": "my_dataset",
  "bucket": "my-bucket",
  "start_date": "2024,01,01",
  "load_historical_data": false,
  "snowflake_stage_name": "STG_MY_DATASET",
  "db_conn_id": "SNOWFLAKE_CONN_ID",
  "s3_connection_id": "aws_default",
  "tasks": [
    "acq_task",
    "download_task",
    "move_to_snowflake_task",
    "snowflake_schema_check_task",
    "copy_to_snowflake_task",
    "snowflake_file_mirror_data_check_task",
    "snowflake_mirror_task",
    "snowflake_mirror_tests_task",
    "snowflake_stage_task",
    "snowflake_stage_tests_task"
  ],
  "mirror_layer": {
    "database": "MIRROR_DB",
    "schema": "MIRROR"
  },
  "stage_layer": {
    "database": "STAGE_DB",
    "schema": "STAGE"
  },
  "schedule_interval": "0 23 * * 1-5"
}
```

## Output Structure

### Generated Configs (AIRFLOW)
```
generated_configs/
└── dataset_name/
    ├── dataset_name.json
    ├── mirror/
    │   ├── dataset_name_mirror_ver.json
    │   └── dataset_name_mirror_v1.json
    └── stage/
        ├── dataset_name_stage_ver.json
        └── dataset_name_stage_v1.json
```

### Generated Pipeline SQL (SNOWPIPE)
```
generated_configs/
└── dataset_name/
    └── pipeline_dataset_name.sql
```

### Generated DAGs and DDLs
```
generated_dags_ddls/
├── dataset_name_dag.py
├── T_ML_DATASET_TR.sql
├── T_ML_DATASET.sql
├── T_STG_DATASET.sql
└── STG_DATASET.sql
```

### Generated dbt Models
```
dbt/
└── models/
    ├── mirror/
    │   └── dataset_name/
    │       ├── T_ML_DATASET.sql
    │       ├── mirror_source_dataset_name.yml
    │       └── mirror_tests_dataset_name.yml
    └── stage/
        └── dataset_name/
            ├── T_STG_DATASET.sql
            ├── stage_source_dataset_name.yml
            └── stage_tests_dataset_name.yml
```

## Supported Transformations

The stage layer supports the following data transformations:

1. **Select**: Column selection
   ```json
   {"type": "select", "columns": ["COL1", "COL2"]}
   ```

2. **Filter**: Row filtering
   ```json
   {"type": "filter", "condition": "COL1 IS NOT NULL"}
   ```

3. **Join**: Table joins
   ```json
   {"type": "join", "table": "other_table", "on": "this.id = other_table.id"}
   ```

4. **Pivot**: Column pivoting
   ```json
   {"type": "pivot", "column": "category", "values": ["A", "B", "C"]}
   ```

5. **Unpivot**: Column unpivoting
   ```json
   {"type": "unpivot", "columns": ["COL1", "COL2"], "alias": ["metric", "value"]}
   ```

## License

MIT License

## Author

Ram Posam - posamram@gmail.com

## Repository

https://rposam-devops@dev.azure.com/rposam-devops/devops-project/_git/custom_utils