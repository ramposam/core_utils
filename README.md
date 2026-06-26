# Core Utils

A comprehensive Python utility library for building data pipelines with Streamlit, Airflow, dbt, Snowflake, and PostgreSQL. This toolkit helps generate configurations, templates, Airflow DAGs, and database DDLs for data ingestion workflows.

## Features

- **Configuration Generation**: Automatically generate pipeline configurations from data files
- **Airflow DAG Generation**: Create production-ready Airflow DAGs with custom operators
- **dbt Model Generation**: Generate dbt models for Mirror and Stage layers
- **Database DDLs**: Generate PostgreSQL and Snowflake table DDLs
- **Multi-Layer Architecture**: Support for Mirror (append-only historical) and Stage (SCD Type 2) layers
- **Database Support**: Works with both Snowflake and PostgreSQL
- **S3 Integration**: Optional S3 bucket support for file storage and retrieval

## Architecture

### Data Layers

1. **Mirror Layer**: Append-only tables for historical data storage
   - Stores raw data with file metadata
   - Tracks file dates, filenames, and row numbers
   - Includes hash-based change detection

2. **Stage Layer**: SCD Type 2 (Slowly Changing Dimension) implementation
   - Applies data transformations
   - Tracks effective start/end dates
   - Maintains active/inactive record flags
   - Supports incremental updates with merge strategies

## Installation

```bash
pip install core_utils
```

### Dependencies

- `custom-operators==1.0.0` - Custom Airflow operators for data pipeline tasks
- `ruamel.yaml` - YAML file handling with order preservation
- Standard Python libraries: `json`, `os`, `pathlib`, `logging`

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
    schedule_interval="0 23 * * 1-5"
)

configs_dir = config_template.generate_configs("/output/dir")
```

### 2. DagGenerator (`dag_generator.py`)

Generates Airflow DAGs and database DDLs from configuration files.

**Key Methods:**
- `generate_dag(dataset_configs, dag_template)`: Creates Airflow DAG Python code
- `generate_ddls(database, schema, table_name, table_schema, layer)`: Generates table DDLs
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

### 3. DBTMirrorModel (`dbt_models.py`)

Generates dbt models for Mirror and Stage layers with configurable materialization strategies.

**Key Methods:**
- `generate_mirror_model()`: Creates dbt model SQL for mirror layer
- `getnerate_stage_model()`: Creates dbt model SQL for stage layer with transformations
- `generate()`: Main method to generate all dbt artifacts (SQL, sources, tests)

**Configuration Parameters:**
- `materialization`: Materialization strategy (e.g., "incremental", "table")
- `scd_config`: SCD configuration for Type 2 implementation
- `db_type`: Database type ("SNOWFLAKE" or "POSTGRES")

**Features:**
- Automatic hash-based change detection (UNIQUE_HASH_ID, ROW_HASH_ID)
- Metadata columns tracking (CREATED_DTS, UPDATED_DTS, etc.)
- Support for data transformations (select, filter, join, pivot, unpivot)
- Database-specific SQL generation (PostgreSQL vs Snowflake)

**Usage Example:**
```python
from core_utils.dbt_models import DBTMirrorModel

dbt_model = DBTMirrorModel(
    configs=dataset_configs,
    layer="mirror",
    db_type="SNOWFLAKE",
    materialization="incremental",
    scd_config={"strategy": "type_2", "key_columns": ["id"]}
)

dbt_model.generate()
```

### 4. ConfigReader (`config_reader.py`)

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

### 5. Meta Classes (`meta_classes.py`)

Dataclass definitions for structured configuration management.

**Classes:**
- `DatasetConfigs`: Main dataset configuration with task definitions
- `DatasetVersion`: Version control for schema changes
- `DatasetMirror`: Mirror layer table configuration
- `DatasetStage`: Stage layer table configuration with transformations

## Pipeline Workflow

### 1. Configuration Generation
```
Data File → Schema Inference → Config Generation → JSON Configs
```

### 2. DAG and DDL Generation
```
JSON Configs → DAG Generator → Airflow DAG + Database DDLs
```

### 3. dbt Model Generation
```
JSON Configs → DBT Model Generator → dbt SQL + Sources + Tests
```

### 4. Pipeline Execution
```
Airflow DAG → Custom Operators → Mirror Layer → Stage Layer
```

## Custom Operators Integration

The generated DAGs use the `custom-operators` library for data pipeline tasks:

- **AcquisitionOperator**: File presence checking
- **DownloadOperator**: File download from S3/local
- **MoveFileToSnowflakeOperator**: Snowflake stage management
- **FileSnowflakeTableSchemaCheckOperator**: Schema validation
- **SnowflakeCopyOperator**: Data loading from stage
- **SnowflakeLoadToMirrorOperator**: Mirror layer population
- **SnowflakeLoadToStageOperator**: Stage layer SCD Type 2 loading
- **PostgreSQL equivalents**: Same functionality for PostgreSQL

## Database Support

### Snowflake
- Native Snowpipe support for continuous ingestion
- Internal stage management
- Stream-based change data capture
- Task automation

### PostgreSQL
- Direct file loading with COPY command
- Schema validation and data quality checks
- Incremental loading strategies
- SCD Type 2 implementation

## Data Quality Features

- **Schema Validation**: Automatic schema checking between files and tables
- **Data Validation**: Row count and data integrity checks
- **Unique Key Testing**: Configurable unique key constraints
- **Not Null Testing**: Column-level not null constraints
- **Hash-based Change Detection**: MD5 hashing for row-level change tracking

## Configuration Examples

### Mirror Layer Configuration
```json
{
  "table_name": "T_ML_MY_DATASET",
  "table_schema": {
    "COLUMN1": "TEXT",
    "COLUMN2": "NUMBER"
  },
  "unique_keys": ["ID"],
  "file_format_params": {
    "delimiter": ",",
    "skip_header": 1,
    "compressed": true
  }
}
```

### Stage Layer Configuration
```json
{
  "table_name": "T_STG_MY_DATASET",
  "table_schema": {
    "COLUMN1": "VARCHAR",
    "COLUMN2": "INTEGER"
  },
  "unique_keys": ["ID"],
  "transformations": [
    {
      "type": "select",
      "columns": ["COLUMN1", "COLUMN2"]
    }
  ]
}
```

## Output Structure

### Generated Configs
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

## License

MIT License

## Author

Ram Posam - posamram@gmail.com

## Repository

https://rposam-devops@dev.azure.com/rposam-devops/devops-project/_git/custom_utils