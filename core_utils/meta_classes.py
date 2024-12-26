from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime

@dataclass
class DatasetConfigs:
    dataset_name: str
    snowflake_stage_name: str
    bucket: str
    tasks: List[str] = field(
        default_factory=lambda: ["acq_task",
                                 "download_task",
                                 "move_task",
                                 "schema_check_task",
                                 "copy_task",
                                 "mirror_task"])
    mirror_layer: Dict = field(default_factory=lambda: {"database": "MIRROR_DB", "schema": "MIRROR"})
    stage_layer: Dict = field(default_factory=lambda: {"database": "STAGE_DB", "schema": "STAGE"})
    s3_connection_id: str = field(default="S3_CONN_ID")
    snowflake_connection_id: str = field(default="SNOWFLAKE_CONN_ID")
    start_date: str = field(default=datetime.strftime(datetime.today(),"%Y,%m,%d"))
    load_historical_data : str = field(default=False)
    schedule_interval: Optional[str] = field(default="0 23 * * 1-5")


@dataclass
class DatasetVersion:
    dataset_name: str
    versions: List[Dict] = field(default_factory=lambda: [{
        "version": "v1",
        "start_date": "2021-01-01",
        "end_date": "9999-12-31"}])


@dataclass
class DatasetMirror:
    table_name: str
    table_schema: Dict
    unique_keys: List
    file_format_params: Dict
    file_schema: Dict
    file_name_pattern: str
    file_path: str
    datetime_pattern: str = field(default="YYYY-MM-DD")


@dataclass
class DatasetStage:
    table_name: str
    table_schema: Dict
    unique_keys: List
    transformations: List[Dict] = field(default_factory=lambda: [{"name": "select_expr",
                                                                  "expr": ["*"]}])
