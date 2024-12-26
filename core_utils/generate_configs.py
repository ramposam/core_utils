import os

from core_utils.file_utils import read_and_infer, write_to_json_file, write_to_file, get_unique_keys
from core_utils.generate_snowflake_pipeline import SnowflakePipeline
from core_utils.meta_classes import DatasetConfigs, DatasetVersion, DatasetMirror, DatasetStage
from pathlib import Path


class ConfigTemplate():
    def __init__(self, bucket, **kwargs):
        self.file_path = kwargs.get("file_path")
        self.pipeline_type = kwargs.get("pipeline_type")
        self.dataset_name = kwargs.get("dataset_name")
        self.bucket = bucket
        self.s3_dataset_path = kwargs.get("s3_dataset_path")
        self.start_date = kwargs.get("start_date")
        self.datetime_format = kwargs.get("datetime_format")
        self.catchup = kwargs.get("catchup")
        self.schedule_interval = kwargs.get("schedule_interval")
        self.aws_access_key = kwargs.get("aws_access_key")
        self.aws_secret_key = kwargs.get("aws_secret_key")
        self.snowflake_stage_name = kwargs.get("snowflake_stage_name")

    def add_meta_cols(self, schema, layer):
        if layer == "MIRROR":
            schema["filename"] = "STRING"
            schema["file_date"] = "TIMESTAMP"
            schema["file_row_number"] = "STRING"
            schema["file_last_modified"] = "TIMESTAMP"
            schema["created_dts"] = "TIMESTAMP"
            schema["created_by"] = "STRING"
        else:
            schema["created_dts"] = "TIMESTAMP"
            schema["created_by"] = "STRING"
            schema["updated_dts"] = "TIMESTAMP"
            schema["updated_by"] = "STRING"
            schema["active_fl"] = "STRING"
            schema["effective_start_date"] = "TIMESTAMP"
            schema["effective_end_date"] = "TIMESTAMP"
            schema["row_hash_id"] = "STRING"
        return schema

    def get_mirror_schema(self, schema):
        mirror_schema = self.add_meta_cols(schema.copy(), "MIRROR")

        return mirror_schema

    def get_file_schema(self, columns):
        schema = {}
        for col_name in columns:
            schema[col_name.replace(" ", "_").upper()] = "STRING"
        return schema

    def get_stage_schema(self, data_types):
        schema = {}
        for col_name, col_dtypes in data_types.items():
            schema[col_name.replace(" ", "_").upper()] = col_dtypes["snowflake_dtype"]

        stage_schema = self.add_meta_cols(schema, "STAGE")

        return stage_schema

    def generate_configs(self, configs_tmp_dir):
        # Trying to apply schema inference on file data and get delimiter, file schema, data types
        delimiter, columns, data_types = read_and_infer(self.file_path)

        # Trying to identify unique keys by extending each column from the first columns
        unique_keys = get_unique_keys(self.file_path, delimiter, 1)

        # Get the file schema which would be used to verify table and file schema is a match
        file_schema = self.get_file_schema(data_types)

        # Mirror schema is always string data types with additional file metadata columns
        mirror_schema = self.get_mirror_schema(file_schema)

        # Stage schema is actual data type of each column after schema inferences with additional file metadata columns
        stage_schema = self.get_stage_schema(data_types)

        dataset_name = self.dataset_name  # os.path.basename(os.path.dirname(self.file_path))

        configs_root_dir = os.path.join(configs_tmp_dir, "generated_configs")
        Path(configs_root_dir).mkdir(parents=True, exist_ok=True)

        # Creating root dir as generated_configs to store all the generated dataset configs
        configs_dataset_dir = os.path.join(configs_root_dir, dataset_name)

        # create folder as dataset name
        Path(configs_dataset_dir).mkdir(parents=True, exist_ok=True)

        # Generates pipelines either for Snowflake using snowpipe, stream, tasks , no airflow dags, operators
        if self.pipeline_type == "SNOWPIPE":
            file_extension = os.path.basename(self.file_path).split(".")[-1]

            pipeline = SnowflakePipeline(bucket=self.bucket, s3_dataset_path=self.s3_dataset_path,
                                         dataset_name=self.dataset_name, file_extension=file_extension,
                                         delimiter=delimiter, mirror_schema=mirror_schema,
                                         aws_access_key=self.aws_access_key, aws_secret_key=self.aws_secret_key,
                                         stage_schema=stage_schema, schedule_interval=self.schedule_interval,
                                         snowflake_stage_name=self.snowflake_stage_name)

            pipeline_sqls = pipeline.get_all_sqls()

            pipeline_sqls_path = os.path.join(configs_dataset_dir, f"pipeline_{dataset_name}.sql")

            if len(pipeline_sqls_path) > 255:
                pipeline_sqls_path = r'\\?\{}'.format(pipeline_sqls_path)

            write_to_file(data=pipeline_sqls, file_path=pipeline_sqls_path)

        # Create pipelines using Airflow, DBT, Snowflake
        else:
            dataset_configs_path = os.path.join(configs_dataset_dir, f"{dataset_name}.json")

            if len(dataset_configs_path) > 255:
                dataset_configs_path = r'\\?\{}'.format(dataset_configs_path)

            ds_configs = DatasetConfigs(dataset_name=dataset_name, bucket=self.bucket,
                                        start_date=self.start_date, load_historical_data=self.catchup,
                                        snowflake_stage_name=f"STG_{dataset_name}".upper())

            write_to_json_file(data=ds_configs.__dict__, file_path=dataset_configs_path)

            dataset_mirror_dir = os.path.join(configs_dataset_dir, "mirror")
            Path(dataset_mirror_dir).mkdir(parents=True, exist_ok=True)

            dataset_configs_mirror_ver_path = os.path.join(dataset_mirror_dir, f"{dataset_name}_mirror_ver.json")

            if len(dataset_configs_mirror_ver_path) > 255:
                dataset_configs_mirror_ver_path = r'\\?\{}'.format(dataset_configs_mirror_ver_path)

            ds_mirror_ver_configs = DatasetVersion(dataset_name=dataset_name)

            write_to_json_file(data=ds_mirror_ver_configs.__dict__, file_path=dataset_configs_mirror_ver_path)

            file_format = {
                "delimiter": delimiter,
                "skip_header": 1,
                "compressed": True
            }
            dataset_configs_mirror_v1_path = os.path.join(dataset_mirror_dir, f"{dataset_name}_mirror_v1.json")

            if len(dataset_configs_mirror_v1_path) > 255:
                dataset_configs_mirror_v1_path = r'\\?\{}'.format(dataset_configs_mirror_v1_path)

            ds_mirror_v1_configs = DatasetMirror(table_name=f"T_ML_{dataset_name}".upper(),
                                                 table_schema=mirror_schema,
                                                 unique_keys=unique_keys,
                                                 file_format_params=file_format,
                                                 file_schema=file_schema,
                                                 file_name_pattern="datetime_pattern.csv",
                                                 file_path=f"datasets/{dataset_name}",
                                                 datetime_pattern=self.datetime_format)

            write_to_json_file(data=ds_mirror_v1_configs.__dict__, file_path=dataset_configs_mirror_v1_path)

            dataset_stg_dir = os.path.join(configs_dataset_dir, "stage")
            Path(dataset_stg_dir).mkdir(parents=True, exist_ok=True)

            dataset_configs_stage_ver_path = os.path.join(dataset_stg_dir, f"{dataset_name}_stage_ver.json")

            if len(dataset_configs_stage_ver_path) > 255:
                dataset_configs_stage_ver_path = r'\\?\{}'.format(dataset_configs_stage_ver_path)

            ds_stage_ver_configs = DatasetVersion(dataset_name=dataset_name)

            write_to_json_file(data=ds_stage_ver_configs.__dict__, file_path=dataset_configs_stage_ver_path)

            dataset_configs_stage_v1_path = os.path.join(dataset_stg_dir, f"{dataset_name}_stage_v1.json")

            if len(dataset_configs_stage_v1_path) > 255:
                dataset_configs_stage_v1_path = r'\\?\{}'.format(dataset_configs_stage_v1_path)

            ds_stage_v1_configs = DatasetStage(table_name=f"T_STG_{dataset_name}".upper(),
                                               table_schema=stage_schema,
                                               unique_keys=unique_keys, )

            write_to_json_file(data=ds_stage_v1_configs.__dict__, file_path=dataset_configs_stage_v1_path)

        return configs_root_dir