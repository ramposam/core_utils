import os

from core_utils.file_utils import read_and_infer, write_to_json_file, write_to_file, get_unique_keys, \
    get_file_name_pattern
from core_utils.generate_snowflake_pipeline import SnowflakePipeline
from core_utils.meta_classes import DatasetConfigs, DatasetVersion, DatasetMirror, DatasetStage
from pathlib import Path


class ConfigTemplate():
    def __init__(self, bucket, **kwargs):
        self.file_path = kwargs.get("file_path")
        self.pipeline_type = kwargs.get("pipeline_type")
        self.dataset_name = kwargs.get("dataset_name")
        self.bucket = bucket
        self.db_type = kwargs.get("db_type","SNOWFLAKE")
        self.s3_dataset_path = kwargs.get("s3_dataset_path")
        self.start_date = kwargs.get("start_date")
        self.datetime_format = kwargs.get("datetime_format")
        self.catchup = kwargs.get("catchup")
        self.schedule_interval = kwargs.get("schedule_interval")
        self.aws_access_key = kwargs.get("aws_access_key")
        self.aws_secret_key = kwargs.get("aws_secret_key")
        self.snowflake_stage_name = kwargs.get("snowflake_stage_name")

    def add_meta_cols(self, schema, layer,db_type):
        if layer == "MIRROR":
            if db_type == "POSTGRES":
                schema['FILE_DATE'] = "TIMESTAMP"
                schema['FILE_NAME'] = "TEXT"
                schema['CREATED_DTS'] = "TIMESTAMP"
                schema['CREATED_BY'] = "TEXT"
            else:
                schema["file_date"] = "TIMESTAMP"
                schema["filename"] = "TEXT"
                schema["file_row_number"] = "TEXT"
                schema["file_last_modified"] = "TIMESTAMP"
                schema['CREATED_DTS'] = "TIMESTAMP"
                schema['CREATED_BY'] = "TEXT"
        else:
            schema["CREATED_DTS"] = "TIMESTAMP"
            schema["CREATED_BY"] = "TEXT"
            schema["UPDATED_DTS"] = "TIMESTAMP"
            schema["UPDATED_BY"] = "TEXT"
            schema["UNIQUE_HASH_ID"] = "TEXT"
            schema["ROW_HASH_ID"] = "TEXT"
            schema["ACTIVE_FL"] = "TEXT"
            schema["EFFECTIVE_START_DATE"] = "TIMESTAMP"
            schema["EFFECTIVE_END_DATE"] = "TIMESTAMP"

        return schema

    def get_mirror_schema(self, schema,db_type="SNOWFLAKE"):
        mirror_schema = self.add_meta_cols(schema.copy(), "MIRROR",db_type)

        return mirror_schema

    def get_file_schema(self, columns):
        file_schema = {}
        for col_name in columns:
            file_schema[col_name.replace(" ", "_").upper()] = "TEXT"

        file_schema = {f'{k}':v for k,v in file_schema.items()}
        return file_schema

    def get_stage_schema(self, data_types,db_type="SNOWFLAKE"):
        schema = {}
        for col_name, col_dtypes in data_types.items():
            if db_type == "POSTGRES":
                schema[col_name.replace(" ", "_").upper()] = col_dtypes["postgres_dtype"]
            else:
                schema[col_name.replace(" ", "_").upper()] = col_dtypes["snowflake_dtype"]

        stage_schema = self.add_meta_cols(schema, "STAGE",db_type)
        stage_schema = {f'{k}': v for k, v in stage_schema.items()}
        return stage_schema

    def generate_configs(self, configs_tmp_dir):
        # Trying to apply schema inference on file data and get delimiter, file schema, data types
        delimiter, columns, data_types = read_and_infer(self.file_path)

        # Trying to identify unique keys by extending each column from the first columns
        unique_keys = get_unique_keys(self.file_path, delimiter, 1)

        # Get the file schema which would be used to verify table and file schema is a match
        file_schema = self.get_file_schema(data_types)

        # Mirror schema is always TEXT data types with additional file metadata columns
        mirror_schema = self.get_mirror_schema(file_schema,self.db_type)

        # Stage schema is actual data type of each column after schema inferences with additional file metadata columns
        stage_schema = self.get_stage_schema(data_types,self.db_type)

        dataset_name = self.dataset_name  # os.path.basename(os.path.dirname(self.file_path))

        configs_root_dir = os.path.join(configs_tmp_dir, "generated_configs")
        Path(configs_root_dir).mkdir(parents=True, exist_ok=True)

        # Creating root dir as generated_configs to store all the generated dataset configs
        configs_dataset_dir = os.path.join(configs_root_dir, dataset_name)

        # create folder as dataset name
        Path(configs_dataset_dir).mkdir(parents=True, exist_ok=True)

        # Generates pipelines either for Snowflake using snowpipe, stream, tasks , no airflow dags, operators
        if self.pipeline_type == "SNOWPIPE":

            validation_procedure_path = os.path.join(os.getcwd(), "helpers",
                                                     "mirror_validation_procedure_function_template.sql")
            sql_file_read = open(validation_procedure_path, 'r')
            mirror_validation_procedure_sqls = sql_file_read.read() + "\n"
            sql_file_read.close()

            debug_sql = """\n /* # Copy Procedure Python code to a file add following lines at the end of the file and debug incase procedure has errors or not working as expected 

                # Connect to Snowflake
                connection_parameters = {
                    "user":os.getenv("SNOWFLAKE_USER"),
                    "password":os.getenv("SNOWFLAKE_PASSWORD"),
                    "account":os.getenv("SNOWFLAKE_ACCOUNT"),
                    "database":os.getenv("SNOWFLAKE_DATABASE"),
                    "schema":os.getenv("SNOWFLAKE_SCHEMA"),
                    "warehouse":os.getenv("SNOWFLAKE_WAREHOUSE"),
                    "role":os.getenv("SNOWFLAKE_ROLE")}


                # Create a Snowpark session
                session = Session.builder.configs(connection_parameters).create()

                result = compare_csv_with_table(session=session,dataset_name='NETFLIX_MOVIES_AND_TV_SHOWS',
                                       database="MIRROR_DB",schema="MIRROR",
                                       stage_name = "STG_NETFLIX_MOVIES_AND_TV_SHOWS",
                                       table_name = 'T_ML_NETFLIX_MOVIES_AND_TV_SHOWS_TR',
                                        run_date="2024-12-21"
                    )

                print(result) */
                """
            file_extension = os.path.basename(self.file_path).split(".")[-1]
            pipeline = SnowflakePipeline(bucket=self.bucket, s3_dataset_path=self.s3_dataset_path,
                                         dataset_name=self.dataset_name, file_extension=file_extension,
                                         delimiter=delimiter, mirror_schema=mirror_schema, file_schema=file_schema,
                                         aws_access_key=self.aws_access_key, aws_secret_key=self.aws_secret_key,
                                         stage_schema=stage_schema, schedule_interval=self.schedule_interval,
                                         snowflake_stage_name=self.snowflake_stage_name)

            pipeline_sqls = pipeline.get_all_sqls()

            pipeline_sqls += mirror_validation_procedure_sqls + "\n" + debug_sql

            pipeline_sqls_path = os.path.join(configs_dataset_dir, f"pipeline_{dataset_name}.sql")

            if len(pipeline_sqls_path) > 255:
                pipeline_sqls_path = r'\\?\{}'.format(pipeline_sqls_path)

            write_to_file(data=pipeline_sqls, file_path=pipeline_sqls_path)

        # Create pipelines using Airflow, DBT, Snowflake
        else:
            dataset_configs_path = os.path.join(configs_dataset_dir, f"{dataset_name}.json")

            if len(dataset_configs_path) > 255:
                dataset_configs_path = r'\\?\{}'.format(dataset_configs_path)

            if self.db_type == "POSTGRES":
                ds_configs = DatasetConfigs(dataset_name=dataset_name, bucket=self.bucket,
                                            start_date=self.start_date, load_historical_data=self.catchup,
                                            snowflake_stage_name="",
                                            db_conn_id = "POSTGRES_CONN_ID_MIRROR",
                                            tasks=["acq_task",
                                                 "download_task",
                                                 "postgres_schema_check_task",
                                                 "copy_to_postgres_task",
                                                 "postgres_file_mirror_data_check_task",
                                                 "postgres_mirror_task",
                                                 "postgres_stage_task"])
            else:
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

            file_name_pattern, datetime_pattern = get_file_name_pattern(os.path.basename(self.file_path))
            datetime_pattern = self.datetime_format if self.datetime_format else datetime_pattern.replace("%Y",
                                                                                                          "YYYY").replace(
                "%m", "MM").replace("%d", "DD")
            ds_mirror_v1_configs = DatasetMirror(table_name=f"T_ML_{dataset_name}".upper(),
                                                 table_schema=mirror_schema,
                                                 unique_keys=unique_keys,
                                                 file_format_params=file_format,
                                                 file_schema=file_schema,
                                                 file_name_pattern=file_name_pattern,
                                                 file_path=self.s3_dataset_path,
                                                 datetime_pattern=datetime_pattern)

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