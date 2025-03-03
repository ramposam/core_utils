import os
from pathlib import Path

from constants.constants import default_args, dag_template
from core_utils.config_reader import ConfigReader
from core_utils.constants import mirror_file_meta_cols
from core_utils.file_utils import write_to_file


class DagGenerator:

    def __init__(self, configs_dir, dataset_name):
        self.configs_dir = configs_dir
        self.dataset_name = dataset_name

    def generate_dag(self, dataset_configs, dag_template):
        mirror_db, mirror_schema = dataset_configs["mirror_layer"]["database"], dataset_configs["mirror_layer"][
            "schema"]
        for task in dataset_configs["tasks"]:
            if task == "acq_task":
                dag_template += "from operators.acquisition_operator import AcquisitionOperator" + "\n"
            elif task == "download_task":
                dag_template += "from operators.download_operator import DownloadOperator" + "\n"
            elif task == "move_to_snowflake_task":
                dag_template += "from operators.move_file_to_snowflake_operator import MoveFileToSnowflakeOperator" + "\n"
            elif task == "copy_to_snowflake_task":
                dag_template += "from operators.snowflake_copy_operator import SnowflakeCopyOperator" + "\n"
            elif task == "snowflake_mirror_task":
                dag_template += "from operators.snowflake_load_to_mirror_operator import SnowflakeLoadToMirrorOperator" + "\n"
            elif task == "snowflake_schema_check_task":
                dag_template += "from operators.file_snowflake_table_schema_check_operator import FileSnowflakeTableSchemaCheckOperator" + "\n"
            elif task == "snowflake_stage_task":
                dag_template += "from operators.snowflake_load_to_stage_operator import SnowflakeLoadToStageOperator" + "\n"
            elif task == "snowflake_file_mirror_data_check_task":
                dag_template += "from operators.file_snowflake_table_data_check_operator import FileSnowflakeTableDataCheckOperator" + "\n"
            elif task == "copy_to_postgres_task":
                dag_template += "from operators.copy_file_to_postgres_operator import CopyFileToPostgresOperator" + "\n"
            elif task == "postgres_mirror_task":
                dag_template += "from operators.postgres_load_to_mirror_operator import PostgresLoadToMirrorOperator" + "\n"
            elif task == "postgres_schema_check_task":
                dag_template += "from operators.file_postgres_table_schema_check_operator import FilePostgresTableSchemaCheckOperator" + "\n"
            elif task == "postgres_stage_task":
                dag_template += "from operators.postgres_load_to_stage_operator import PostgresLoadToStageOperator" + "\n"
            elif task == "postgres_file_mirror_data_check_task":
                dag_template += "from operators.file_postgres_table_data_check_operator import FilePostgresTableDataCheckOperator" + "\n"
            elif task == "snowflake_mirror_tests_task":
                dag_template += "from operators.snowflake_mirror_tests_operator import SnowflakeMirrorTestsOperator" + "\n"
            elif task == "snowflake_stage_tests_task":
                dag_template += "from operators.snowflake_stage_tests_operator import SnowflakeStageTestsOperator" + "\n"

            elif task == "postgres_mirror_tests_task":
                dag_template += "from operators.postgres_mirror_tests_operator import PostgresMirrorTestsOperator" + "\n"
            elif task == "postgres_stage_tests_task":
                dag_template += "from operators.postgres_stage_tests_operator import PostgresStageTestsOperator" + "\n"

        datetime_format = dataset_configs["mirror"]["v1"].get("datetime_pattern","").upper().replace("YYYY", "%Y").replace("MM", "%m").replace( "DD", "%d")

        dag_template += default_args

        dag_body = f"""
# Define the DAG 
with DAG(
    dag_id="{dataset_configs["dataset_name"]}_dag",
    default_args=default_args,
    description="A simple DAG with a Data ingestion",
    schedule_interval="{dataset_configs["schedule_interval"]}",  # No schedule, triggered manually
    start_date=datetime({dataset_configs["start_date"]}),
    max_active_runs=1 ,
    catchup={dataset_configs["load_historical_data"]},
        ) as dag:

        start = EmptyOperator(
            task_id="start"
        )


        # End task
        end = EmptyOperator(
            task_id="end"
        )

        """

        dag_template += dag_body
        if "acq_task" in dataset_configs["tasks"]:
            dag_template += f"""
         # Task 1: Using the AcquisitionOperator
        acq_task = AcquisitionOperator(
            task_id="check_file_present_on_s3",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            bucket_name="{dataset_configs["bucket"]}",
            dataset_dir="{dataset_configs["mirror"]["v1"]["file_path"]}",
            file_pattern="{dataset_configs["mirror"]["v1"]["file_name_pattern"]}",
            datetime_pattern="{datetime_format}"
        ) 
            """
        if "download_task" in dataset_configs["tasks"]:
            dag_template += f"""
        download_task = DownloadOperator(
            task_id="download_file_from_s3_to_airflow_tmp_area",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            bucket_name="{dataset_configs["bucket"]}",
            dataset_dir="{dataset_configs["mirror"]["v1"]["file_path"]}",
            file_name="{dataset_configs["mirror"]["v1"]["file_name_pattern"]}",
            datetime_pattern="{datetime_format}"
        )
            """
        if "move_to_snowflake_task" in dataset_configs["tasks"]:
            dag_template += f"""
        move_to_snowflake_task = MoveFileToSnowflakeOperator(
            task_id="move_file_to_snowflake_internal_stage",
            db_conn_id="{dataset_configs["db_conn_id"]}",
            stage_name="{mirror_db}.{mirror_schema}.{dataset_configs["snowflake_stage_name"]}"
        )
            """
        if "snowflake_schema_check_task" in dataset_configs["tasks"]:
            dag_template += f"""
        snowflake_schema_check_task = FileSnowflakeTableSchemaCheckOperator(
            task_id="check_schema_of_config_n_received_file",
            db_conn_id="{dataset_configs["db_conn_id"]}",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            bucket_name="{dataset_configs["bucket"]}",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="{dataset_configs["dataset_name"]}",
            encoding="{dataset_configs["mirror"]["v1"]["encoding"]}",
            stage_name="{mirror_db}.{mirror_schema}.{dataset_configs["snowflake_stage_name"]}",
            table_name="{mirror_db}.{mirror_schema}.{dataset_configs["mirror"]["v1"]["table_name"]}_TR"
        )
            """

        if "postgres_schema_check_task" in dataset_configs["tasks"]:
            dag_template += f"""
        postgres_schema_check_task = FilePostgresTableSchemaCheckOperator(
            task_id="check_schema_of_config_n_received_file",
            db_conn_id="{dataset_configs["db_conn_id"]}",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            bucket_name="{dataset_configs["bucket"]}",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="{dataset_configs["dataset_name"]}",
            encoding="{dataset_configs["mirror"]["v1"]["encoding"]}"
        )
            """

        if "copy_to_snowflake_task" in dataset_configs["tasks"]:
            dag_template += f"""
        copy_to_snowflake_task = SnowflakeCopyOperator(
            task_id="copy_data_from_internal_stage",
            db_conn_id="{dataset_configs["db_conn_id"]}",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            bucket_name="{dataset_configs["bucket"]}",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="{dataset_configs["dataset_name"]}",
            encoding="{dataset_configs["mirror"]["v1"]["encoding"]}",
            stage_name="{mirror_db}.{mirror_schema}.{dataset_configs["snowflake_stage_name"]}",
            table_name="{mirror_db}.{mirror_schema}.{dataset_configs["mirror"]["v1"]["table_name"]}_TR"
        )
            """

        if "copy_to_postgres_task" in dataset_configs["tasks"]:
            dag_template += f"""
        copy_to_postgres_task = CopyFileToPostgresOperator(
            task_id="copy_data_from_file_to_postgres",
            db_conn_id="{dataset_configs["db_conn_id"]}",
            encoding="{dataset_configs["mirror"]["v1"]["encoding"]}",            
            table_name="{mirror_db}.{mirror_schema}.{dataset_configs["mirror"]["v1"]["table_name"]}_TR",
            file_format_params={dataset_configs["mirror"]["v1"]["file_format_params"]},
            datetime_pattern="{dataset_configs["mirror"]["v1"].get("datetime_pattern","").upper()}"
        )
            """

        if "snowflake_file_mirror_data_check_task" in dataset_configs["tasks"]:
            dag_template += f"""
        snowflake_file_mirror_data_check_task = FileSnowflakeTableDataCheckOperator(
            task_id="check_file_n_mirror_table_data",
            db_conn_id="{dataset_configs["db_conn_id"]}",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            bucket_name="{dataset_configs["bucket"]}",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="{dataset_configs["dataset_name"]}",
            encoding="{dataset_configs["mirror"]["v1"]["encoding"]}",
            table_name="{mirror_db}.{mirror_schema}.{dataset_configs["mirror"]["v1"]["table_name"]}_TR"
        )
            """

        if "postgres_file_mirror_data_check_task" in dataset_configs["tasks"]:
            dag_template += f"""
        postgres_file_mirror_data_check_task = FilePostgresTableDataCheckOperator(
            task_id="check_file_n_mirror_table_data",
            db_conn_id="{dataset_configs["db_conn_id"]}",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            bucket_name="{dataset_configs["bucket"]}",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="{dataset_configs["dataset_name"]}",
            encoding="{dataset_configs["mirror"]["v1"]["encoding"]}",
            table_name="{mirror_db}.{mirror_schema}.{dataset_configs["mirror"]["v1"]["table_name"]}_TR"
        )
            """

        if "snowflake_mirror_task" in dataset_configs["tasks"]:
            dag_template += f"""
        snowflake_mirror_task = SnowflakeLoadToMirrorOperator(
            task_id="load_to_mirror_table",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            db_conn_id="{dataset_configs["db_conn_id"]}",
            bucket_name="{dataset_configs["bucket"]}",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="{dataset_configs["dataset_name"]}"
        )
            """

        if "postgres_mirror_task" in dataset_configs["tasks"]:
            dag_template += f"""
        postgres_mirror_task = PostgresLoadToMirrorOperator(
            task_id="load_to_mirror_table",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            db_conn_id="{dataset_configs["db_conn_id"]}",
            bucket_name="{dataset_configs["bucket"]}",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="{dataset_configs["dataset_name"]}"
        )
            """

        if "postgres_mirror_tests_task" in dataset_configs["tasks"]:
            dag_template += f"""
        postgres_mirror_tests_task = PostgresMirrorTestsOperator(
            task_id="mirror_data_tests",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            db_conn_id="{dataset_configs["db_conn_id"]}",
            bucket_name="{dataset_configs["bucket"]}",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="{dataset_configs["dataset_name"]}"
        )
            """

        if "snowflake_mirror_tests_task" in dataset_configs["tasks"]:
            dag_template += f"""
        snowflake_mirror_tests_task = SnowflakeMirrorTestsOperator(
            task_id="mirror_data_tests",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            db_conn_id="{dataset_configs["db_conn_id"]}",
            bucket_name="{dataset_configs["bucket"]}",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="{dataset_configs["dataset_name"]}"
        )
            """

        if "snowflake_stage_task" in dataset_configs["tasks"]:
            dag_template += f"""
        snowflake_stage_task = SnowflakeLoadToStageOperator(
            task_id="load_to_stage_table",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            db_conn_id="{dataset_configs["db_conn_id"]}",
            bucket_name="{dataset_configs["bucket"]}",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="{dataset_configs["dataset_name"]}"
        )
            """

        if "postgres_stage_task" in dataset_configs["tasks"]:
            dag_template += f"""
        postgres_stage_task = PostgresLoadToStageOperator(
            task_id="load_to_stage_table",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            db_conn_id="{dataset_configs["db_conn_id"]}",
            bucket_name="{dataset_configs["bucket"]}",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="{dataset_configs["dataset_name"]}"
        )
            """

        if "snowflake_stage_tests_task" in dataset_configs["tasks"]:
            dag_template += f"""
        snowflake_stage_tests_task = SnowflakeStageTestsOperator(
            task_id="stage_data_tests",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            db_conn_id="{dataset_configs["db_conn_id"]}",
            bucket_name="{dataset_configs["bucket"]}",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="{dataset_configs["dataset_name"]}"
        )
            """

        if "postgres_stage_tests_task" in dataset_configs["tasks"]:
            dag_template += f"""
        postgres_stage_tests_task = PostgresStageTestsOperator(
            task_id="stage_data_tests",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            db_conn_id="{dataset_configs["db_conn_id"]}",
            bucket_name="{dataset_configs["bucket"]}",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="{dataset_configs["dataset_name"]}"
        )
            """

        dag_tasks = f"""  
        # Define task dependencies
        start >>  {" >> ".join(dataset_configs["tasks"])} >> end

        """
        dag_template += dag_tasks
        return dag_template

    def generate_ddls(self, database, schema, table_name, table_schema,layer):

        """
        Generate Snowflake table DDL from table name and schema.
        :param database: Name of the database
        :param schema: Name of the schema
        :param table_name: Name of the table
        :param table_schema: Dictionary with column names as keys and data types as values
        :return: DDL string for creating the table
        """
        ddl = f""" CREATE DATABASE IF NOT EXISTS {database};\n CREATE SCHEMA IF NOT EXISTS {schema};\n """
        ddl += f' CREATE TABLE IF NOT EXISTS "{database}"."{schema}"."{table_name}" (\n'
        column_definitions = []

        for column_name, data_type in table_schema.items():
            if not '"' in column_name and column_name not in mirror_file_meta_cols:
                column_definitions.append(f'    "{column_name}" {data_type}')
            else:
                column_definitions.append(f"    {column_name} {data_type}")

        if layer.upper() =="MIRROR" and not table_name.endswith("_TR"):
            column_definitions.append(f'    "UPDATED_DTS" TIMESTAMP')
            column_definitions.append(f'    "UPDATED_BY" TEXT')
            column_definitions.append(f'    "UNIQUE_HASH_ID" TEXT')
            column_definitions.append(f'    "ROW_HASH_ID" TEXT')

        ddl += ",\n".join(column_definitions)
        ddl += "\n);"

        return ddl

    def generate_dag_ddls(self):

        dataset_name = self.dataset_name

        configs_root_dir = os.path.join(self.configs_dir, dataset_name)

        dataset_configs = ConfigReader(configs_root_dir, dataset_name).read_configs()

        dag_data = self.generate_dag(dataset_configs, dag_template)

        dag_gen_dir = os.path.join(self.configs_dir, "generated_dags_ddls")
        Path(dag_gen_dir).mkdir(parents=True, exist_ok=True)

        write_to_file(dag_data, os.path.join(dag_gen_dir, dataset_name + "_dag.py"))

        mirror_db, mirror_schema = dataset_configs["mirror_layer"]["database"], dataset_configs["mirror_layer"][
            "schema"]
        table_name, table_schema = dataset_configs["mirror"]["v1"]["table_name"], dataset_configs["mirror"]["v1"][
            "table_schema"]

        mirror_tr_ddls = self.generate_ddls(mirror_db, mirror_schema, f"{table_name}_TR", table_schema,"mirror")

        write_to_file(mirror_tr_ddls, os.path.join(dag_gen_dir, f"{table_name}_TR.sql"))

        mirror_ddls = self.generate_ddls(mirror_db, mirror_schema, table_name, table_schema,"mirror")

        write_to_file(mirror_ddls, os.path.join(dag_gen_dir, table_name + ".sql"))

        stage_db, stage_schema = dataset_configs["stage_layer"]["database"], dataset_configs["stage_layer"]["schema"]
        table_name, table_schema = dataset_configs["stage"]["v1"]["table_name"], dataset_configs["stage"]["v1"][
            "table_schema"]

        stage_ddls = self.generate_ddls(stage_db, stage_schema, table_name, table_schema,"stage")

        write_to_file(stage_ddls, os.path.join(dag_gen_dir, table_name + ".sql"))

        stage_sql = f""" CREATE STAGE IF NOT EXISTS  {mirror_db}.{mirror_schema}.{dataset_configs["snowflake_stage_name"]} ;"""

        write_to_file(stage_sql, os.path.join(dag_gen_dir, dataset_configs["snowflake_stage_name"] + ".sql"))


