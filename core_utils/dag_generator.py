import os
from pathlib import Path

from constants.constants import default_args, dag_template
from core_utils.config_reader import ConfigReader
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
            elif task == "move_task":
                dag_template += "from operators.move_file_to_snowflake_operator import MoveFileToSnowflakeOperator" + "\n"
            elif task == "copy_task":
                dag_template += "from operators.snowflake_copy_operator import SnowflakeCopyOperator" + "\n"
            elif task == "mirror_task":
                dag_template += "from operators.mirror_load_operator import MirrorLoadOperator" + "\n"
            elif task == "schema_check_task":
                dag_template += "from operators.file_table_schema_check_operator import FileTableSchemaCheckOperator" + "\n"

        datetime_format = dataset_configs["mirror"]["v1"].get("datetime_pattern","").replace("YYYY", "%Y").replace("MM", "%m").replace( "DD", "%d")

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
            task_id="s3_file_check",
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
            task_id="download_file_from_s3",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            bucket_name="{dataset_configs["bucket"]}",
            dataset_dir="{dataset_configs["mirror"]["v1"]["file_path"]}",
            file_name="{dataset_configs["mirror"]["v1"]["file_name_pattern"]}",
            datetime_pattern="{datetime_format}"
        )
            """
        if "move_task" in dataset_configs["tasks"]:
            dag_template += f"""
        move_task = MoveFileToSnowflakeOperator(
            task_id="move_file_to_snowflake",
            snowflake_conn_id="{dataset_configs["snowflake_connection_id"]}",
            stage_name="{mirror_db}.{mirror_schema}.{dataset_configs["snowflake_stage_name"]}"
        )
            """
        if "schema_check_task" in dataset_configs["tasks"]:
            dag_template += f"""
        schema_check_task = FileTableSchemaCheckOperator(
            task_id="check_file_table_schema",
            snowflake_conn_id="{dataset_configs["snowflake_connection_id"]}",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            bucket_name="{dataset_configs["bucket"]}",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="{dataset_configs["dataset_name"]}",
            stage_name="{mirror_db}.{mirror_schema}.{dataset_configs["snowflake_stage_name"]}",
            table_name="{mirror_db}.{mirror_schema}.{dataset_configs["mirror"]["v1"]["table_name"]}_TR"
        )
            """

        if "copy_task" in dataset_configs["tasks"]:
            dag_template += f"""
        copy_task = SnowflakeCopyOperator(
            task_id="copy_file_from_stage",
            snowflake_conn_id="{dataset_configs["snowflake_connection_id"]}",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            bucket_name="{dataset_configs["bucket"]}",
            s3_configs_path="dataset_configs/dev/",
            dataset_name="{dataset_configs["dataset_name"]}",
            stage_name="{mirror_db}.{mirror_schema}.{dataset_configs["snowflake_stage_name"]}",
            table_name="{mirror_db}.{mirror_schema}.{dataset_configs["mirror"]["v1"]["table_name"]}_TR"
        )
            """
        if "mirror_task" in dataset_configs["tasks"]:
            dag_template += f"""
        mirror_task = MirrorLoadOperator(
            task_id="load_to_mirror",
            s3_conn_id="{dataset_configs["s3_connection_id"]}",
            snowflake_conn_id="{dataset_configs["snowflake_connection_id"]}",
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

    def generate_mirror_stage_ddls(self, database, schema, table_name, table_schema):

        """
        Generate Snowflake table DDL from table name and schema.
        :param database: Name of the database
        :param schema: Name of the schema
        :param table_name: Name of the table
        :param table_schema: Dictionary with column names as keys and data types as values
        :return: DDL string for creating the table
        """
        ddl = f""" CREATE DATABASE IF NOT EXISTS {database};\n CREATE SCHEMA IF NOT EXISTS {schema};\n """
        ddl += f"CREATE OR REPLACE TABLE {database}.{schema}.{table_name} (\n"
        column_definitions = []

        for column_name, data_type in table_schema.items():
            column_definitions.append(f"    {column_name} {data_type}")

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

        mirror_tr_ddls = self.generate_mirror_stage_ddls(mirror_db, mirror_schema, f"{table_name}_TR", table_schema)

        write_to_file(mirror_tr_ddls, os.path.join(dag_gen_dir, f"{table_name}_TR.sql"))

        mirror_ddls = self.generate_mirror_stage_ddls(mirror_db, mirror_schema, table_name, table_schema)

        write_to_file(mirror_ddls, os.path.join(dag_gen_dir, table_name + ".sql"))

        stage_db, stage_schema = dataset_configs["stage_layer"]["database"], dataset_configs["stage_layer"]["schema"]
        table_name, table_schema = dataset_configs["stage"]["v1"]["table_name"], dataset_configs["stage"]["v1"][
            "table_schema"]

        stage_ddls = self.generate_mirror_stage_ddls(stage_db, stage_schema, table_name, table_schema)

        write_to_file(stage_ddls, os.path.join(dag_gen_dir, table_name + ".sql"))

        stage_sql = f"""CREATE OR REPLACE  STAGE {mirror_db}.{mirror_schema}.{dataset_configs["snowflake_stage_name"]} ;"""

        write_to_file(stage_sql, os.path.join(dag_gen_dir, dataset_configs["snowflake_stage_name"] + ".sql"))


