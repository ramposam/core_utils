import json

from core_utils.constants import snowflake_stage_template, snowflake_pipe_template, mirror_addl_meta_cols, \
    stage_addl_meta_cols
from core_utils.snowflake_utils import SnowflakeUtils


class SnowflakePipeline():
    def __init__(self, **kwargs):
        self.s3_bucket = kwargs.get("bucket")
        self.aws_access_key = kwargs.get("aws_access_key")
        self.aws_secret_key = kwargs.get("aws_secret_key")
        self.s3_dataset_path = kwargs.get("s3_dataset_path")
        self.dataset_name = kwargs.get("dataset_name")
        self.file_extension = kwargs.get("file_extension")
        self.delimiter = kwargs.get("delimiter")
        self.mirror_schema = kwargs.get("mirror_schema")
        self.file_schema = kwargs.get("file_schema")
        self.stage_schema = kwargs.get("stage_schema")
        self.schedule_interval = kwargs.get("schedule_interval")
        self.warehouse = "COMPUTE_WH"
        self.snowflake_stage_name = kwargs.get("snowflake_stage_name")

    def get_stage_sql(self):
        stage_sql = snowflake_stage_template.format(s3_bucket=self.s3_bucket,
                                                    s3_dataset_path=self.s3_dataset_path,
                                                    dataset_name=self.dataset_name.upper(),
                                                    aws_access_key=self.aws_access_key,
                                                    aws_secret_key=self.aws_secret_key)
        return stage_sql

    def get_snowpipe_sql(self, copy_statement):

        snowflake_pipe_sql = snowflake_pipe_template.format(dataset_name=self.dataset_name.upper(),
                                                            file_extension=self.file_extension,
                                                            copy_statement=copy_statement)
        return snowflake_pipe_sql

    def get_stream_sql(self, stream_name, table_name):
        stream_sql = f"""CREATE OR REPLACE STREAM {stream_name} ON TABLE {table_name}
         append_only = true; 
         """
        return stream_sql

    def get_task_sql(self, stream_name, task_name, table_name,table_schema,layer):
        insert_statement = self.get_layer_insert_statement(stream_name,table_name,table_schema,layer)
        task_sql = f"""CREATE OR REPLACE TASK {task_name}
            SCHEDULE = 'USING CRON {self.schedule_interval} UTC'
            WAREHOUSE = '{self.warehouse}'
            -- without condition, always try to execute the task
            WHEN
             SYSTEM$STREAM_HAS_DATA('{stream_name}') -- skips when stream has no data
            AS
            -- you could write merge statement incase you wanted upsert target, src as stream
            {insert_statement} \n ALTER TASK {task_name} RESUME;
        """

        return task_sql

    def get_layer_insert_statement(self,stream_name,table_name,table_schema,layer):

        columns = []
        for column_name, data_type in table_schema.items():
            columns.append(column_name.upper())

        if layer.upper() == "MIRROR":

            insert_columns = columns + mirror_addl_meta_cols
            select_columns = columns + ["NULL as UPDATED_DTS", "NULL AS UPDATED_BY" , "NULL AS UNIQUE_HASH_ID", "NULL AS ROW_HASH_ID"]
            # statement += f"INSERT INTO {table_name} (" + " , ".join(insert_columns) + ") \n"
            # statement += f"""SELECT {" , ".join(select_columns)} FROM {stream_name} ; \n"""
            update_stmt = ",".join([f"TARGET.{col} = SOURCE.{col}" for col in insert_columns])

        elif layer.upper() == "STAGE":

            mirror_cols = columns + mirror_addl_meta_cols
            remove_file_meta_columns = [column for column in mirror_cols if column not in ["FILE_DATE" , "FILENAME" , "FILE_ROW_NUMBER" , "FILE_LAST_MODIFIED"]]
            select_columns = remove_file_meta_columns  + ["'Y' as ACTIVE_FL","FILE_DATE AS EFFECTIVE_START_DATE", "'9999-12-31' AS EFFECTIVE_END_DATE"]
            insert_columns = remove_file_meta_columns + stage_addl_meta_cols
            # statement += f"INSERT INTO {table_name} (" + " , ".join(insert_columns) + ") \n"
            # statement += f"""SELECT {" , ".join(select_columns)} ,'Y' as ACTIVE_FL,FILE_DATE AS EFFECTIVE_START_DATE, '9999-12-31' AS EFFECTIVE_END_DATE FROM {stream_name} ; \n"""
            update_stmt = ",".join([f"TARGET.{col} = SOURCE.{col}" for col in insert_columns])

        merge_stmt = f"""MERGE INTO {table_name} AS TARGET
                        USING (
                            SELECT 
                                {" , ".join(select_columns)}
                            FROM {stream_name}
                        ) AS SOURCE
                        ON TARGET.UNIQUE_HASH_ID = SOURCE.UNIQUE_HASH_ID
                        WHEN MATCHED THEN
                            UPDATE SET 
                                {update_stmt}
                        WHEN NOT MATCHED THEN
                            INSERT (
                                {" , ".join(insert_columns)}
                            )
                            VALUES (
                                {" , ".join([f"SOURCE.{col}" for col in insert_columns])}
                                );
                        """

        return merge_stmt

    def get_file_meta_sql(self, database, schema, table_name,dataset_name, version, start_date, end_date):
        print(self.file_schema)
        indexed_file_schema = []
        for key, val in self.file_schema.items():
            indexed_file_schema.append({key: val})

        file_meta_sql = f""" CREATE DATABASE IF NOT EXISTS META_DB;\n CREATE SCHEMA IF NOT EXISTS META;\n """

        file_meta_sql += f"""CREATE OR REPLACE TABLE META_DB.META.T_FILE_META_DETAILS ( 
            DATABASE STRING,
            SCHEMA STRING,
            TABLE_NAME STRING,
            DATASET_NAME STRING,
            VERSION STRING,
            ACTIVE_FL STRING,
            START_DATE DATE,
            END_DATE DATE,
            FILE_SCHEMA VARIANT,
            CREATED_BY STRING,
            CREATED_DATE TIMESTAMP);
         """

        file_meta_sql += """
        CREATE TABLE IF NOT EXISTS META_DB.META.T_FILE_VALIDATION (
          VALIDATION_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
          DATASET_NAME VARCHAR(16777216), 
          STATUS VARCHAR(16777216), 
          TYPE VARCHAR(16777216), 
          CREATED_DTS TIMESTAMP_NTZ(9), 
          CREATED_BY VARCHAR(16777216)
        );
            
        CREATE TABLE IF NOT EXISTS META_DB.META.T_FILE_VALIDATION_DETAILS (
          VALIDATION_DETAIL_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
          VALIDATION_ID NUMBER, 
          DATASET_NAME STRING,
          MSG VARCHAR(16777216), 
          DETAILS VARIANT,
          CREATED_DTS TIMESTAMP_NTZ(9), 
          CREATED_BY VARCHAR(16777216)
        );
        """

        if "." in table_name:
            formatted_table_name = table_name.split(".")[-1]
        else:
            formatted_table_name = table_name

        file_meta_sql += f""" INSERT INTO META_DB.META.T_FILE_META_DETAILS
            SELECT *,PARSE_JSON('{json.dumps(indexed_file_schema)}'),current_user(),current_timestamp()  from values
            ('{database}','{schema}','{formatted_table_name}','{dataset_name}','{version}','Y','{start_date}','{end_date}');
        """

        return file_meta_sql

    def get_mirror_validation_task(self, stream_name,dataset_name,stage_name,database,schema, task_name, table_name):
        validation_sql = f"""CREATE OR REPLACE TASK {task_name}
            WAREHOUSE = COMPUTE_WH
            AFTER MIRROR_DB.MIRROR.TASK_LOG_SNOWPIPE_ERRORS            
            WHEN SYSTEM$STREAM_HAS_DATA('{stream_name}') -- Skips execution if the stream has no data
            AS
            CALL META_DB.META.VALIDATE_FILE_AND_TABLE(
                '{dataset_name}',
                '{stage_name.split(".")[-1]}',
                '{database}',
                '{schema}',
                '{table_name.split(".")[-1]}',
                '',
                1
            );
        """
        return validation_sql

    def get_all_sqls(self):

        dataset_name_upper = self.dataset_name.upper()
        mirror_tr_table_name = f"MIRROR_DB.MIRROR.T_ML_{dataset_name_upper}_TR"
        file_format_name = f"MIRROR_DB.MIRROR.FF_{dataset_name_upper}"
        mirror_stream_name = f"MIRROR_DB.MIRROR.STREAM_{dataset_name_upper}"
        mirror_tr_stream_name = f"MIRROR_DB.MIRROR.STREAM_{dataset_name_upper}_TR"
        mirror_task_name = f"MIRROR_DB.MIRROR.TASK_{dataset_name_upper}"
        mirror_tr_validation_task_name = f"MIRROR_DB.MIRROR.TASK_{dataset_name_upper}_TR_VALIDATION"
        mirror_table_name = f"MIRROR_DB.MIRROR.T_ML_{dataset_name_upper}"
        stg_table_name = f"STAGE_DB.STAGE.T_STG_{dataset_name_upper}"
        stg_stream_name = f"STAGE_DB.STAGE.STREAM_{dataset_name_upper}"
        stg_task_name = f"STAGE_DB.STAGE.TASK_{dataset_name_upper}"

        if self.aws_access_key and self.aws_secret_key:
            stage_sql = self.get_stage_sql()
            stage_name = f"MIRROR_DB.MIRROR.STG_{dataset_name_upper}_S3"
        else:
            stage_sql = ""
            stage_name = self.snowflake_stage_name

        util = SnowflakeUtils(
            stage_name=stage_name,
            table_name=mirror_tr_table_name)

        file_format_sql = util.get_file_format_sql(file_format_name=file_format_name,
                                                   delimiter=self.delimiter)

        mirror_tr_table_sql = util.get_mirror_stage_ddls("MIRROR_DB", "MIRROR", mirror_tr_table_name,
                                                         self.mirror_schema,'MIRROR')
        mirror_table_sql = util.get_mirror_stage_ddls("MIRROR_DB", "MIRROR", mirror_table_name, self.mirror_schema,'MIRROR')
        stage_table_sql = util.get_mirror_stage_ddls("STAGE_DB", "STAGE", stg_table_name, self.stage_schema,'STAGE')

        if isinstance(self.mirror_schema, dict):
            columns = list(self.mirror_schema.keys())
        else:
            columns = []

        copy_statement = util.get_copy_into_table_sql(columns=columns,
                                                      file_extension=self.file_extension,
                                                      file_format_name=file_format_name)

        snowpipe_sql = self.get_snowpipe_sql(copy_statement)

        mirror_stream_sql = self.get_stream_sql(stream_name=mirror_tr_stream_name, table_name=mirror_tr_table_name)

        mirror_validation_sql = self.get_mirror_validation_task(stream_name=mirror_tr_stream_name,dataset_name=dataset_name_upper,
                                                                stage_name=stage_name,database="MIRROR_DB",schema="MIRROR",task_name=mirror_tr_validation_task_name,
                                                                table_name=mirror_tr_table_name)

        mirror_task_sql = self.get_task_sql(stream_name=mirror_tr_stream_name, task_name=mirror_task_name,
                                            table_name=mirror_table_name,table_schema=self.mirror_schema,layer="MIRROR")

        stage_stream_sql = self.get_stream_sql(stream_name=stg_stream_name, table_name=mirror_table_name)

        stage_task_sql = self.get_task_sql(stream_name=stg_stream_name, task_name=stg_task_name,
                                           table_name=stg_table_name,table_schema=self.mirror_schema,layer="STAGE")

        file_meta_sql = self.get_file_meta_sql("MIRROR_DB", "MIRROR", mirror_tr_table_name,
                                               dataset_name_upper, "V1", "2021-01-01",
                                               "9999-12-31")

        drop_sql = """
        DROP DATABASE IF EXISTS MIRROR_DB;
        DROP DATABASE IF EXISTS STAGE_DB;
        DROP DATABASE IF EXISTS META_DB;
        """
        all_sqls = "\n".join([drop_sql,file_meta_sql, mirror_tr_table_sql, mirror_table_sql, stage_table_sql,
                              stage_sql, file_format_sql, snowpipe_sql, mirror_stream_sql,
                              mirror_validation_sql,mirror_task_sql, stage_stream_sql, stage_task_sql])

        return all_sqls
