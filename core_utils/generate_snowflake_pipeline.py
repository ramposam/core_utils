from core_utils.constants import snowflake_stage_template, snowflake_pipe_template
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

    def get_task_sql(self, stream_name, task_name, table_name):
        task_sql = f"""CREATE OR REPLACE TASK {task_name}
            SCHEDULE = 'USING CRON {self.schedule_interval} UTC'
            WAREHOUSE = '{self.warehouse}'
            -- without condition, always try to execute the task
            WHEN
             SYSTEM$STREAM_HAS_DATA('{stream_name}') -- skips when stream has no data
            AS
            -- you could write merge statement incase you wanted upsert target, src as stream
            INSERT INTO {table_name}
            SELECT *  exclude (METADATA$ACTION,METADATA$ISUPDATE,METADATA$ROW_ID)
            FROM {stream_name}; \n ALTER TASK {task_name} RESUME;
        """

        return task_sql

    def get_all_sqls(self):

        dataset_name_upper = self.dataset_name.upper()
        mirror_tr_table_name = f"MIRROR_DB.MIRROR.T_ML_{dataset_name_upper}_TR"
        file_format_name = f"MIRROR_DB.MIRROR.FF_{dataset_name_upper}"
        mirror_stream_name = f"MIRROR_DB.MIRROR.STREAM_{dataset_name_upper}"
        mirror_task_name = f"MIRROR_DB.MIRROR.TASK_{dataset_name_upper}"
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

        mirror_tr_table_sql = util.get_mirror_stage_ddls("MIRROR_DB","MIRROR",mirror_tr_table_name,self.mirror_schema)
        mirror_table_sql = util.get_mirror_stage_ddls("MIRROR_DB", "MIRROR", mirror_table_name, self.mirror_schema)
        stage_table_sql = util.get_mirror_stage_ddls("STAGE_DB", "STAGE", stg_table_name, self.stage_schema)

        if isinstance(self.mirror_schema, dict):
            columns = list(self.mirror_schema.keys())
        else:
            columns = []

        copy_statement = util.get_copy_into_table_sql(columns=columns,
                                                      file_extension=self.file_extension,
                                                      file_format_name=file_format_name)

        snowpipe_sql = self.get_snowpipe_sql(copy_statement)

        mirror_stream_sql = self.get_stream_sql(stream_name=mirror_stream_name, table_name=mirror_tr_table_name)

        mirror_task_sql = self.get_task_sql(stream_name=mirror_stream_name, task_name=mirror_task_name, table_name=mirror_table_name)

        stage_stream_sql = self.get_stream_sql(stream_name=stg_stream_name, table_name=mirror_table_name)

        stage_task_sql = self.get_task_sql(stream_name=stg_stream_name, task_name=stg_task_name, table_name=stg_table_name)

        all_sqls = "\n".join([mirror_tr_table_sql,mirror_table_sql,stage_table_sql,
                              stage_sql, file_format_sql, snowpipe_sql, mirror_stream_sql,
                              mirror_task_sql,stage_stream_sql,stage_task_sql])

        return all_sqls





