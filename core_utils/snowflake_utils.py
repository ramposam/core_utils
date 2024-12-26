import logging

from core_utils.constants import mirror_file_meta_cols, mirror_meta_cols


class SnowflakeUtils:

    def __init__(self, stage_name,table_name):
        self.stage_name = stage_name
        self.table_name = table_name


    def get_file_format_sql(self,file_format_name, file_type="CSV", delimiter=",",skip_header=1, compression="NONE"):
        # Define the SQL command to create the file format

        file_format_sql = f""" CREATE OR REPLACE FILE FORMAT {file_format_name}
        TYPE = {file_type}
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        FIELD_DELIMITER = '{delimiter}'
        SKIP_HEADER = {skip_header}      
        TRIM_SPACE=TRUE,
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT='YYYY-MM-DD',
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
        ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
        COMPRESSION = {compression};
        """
        logging.info(f"File format sql: {file_format_sql}")
        return file_format_sql



    def get_copy_into_table_sql(self, columns,file_extension, file_format_name, file_path=None):

        cols_list_str = ",".join([f"${index+1} as {col_name.upper()}" for index, col_name in enumerate(columns) if col_name not in mirror_file_meta_cols + mirror_meta_cols ])

        meta_cols = []
        for col in mirror_file_meta_cols:
            meta_cols.append(f"metadata${col} as {col}")

        meta_cols_list_str = ",".join(meta_cols)

        # Define the SQL command to load data from the stage into the table
        copy_sql = f"""COPY INTO {self.table_name}
        FROM (
            SELECT {cols_list_str},{meta_cols_list_str},
            current_timestamp as created_dts, current_user as created_by
            FROM '@{self.stage_name}'
        )
        PATTERN = '.*\.{file_extension}$'
        FILE_FORMAT = (FORMAT_NAME={file_format_name})
        ON_ERROR = 'CONTINUE' """

        logging.info(f"File format sql: {copy_sql}")
        return copy_sql

    def get_mirror_stage_ddls(self, database, schema, table_name, table_schema):

        """
        Generate Snowflake table DDL from table name and schema.

        :param table_name: Name of the table
        :param table_schema: Dictionary with column names as keys and data types as values
        :return: DDL string for creating the table
        """
        ddl = f""" CREATE DATABASE IF NOT EXISTS {database};\n CREATE SCHEMA IF NOT EXISTS {schema};\n """
        ddl += f"CREATE OR REPLACE TABLE {table_name} (\n"
        column_definitions = []

        for column_name, data_type in table_schema.items():
            column_definitions.append(f"    {column_name} {data_type}")

        ddl += ",\n".join(column_definitions)
        ddl += "\n);"

        return ddl

