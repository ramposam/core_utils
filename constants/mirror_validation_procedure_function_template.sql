


CREATE OR REPLACE PROCEDURE META_DB.META.VALIDATE_FILE_AND_TABLE(dataset_name STRING,stage_name STRING,database STRING,schema STRING,  table_name STRING,run_date STRING,header_row_no NUMBER)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'compare_csv_with_table'
AS $$

import json
import os

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, upper
from snowflake.snowpark.types import StructType, StringType, StructField
from datetime import datetime

def compare_csv_with_table(session: Session,dataset_name, stage_name,database,schema,run_date, table_name,header_row_no=1):
    # Construct the full path to the file in the stage
    executed_sqls = ""
    list_of_files = session.sql(f"list @{database}.{schema}.{stage_name}").collect()
    for full_file_path in list_of_files:
        file_path = f"""@{database}.{schema}.{stage_name}/{"/".join(full_file_path["name"].split("/")[1:])}"""

        executed_sqls += "file path query: " + file_path + "\n"

        # Read staged file into a DataFrame
        stage_df = session.read.option("FIELD_OPTIONALLY_ENCLOSED_BY", '"').csv(file_path)

        # Extract the third row as header
        header_row = stage_df.limit(header_row_no).collect()[-1]

        executed_sqls += "header_row: " + str(header_row) + "\n"

        if run_date in ["",None]:
            run_date_str = datetime.today().strftime("%Y-%m-%d")
        else:
            run_date_str = run_date

        file_defined_cols_query = f"""
            select file_schema from META_DB.META.T_FILE_META_DETAILS
            where database = '{database}'
            and schema = '{schema}'
            and table_name = '{table_name}'
            and active_fl = 'Y'
            and '{run_date_str}' between start_date and end_date """

        executed_sqls +=  "file_defined_cols_query: " + file_defined_cols_query + "\n"

        configured_file_schema = session.sql(file_defined_cols_query).collect()[0]["FILE_SCHEMA"]

        executed_sqls += "configured_file_schema: " + configured_file_schema + "\n"

        file_schema = json.loads(configured_file_schema)
        configured_file_cols = ",".join([key for schema in file_schema for key in schema.keys()])

        executed_sqls += "configured_file_cols: " + configured_file_cols + "\n"

        received_file_cols_dict = header_row.asDict().items()
        received_file_cols_str = ",".join([col_name.strip('"').replace(" ","_").upper() for col, col_name in received_file_cols_dict])

        executed_sqls += "received_file_cols_str: " + received_file_cols_str + "\n"

        if configured_file_cols != received_file_cols_str:
            insert_query = f""" INSERT INTO META_DB.META.T_FILE_VALIDATION(DATASET_NAME, STATUS, TYPE ,CREATED_DTS,CREATED_BY)
                    SELECT '{dataset_name.upper()}','ERROR','SCHEMA_MISMATCH',CURRENT_TIMESTAMP(),CURRENT_USER()
                """
            executed_sqls += insert_query + " \n"
            _ = session.sql(insert_query).collect()

        index=0
        datatypes = []
        for key, value in received_file_cols_dict:
            if value is None:
                index+=1
                datatypes.append(StructField(f"None_{index}".upper(), StringType()))
            else:
                datatypes.append(StructField(value.replace(" ","_").upper(), StringType()) )

        snowpark_schema = StructType(datatypes)

        executed_sqls += "snowpark_schema: " + str(snowpark_schema) + "\n"

        # Convert column names to uppercase
        schema_stage_df = session.read.option("FIELD_OPTIONALLY_ENCLOSED_BY", '"').options({"SKIP_HEADER":header_row_no}).schema(schema=snowpark_schema).csv(file_path)

        table_cols_query = f"""
        SELECT LISTAGG(COLUMN_NAME,',') WITHIN GROUP(ORDER BY ORDINAL_POSITION)  AS COLS
        FROM {database}.INFORMATION_SCHEMA.COLUMNS C
        WHERE TABLE_SCHEMA ='{schema}'
        AND TABLE_NAME = '{table_name}'
        AND COLUMN_NAME NOT IN ('CREATED_BY','CREATED_DTS','FILE_DATE','FILE_LAST_MODIFIED','FILE_ROW_NUMBER','FILENAME',
        'ROW_HASH_ID','UNIQUE_HASH_ID','UPDATED_DTS','UPDATED_BY')
        """

        executed_sqls += "table_cols_query: " + table_cols_query + "\n"

        table_cols = session.sql(table_cols_query).collect()[0]["COLS"]

        executed_sqls += "table_cols: " + table_cols + "\n"

        tr_table_query = f"""select {table_cols} from {database}.{schema}.{table_name} where FILE_DATE = '{run_date_str}' """

        executed_sqls += "tr_table_query: " + tr_table_query + "\n"

        # Load the Snowflake table data
        tr_table_df = session.sql(tr_table_query)

        # Find differences: Rows in CSV but not in the table
        data_diff = schema_stage_df.minus(tr_table_df)

        executed_sqls += f"tr_table_query: Has no of mismatch records count: {data_diff.count()} \n"

        if data_diff.count()>0:
            sample_10_records_mismatch = data_diff.limit(10).collect()

            insert_query = f""" INSERT INTO META_DB.META.T_FILE_VALIDATION_DETAILS(DATASET_NAME, MSG, DETAILS ,CREATED_DTS,CREATED_BY)
                                SELECT '{dataset_name.upper()}','Has no of mismatch records count:{data_diff.count()}',PARSE_JSON('{json.dumps(sample_10_records_mismatch)}'),CURRENT_TIMESTAMP(),CURRENT_USER()
                            """
            executed_sqls += insert_query + " \n"
            _ = session.sql(insert_query).collect()

        remove_stage_file = f"remove {file_path}"
        executed_sqls += remove_stage_file + " \n"

        session.sql(remove_stage_file).collect()

        return executed_sqls

$$;