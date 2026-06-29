snowflake_stage_template = """
 CREATE STAGE IF NOT EXISTS {layer_0_db}.{layer_0_schema}.STG_{dataset_name}_S3
  URL='s3://{bucket}/{dataset_path}/'
  CREDENTIALS=(AWS_KEY_ID='{aws_access_key}' AWS_SECRET_KEY='{aws_secret_key}')
  ENCRYPTION=(TYPE='AWS_SSE_KMS' KMS_KEY_ID = '{kms_key_id}');
  """

snowflake_pipe_template = """
 CREATE PIPE IF NOT EXISTS  {layer_0_db}.{layer_0_schema}.PIPE_{dataset_name}
    AUTO_INGEST = TRUE     
AS {copy_statement} ;

-- To load all the available files under the path(Historical Data)
ALTER PIPE {layer_0_db}.{layer_0_schema}.PIPE_{dataset_name} REFRESH;

-- Create event notification on corresponding bucket using the arn.
-- Get the arn using below query.
-- Unless you create event notification, snowpipe is not going to copy data.
select  SYSTEM$PIPE_STATUS('{layer_0_db}.{layer_0_schema}.PIPE_{dataset_name}');

-- Creating table to log snowpipe failures
CREATE TABLE IF NOT EXISTS {layer_0_db}.{layer_0_schema}.T_SNOWPIPE_ERRORS (
    PIPE_NAME TEXT,
    FILE_NAME TEXT,
    ERROR_MESSAGE TEXT,
    TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Logging errors into a snowpipe error table, so that every pipeline status would be knowing.
 CREATE TASK IF NOT EXISTS  {layer_0_db}.{layer_0_schema}.TASK_LOG_SNOWPIPE_ERRORS
SCHEDULE = '1 MINUTE'
AS
INSERT INTO {layer_0_db}.{layer_0_schema}.T_SNOWPIPE_ERRORS (PIPE_NAME, FILE_NAME, ERROR_MESSAGE)
SELECT 
    '{layer_0_db}.{layer_0_schema}.PIPE_{dataset_name}' AS pipe_name,
    FILE_NAME,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.LOAD_HISTORY_BY_PIPE('{layer_0_db}.{layer_0_schema}.PIPE_{dataset_name}'))
WHERE STATUS = 'LOAD_FAILED';

"""
mirror_file_meta_cols = ["filename","file_row_number","file_last_modified"]
mirror_tr_meta_cols = ["CREATED_DTS","CREATED_BY"]
mirror_addl_meta_cols = ["UPDATED_DTS","UPDATED_BY","UNIQUE_HASH_ID","ROW_HASH_ID"]
stage_file_meta_cols = ["filename","file_row_number","file_last_modified"]
stage_addl_meta_cols = ["ACTIVE_FL", "EFFECTIVE_START_DATE", "EFFECTIVE_END_DATE" ]
