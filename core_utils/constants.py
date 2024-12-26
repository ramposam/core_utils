snowflake_stage_template = """
CREATE OR REPLACE STAGE MIRROR_DB.MIRROR.STG_{dataset_name}_S3
  URL='s3://{s3_bucket}/{s3_dataset_path}/'
  CREDENTIALS=(AWS_KEY_ID='{aws_access_key}' AWS_SECRET_KEY='{aws_secret_key}')
  ENCRYPTION=(TYPE='AWS_SSE_KMS' KMS_KEY_ID = 'aws/key');
  """

snowflake_pipe_template = """
CREATE OR REPLACE PIPE MIRROR_DB.MIRROR.PIPE_{dataset_name}
    AUTO_INGEST = TRUE     
AS {copy_statement} ;

-- To load all the available files under the path(Historical Data)
ALTER PIPE MIRROR_DB.MIRROR.PIPE_{dataset_name} REFRESH;

-- Create event notification on corresponding bucket using the arn.
-- Get the arn using below query.
-- Unless you create event notification, snowpipe is not going to copy data.
select  SYSTEM$PIPE_STATUS('MIRROR_DB.MIRROR.PIPE_{dataset_name}');

-- Creating table to log snowpipe failures
CREATE TABLE IF NOT EXISTS MIRROR_DB.MIRROR.T_SNOWPIPE_ERRORS (
    PIPE_NAME STRING,
    FILE_NAME STRING,
    ERROR_MESSAGE STRING,
    TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Logging errors into a snowpipe error table, so that every pipeline status would be knowing.
CREATE OR REPLACE TASK MIRROR_DB.MIRROR.TASK_LOG_SNOWPIPE_ERRORS
SCHEDULE = '1 MINUTE'
AS
INSERT INTO MIRROR_DB.MIRROR.T_SNOWPIPE_ERRORS (PIPE_NAME, FILE_NAME, ERROR_MESSAGE)
SELECT 
    'PIPE_{dataset_name}' AS pipe_name,
    FILE_NAME,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.LOAD_HISTORY_BY_PIPE('PIPE_{dataset_name}'))
WHERE STATUS = 'LOAD_FAILED';

"""
mirror_file_meta_cols = ["filename","file_row_number","file_last_modified"]
mirror_meta_cols = ["created_dts","created_by"]
stage_file_meta_cols = ["filename","file_row_number","file_last_modified"]
stage_meta_cols = ["created_dts","created_by","updated_dts","updated_by","active",
                   "effective_start_date", "effective_end_date","row_hash_id" ]
