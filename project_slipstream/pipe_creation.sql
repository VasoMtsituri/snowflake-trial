CREATE OR REPLACE PIPE events_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = 'GCS_PUBSUB_INT'
  AS
  COPY INTO raw_events (raw_data) -- Specify the target column
  FROM (
    SELECT $1 FROM @my_gcs_stage -- Explicitly select the first column of the file
  )
  FILE_FORMAT = (TYPE = 'JSON');