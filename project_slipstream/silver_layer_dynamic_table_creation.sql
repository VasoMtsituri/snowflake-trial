CREATE OR REPLACE DYNAMIC TABLE structured_events
  TARGET_LAG = '1 minute'
  WAREHOUSE = compute_wh
  AS
  SELECT
    raw_data:event_id::STRING as event_id,
    raw_data:user:id::INTEGER as user_id,
    raw_data:transaction:amount::FLOAT as amount,
    TO_TIMESTAMP(raw_data:event_time::STRING) as event_time
  FROM raw_events;