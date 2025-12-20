gcloud pubsub subscriptions create snowflake-ingest-sub \
    --topic=snowflake-ingest-topic \
    --ack-deadline=10 \
    --message-retention-duration=7d \
    --min-retry-delay=10s \
    --max-retry-delay=600s \
    --expiration-period=never