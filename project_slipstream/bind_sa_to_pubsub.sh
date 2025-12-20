gcloud pubsub subscriptions add-iam-policy-binding snowflake-ingest-sub \
    --member="serviceAccount:<SA>" \
    --role="roles/pubsub.subscriber"

gcloud projects add-iam-policy-binding project-6e4dc205-0d7d-44c6-975 \
    --member="serviceAccount:<SA>" \
    --role="roles/monitoring.viewer"