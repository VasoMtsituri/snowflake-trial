# Granting permission for Snowflake to view and read objects in the bucket
gcloud storage buckets add-iam-policy-binding gs://<BUCKET_NAME> \
    --member="serviceAccount:<SA>" \
    --role="roles/storage.objectViewer"

gcloud projects add-iam-policy-binding project-6e4dc205-0d7d-44c6-975 \
    --member="serviceAccount:<SA>" \
    --role="roles/storage.insightsCollectorService"