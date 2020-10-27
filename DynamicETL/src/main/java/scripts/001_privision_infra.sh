GCP_PROJECT=geometric-ocean-284614
GCP_LOCATION=EU

DATASET_ID=dynamic_etl
TABLE_ID=envelope

# Create topic
gcloud pubsub topics create --project ${GCP_PROJECT} dynamic_etl

# Create subscription
gcloud pubsub subscriptions create --project ${GCP_PROJECT} dynamic_etl_subscription --topic dynamic_etl