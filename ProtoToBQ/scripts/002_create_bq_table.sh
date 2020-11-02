#!/usr/bin/env bash

GCP_PROJECT=geometric-ocean-284614
GCP_LOCATION=EU

DATASET_ID=dynamic_etl
TABLE_ID=event

# Create dataset
bq --location=${GCP_LOCATION} mk \
  --dataset \
  --description "Proto BigQuery schema generation" \
${GCP_PROJECT}:${DATASET_ID}

# Create table
bq mk \
  --table \
  --description "Proto generated table" \
${GCP_PROJECT}:${DATASET_ID}.${TABLE_ID} \
output/bigquery/Event.json