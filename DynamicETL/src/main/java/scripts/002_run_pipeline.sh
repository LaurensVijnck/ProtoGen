#!/usr/bin/env bash

export GOOGLE_APPLICATION_CREDENTIALS="/Users/lvijnck/Documents/google-cloud-sdk/geometric-ocean-284614-77fba73ca7b0.json"

GCP_PROJECT=geometric-ocean-284614

mvn compile exec:java \
    -Pdataflow-runner \
    -Dexec.mainClass="pipelines.DynamicETL" \
    -Dexec.args="\
    --runner=DataflowRunner \
    --jobName=dynamic-ETL-v2 \
    --update=false \
    --project=${GCP_PROJECT} \
    --region=europe-west1 \
    --zone=europe-west1-b \
    --workerMachineType=n1-standard-1 \
    --diskSizeGb=30 \
    --streaming=true \
    "
