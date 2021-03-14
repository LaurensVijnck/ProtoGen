#!/usr/bin/env bash

# FUTURE: Extract
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_241.jdk/contents/Home
export GOOGLE_APPLICATION_CREDENTIALS="/Users/lvijnck/Documents/google-cloud-sdk/geometric-ocean-284614-77fba73ca7b0.json"

# FUTURE: Extract
gcloud config set project geometric-ocean-284614

# Push template to gcs
# FUTURE: Supply target location via options
mvn compile exec:java \
    -Pdataflow-runner \
    -Dexec.mainClass="pipelines.DynamicETL"