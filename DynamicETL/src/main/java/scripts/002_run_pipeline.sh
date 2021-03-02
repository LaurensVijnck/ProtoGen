#!/usr/bin/env bash

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_241.jdk/contents/Home
export GOOGLE_APPLICATION_CREDENTIALS="/Users/lvijnck/Documents/google-cloud-sdk/geometric-ocean-284614-77fba73ca7b0.json"

gcloud config set project geometric-ocean-284614

mvn compile exec:java \
    -Pdataflow-runner \
    -Dexec.mainClass="pipelines.DynamicETL"