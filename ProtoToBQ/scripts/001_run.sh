#!/bin/bash

DST_DIR=output

export GOOGLE_APPLICATION_CREDENTIALS=/Users/lvijnck/Documents/google-cloud-sdk/geometric-ocean-284614-77fba73ca7b0.json

# Make file executable
chmod +x proto-to-bq.py

# create dirs
rm -rf $DST_DIR
mkdir -p $DST_DIR/java
mkdir -p $DST_DIR/python
mkdir -p $DST_DIR/bigquery

# Compile protos
protoc -I=. \
    --java_out=$DST_DIR/java \
    --python_out=$DST_DIR/python \
    --experimental_allow_proto3_optional \
   protos/*

# Invoke Custom BigQuery pluging
protoc -I=. \
  --experimental_allow_proto3_optional \
  --plugin=protoc-gen-bq=proto-to-bq.py \
  --bq_out=$DST_DIR/bigquery protos/*