#!/bin/bash

DST_DIR=output

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
   protos/*

# Invoke Custom BigQuery plugin
protoc -I=. --plugin=protoc-gen-bq=proto-to-bq.py --bq_out=$DST_DIR/bigquery protos/*