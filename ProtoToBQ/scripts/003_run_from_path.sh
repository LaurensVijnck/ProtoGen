#!/usr/bin/env bash

DST_DIR=output

# Make file executable
chmod +rx protoc-gen-bq

# Add path to executable to path variable
SRC_DIR=$PWD
export PATH=$SRC_DIR:$PATH

# create dirs
DST_DIR=output
rm -rf $DST_DIR
mkdir -p $DST_DIR/java
mkdir -p $DST_DIR/python
mkdir -p $DST_DIR/bigquery

### Compile protos
protoc -I=. \
    --java_out=$DST_DIR/java \
    --python_out=$DST_DIR/python \
    --experimental_allow_proto3_optional \
   protos/*

# Invoke Custom BigQuery pluging
protoc ./protos/* \
  --experimental_allow_proto3_optional \
  --bq_out=$DST_DIR/bigquery \
